from __future__ import annotations

import base64
import gzip
import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from server.connection_spike_journal import CreationScope, JournalEvent, ResourceSpec
from server.connection_spike_live import (
    ConnectionSpikeLiveConfigurationError,
    ConnectionSpikeLiveOperationError,
    ConnectionSpikeSetupConfig,
    LakebaseCreationJournalStore,
    LiveConnectionSpikeEngine,
    LiveConnectionSpikeSetupOrchestrator,
    connection_spike_config_sha256,
    connection_spike_live_config_from_manifest,
    connection_spike_setup_config_from_manifest,
)
from server.coordination import round_ring_key
from server.manager import RunManager, operator_diagnosis

ACCOUNT = "123456789012"


async def test_creation_journal_store_uses_parameterized_append_only_boundaries() -> None:
    rows: list[tuple[object, ...]] = []
    calls: list[tuple[str, tuple[object, ...]]] = []

    class Cursor:
        statement = ""

        async def execute(self, statement, parameters):
            self.statement = statement
            calls.append((statement, parameters))
            if "INSERT INTO" in statement:
                rows.append(parameters)

        async def fetchone(self):
            return (1,)

        async def fetchall(self):
            if "GROUP BY" in self.statement:
                return [(row[0], row[1], row[9]) for row in rows]
            return [
                (
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                    json.loads(str(row[8])),
                    row[9],
                    row[10],
                    row[11],
                    row[12],
                    row[13],
                )
                for row in rows
            ]

    async def run(operation):
        return await operation(Cursor())

    store = LakebaseCreationJournalStore(run)
    scope = CreationScope("bout-parameterized", 7, "a" * 64)
    event = JournalEvent.creation_intent(
        scope,
        ResourceSpec(
            ordinal=1,
            resource_kind="proxy_secret",
            deterministic_name="r5-deadbeef-secret",
            metadata={"resource_id": "safe-id"},
        ),
        now=datetime.now(UTC),
    )

    authority = CreationScope("bout-parameterized", 8, "a" * 64)
    await store.commit(event, authority_scope=authority)
    measured = await store.events(scope)
    scopes = await store.scopes(scope.bout_id)

    assert measured == (event,)
    assert scopes == (scope,)
    insert, select, list_scopes = calls
    assert "bout-parameterized" not in insert[0]
    assert insert[1][0:4] == ("bout-parameterized", 7, 1, "proxy_secret")
    assert "expires_at > clock_timestamp()" in insert[0]
    assert insert[1][-3:] == ("main", "bout-parameterized", 8)
    assert "WHERE bout_id = %s AND fencing_token = %s" in select[0]
    assert select[1] == ("bout-parameterized", 7)
    assert list_scopes[1] == ("bout-parameterized",)


async def test_creation_journal_store_targets_configured_authority_ring() -> None:
    authority_keys: list[str] = []

    class Cursor:
        async def execute(self, statement, parameters):
            assert "expires_at > clock_timestamp()" in statement
            authority_keys.append(str(parameters[-3]))

        async def fetchone(self):
            return (1,)

    async def run(operation):
        return await operation(Cursor())

    scope = CreationScope("bout-authority-ring", 11, "b" * 64)
    event = JournalEvent.creation_intent(
        scope,
        ResourceSpec(
            ordinal=1,
            resource_kind="proxy_secret",
            deterministic_name="r5-authority-secret",
            metadata={"resource_id": "safe-id"},
        ),
        now=datetime.now(UTC),
    )

    await LakebaseCreationJournalStore(
        run,
        authority_ring_key="round5",
    ).commit(event)
    scoped_key = round_ring_key(
        "install-a",
        "survive_connection_spike",
        cleanup=True,
    )
    await LakebaseCreationJournalStore(
        run,
        authority_ring_key=scoped_key,
    ).commit(event)
    await LakebaseCreationJournalStore(run).commit(event)

    assert authority_keys == ["round5", scoped_key, "main"]
    with pytest.raises(
        ConnectionSpikeLiveConfigurationError,
        match="authority ring key is invalid",
    ):
        LakebaseCreationJournalStore(run, authority_ring_key="round5 cleanup")


async def test_creation_journal_store_discovers_only_unresolved_bouts() -> None:
    statements: list[str] = []

    class Cursor:
        async def execute(self, statement):
            statements.append(statement)

        async def fetchall(self):
            return [("bout-stale-a",), ("bout-stale-b",)]

    async def run(operation):
        return await operation(Cursor())

    store = LakebaseCreationJournalStore(run)

    assert await store.unresolved_bout_ids() == (
        "bout-stale-a",
        "bout-stale-b",
    )
    assert "row_number() OVER" in statements[0]
    assert "lifecycle_state <> 'deleted'" in statements[0]


def test_manifest_factories_select_static_proxy_secret_and_checksum_binding() -> None:
    contract_sha256 = "f0e9a6960fb22cc052486b62cf01e32dcaabacf70508a2bc087ddc25deafa81c"
    baseline_sha256 = "d" * 64
    config_sha256 = hashlib.sha256(
        json.dumps(
            {
                "baseline_sha256": baseline_sha256,
                "contract_sha256": contract_sha256,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    resources = SimpleNamespace(
        control_role_arn=f"arn:aws:iam::{ACCOUNT}:role/baseline-control",
        runner_instance_id="i-0123456789abcdef0",
        runner_instance_profile_arn=f"arn:aws:iam::{ACCOUNT}:instance-profile/runner",
        runner_subnet_id="subnet-a",
        runner_security_group_id="sg-runner",
        runner_role_arn=f"arn:aws:iam::{ACCOUNT}:role/runner",
        vpc_id="vpc-sealed",
        proxy_subnet_ids=("subnet-a", "subnet-b"),
        proxy_service_role_arn=f"arn:aws:iam::{ACCOUNT}:role/proxy-service",
        proxy_service_policy_name="proxy-service-secrets",
        lakebase_direct_host="lakebase-direct.test",
        lakebase_pooled_host="lakebase-pooled.test",
        aurora_cluster_id="aurora-source",
        aurora_cluster_resource_id="cluster-RESOURCE",
        aurora_direct_host="aurora-direct.test",
        aurora_credential_sha256="a" * 64,
        aurora_proxy_secret_arn=(
            f"arn:aws:secretsmanager:us-west-2:{ACCOUNT}:secret:aurora-proxy"
        ),
        rds_resource_id="db-RESOURCE",
        rds_direct_host="rds-direct.test",
        rds_credential_sha256="f" * 64,
        rds_proxy_secret_arn=(
            f"arn:aws:secretsmanager:us-west-2:{ACCOUNT}:secret:rds-proxy"
        ),
        bout_name_prefix="anti-demo-r5",
        ownership_tags=SimpleNamespace(
            as_aws_tags=lambda: {"Owner": "anti-demo", "owner": "anti-demo"}
        ),
        trust_bundle_path="/opt/lakebase-anti-demo/round5/round5-ca.pem",
        trust_bundle_sha256="b" * 64,
        runner_public_key_sha256="c" * 64,
        baseline_sha256=baseline_sha256,
        lakebase_credential_sha256="e" * 64,
        runner_path="/opt/lakebase-anti-demo/round5/run_connection_spike.sh",
        runner_harness_sha256="1" * 64,
        ssm_document_name="AWS-RunShellScript",
        native_role="anti_demo_burst",
        frozen_constants=SimpleNamespace(
            rds_proxy_max_connections_percent=90,
            rds_proxy_borrow_timeout_seconds=120,
        ),
        contract_sha256=contract_sha256,
        config_sha256=config_sha256,
    )
    manifest = SimpleNamespace(
        aws=SimpleNamespace(
            region="us-west-2",
            account_id=ACCOUNT,
            resources=SimpleNamespace(
                rds_instance_id="rds-source",
                security_group_id="sg-aurora",
                rds_security_group_id="sg-rds",
            ),
        ),
        databricks=SimpleNamespace(database="anti_demo"),
        expiry_warning=lambda: None,
        require_round5_resources=lambda: resources,
    )

    rds_live = connection_spike_live_config_from_manifest(manifest, "rds_postgres")
    aurora_live = connection_spike_live_config_from_manifest(
        manifest, "aurora_serverless_v2"
    )
    rds_setup = connection_spike_setup_config_from_manifest(manifest, "rds_postgres")
    aurora_setup = connection_spike_setup_config_from_manifest(
        manifest, "aurora_serverless_v2"
    )

    assert rds_live.targets[1].secret_arn == resources.rds_proxy_secret_arn
    assert aurora_live.targets[1].secret_arn == resources.aurora_proxy_secret_arn
    assert rds_setup.proxy_secret_arn == resources.rds_proxy_secret_arn
    assert aurora_setup.proxy_secret_arn == resources.aurora_proxy_secret_arn
    assert rds_setup.proxy_service_role_arn == resources.proxy_service_role_arn
    assert connection_spike_config_sha256(rds_live) != connection_spike_config_sha256(
        aurora_live
    )


def test_v7_round5_rds_setup_uses_dedicated_instance_and_security_group(tmp_path) -> None:
    from test_manifest import _v7_manifest

    manifest = _v7_manifest(tmp_path)
    round5 = manifest.round_environment("survive_connection_spike")
    assert round5.rds is not None

    config = connection_spike_setup_config_from_manifest(manifest, "rds_postgres")

    assert config.competitor_target_id == round5.rds.instance_id
    assert config.competitor_security_group_id == round5.rds.security_group_id
    assert config.competitor_target_id != manifest.aws.resources.rds_instance_id
    assert (
        config.competitor_security_group_id
        != manifest.aws.resources.rds_security_group_id
    )


def test_no_round5_manifest_gate_consults_expiry_at_all(
    tmp_path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    """Both Round 5 gates must report a passed TTL, never refuse on it.

    This used to install a detonating `assert_not_expired` to prove the call was
    gone rather than merely no longer fatal. That method has since been deleted
    outright -- `tests/test_expiry_renew.py` asserts it cannot come back -- so
    there is nothing left to detonate, and what remains to check here is the
    behaviour: both configs build from a manifest that is hours past its TTL, and
    the only trace of the expiry is the advisory line.
    """
    from test_manifest import _v7_manifest

    manifest = _v7_manifest(tmp_path)
    assert manifest.expires_at < datetime.now(UTC), "fixture must be past its TTL"
    assert not hasattr(type(manifest), "assert_not_expired"), (
        "a refusing expiry gate is back on the manifest; these builders swallow "
        "RuntimeError, so it would silently stop Round 5 arming again"
    )

    for competitor_id in ("rds_postgres", "aurora_serverless_v2"):
        assert connection_spike_live_config_from_manifest(manifest, competitor_id)
        assert connection_spike_setup_config_from_manifest(manifest, competitor_id)

    # The same line the Round 2/3 builder prints, so an operator reading an
    # expired installation's log cannot tell the rounds apart.
    assert f"WARN  {manifest.expiry_warning()}" in capsys.readouterr().out


def test_expired_manifest_no_longer_deletes_round5_from_a_running_installation(
    tmp_path,
) -> None:
    """The live symptom, not just the call site that caused it.

    `connection_spike_factory_from_manifest` builds both Round 5 configs inside
    `except (RuntimeError, ValueError): return None`, and `assert_not_expired`
    raises `RuntimeError`.  So a passed TTL did not surface as a refusal an
    operator could read: it silently returned no factory, and Round 5 alone
    stopped being able to arm while Rounds 1-4 and 6 carried on.
    """
    import app as app_module

    manifest = _v7_manifest_past_ttl(tmp_path)

    class LakebaseLeaseStore:
        mode = "lakebase"
        ring_key = ""

        def _run(self, *_args, **_kwargs) -> None: ...

        async def current(self) -> None:
            return None

    lease_store = LakebaseLeaseStore()
    lease_store.ring_key = app_module._round5_lease_ring_key(manifest)

    factory = app_module.connection_spike_factory_from_manifest(
        manifest, lease_store=lease_store
    )

    assert factory is not None, "Round 5 must still be offered past the TTL"


def _v7_manifest_past_ttl(tmp_path):
    from test_manifest import _v7_manifest

    manifest = _v7_manifest(tmp_path)
    assert manifest.expires_at < datetime.now(UTC), "fixture must be past its TTL"
    assert manifest.round5_ready
    return manifest


async def test_two_phase_setup_uses_assumed_clients_shared_t0_and_defers_burst(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_requests: list[dict[str, object]] = []

    class Sts:
        def assume_role(self, **kwargs):
            assert kwargs["RoleArn"] == f"arn:aws:iam::{ACCOUNT}:role/baseline-control"
            assert kwargs["DurationSeconds"] == 3600
            return {
                "Credentials": {
                    "AccessKeyId": "temporary-access",
                    "SecretAccessKey": "temporary-secret",
                    "SessionToken": "temporary-token",
                    "Expiration": datetime.now(UTC) + timedelta(hours=1),
                },
                "AssumedRoleUser": {
                    "Arn": f"arn:aws:sts::{ACCOUNT}:assumed-role/baseline-control/setup"
                },
            }

    class Ssm:
        def send_command(self, **kwargs):
            command = kwargs["Parameters"]["commands"][0]
            encoded = command.rsplit(" ", 1)[1]
            request = json.loads(gzip.decompress(base64.urlsafe_b64decode(encoded)))
            setup_requests.append(request)
            return {"Command": {"CommandId": f"command-{len(setup_requests)}"}}

        def get_command_invocation(self, **kwargs):
            request = setup_requests[int(kwargs["CommandId"].rsplit("-", 1)[1]) - 1]
            receipt = {
                "protocol": "connection-spike-setup-v1",
                "action": request["action"],
                "bout_id": request["bout_id"],
                "lane_id": request["lane_id"],
                "nonce": request["nonce"],
                "status": "verified",
            }
            return {
                "Status": "Success",
                "StandardOutputContent": (
                    f"SETUP_RESULT:{json.dumps(receipt, separators=(',', ':'))}\n"
                    f"SETUP_SETTLED:{request['nonce']}\n"
                    f"RUNNER_FLOCK_RELEASED:{request['bout_id']}\n"
                ),
            }

    ssm = Ssm()
    origins: list[tuple[str, str]] = []

    class SessionFactory:
        def __call__(self, **kwargs):
            origin = "assumed" if "aws_access_key_id" in kwargs else "ambient"

            class Session:
                def client(self, name, **client_kwargs):
                    assert client_kwargs == {"region_name": "us-west-2"}
                    origins.append((origin, name))
                    if origin == "ambient":
                        assert name == "sts"
                        return Sts()
                    return ssm if name == "ssm" else SimpleNamespace()

            return Session()

    config = ConnectionSpikeSetupConfig(
        region="us-west-2",
        expected_account_id=ACCOUNT,
        baseline_control_role_arn=f"arn:aws:iam::{ACCOUNT}:role/baseline-control",
        runner_instance_id="i-0123456789abcdef0",
        vpc_id="vpc-sealed",
        proxy_subnet_ids=("subnet-a", "subnet-b"),
        lakebase_direct_host="lakebase-direct.test",
        lakebase_pooled_host="lakebase-pooled.test",
        competitor_id="rds_postgres",
        competitor_target_id="rds-source",
        competitor_resource_id="db-RESOURCE",
        competitor_direct_host="rds-direct.test",
        competitor_security_group_id="sg-rds",
        runner_security_group_id="sg-runner",
        proxy_service_role_arn=f"arn:aws:iam::{ACCOUNT}:role/proxy-service",
        proxy_service_policy_name="proxy-service-secrets",
        aurora_proxy_secret_arn=(
            f"arn:aws:secretsmanager:us-west-2:{ACCOUNT}:secret:aurora-proxy"
        ),
        rds_proxy_secret_arn=(
            f"arn:aws:secretsmanager:us-west-2:{ACCOUNT}:secret:rds-proxy"
        ),
        deterministic_name_prefix="anti-demo-r5",
        ownership_tags=(("Owner", "anti-demo"), ("owner", "anti-demo")),
        trust_bundle_path="/opt/lakebase-anti-demo/round5/round5-ca.pem",
        trust_bundle_sha256="b" * 64,
        runner_public_key_sha256="c" * 64,
        baseline_sha256="d" * 64,
        lakebase_credential_sha256="e" * 64,
        competitor_credential_sha256="f" * 64,
        runner_role_arn=f"arn:aws:iam::{ACCOUNT}:role/runner",
        proxy_role_permissions_boundary_arn=(f"arn:aws:iam::{ACCOUNT}:policy/proxy-boundary"),
        secret_name_prefix="anti-demo/r5",
        competitor_master_secret_arn=(
            f"arn:aws:secretsmanager:us-west-2:{ACCOUNT}:secret:rds-master"
        ),
    )
    assert config.proxy_registration == {"DBInstanceIdentifiers": ["rds-source"]}

    class Fence:
        async def assert_current(self, scope):
            assert scope.fencing_token == 11

    class Journal:
        async def events(self, scope):
            return ()

        async def commit(self, event, *, authority_scope=None):
            del authority_scope
            raise AssertionError("fake coordinator owns this test boundary")

        async def scopes(self, bout_id):
            del bout_id
            return ()

    ticks = 1_000_000_000

    def monotonic_ns():
        nonlocal ticks
        ticks += 1_000_000
        return ticks

    orchestrator = LiveConnectionSpikeSetupOrchestrator(
        config,
        journal=Journal(),
        fence=Fence(),
        fresh_lakebase_host=lambda: _value("lakebase-pooled.test"),
        session_factory=SessionFactory(),
        monotonic_ns=monotonic_ns,
    )
    iam_calls: list[tuple[str, dict[str, object]]] = []

    class ServiceIam:
        def __init__(self, *, drift: bool = False):
            self.drift = drift

        def get_role(self, **kwargs):
            iam_calls.append(("get_role", kwargs))
            return {
                "Role": {
                    "RoleName": "proxy-service",
                    "Arn": config.proxy_service_role_arn,
                    "AssumeRolePolicyDocument": orchestrator._proxy_trust_policy(),
                }
            }

        def get_role_policy(self, **kwargs):
            iam_calls.append(("get_role_policy", kwargs))
            policy = orchestrator._proxy_service_policy()
            if self.drift:
                policy = {**policy, "Statement": [*policy["Statement"], {"Effect": "Allow"}]}
            return {"PolicyDocument": policy}

    await orchestrator._verify_proxy_service_role(SimpleNamespace(iam=ServiceIam()))
    assert (
        "get_role_policy",
        {
            "RoleName": "proxy-service",
            "PolicyName": "proxy-service-secrets",
        },
    ) in iam_calls
    with pytest.raises(ConnectionSpikeLiveConfigurationError, match="policy document changed"):
        await orchestrator._verify_proxy_service_role(
            SimpleNamespace(iam=ServiceIam(drift=True))
        )
    original_coordinator = orchestrator._coordinator
    creation_order: list[str] = []
    coordinators: list[Coordinator] = []

    class Coordinator:
        def __init__(self, resources, clients, scope):
            self.resources = resources
            self.clients = clients
            self.scope = scope

        async def create_resource(self, scope, spec):
            del scope
            creation_order.append(spec.resource_kind)
            if spec.ordinal == 7:
                self.resources.proxy_endpoint = "dynamic-proxy.test"
            return SimpleNamespace(provider_id=f"provider-{spec.ordinal}")

        async def seal(self, scope):
            del scope
            return SimpleNamespace()

        async def reconcile_incomplete(self, scope):
            del scope
            return SimpleNamespace(complete=True)

    resource_kinds = (
        "proxy_security_group",
        "proxy_default_egress",
        "proxy_ingress",
        "proxy_egress",
        "runner_egress",
        "rds_ingress",
        "rds_proxy",
        "proxy_target_group",
        "proxy_target",
    )
    specs = tuple(
        ResourceSpec(index, kind, f"resource-{index}", metadata={"resource_id": index})
        for index, kind in enumerate(resource_kinds, 1)
    )

    def coordinator(scope, clients, resources):
        value = Coordinator(resources, clients, scope)
        coordinators.append(value)
        return value, specs

    async def preflight(*args):
        nonlocal ticks
        del args
        # Model slow, unscored preflight work. The shared setup T0 must be
        # captured only after this time has passed.
        ticks += 5_000_000_000

    async def verify_topology(clients, resources):
        del clients
        assert resources.proxy_endpoint == "dynamic-proxy.test"
        creation_order.append("topology_reread")

    async def wait_proxy_available(clients, resources):
        del clients
        assert resources.proxy_endpoint == "dynamic-proxy.test"

    async def verify_journaled_resources(scope, coordinator, measured_specs):
        del scope, coordinator
        assert tuple(item.resource_kind for item in measured_specs) == resource_kinds

    monkeypatch.setattr(orchestrator, "_coordinator", coordinator)
    monkeypatch.setattr(orchestrator, "_preflight_baseline", preflight)
    monkeypatch.setattr(orchestrator, "_verify_proxy_topology", verify_topology)
    monkeypatch.setattr(orchestrator, "_wait_proxy_available", wait_proxy_available)
    monkeypatch.setattr(orchestrator, "_verify_journaled_resources", verify_journaled_resources)

    adapter = SimpleNamespace(
        config=SimpleNamespace(
            targets=(SimpleNamespace(lane_id="lakebase"), SimpleNamespace(lane_id="competitor"))
        ),
        check=lambda: _value(None),
    )
    engine = LiveConnectionSpikeEngine(adapter, setup_orchestrator=orchestrator)
    with pytest.raises(ConnectionSpikeLiveOperationError, match="before both timed setup stops"):
        await engine.check()

    progress = []

    async def capture_progress(value):
        progress.append(value)

    setup = await engine.setup("bout-two-phase", 11, capture_progress)
    arm = await engine.check()

    assert arm.schedule.lane_ids == ("lakebase", "competitor")
    assert setup.deadline_ns - setup.t0_ns == 30 * 60 * 1_000_000_000
    assert setup.t0_ns >= 6_000_000_000
    assert setup.launch_skew_ms <= 10
    assert progress
    assert all(item.setup_elapsed_ms is not None for item in progress)
    assert progress[0].setup_elapsed_ms < 100
    finalized_setup = RunManager._round_five_finalize_setup(setup, {})
    assert finalized_setup is not None
    for lane_id, lane_stop in (
        ("lakebase", setup.lakebase),
        ("competitor", setup.competitor),
    ):
        stop_progress = next(
            item
            for item in progress
            if item.lane_id == lane_id and item.phase == "setup_stop"
        )
        exact_elapsed_ms = (lane_stop.stopped_ns - setup.t0_ns) / 1_000_000
        assert stop_progress.status == "verified"
        assert stop_progress.setup_elapsed_ms == pytest.approx(exact_elapsed_ms)
        assert finalized_setup.lanes[lane_id].setup_elapsed_ms == pytest.approx(
            exact_elapsed_ms
        )
    assert all(
        item.stop_gate_evidence and item.stop_gate_evidence.exact for item in setup.observations
    )
    public_gates = [
        RunManager._round_five_public_gate(item.stop_gate_evidence)
        for item in setup.observations
    ]
    assert all(gate is not None and gate.exact for gate in public_gates)
    assert creation_order == [*resource_kinds, "topology_reread"]
    assert [request["action"] for request in setup_requests if request["lane_id"] == "rds"] == [
        "verify"
    ]
    competitor_request = next(
        request for request in setup_requests if request["lane_id"] == "rds"
    )
    assert competitor_request["endpoint_host"] == "dynamic-proxy.test"
    assert competitor_request["credential_host"] == "rds-direct.test"
    assert competitor_request["endpoint_host"] != competitor_request["credential_host"]
    assert [
        request["action"] for request in setup_requests if request["lane_id"] == "lakebase"
    ] == ["verify"]
    setup_common = {
        "protocol",
        "action",
        "nonce",
        "bout_id",
        "lane_id",
        "endpoint_host",
        "credential_host",
        "port",
        "dbname",
        "username",
        "trust_bundle_path",
        "trust_bundle_sha256",
        "credential_sha256",
    }
    assert all(set(request) == setup_common for request in setup_requests)
    assert origins == [
        ("ambient", "sts"),
        ("assumed", "ssm"),
        ("assumed", "rds"),
        ("assumed", "ec2"),
        ("assumed", "iam"),
        ("assumed", "secretsmanager"),
    ]
    assert all("password" not in json.dumps(request).lower() for request in setup_requests)
    owned = ResourceSpec(
        1,
        "proxy_secret",
        "owned",
        metadata={"tags": {"anti-demo-bout-id": "bout-two-phase", "owner": "anti-demo"}},
    )
    with pytest.raises(ConnectionSpikeLiveConfigurationError, match="ownership tags changed"):
        orchestrator._require_exact_tags(
            owned,
            [
                {"Key": "anti-demo-bout-id", "Value": "foreign-bout"},
                {"Key": "owner", "Value": "anti-demo"},
            ],
        )

    resources = coordinators[0].resources
    assert resources.secret_arn == config.rds_proxy_secret_arn
    assert resources.proxy_role_arn == config.proxy_service_role_arn
    resources.proxy_security_group_id = "sg-proxy"
    resources.rds_security_group_id = "sg-rds"
    rule_calls: list[tuple[str, dict[str, object]]] = []

    class RuleEc2:
        def authorize_security_group_ingress(self, **kwargs):
            rule_calls.append(("authorize", kwargs))
            return {"SecurityGroupRules": [{"SecurityGroupRuleId": "sgr-exact"}]}

        def describe_security_group_rules(self, **kwargs):
            rule_calls.append(("describe", kwargs))
            rule_spec = next(item for item in real_specs if item.resource_kind == "proxy_ingress")
            return {
                "SecurityGroupRules": [
                    {
                        "SecurityGroupRuleId": "sgr-exact",
                        "GroupId": "sg-proxy",
                        "IsEgress": False,
                        "ReferencedGroupInfo": {"GroupId": "sg-runner"},
                        "IpProtocol": "tcp",
                        "FromPort": 5432,
                        "ToPort": 5432,
                        "Description": rule_spec.deterministic_name,
                        "Tags": orchestrator._tags(rule_spec),
                    }
                ]
            }

        def revoke_security_group_ingress(self, **kwargs):
            rule_calls.append(("revoke", kwargs))
            return {}

        authorize_security_group_egress = authorize_security_group_ingress
        revoke_security_group_egress = revoke_security_group_ingress

    rule_clients = SimpleNamespace(ec2=RuleEc2())
    real_coordinator, real_specs = original_coordinator(
        CreationScope("bout-two-phase", 11, config.baseline_sha256),
        rule_clients,
        resources,
    )
    network_spec = next(item for item in real_specs if item.resource_kind == "proxy_security_group")
    assert network_spec.metadata["competitor_id"] == "rds_postgres"
    assert network_spec.metadata["competitor_target_id"] == "rds-source"
    assert tuple(item.ordinal for item in real_specs) == tuple(range(1, 10))
    rule_spec = next(item for item in real_specs if item.resource_kind == "proxy_ingress")
    assert "Owner" in {tag["Key"] for tag in orchestrator._tags(rule_spec)}
    rule_adapter = real_coordinator._adapters["proxy_ingress"]
    created_rule = await rule_adapter.create(rule_spec)
    recovered_rule = await rule_adapter.inspect(rule_spec, provider_id=None)
    await rule_adapter.delete(created_rule)
    authorize = rule_calls[0][1]
    assert authorize["TagSpecifications"][0]["ResourceType"] == "security-group-rule"
    assert authorize["IpPermissions"][0]["UserIdGroupPairs"][0]["Description"] == (
        rule_spec.deterministic_name
    )
    assert recovered_rule == created_rule
    assert rule_calls[-1][1]["SecurityGroupRuleIds"] == ["sgr-exact"]

    default_spec = next(
        item for item in real_specs if item.resource_kind == "proxy_default_egress"
    )
    default_adapter = real_coordinator._adapters["proxy_default_egress"]
    await default_adapter.delete(
        orchestrator._observation(default_spec, "sg-proxy:default-egress")
    )
    restored_default_egress = rule_calls[-1][1]
    assert restored_default_egress["GroupId"] == "sg-proxy"
    assert restored_default_egress["IpPermissions"] == [
        {"IpProtocol": "-1", "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}
    ]
    assert restored_default_egress["TagSpecifications"] == [
        {
            "ResourceType": "security-group-rule",
            "Tags": orchestrator._tags(default_spec),
        }
    ]

    target_group_spec = next(
        item for item in real_specs if item.resource_kind == "proxy_target_group"
    )
    target_group_arn = (
        f"arn:aws:rds:us-west-2:{ACCOUNT}:target-group:prx-tg-owned"
    )
    target_group_calls: list[tuple[str, dict[str, object]]] = []

    class TargetGroupRds:
        tags: list[dict[str, str]] = []
        modified = False

        def describe_db_proxy_target_groups(self, **kwargs):
            target_group_calls.append(("describe", kwargs))
            return {
                "TargetGroups": [
                    {
                        "TargetGroupName": "default",
                        "TargetGroupArn": target_group_arn,
                        "ConnectionPoolConfig": {
                            "MaxConnectionsPercent": 90 if self.modified else 100,
                            "ConnectionBorrowTimeout": 120,
                        },
                    }
                ]
            }

        def add_tags_to_resource(self, **kwargs):
            target_group_calls.append(("tag", kwargs))
            self.tags = kwargs["Tags"]
            return {}

        def list_tags_for_resource(self, **kwargs):
            target_group_calls.append(("list_tags", kwargs))
            return {"TagList": self.tags}

        def modify_db_proxy_target_group(self, **kwargs):
            target_group_calls.append(("modify", kwargs))
            self.modified = True
            return {}

    target_group_rds = TargetGroupRds()
    target_group_clients = SimpleNamespace(rds=target_group_rds)
    created_target_group = await orchestrator._configure_target_group(
        target_group_clients,
        resources,
        target_group_spec,
    )
    assert [name for name, _ in target_group_calls] == [
        "describe",
        "tag",
        "list_tags",
        "modify",
    ]
    assert target_group_calls[1][1] == {
        "ResourceName": target_group_arn,
        "Tags": orchestrator._tags(target_group_spec),
    }
    inspected_target_group = await orchestrator._inspect_target_group(
        target_group_clients,
        resources,
        target_group_spec,
        created_target_group.provider_id,
    )
    assert inspected_target_group == created_target_group
    assert [name for name, _ in target_group_calls[-2:]] == ["describe", "list_tags"]

    discovery_arguments: dict[str, object] = {}

    class DiscoverySecrets:
        def list_secrets(self, **kwargs):
            discovery_arguments.update(kwargs)
            return {
                "SecretList": [
                    {
                        "Name": "anti-demo/r5/drifted",
                        "DeletedDate": datetime.now(UTC),
                        "Tags": [],
                    }
                ]
            }

    discovery_clients = SimpleNamespace(
        secretsmanager=DiscoverySecrets(),
        iam=SimpleNamespace(
            list_roles=lambda **kwargs: {"Roles": []},
            list_role_policies=lambda **kwargs: {"PolicyNames": []},
        ),
        ec2=SimpleNamespace(
            describe_security_groups=lambda **kwargs: {"SecurityGroups": []},
            describe_security_group_rules=lambda **kwargs: {"SecurityGroupRules": []},
        ),
        rds=SimpleNamespace(describe_db_proxies=lambda **kwargs: {"DBProxies": []}),
    )
    with pytest.raises(ConnectionSpikeLiveConfigurationError, match="prior-bout add-ons"):
        await orchestrator._discover_orphaned_addons(discovery_clients, "sg-rds")
    assert discovery_arguments["IncludePlannedDeletion"] is True

    aurora_config = replace(
        config,
        competitor_id="aurora_serverless_v2",
        competitor_target_id="aurora-source",
        competitor_resource_id="cluster-RESOURCE",
        competitor_direct_host="aurora-direct.test",
        competitor_credential_sha256="a" * 64,
        competitor_master_secret_arn=(
            f"arn:aws:secretsmanager:us-west-2:{ACCOUNT}:secret:aurora-master"
        ),
    )
    aurora = LiveConnectionSpikeSetupOrchestrator(
        aurora_config,
        journal=Journal(),
        fence=Fence(),
        fresh_lakebase_host=lambda: _value("lakebase-pooled.test"),
        session_factory=SessionFactory(),
    )
    assert aurora_config.proxy_registration == {"DBClusterIdentifiers": ["aurora-source"]}
    assert aurora_config.competitor_credential_id == "aurora"
    assert aurora_config.proxy_secret_arn == aurora_config.aurora_proxy_secret_arn
    aurora_targets = (
        {
            "Type": "RDS_INSTANCE",
            "TrackedClusterId": "aurora-source",
            "RdsResourceId": "db-WRITER",
            "TargetHealth": {"State": "AVAILABLE"},
        },
        {
            "Type": "RDS_INSTANCE",
            "TrackedClusterId": "aurora-source",
            "RdsResourceId": "db-READER",
        },
        {
            "Type": "TRACKED_CLUSTER",
            "RdsResourceId": "aurora-source",
        },
    )
    assert aurora._proxy_targets_match(aurora_targets)
    assert aurora._proxy_targets_available(aurora_targets)
    assert not aurora._proxy_targets_match(
        (*aurora_targets, {"Type": "TRACKED_CLUSTER", "RdsResourceId": "foreign-cluster"})
    )

    class AuroraRds:
        def describe_db_clusters(self, **kwargs):
            assert kwargs == {"DBClusterIdentifier": "aurora-source"}
            return {
                "DBClusters": [
                    {
                        "DBClusterIdentifier": "aurora-source",
                        "DbClusterResourceId": "cluster-RESOURCE",
                        "Endpoint": "aurora-direct.test",
                        "Status": "available",
                        "DBSubnetGroup": "aurora-subnets",
                        "VpcSecurityGroups": [{"VpcSecurityGroupId": "sg-aurora"}],
                    }
                ]
            }

        def describe_db_subnet_groups(self, **kwargs):
            assert kwargs == {"DBSubnetGroupName": "aurora-subnets"}
            return {"DBSubnetGroups": [{"VpcId": "vpc-sealed"}]}

        def register_db_proxy_targets(self, **kwargs):
            assert kwargs == {
                "DBProxyName": "aurora-proxy",
                "DBClusterIdentifiers": ["aurora-source"],
            }
            return {}

    source = await aurora._read_competitor_source(SimpleNamespace(rds=AuroraRds()))
    assert source.direct_host == "aurora-direct.test"
    assert source.security_group_ids == ("sg-aurora",)
    registration = await aurora._register_proxy_target(
        SimpleNamespace(rds=AuroraRds()),
        SimpleNamespace(names=SimpleNamespace(proxy_name="aurora-proxy")),
        ResourceSpec(14, "proxy_target", "aurora-target"),
    )
    assert registration.provider_id == "cluster-RESOURCE"


def test_proxy_secret_policy_grants_only_exact_secret_without_stage_context() -> None:
    secret_arn = f"arn:aws:secretsmanager:us-west-2:{ACCOUNT}:secret:bout-proxy"

    assert LiveConnectionSpikeSetupOrchestrator._proxy_secret_policy(secret_arn) == {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": "secretsmanager:GetSecretValue",
                "Resource": secret_arn,
            }
        ],
    }


def test_iam_policy_comparison_ignores_only_semantically_irrelevant_array_order() -> None:
    first = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["secretsmanager:DescribeSecret", "secretsmanager:GetSecretValue"],
                "Resource": ["arn:secret:aurora", "arn:secret:rds"],
            }
        ],
    }
    reordered = {
        "Statement": [
            {
                "Resource": ["arn:secret:rds", "arn:secret:aurora"],
                "Action": ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"],
                "Effect": "Allow",
            }
        ],
        "Version": "2012-10-17",
    }
    broadened = {
        **reordered,
        "Statement": [
            {
                **reordered["Statement"][0],
                "Action": [
                    "secretsmanager:GetSecretValue",
                    "secretsmanager:DescribeSecret",
                    "secretsmanager:PutSecretValue",
                ],
            }
        ],
    }

    canonical = LiveConnectionSpikeSetupOrchestrator._canonical_policy
    assert canonical(first) == canonical(reordered)
    assert canonical(first) != canonical(broadened)


@pytest.mark.parametrize(
    ("policy_kind", "accepted"),
    (("legacy", True), ("extra_action", False)),
)
async def test_proxy_policy_cleanup_accepts_only_known_legacy_policy(
    policy_kind: str, accepted: bool
) -> None:
    secret_arn = f"arn:aws:secretsmanager:us-west-2:{ACCOUNT}:secret:bout-proxy"
    action: object = "secretsmanager:GetSecretValue"
    condition: dict[str, object] = {
        "StringEquals": {"secretsmanager:VersionStage": "AWSCURRENT"}
    }
    if policy_kind == "extra_action":
        action = ["secretsmanager:GetSecretValue", "secretsmanager:PutSecretValue"]
        condition = {}
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": action,
                "Resource": secret_arn,
                **({"Condition": condition} if condition else {}),
            }
        ],
    }
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)

    class Iam:
        def get_role_policy(self, **kwargs):
            assert kwargs == {"RoleName": "owned-role", "PolicyName": "owned-policy"}
            return {"PolicyDocument": policy}

    resources = SimpleNamespace(
        names=SimpleNamespace(proxy_role_name="owned-role", proxy_policy_name="owned-policy"),
        proxy_role_arn=f"arn:aws:iam::{ACCOUNT}:role/owned-role",
        secret_arn=secret_arn,
    )
    inspection = orchestrator._inspect_proxy_policy(
        SimpleNamespace(iam=Iam()),
        resources,
        ResourceSpec(5, "proxy_iam_policy", "owned-policy"),
        f"arn:aws:iam::{ACCOUNT}:role/owned-role:policy/owned-policy",
    )
    if accepted:
        assert await inspection is not None
    else:
        with pytest.raises(
            ConnectionSpikeLiveConfigurationError, match="Proxy secret policy changed"
        ):
            await inspection


async def test_aurora_pending_proxy_capacity_is_woken_before_strict_topology_check() -> None:
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    orchestrator.config = SimpleNamespace(
        competitor_id="aurora_serverless_v2",
        competitor_target_id="aurora-source",
        competitor_resource_id="cluster-RESOURCE",
        competitor_credential_id="aurora",
        competitor_direct_host="aurora-direct.test",
        competitor_credential_sha256="a" * 64,
        poll_interval_seconds=0.5,
    )
    orchestrator._monotonic_ns = lambda: 1_000_000_000
    order: list[str] = []
    target_health = [
        {"State": "UNAVAILABLE", "Reason": "PENDING_PROXY_CAPACITY"},
        {"State": "AVAILABLE"},
    ]

    async def unexpected_sleep(_seconds):
        raise AssertionError("expected Aurora target states must not poll again")

    orchestrator._sleep = unexpected_sleep

    class Rds:
        def describe_db_proxies(self, **kwargs):
            assert kwargs == {"DBProxyName": "aurora-proxy"}
            return {"DBProxies": [{"Status": "available"}]}

        def describe_db_proxy_targets(self, **kwargs):
            assert kwargs == {"DBProxyName": "aurora-proxy"}
            health = target_health.pop(0)
            order.append(
                "pending_capacity"
                if health.get("Reason") == "PENDING_PROXY_CAPACITY"
                else "available"
            )
            return {
                "Targets": [
                    {
                        "Type": "RDS_INSTANCE",
                        "TrackedClusterId": "aurora-source",
                        "RdsResourceId": "db-WRITER",
                        "TargetHealth": health,
                    },
                    {
                        "Type": "RDS_INSTANCE",
                        "TrackedClusterId": "aurora-source",
                        "RdsResourceId": "db-READER",
                    },
                    {
                        "Type": "TRACKED_CLUSTER",
                        "RdsResourceId": "aurora-source",
                    },
                ]
            }

    async def verify_journaled_resources(*args):
        del args

    async def runner_action(ssm, **kwargs):
        del ssm
        assert kwargs["lane_id"] == "aurora"
        assert kwargs["credential_host"] == "aurora-direct.test"
        endpoint = kwargs["endpoint_host"]
        assert endpoint in {"aurora-direct.test", "dynamic-proxy.test"}
        order.append(
            "direct_transaction" if endpoint == "aurora-direct.test" else "proxy_transaction"
        )

    async def verify_topology(clients, resources):
        del clients, resources
        order.append("strict_topology")

    orchestrator._verify_journaled_resources = verify_journaled_resources
    orchestrator._runner_action = runner_action
    orchestrator._verify_proxy_topology = verify_topology

    class Gate:
        async def wait(self):
            return None

    stop = await orchestrator._setup_competitor(
        "bout-aurora-wake",
        CreationScope("bout-aurora-wake", 7, "b" * 64),
        SimpleNamespace(rds=Rds(), ssm=SimpleNamespace()),
        SimpleNamespace(),
        (),
        SimpleNamespace(
            names=SimpleNamespace(proxy_name="aurora-proxy"),
            proxy_endpoint="dynamic-proxy.test",
            secret_arn="aurora-secret",
        ),
        Gate(),
        [1_000_000_000],
        None,
    )

    assert order == [
        "pending_capacity",
        "direct_transaction",
        "available",
        "strict_topology",
        "proxy_transaction",
    ]
    assert not target_health
    assert stop.endpoint_host == "dynamic-proxy.test"


async def test_cleanup_inspection_accepts_owned_aurora_provider_target_shape() -> None:
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    orchestrator.config = SimpleNamespace(
        competitor_id="aurora_serverless_v2",
        competitor_target_id="aurora-source",
        competitor_resource_id="cluster-RESOURCE",
    )

    class Rds:
        def describe_db_proxy_targets(self, **kwargs):
            assert kwargs == {"DBProxyName": "aurora-proxy"}
            return {
                "Targets": [
                    {
                        "Type": "RDS_INSTANCE",
                        "TrackedClusterId": "aurora-source",
                        "RdsResourceId": "aurora-writer",
                    },
                    {"Type": "TRACKED_CLUSTER", "RdsResourceId": "aurora-source"},
                ]
            }

    observed = await orchestrator._inspect_proxy_target(
        SimpleNamespace(rds=Rds()),
        SimpleNamespace(names=SimpleNamespace(proxy_name="aurora-proxy")),
        ResourceSpec(14, "proxy_target", "aurora-target"),
        "cluster-RESOURCE",
    )

    assert observed is not None
    assert observed.provider_id == "cluster-RESOURCE"


@pytest.mark.parametrize(
    ("method_name", "resource_kind"),
    (
        ("_inspect_proxy_target", "proxy_target"),
        ("_inspect_target_group", "proxy_target_group"),
    ),
)
@pytest.mark.parametrize(
    ("error_code", "expected_absent"),
    (("DBProxyNotFoundFault", True), ("AccessDenied", False)),
)
async def test_proxy_child_inspection_only_accepts_parent_not_found(
    method_name: str,
    resource_kind: str,
    error_code: str,
    expected_absent: bool,
) -> None:
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)

    class ProviderError(Exception):
        response = {"Error": {"Code": error_code}}

    class Rds:
        def describe_db_proxy_targets(self, **kwargs):
            assert kwargs == {"DBProxyName": "deleted-proxy"}
            raise ProviderError

        def describe_db_proxy_target_groups(self, **kwargs):
            assert kwargs == {"DBProxyName": "deleted-proxy"}
            raise ProviderError

    inspection = getattr(orchestrator, method_name)(
        SimpleNamespace(rds=Rds()),
        SimpleNamespace(names=SimpleNamespace(proxy_name="deleted-proxy")),
        ResourceSpec(14, resource_kind, "deleted-proxy-child"),
        "owned-provider-id",
    )
    if expected_absent:
        assert await inspection is None
    else:
        with pytest.raises(ProviderError):
            await inspection


@pytest.mark.parametrize(
    ("error_code", "expected_absent"),
    (("InvalidSecurityGroupRuleId.NotFound", True), ("AccessDenied", False)),
)
async def test_security_rule_inspection_only_accepts_exact_rule_not_found(
    error_code: str, expected_absent: bool
) -> None:
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    orchestrator.config = SimpleNamespace(runner_security_group_id="sg-runner")
    resources = SimpleNamespace(
        proxy_security_group_id="sg-proxy",
        rds_security_group_id="sg-rds",
    )

    class ProviderError(Exception):
        response = {"Error": {"Code": error_code}}

    class Ec2:
        def describe_security_group_rules(self, **kwargs):
            assert kwargs == {"SecurityGroupRuleIds": ["sgr-owned"]}
            raise ProviderError

        def unused(self, **kwargs):
            raise AssertionError(kwargs)

        authorize_security_group_ingress = unused
        authorize_security_group_egress = unused
        revoke_security_group_ingress = unused
        revoke_security_group_egress = unused

    adapter = orchestrator._security_rule_adapter(
        SimpleNamespace(ec2=Ec2()), resources, "rds_ingress"
    )
    inspection = adapter.inspect(
        ResourceSpec(11, "rds_ingress", "owned-rds-ingress"),
        provider_id="sgr-owned",
    )
    if expected_absent:
        assert await inspection is None
    else:
        with pytest.raises(ProviderError):
            await inspection


@pytest.mark.parametrize(
    ("error_code", "expected_absent"),
    (("InvalidGroup.NotFound", True), ("AccessDenied", False)),
)
async def test_default_egress_inspection_treats_only_missing_parent_as_absent(
    error_code: str, expected_absent: bool
) -> None:
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    resources = SimpleNamespace(proxy_security_group_id="sg-deleted")

    class ProviderError(Exception):
        response = {"Error": {"Code": error_code}}

    class Ec2:
        def describe_security_groups(self, **kwargs):
            assert kwargs == {"GroupIds": ["sg-deleted"]}
            raise ProviderError

    adapter = orchestrator._default_egress_adapter(SimpleNamespace(ec2=Ec2()), resources)
    inspection = adapter.inspect(
        ResourceSpec(7, "proxy_default_egress", "owned-default-egress"),
        provider_id="sg-deleted:default-egress",
    )
    if expected_absent:
        assert await inspection is None
    else:
        with pytest.raises(ProviderError):
            await inspection


async def test_default_egress_inspection_accepts_only_scoped_replacement_rule() -> None:
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    resources = SimpleNamespace(proxy_security_group_id="sg-proxy")

    class Ec2:
        def describe_security_groups(self, **kwargs):
            assert kwargs == {"GroupIds": ["sg-proxy"]}
            return {
                "SecurityGroups": [
                    {
                        "IpPermissionsEgress": [
                            {
                                "IpProtocol": "tcp",
                                "FromPort": 5432,
                                "ToPort": 5432,
                                "UserIdGroupPairs": [
                                    {
                                        "GroupId": "sg-database",
                                        "Description": "owned-proxy-egress",
                                    }
                                ],
                            }
                        ]
                    }
                ]
            }

    adapter = orchestrator._default_egress_adapter(SimpleNamespace(ec2=Ec2()), resources)
    observed = await adapter.inspect(
        ResourceSpec(2, "proxy_default_egress", "owned-default-egress"),
        provider_id="sg-proxy:default-egress",
    )

    assert observed is not None
    assert observed.provider_id == "sg-proxy:default-egress"


async def test_default_egress_inspection_rejects_restored_world_egress() -> None:
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    resources = SimpleNamespace(proxy_security_group_id="sg-proxy")

    class Ec2:
        def describe_security_groups(self, **kwargs):
            assert kwargs == {"GroupIds": ["sg-proxy"]}
            return {
                "SecurityGroups": [
                    {
                        "IpPermissionsEgress": [
                            {
                                "IpProtocol": "-1",
                                "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                            }
                        ]
                    }
                ]
            }

    adapter = orchestrator._default_egress_adapter(SimpleNamespace(ec2=Ec2()), resources)
    observed = await adapter.inspect(
        ResourceSpec(2, "proxy_default_egress", "owned-default-egress"),
        provider_id="sg-proxy:default-egress",
    )

    assert observed is None


@pytest.mark.parametrize(
    ("error_code", "expected_absent"),
    (("DBProxyNotFoundFault", True), ("AccessDenied", False)),
)
async def test_proxy_inspection_only_accepts_not_found_tag_lookup_race(
    error_code: str, expected_absent: bool
) -> None:
    proxy_arn = f"arn:aws:rds:us-west-2:{ACCOUNT}:db-proxy:prx-owned"
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    orchestrator.config = SimpleNamespace(region="us-west-2", expected_account_id=ACCOUNT)

    class ProviderError(Exception):
        response = {"Error": {"Code": error_code}}

    class Rds:
        def describe_db_proxies(self, **kwargs):
            assert kwargs == {"DBProxyName": "owned-proxy"}
            return {
                "DBProxies": [
                    {
                        "DBProxyName": "owned-proxy",
                        "DBProxyArn": proxy_arn,
                        "Status": "deleting",
                    }
                ]
            }

        def list_tags_for_resource(self, **kwargs):
            assert kwargs == {"ResourceName": proxy_arn}
            raise ProviderError

    inspection = orchestrator._inspect_proxy(
        SimpleNamespace(rds=Rds()),
        ResourceSpec(12, "rds_proxy", "owned-proxy"),
        proxy_arn,
    )
    if expected_absent:
        assert await inspection is None

        polls = 0

        class DelayedRds:
            def delete_db_proxy(self, **kwargs):
                assert kwargs == {"DBProxyName": "owned-proxy"}

            def describe_db_proxies(self, **kwargs):
                nonlocal polls
                assert kwargs == {"DBProxyName": "owned-proxy"}
                polls += 1
                if polls > 241:
                    raise ProviderError
                return {
                    "DBProxies": [
                        {
                            "DBProxyName": "owned-proxy",
                            "DBProxyArn": proxy_arn,
                            "Status": "deleting",
                        }
                    ]
                }

            def list_tags_for_resource(self, **kwargs):
                assert kwargs == {"ResourceName": proxy_arn}
                return {"TagList": []}

        async def no_sleep(_seconds):
            return None

        delayed = object.__new__(LiveConnectionSpikeSetupOrchestrator)
        delayed.config = SimpleNamespace(
            region="us-west-2",
            expected_account_id=ACCOUNT,
            poll_interval_seconds=0.5,
        )
        delayed._sleep = no_sleep
        await delayed._delete_proxy(
            SimpleNamespace(rds=DelayedRds()),
            delayed._observation(
                ResourceSpec(
                    12,
                    "rds_proxy",
                    "owned-proxy",
                    metadata={"tags": {}},
                ),
                proxy_arn,
            ),
        )
        assert polls == 242
    else:
        with pytest.raises(ProviderError):
            await inspection


async def test_wait_proxy_available_fails_fast_with_sanitized_auth_error() -> None:
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    orchestrator.config = SimpleNamespace(
        competitor_id="rds_postgres",
        competitor_target_id="rds-source",
        competitor_resource_id="db-RESOURCE",
        poll_interval_seconds=0.5,
    )

    async def unexpected_sleep(_seconds):
        raise AssertionError("AUTH_FAILURE must not poll again")

    orchestrator._sleep = unexpected_sleep

    class Rds:
        def describe_db_proxies(self, **kwargs):
            assert kwargs == {"DBProxyName": "rds-proxy"}
            return {"DBProxies": [{"Status": "available"}]}

        def describe_db_proxy_targets(self, **kwargs):
            assert kwargs == {"DBProxyName": "rds-proxy"}
            return {
                "Targets": [
                    {
                        "Type": "RDS_INSTANCE",
                        "RdsResourceId": "db-RESOURCE",
                        "TargetHealth": {
                            "State": "UNAVAILABLE",
                            "Reason": "AUTH_FAILURE",
                            "Description": "Proxy leaked-secret diagnostic must stay hidden",
                        },
                    }
                ]
            }

    with pytest.raises(
        ConnectionSpikeLiveOperationError,
        match="^Round 5 RDS Proxy target credential registration failed$",
    ) as raised:
        await orchestrator._wait_proxy_available(
            SimpleNamespace(rds=Rds()),
            SimpleNamespace(names=SimpleNamespace(proxy_name="rds-proxy")),
        )
    assert "leaked-secret" not in str(raised.value)


def _refusal(output: str) -> str:
    """The message `_validate_setup_output` raises for a runner that refused."""

    with pytest.raises(ConnectionSpikeLiveOperationError) as raised:
        LiveConnectionSpikeSetupOrchestrator._validate_setup_output(
            output,
            bout_id="b" * 32,
            lane_id="competitor",
            action="verify",
            nonce="n" * 64,
        )
    return str(raised.value)


def test_setup_output_failure_repeats_the_runner_refusal_token() -> None:
    """A refused runner must be quoted, not summarised into one useless class.

    This is the 2026-08-24 failure exactly: the runner printed
    `RUNNER_ERROR:baseline_auth_hash_invalid`, settled, and released its flock,
    and the app turned all of that into a bare
    `ConnectionSpikeLiveOperationError`. Two bouts died that way, and the word
    the runner had already said was recovered only from SSM afterwards.
    """

    message = _refusal(
        "RUNNER_ERROR:baseline_auth_hash_invalid\n"
        f"SETUP_SETTLED:{'n' * 64}\n"
        f"RUNNER_FLOCK_RELEASED:{'b' * 32}\n"
    )
    assert "baseline_auth_hash_invalid" in message


def test_setup_output_and_operator_log_preserve_only_the_typed_deadline_category() -> None:
    token = "setup_verify_deadline_state_none_attempts_7_elapsed_100s"
    output = (
        f"RUNNER_ERROR:{token}\n"
        "provider said host=private.example.test password=must-not-escape\n"
        f"SETUP_SETTLED:{'n' * 64}\n"
        f"RUNNER_FLOCK_RELEASED:{'b' * 32}\n"
    )

    with pytest.raises(ConnectionSpikeLiveOperationError) as raised:
        LiveConnectionSpikeSetupOrchestrator._validate_setup_output(
            output,
            bout_id="b" * 32,
            lane_id="aurora",
            action="verify",
            nonce="n" * 64,
        )

    diagnosis = operator_diagnosis(raised.value)
    assert token in diagnosis
    assert "private.example.test" not in diagnosis
    assert "must-not-escape" not in diagnosis


def test_setup_output_failure_bounds_what_it_repeats() -> None:
    """The runner chooses the word; it does not choose how much of it we print.

    The refusal line is remote text on its way to a log, so anything that is
    not one short lowercase identifier is dropped back to the plain sentence.
    A `RUNNER_ERROR:` line carrying a host, an ARN or a password must not
    become the thing this improvement prints.
    """

    leaky = _refusal(
        "RUNNER_ERROR:failed password=hunter2 host=db.internal.example.com\n"
        f"SETUP_SETTLED:{'n' * 64}\n"
    )
    assert "hunter2" not in leaky
    assert "db.internal.example.com" not in leaky
    assert leaky == "Round 5 setup runner did not return exact sanitized evidence"

    silent = _refusal(f"SETUP_SETTLED:{'n' * 64}\n")
    assert silent == "Round 5 setup runner did not return exact sanitized evidence"


def test_setup_output_still_accepts_the_exact_sealed_evidence() -> None:
    """The quoting change must not widen what counts as a verified setup."""

    nonce, bout = "n" * 64, "b" * 32
    receipt = {
        "protocol": "connection-spike-setup-v1",
        "action": "verify",
        "bout_id": bout,
        "lane_id": "competitor",
        "nonce": nonce,
        "status": "verified",
    }
    LiveConnectionSpikeSetupOrchestrator._validate_setup_output(
        f"SETUP_RESULT:{json.dumps(receipt, separators=(',', ':'))}\n"
        f"SETUP_SETTLED:{nonce}\n"
        f"RUNNER_FLOCK_RELEASED:{bout}\n",
        bout_id=bout,
        lane_id="competitor",
        action="verify",
        nonce=nonce,
    )


async def _value(value):
    return value
