"""Deterministic contract: the tags the code stamps on the timed CreateDBProxy
(and its dependent target-group AddTagsToResource) must line up EXACTLY with the
Round 5 execution-proxy IAM policy in ``infra/aws``.

Why this test exists
--------------------
The competitor lane's only timed AWS mutation is ``rds:CreateDBProxy``.  IAM
authorises the dependent ``rds:AddTagsToResource`` with
``ForAllValues:StringEquals aws:TagKeys = local.round5_bout_tag_keys`` -- so a
single tag key the code emits that the Terraform allow-list does not contain
denies the whole create, and the lane fails at the bell (this is exactly the
``anti-demo:bout-fence`` incident: the key was stamped by the code but missing
from the allow-list).  The policy also *requires the presence* of the immutable
per-bout fence (``Null = false`` on ``aws:RequestTag/anti-demo:bout-fence``), so
a create that drops it fails closed rather than making an unfenced Proxy.

This test parses the actual Terraform and the actual code path and asserts they
agree, so a future edit to either side that breaks the contract turns red here
instead of at a live bell.  The final case drives the real ``_create_proxy``
through a ``botocore.stub.Stubber`` and asserts the exact ``Tags`` on the wire.
"""

from __future__ import annotations

import pathlib
import re
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import boto3
from botocore.stub import Stubber

from server.connection_spike_journal import CreationScope
from server.connection_spike_live import (
    ConnectionSpikeSetupConfig,
    ConnectionSpikeSetupNames,
    LiveConnectionSpikeSetupOrchestrator,
    _SetupAwsClients,
    _SetupResources,
)
from server.manifest import Round5OwnershipTags

ACCOUNT = "123456789012"
RUN_ID = "ad-20260820-1446-abcd"
OWNER = "00000000-0000-4000-8000-00000000abcd"
EXPIRES_AT = "2026-09-23T23:19:46Z"
SLUG = "i" + "b102685e189ce138280f"[:20].ljust(20, "0") + "-r5"
BOUT_ID = "bout0123456789abcdef0123456789ab"
TOKEN = "bt-0123456789abcdef"
FENCE = 42
RUNTIME_SEAL = "d" * 64

_AWS_DIR = pathlib.Path(__file__).resolve().parents[1] / "infra" / "aws"
_CONTROL_TF = (_AWS_DIR / "round5_control.tf").read_text()
_LOCALS_TF = (_AWS_DIR / "locals.tf").read_text()

# Terraform emits ``anti-demo-installation-slug`` and ``anti-demo-round`` through
# the ``dynamic "condition"`` over ``local.round5_ownership_tags``; those are the
# only two request-tag conditions whose key is templated (``${condition.key}``)
# rather than a literal, so they are added explicitly after literal parsing.
_DYNAMIC_OWNERSHIP_KEYS = frozenset({"anti-demo-installation-slug", "anti-demo-round"})


# --------------------------------------------------------------------------- #
# Terraform parsing (the source of truth for "allow" and "required" keys)
# --------------------------------------------------------------------------- #
def _proxy_policy_document() -> str:
    """The ``round5_execution_proxy`` policy *document* only (not the Deny loop
    that follows it in the same data source, and not the other role policy)."""

    start = _CONTROL_TF.index(
        'data "aws_iam_policy_document" "round5_execution_proxy"'
    )
    end = _CONTROL_TF.index('resource "aws_iam_policy" "round5_execution_proxy"', start)
    return _CONTROL_TF[start:end]


def _statement_for_action(doc: str, action_regex: str) -> str:
    """Return one ``statement { ... }`` block: from the matched action to the
    next ``statement``/``dynamic "statement"`` boundary (or the document end)."""

    action = re.search(action_regex, doc)
    assert action is not None, f"action {action_regex!r} not found in policy document"
    rest = doc[action.start() :]
    boundary = re.search(r'\n\s*statement \{|\n\s*dynamic "statement"', rest[1:])
    return rest[: boundary.start() + 1] if boundary else rest


def _request_tag_keys(statement: str, *, test: str, values: str | None = None) -> set[str]:
    """Literal ``aws:RequestTag/<key>`` keys guarded by ``test`` in ``statement``.

    ``${...}`` templated keys (the dynamic ownership loop) are skipped; the
    optional ``values`` regex pins e.g. ``["false"]`` for a ``Null`` presence.
    """

    tail = r'\s*values\s*=\s*' + values if values else ""
    pattern = (
        r'test\s*=\s*"' + re.escape(test) + r'"\s*'
        r'variable\s*=\s*"aws:RequestTag/([^"]+)"' + tail
    )
    return {
        key
        for key in re.findall(pattern, statement)
        if "$" not in key and "{" not in key
    }


def _create_db_proxy_statement() -> str:
    return _statement_for_action(
        _proxy_policy_document(), r'actions\s*=\s*\["rds:CreateDBProxy"\]'
    )


def _add_tags_statement() -> str:
    # The first AddTagsToResource in the document is the Allow statement; the
    # tag-hijack Deny lives inside a later ``dynamic "statement"`` block.
    return _statement_for_action(
        _proxy_policy_document(), r'actions\s*=\s*\["rds:AddTagsToResource"\]'
    )


def _terraform_present_keys(statement: str) -> set[str]:
    return _request_tag_keys(statement, test="Null", values=r'\[\s*"false"\s*\]')


def _terraform_equals_keys(statement: str) -> set[str]:
    return _request_tag_keys(statement, test="StringEquals")


def _terraform_allow_list() -> set[str]:
    """The full ``round5_bout_tag_keys`` allow-list, reconstructed as the union of
    every request-tag key the two tag-bearing statements reference."""

    create = _create_db_proxy_statement()
    add_tags = _add_tags_statement()
    keys: set[str] = set()
    for statement in (create, add_tags):
        keys |= _terraform_present_keys(statement)
        keys |= _terraform_equals_keys(statement)
    return keys | _DYNAMIC_OWNERSHIP_KEYS


def _locals_bout_tag_keys() -> set[str]:
    """Independent cross-check: rebuild ``round5_bout_tag_keys`` from locals.tf.

    = required_tags keys + dynamic ownership keys + the three literal bout keys
    added inside the ``round5_bout_tag_keys`` merge.
    """

    required_block = re.search(r"required_tags\s*=\s*\{([^}]*)\}", _LOCALS_TF)
    assert required_block is not None
    required_keys = set(re.findall(r'"([^"]+)"\s*=', required_block.group(1)))

    bout_merge = re.search(
        r"round5_bout_tag_keys\s*=\s*sort\(keys\(merge\(\s*"
        r"local\.round5_bout_base_tags,\s*\{(.*?)\}\)\)\)",
        _LOCALS_TF,
        re.DOTALL,
    )
    assert bout_merge is not None
    bout_keys = set(re.findall(r'"([^"]+)"\s*=', bout_merge.group(1)))

    return required_keys | _DYNAMIC_OWNERSHIP_KEYS | bout_keys


# --------------------------------------------------------------------------- #
# The code path (the source of truth for the tags actually emitted)
# --------------------------------------------------------------------------- #
def _config() -> ConnectionSpikeSetupConfig:
    ownership = Round5OwnershipTags(
        anti_demo_run_id=RUN_ID,
        owner=OWNER,
        expires_at=EXPIRES_AT,
        anti_demo_installation_slug=SLUG,
        anti_demo_round="r5",
    ).as_aws_tags()
    return ConnectionSpikeSetupConfig(
        region="us-west-2",
        expected_account_id=ACCOUNT,
        baseline_control_role_arn=f"arn:aws:iam::{ACCOUNT}:role/baseline-control",
        runner_instance_id="i-0123456789abcdef0",
        competitor_runner_instance_id="i-0fedcba9876543210",
        vpc_id="vpc-sealed",
        proxy_subnet_ids=("subnet-a", "subnet-b"),
        lakebase_direct_host="lakebase-direct.test",
        lakebase_pooled_host="lakebase-pooled.test",
        competitor_id="aurora_serverless_v2",
        competitor_target_id="aurora-source",
        competitor_resource_id="cluster-RESOURCE",
        competitor_direct_host="aurora-direct.test",
        competitor_security_group_id="sg-aurora",
        runner_security_group_id="sg-runner",
        proxy_security_group_id="sg-proxy",
        proxy_service_role_arn=f"arn:aws:iam::{ACCOUNT}:role/proxy-service",
        proxy_service_policy_name="proxy-service-secrets",
        aurora_proxy_secret_arn=f"arn:aws:secretsmanager:us-west-2:{ACCOUNT}:secret:aurora-proxy",
        rds_proxy_secret_arn=f"arn:aws:secretsmanager:us-west-2:{ACCOUNT}:secret:rds-proxy",
        deterministic_name_prefix="anti-demo-r5",
        ownership_tags=tuple(sorted(ownership.items())),
        trust_bundle_path="/opt/lakebase-anti-demo/round5/round5-ca.pem",
        trust_bundle_sha256="b" * 64,
        runner_public_key_sha256="c" * 64,
        baseline_sha256="d" * 64,
        lakebase_credential_sha256="e" * 64,
        competitor_credential_sha256="f" * 64,
    )


class _FakeJournal:
    async def events(self, scope):  # pragma: no cover - not exercised here
        return ()

    async def commit(self, event, *, authority_scope=None):  # pragma: no cover
        raise AssertionError("the tag contract never commits to the journal")

    async def scopes(self, bout_id):  # pragma: no cover - not exercised here
        return ()


class _FakeFence:
    async def assert_current(self, scope):  # pragma: no cover - not exercised here
        return None


def _orchestrator(config: ConnectionSpikeSetupConfig) -> LiveConnectionSpikeSetupOrchestrator:
    return LiveConnectionSpikeSetupOrchestrator(
        config,
        journal=_FakeJournal(),
        fence=_FakeFence(),
        fresh_lakebase_host=lambda: SimpleNamespace(host="lakebase-pooled.test"),
    )


def _resources() -> _SetupResources:
    names = ConnectionSpikeSetupNames(
        token=TOKEN,
        proxy_security_group_name="anti-demo-r5-bout-proxy-sg",
        proxy_name="anti-demo-r5-bout-proxy",
    )
    return _SetupResources(
        names=names,
        secret_arn=f"arn:aws:secretsmanager:us-west-2:{ACCOUNT}:secret:anti-demo-r5-bout",
        proxy_role_arn=f"arn:aws:iam::{ACCOUNT}:role/anti-demo-r5-proxy",
        proxy_security_group_id="sg-per-bout-proxy",
    )


def _rds_proxy_spec(orchestrator, resources):
    scope = CreationScope(
        bout_id=BOUT_ID, fencing_token=FENCE, runtime_seal_sha256=RUNTIME_SEAL
    )
    clients = _SetupAwsClients(
        ssm=SimpleNamespace(),
        rds=SimpleNamespace(),
        ec2=SimpleNamespace(),
        iam=SimpleNamespace(),
        secretsmanager=SimpleNamespace(),
        expires_at=datetime.now(UTC) + timedelta(hours=1),
    )
    _coordinator, specs = orchestrator._coordinator(scope, clients, resources)
    spec = next(spec for spec in specs if spec.resource_kind == "rds_proxy")
    return spec


def _emitted_tag_pairs(orchestrator, spec) -> dict[str, str]:
    return {tag["Key"]: tag["Value"] for tag in orchestrator._tags(spec)}


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #
def test_terraform_requires_present_fence_on_create_and_add_tags():
    """A.2: the immutable per-bout fence must be *required present* (not merely
    allowed) on both the CreateDBProxy and the AddTagsToResource statements."""

    create_present = _terraform_present_keys(_create_db_proxy_statement())
    add_present = _terraform_present_keys(_add_tags_statement())

    assert "anti-demo:bout-fence" in create_present, (
        "CreateDBProxy must require Null=false on aws:RequestTag/anti-demo:bout-fence"
    )
    assert "anti-demo:bout-fence" in add_present, (
        "AddTagsToResource must require Null=false on aws:RequestTag/anti-demo:bout-fence"
    )
    # bout-id presence was already required; keep it asserted so a regression that
    # drops the whole presence family is caught here too.
    assert "anti-demo-bout-id" in create_present
    assert {"anti-demo-bout-id", "anti-demo:bout-token", "anti-demo:bout-fence"} <= add_present


def test_terraform_allow_list_is_self_consistent():
    """The allow-list reconstructed from the policy statements matches the one
    reconstructed from locals.tf's ``round5_bout_tag_keys`` merge."""

    assert _terraform_allow_list() == _locals_bout_tag_keys()


def test_emitted_create_db_proxy_tags_equal_terraform_allow_list():
    """Every key the code stamps is allow-listed, and every allow-listed key is
    stamped -- exact set equality, which is what ``ForAllValues`` needs."""

    config = _config()
    orchestrator = _orchestrator(config)
    spec = _rds_proxy_spec(orchestrator, _resources())
    emitted = _emitted_tag_pairs(orchestrator, spec)

    assert set(emitted) == _terraform_allow_list()


def test_emitted_tags_satisfy_required_presence_and_values():
    """Each ``Null=false`` presence key is emitted, and each ``StringEquals`` key
    the CreateDBProxy statement pins carries the value the policy compares to."""

    config = _config()
    orchestrator = _orchestrator(config)
    spec = _rds_proxy_spec(orchestrator, _resources())
    emitted = _emitted_tag_pairs(orchestrator, spec)

    create = _create_db_proxy_statement()
    for key in _terraform_present_keys(create) | _terraform_present_keys(_add_tags_statement()):
        assert key in emitted, f"policy requires tag {key!r} present but code omits it"

    expected_values = {
        "anti-demo-run-id": RUN_ID,
        "managed-by": "round5-lifecycle",
        "Owner": OWNER,
        "owner": OWNER,
        "expires-at": EXPIRES_AT,
    }
    for key in _terraform_equals_keys(create):
        if key in expected_values:
            assert emitted[key] == expected_values[key]

    # The fence is the fencing token; the token/bout-id are the per-bout identity.
    assert emitted["anti-demo:bout-fence"] == str(FENCE)
    assert emitted["anti-demo:bout-token"] == TOKEN
    assert emitted["anti-demo-bout-id"] == BOUT_ID


async def test_create_db_proxy_stubber_sends_exactly_the_allow_listed_tags():
    """Drive the real ``_create_proxy`` through a botocore Stubber and assert the
    exact ``Tags`` on the CreateDBProxy request equal the sorted allow-listed set.

    Stubber answers on the ``before-call`` event (above the tests/conftest.py AWS
    guard), so this exercises real botocore request serialisation.  ``expected_params``
    is the whole request, so any drift in ``Tags`` -- an extra key, a missing key,
    a changed value, or a changed order -- raises here.
    """

    config = _config()
    orchestrator = _orchestrator(config)
    resources = _resources()
    spec = _rds_proxy_spec(orchestrator, resources)
    emitted = _emitted_tag_pairs(orchestrator, spec)

    expected_tags = [
        {"Key": key, "Value": value} for key, value in sorted(emitted.items())
    ]
    # Tie the wire assertion to the policy: the exact keys sent are the allow-list.
    assert {tag["Key"] for tag in expected_tags} == _terraform_allow_list()

    proxy_name = resources.names.proxy_name
    proxy_arn = f"arn:aws:rds:us-west-2:{ACCOUNT}:db-proxy:{proxy_name}"
    expected_params = {
        "DBProxyName": proxy_name,
        "EngineFamily": "POSTGRESQL",
        "Auth": [
            {
                "AuthScheme": "SECRETS",
                "SecretArn": resources.secret_arn,
                "IAMAuth": "DISABLED",
                "ClientPasswordAuthType": "POSTGRES_SCRAM_SHA_256",
            }
        ],
        "RoleArn": resources.proxy_role_arn,
        "VpcSubnetIds": list(config.proxy_subnet_ids),
        "VpcSecurityGroupIds": [resources.proxy_security_group_id],
        "RequireTLS": True,
        "Tags": expected_tags,
    }

    rds = boto3.client(
        "rds",
        region_name="us-west-2",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    stubber = Stubber(rds)
    stubber.add_response(
        "create_db_proxy", {"DBProxy": {"DBProxyName": proxy_name}}, expected_params
    )
    stubber.add_response(
        "describe_db_proxies",
        {
            "DBProxies": [
                {
                    "DBProxyName": proxy_name,
                    "DBProxyArn": proxy_arn,
                    "Endpoint": "anti-demo-r5-bout-proxy.proxy-x.us-west-2.rds.amazonaws.com",
                    "Status": "available",
                }
            ]
        },
        {"DBProxyName": proxy_name},
    )

    clients = _SetupAwsClients(
        ssm=SimpleNamespace(),
        rds=rds,
        ec2=SimpleNamespace(),
        iam=SimpleNamespace(),
        secretsmanager=SimpleNamespace(),
        expires_at=datetime.now(UTC) + timedelta(hours=1),
    )

    with stubber:
        try:
            observation = await orchestrator._create_proxy(
                clients, resources, spec, scope=None
            )
        finally:
            executor = orchestrator._createproxy_executor
            if executor is not None:
                executor.shutdown(wait=True)
        stubber.assert_no_pending_responses()

    assert observation.provider_id == proxy_arn
