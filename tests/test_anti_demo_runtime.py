"""The sealed runtime role: its seal, its trust policy, and the trap in it.

The failure these exist for is the one that cannot be seen by anything that
compares names. A fortnightly account sweep deletes the app's IAM user;
the installer recreates it with an identical name, so it has an identical ARN.
`principal_matches` therefore reports it as the trusted principal, `/readyz`
stays clean, the catalog offers Round 5 -- and the `AssumeRole` is denied,
because IAM stored the *old* user's unique principal ID in the trust policy and
a recreated user has a new one.

`test_recreated_user_passes_principal_matches_and_fails_doctor` is the test that
matters: it asserts both halves at once, so the trap and its detection can never
drift apart silently.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from botocore.exceptions import ClientError

from server import lifecycle
from server.aws_credential_probe import principal_matches
from server.manifest import AwsManifest, DatabricksManifest, DemoManifest

ACCOUNT = "123456789012"
RUNTIME_ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/anti-demo-runtime"
APP_USER_ARN = f"arn:aws:iam::{ACCOUNT}:user/anti-demo-app"
SSO_ROLE_ARN = (
    f"arn:aws:iam::{ACCOUNT}:role/aws-reserved/sso.amazonaws.com/us-west-2/"
    "AWSReservedSSO_ExampleAdmin_aaaabbbbccccdddd"
)
#: What IAM actually returns in `Principal` once the user it named is gone. AWS
#: reverse-maps the stored unique ID back to an ARN only while the principal
#: exists; after a delete it can no longer map it, so the bare ID surfaces.
ORPHANED_USER_ID = "AIDAEXAMPLEEXAMPLE"


def manifest_stub(**aws_updates) -> DemoManifest:
    return DemoManifest(
        run_id="ad-test-001",
        owner="operator@databricks.com",
        created_at=datetime.now(UTC),
        expires_at=datetime.now(UTC) + timedelta(hours=24),
        status="ready",
        aws=AwsManifest(
            profile="sandbox-admin",
            account_id=ACCOUNT,
            region="us-west-2",
            operator_cidr="203.0.113.10/32",
            terraform_state="/tmp/anti-demo-test.tfstate",
            **aws_updates,
        ),
        databricks=DatabricksManifest(
            profile="fe-vm-test",
            project_id="ad-test-001",
            endpoint_name="projects/ad-test-001/branches/production/endpoints/primary",
            user="operator@databricks.com",
        ),
        schema_sha256="abc123",
    )


def sealed_manifest(principals: tuple[str, ...] = (APP_USER_ARN, SSO_ROLE_ARN)) -> DemoManifest:
    return manifest_stub(
        runtime_role_arn=RUNTIME_ROLE_ARN,
        runtime_role_trusted_principal_arns=principals,
    )


def trust_document(principals: list[str]) -> dict[str, object]:
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": principals},
                "Action": "sts:AssumeRole",
            }
        ],
    }


class FakeIam:
    """Just enough IAM to answer the three calls the trust check makes."""

    def __init__(self, *, trust: dict[str, object], existing: set[str] = frozenset()) -> None:
        self._trust = trust
        self._existing = set(existing)

    def get_role(self, RoleName: str) -> dict[str, object]:  # noqa: N803 - boto3 spelling
        if RoleName == RUNTIME_ROLE_ARN.rsplit("/", 1)[-1]:
            return {"Role": {"Arn": RUNTIME_ROLE_ARN, "AssumeRolePolicyDocument": self._trust}}
        if RoleName in self._existing:
            return {"Role": {"Arn": f"arn:aws:iam::{ACCOUNT}:role/{RoleName}"}}
        raise ClientError({"Error": {"Code": "NoSuchEntity"}}, "GetRole")

    def get_user(self, UserName: str) -> dict[str, object]:  # noqa: N803 - boto3 spelling
        if UserName in self._existing:
            return {"User": {"Arn": f"arn:aws:iam::{ACCOUNT}:user/{UserName}"}}
        raise ClientError({"Error": {"Code": "NoSuchEntity"}}, "GetUser")


class FakeSession:
    def __init__(self, iam: FakeIam) -> None:
        self._iam = iam

    def client(self, name: str, **_kwargs) -> FakeIam:
        assert name == "iam"
        return self._iam


@pytest.fixture
def bind_iam(monkeypatch):
    def bind(iam: FakeIam) -> None:
        monkeypatch.setattr(lifecycle, "_aws_session", lambda manifest: FakeSession(iam))

    return bind


def test_recreated_user_passes_principal_matches_and_fails_doctor(bind_iam):
    """The trap and its only detection, asserted together.

    The recreated user is byte-identical to the deleted one as far as every name
    comparison in this project is concerned. Only the live trust document knows,
    and only because IAM has stopped being able to render the stored unique ID
    as an ARN.
    """

    # The trap: the process authenticating as the recreated user still looks
    # exactly like the sealed principal, so nothing on the readiness path fires.
    assert principal_matches(APP_USER_ARN, APP_USER_ARN) is True

    # The detection: the trust policy names an ID nothing can resolve.
    bind_iam(
        FakeIam(
            trust=trust_document([ORPHANED_USER_ID, SSO_ROLE_ARN]),
            existing={"anti-demo-app"},
        )
    )
    check = lifecycle._anti_demo_runtime_trust_check(sealed_manifest())
    assert check.ok is False
    assert check.advisory is False
    assert ORPHANED_USER_ID in check.detail
    assert "antidemo renew" in check.detail


def test_trust_check_passes_when_both_principals_resolve(bind_iam):
    bind_iam(FakeIam(trust=trust_document([APP_USER_ARN, SSO_ROLE_ARN])))
    check = lifecycle._anti_demo_runtime_trust_check(sealed_manifest())
    assert check.ok is True
    assert "2 sealed principals" in check.detail


def test_trust_check_ignores_principal_order(bind_iam):
    """Two principals compared as a set, because Terraform does not promise order."""
    bind_iam(FakeIam(trust=trust_document([SSO_ROLE_ARN, APP_USER_ARN])))
    assert lifecycle._anti_demo_runtime_trust_check(sealed_manifest()).ok is True


def test_trust_check_names_the_principal_the_sweep_removed(bind_iam):
    bind_iam(FakeIam(trust=trust_document([SSO_ROLE_ARN]), existing=set()))
    check = lifecycle._anti_demo_runtime_trust_check(sealed_manifest())
    assert check.ok is False
    assert APP_USER_ARN in check.detail
    assert "does not exist in IAM at all" in check.detail


def test_trust_check_refuses_a_principal_nobody_sealed(bind_iam):
    intruder = f"arn:aws:iam::{ACCOUNT}:user/somebody-else"
    bind_iam(FakeIam(trust=trust_document([APP_USER_ARN, SSO_ROLE_ARN, intruder])))
    check = lifecycle._anti_demo_runtime_trust_check(sealed_manifest())
    assert check.ok is False
    assert intruder in check.detail


def test_unsealed_installation_is_advisory_and_never_calls_aws(monkeypatch):
    """Every installation that predates the runtime role must be unaffected."""

    def explode(manifest):
        raise AssertionError("an unsealed installation must not reach AWS")

    monkeypatch.setattr(lifecycle, "_aws_session", explode)
    check = lifecycle._anti_demo_runtime_trust_check(manifest_stub())
    assert check.ok is True
    assert check.advisory is True
    assert "seals no runtime role" in check.detail


def test_seal_outranks_a_contradicting_environment(monkeypatch):
    monkeypatch.setenv(
        lifecycle.ANTI_DEMO_RUNTIME_PRINCIPALS_ENV,
        f"{APP_USER_ARN},arn:aws:iam::{ACCOUNT}:user/someone-new",
    )
    with pytest.raises(RuntimeError, match="disagrees with the principals this installation"):
        lifecycle.anti_demo_runtime_principals(sealed_manifest())


def test_seal_is_returned_when_the_environment_agrees(monkeypatch):
    monkeypatch.setenv(
        lifecycle.ANTI_DEMO_RUNTIME_PRINCIPALS_ENV, f"{SSO_ROLE_ARN},{APP_USER_ARN}"
    )
    assert lifecycle.anti_demo_runtime_principals(sealed_manifest()) == (
        APP_USER_ARN,
        SSO_ROLE_ARN,
    )


def test_first_provision_reads_the_environment(monkeypatch):
    monkeypatch.setenv(
        lifecycle.ANTI_DEMO_RUNTIME_PRINCIPALS_ENV, f"{APP_USER_ARN}, {SSO_ROLE_ARN}"
    )
    assert lifecycle.anti_demo_runtime_principals(manifest_stub()) == (
        APP_USER_ARN,
        SSO_ROLE_ARN,
    )


def test_first_provision_without_the_environment_creates_nothing(monkeypatch):
    monkeypatch.delenv(lifecycle.ANTI_DEMO_RUNTIME_PRINCIPALS_ENV, raising=False)
    assert lifecycle.anti_demo_runtime_principals(manifest_stub()) == ()


def test_a_principal_from_another_account_is_refused(monkeypatch):
    monkeypatch.setenv(
        lifecycle.ANTI_DEMO_RUNTIME_PRINCIPALS_ENV, "arn:aws:iam::999988887777:user/elsewhere"
    )
    with pytest.raises(RuntimeError, match="exact IAM role or user ARNs"):
        lifecycle.anti_demo_runtime_principals(manifest_stub())


def test_terraform_variables_carry_the_principals_as_hcl(monkeypatch):
    # Set to the caller's own ARN, which is what bootstrap derives, to prove the
    # derivation below is not just reading this back.
    monkeypatch.setenv("ROUND5_APP_PRINCIPAL_ARN", APP_USER_ARN)
    arguments = lifecycle._terraform_variables(sealed_manifest())
    pairs = dict(
        argument.split("=", 1) for argument in arguments[1::2]
    )
    assert pairs["anti_demo_runtime_principal_arns"] == f'["{APP_USER_ARN}","{SSO_ROLE_ARN}"]'
    # The Round 5 control role still trusts exactly one principal -- the runtime
    # role -- which is what keeps `round5_secret_free_topology`'s one-element
    # expectation correct while the two-principal document lives one level up.
    assert pairs["round5_app_principal_arn"] == RUNTIME_ROLE_ARN
    assert pairs["anti_demo_runtime_role_name"] == "anti-demo-runtime"


def test_a_first_provision_derives_the_role_arn_before_it_exists(monkeypatch):
    """Zero new operator inputs depends on this ARN being knowable in advance."""
    monkeypatch.setenv("ROUND5_APP_PRINCIPAL_ARN", APP_USER_ARN)
    monkeypatch.setenv(
        lifecycle.ANTI_DEMO_RUNTIME_PRINCIPALS_ENV, f"{APP_USER_ARN},{SSO_ROLE_ARN}"
    )
    pairs = dict(
        argument.split("=", 1)
        for argument in lifecycle._terraform_variables(manifest_stub())[1::2]
    )
    assert pairs["round5_app_principal_arn"] == RUNTIME_ROLE_ARN


def test_unsealed_installation_asks_terraform_for_no_role(monkeypatch):
    monkeypatch.setenv("ROUND5_APP_PRINCIPAL_ARN", APP_USER_ARN)
    monkeypatch.delenv(lifecycle.ANTI_DEMO_RUNTIME_PRINCIPALS_ENV, raising=False)
    arguments = lifecycle._terraform_variables(manifest_stub())
    pairs = dict(argument.split("=", 1) for argument in arguments[1::2])
    assert pairs["anti_demo_runtime_principal_arns"] == "[]"


def test_state_addresses_expand_only_for_a_sealed_installation():
    unsealed = lifecycle._expected_aws_state_addresses(manifest_stub())
    sealed = lifecycle._expected_aws_state_addresses(sealed_manifest())
    assert lifecycle._ANTI_DEMO_RUNTIME_STATE_ADDRESSES & unsealed == set()
    assert lifecycle._ANTI_DEMO_RUNTIME_STATE_ADDRESSES <= sealed
    # Seven free resources: the role, three policies, three attachments.
    assert len(sealed - unsealed) == 7


def test_renew_may_rewrite_the_trust_policy_but_not_the_role():
    """The repair is only possible because `assume_role_policy` is renewable."""
    assert "assume_role_policy" in lifecycle._RENEW_ALLOWED_PLAN_ATTRIBUTES
    plan = {
        "resource_changes": [
            {
                "address": "aws_iam_role.anti_demo_runtime[0]",
                "change": {
                    "actions": ["update"],
                    "before": {"assume_role_policy": '{"stale": true}'},
                    "after": {"assume_role_policy": '{"stale": false}'},
                },
            }
        ]
    }
    assert lifecycle._renew_plan_violations(sealed_manifest(), plan) == []

    replacement = {
        "resource_changes": [
            {
                "address": "aws_iam_role.anti_demo_runtime[0]",
                "change": {"actions": ["delete", "create"], "before": {}, "after": {}},
            }
        ]
    }
    assert lifecycle._renew_plan_violations(sealed_manifest(), replacement) == [
        "aws_iam_role.anti_demo_runtime[0]: plans delete+create, not a tag update"
    ]


def test_the_deployed_app_cannot_reach_the_runtime_role_by_configuration_alone():
    """The half of "no application code change needed" that is not true.

    Pinned as a test rather than left as a note, because it is the one claim in
    this design that reads as harmless and is not: the operator's laptop really
    does reach the role through `~/.aws/config` with no code change, so it is
    easy to conclude the app does too. It cannot. Every door is shut
    independently, which is why no combination of environment variables opens
    one. Delete this test only alongside the code change that makes it false.
    """
    from server import aws_auth

    # A profile is refused outright in the only mode the app may run in.
    with pytest.raises(aws_auth.AwsAuthConfigurationError):
        aws_auth.validate_runtime_auth(
            "environment",
            "",
            {
                "AWS_ACCESS_KEY_ID": "AKIAEXAMPLEEXAMPLE",
                "AWS_SECRET_ACCESS_KEY": "x" * 40,
                "AWS_PROFILE": "anti-demo-runtime",
            },
        )
    # And no profile is passed to boto3 even if one somehow survived.
    assert "profile_name" not in aws_auth.session_arguments("environment", "p", "us-west-2")
    # And the config file that would carry `role_arn` is stripped from children.
    child = aws_auth.selected_subprocess_environment(
        {
            "AWS_ACCESS_KEY_ID": "AKIAEXAMPLEEXAMPLE",
            "AWS_SECRET_ACCESS_KEY": "x" * 40,
            "AWS_CONFIG_FILE": "/home/app/.aws/config",
            "AWS_ROLE_ARN": RUNTIME_ROLE_ARN,
        },
        aws_auth.AwsAuthSelection(mode="environment"),
        "us-west-2",
    )
    assert "AWS_CONFIG_FILE" not in child
    assert "AWS_ROLE_ARN" not in child


def _stub_renew_terraform(monkeypatch, tmp_path) -> list[str]:
    """Everything `_renew_locked` does other than the trust repair, silenced."""
    applied: list[str] = []
    monkeypatch.setattr(lifecycle, "_write_renew_journal", lambda *args: None)
    monkeypatch.setattr(lifecycle, "_renew_journal_path", lambda: tmp_path / "journal")
    monkeypatch.setattr(lifecycle, "_terraform_init", lambda manifest: None)
    monkeypatch.setattr(lifecycle, "_terraform_plan", lambda manifest, **kwargs: "plan")
    monkeypatch.setattr(lifecycle, "_terraform_plan_json", lambda manifest, plan: {})
    monkeypatch.setattr(
        lifecycle, "_terraform_apply", lambda manifest, plan: applied.append(plan)
    )
    monkeypatch.setattr(lifecycle, "save_manifest", lambda manifest: None)
    return applied


def test_renew_repairs_the_trust_and_leaves_the_seal_alone(monkeypatch, tmp_path, bind_iam):
    """The fortnightly repair, end to end.

    Terraform re-resolves the sealed ARNs to the recreated user's new unique ID,
    so the document AWS holds changes while the strings the manifest sealed do
    not. Asserting the seal is byte-identical afterwards is the point: renew
    repairs a relationship, it does not re-decide who is trusted.
    """
    applied = _stub_renew_terraform(monkeypatch, tmp_path)
    manifest = sealed_manifest()
    sealed_before = manifest.aws.runtime_role_trusted_principal_arns

    documents = iter(
        [
            trust_document([ORPHANED_USER_ID, SSO_ROLE_ARN]),
            trust_document([APP_USER_ARN, SSO_ROLE_ARN]),
        ]
    )
    monkeypatch.setattr(
        lifecycle,
        "_aws_session",
        lambda _manifest: FakeSession(
            FakeIam(trust=next(documents), existing={"anti-demo-app"})
        ),
    )

    target = manifest.expires_at + timedelta(hours=12)
    renewed = lifecycle._renew_locked(manifest, target, "20260822T000000Z", timeout_seconds=5.0)

    assert applied == ["plan"]
    assert renewed.expires_at == target
    assert renewed.aws.runtime_role_arn == RUNTIME_ROLE_ARN
    assert renewed.aws.runtime_role_trusted_principal_arns == sealed_before


def test_renew_refuses_to_report_success_when_the_repair_did_not_take(
    monkeypatch, tmp_path
):
    """An apply that returns zero is not evidence the trust resolves again."""
    _stub_renew_terraform(monkeypatch, tmp_path)
    manifest = sealed_manifest()
    monkeypatch.setattr(
        lifecycle,
        "_aws_session",
        lambda _manifest: FakeSession(
            FakeIam(trust=trust_document([ORPHANED_USER_ID, SSO_ROLE_ARN]))
        ),
    )

    with pytest.raises(RuntimeError, match="trust is still broken"):
        lifecycle._renew_locked(
            manifest,
            manifest.expires_at + timedelta(hours=12),
            "20260822T000000Z",
            timeout_seconds=5.0,
        )


def test_round5_keeps_its_one_element_trust_expectation():
    """The two-principal document lives on the runtime role, not on Round 5.

    `_round5_topology_check` canonicalises the control role's live trust and
    compares it against `Round5Resources.control_trust_policy`, which seals a
    single ARN. That check would indeed break if a second principal were added
    there -- so none is. The control role trusts the runtime role, and the
    runtime role is the one document naming two principals.
    """
    control_trust = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": RUNTIME_ROLE_ARN},
                "Action": "sts:AssumeRole",
            }
        ],
    }
    sealed = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": RUNTIME_ROLE_ARN},
                "Action": "sts:AssumeRole",
            }
        ],
    }
    assert lifecycle._canonical_iam_policy(control_trust) == lifecycle._canonical_iam_policy(
        sealed
    )
    # And the two-principal shape is what would have broken it.
    assert lifecycle._canonical_iam_policy(
        trust_document([APP_USER_ARN, SSO_ROLE_ARN])
    ) != lifecycle._canonical_iam_policy(sealed)


def test_the_seal_refuses_to_follow_terraform_once_written():
    manifest = sealed_manifest()
    with pytest.raises(RuntimeError, match="differs from the seal"):
        lifecycle._seal_anti_demo_runtime(
            manifest,
            {
                "anti_demo_runtime_role_arn": RUNTIME_ROLE_ARN,
                "anti_demo_runtime_trusted_principal_arns": [APP_USER_ARN],
            },
        )


def test_the_seal_is_written_once_on_a_first_provision():
    manifest = manifest_stub()
    lifecycle._seal_anti_demo_runtime(
        manifest,
        {
            "anti_demo_runtime_role_arn": RUNTIME_ROLE_ARN,
            "anti_demo_runtime_trusted_principal_arns": [APP_USER_ARN, SSO_ROLE_ARN],
        },
    )
    assert manifest.aws.runtime_role_arn == RUNTIME_ROLE_ARN
    assert manifest.aws.runtime_role_trusted_principal_arns == (APP_USER_ARN, SSO_ROLE_ARN)


def test_a_vanished_role_is_refused_rather_than_re_sealed_as_absent():
    with pytest.raises(RuntimeError, match="Terraform reports none"):
        lifecycle._seal_anti_demo_runtime(sealed_manifest(), {})


def test_trust_principals_are_returned_verbatim():
    """Canonicalising here would destroy the only evidence the break leaves."""
    document = trust_document([ORPHANED_USER_ID, SSO_ROLE_ARN])
    assert lifecycle._iam_trust_principals(document) == (ORPHANED_USER_ID, SSO_ROLE_ARN)
    # URL-encoded JSON is what `iam.get_role` returns over the wire.
    assert lifecycle._iam_trust_principals(
        '{"Version":"2012-10-17","Statement":[{"Effect":"Allow",'
        '"Principal":{"AWS":"' + SSO_ROLE_ARN + '"},"Action":"sts:AssumeRole"}]}'
    ) == (SSO_ROLE_ARN,)
