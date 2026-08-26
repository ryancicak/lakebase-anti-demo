from __future__ import annotations

import os

import pytest

from server.aws_auth import (
    AwsAuthConfigurationError,
    AwsAuthSelection,
    select_setup_auth,
    selected_subprocess_environment,
    session_arguments,
    validate_app_aws_environment,
)
from tests.conftest import AMBIENT_AWS_NAMES


def keys(*, token: bool = False) -> dict[str, str]:
    environment = {
        "AWS_ACCESS_KEY_ID": "test-access-key",
        "AWS_SECRET_ACCESS_KEY": "test-secret-key",
    }
    if token:
        environment["AWS_SESSION_TOKEN"] = "test-session-token"
    return environment


@pytest.mark.parametrize("token", [False, True])
def test_setup_accepts_complete_ambient_credentials(token: bool) -> None:
    assert select_setup_auth(keys(token=token)) == AwsAuthSelection(mode="environment")


@pytest.mark.parametrize(
    ("environment", "message"),
    [
        ({"AWS_ACCESS_KEY_ID": "only-access"}, "must be provided together"),
        ({"AWS_SECRET_ACCESS_KEY": "only-secret"}, "must be provided together"),
        ({"AWS_SESSION_TOKEN": "only-token"}, "requires AWS_ACCESS_KEY_ID"),
        (
            {**keys(), "AWS_PROFILE": "sandbox"},
            "cannot be used together",
        ),
    ],
)
def test_setup_rejects_incomplete_or_ambiguous_credentials(
    environment: dict[str, str],
    message: str,
) -> None:
    with pytest.raises(AwsAuthConfigurationError, match=message):
        select_setup_auth(environment, environment.get("AWS_PROFILE", ""))


def test_selected_terraform_environment_contains_only_profile_source() -> None:
    environment = selected_subprocess_environment(
        {
            **keys(token=True),
            "AWS_ROLE_ARN": "arn:aws:iam::123456789012:role/wrong",
            "AWS_WEB_IDENTITY_TOKEN_FILE": "/tmp/wrong-token",
            "UNRELATED": "kept",
        },
        AwsAuthSelection(mode="profile", profile="sandbox"),
        "us-west-2",
    )

    assert environment["AWS_PROFILE"] == "sandbox"
    assert environment["AWS_REGION"] == "us-west-2"
    assert environment["UNRELATED"] == "kept"
    assert "AWS_ACCESS_KEY_ID" not in environment
    assert "AWS_SECRET_ACCESS_KEY" not in environment
    assert "AWS_SESSION_TOKEN" not in environment
    assert "AWS_ROLE_ARN" not in environment
    assert "AWS_WEB_IDENTITY_TOKEN_FILE" not in environment


def test_selected_terraform_environment_contains_only_ambient_source() -> None:
    base = {**keys(token=True), "AWS_PROFILE": "wrong", "AWS_DEFAULT_PROFILE": "wrong"}
    environment = selected_subprocess_environment(
        base,
        AwsAuthSelection(mode="environment"),
        "us-west-2",
    )

    assert environment["AWS_ACCESS_KEY_ID"] == "test-access-key"
    assert environment["AWS_SECRET_ACCESS_KEY"] == "test-secret-key"
    assert environment["AWS_SESSION_TOKEN"] == "test-session-token"
    assert "AWS_PROFILE" not in environment
    assert "AWS_DEFAULT_PROFILE" not in environment


def test_environment_sessions_do_not_receive_credentials_or_profile() -> None:
    assert session_arguments("environment", "", "us-west-2") == {
        "region_name": "us-west-2"
    }
    assert session_arguments("profile", "sandbox", "us-west-2") == {
        "profile_name": "sandbox",
        "region_name": "us-west-2",
    }


class FakeSts:
    def __init__(self, account: str) -> None:
        self.account = account

    def get_caller_identity(self) -> dict[str, str]:
        return {"Account": self.account}


class FakeSession:
    def __init__(self, account: str) -> None:
        self.account = account

    def client(self, service: str) -> FakeSts:
        assert service == "sts"
        return FakeSts(self.account)


def app_environment() -> dict[str, str]:
    return {
        **keys(),
        "AWS_AUTH_MODE": "environment",
        "AWS_REGION": "us-west-2",
        "AWS_EXPECTED_ACCOUNT_ID": "123456789012",
        "AURORA_CLUSTER_ID": "anti-demo-aurora",
        "AURORA_SECRET_ARN": "arn:aws:secretsmanager:us-west-2:123456789012:secret:a",
        "RDS_INSTANCE_ID": "anti-demo-rds",
        "RDS_SECRET_ARN": "arn:aws:secretsmanager:us-west-2:123456789012:secret:r",
    }


def test_app_preflight_rejects_the_wrong_exact_sts_account() -> None:
    with pytest.raises(AwsAuthConfigurationError, match="999999999999"):
        validate_app_aws_environment(
            app_environment(),
            session_factory=lambda **_: FakeSession("999999999999"),
        )


def test_an_empty_session_token_is_the_keys_only_app_binding() -> None:
    """app.yaml cannot make a binding optional, so empty has to mean absent.

    Databricks Apps fails an app whose valueFrom names a missing resource, so a
    keys-only deployment supplies aws-session-token as an empty secret. botocore
    reads the token with `if token:`; this is the same decision on our side.
    """

    empty = {**app_environment(), "AWS_SESSION_TOKEN": ""}

    validate_app_aws_environment(
        empty,
        session_factory=lambda **_: FakeSession("123456789012"),
    )
    assert select_setup_auth(
        {**keys(), "AWS_SESSION_TOKEN": ""}
    ) == AwsAuthSelection(mode="environment")
    forwarded = selected_subprocess_environment(
        {**keys(), "AWS_SESSION_TOKEN": ""},
        AwsAuthSelection(mode="environment"),
        "us-west-2",
    )
    assert "AWS_SESSION_TOKEN" not in forwarded


def test_a_whitespace_session_token_is_not_treated_as_absent() -> None:
    """A space is truthy to botocore too, so it would be signed as a real token."""

    forwarded = selected_subprocess_environment(
        {**keys(), "AWS_SESSION_TOKEN": " "},
        AwsAuthSelection(mode="environment"),
        "us-west-2",
    )
    assert forwarded["AWS_SESSION_TOKEN"] == " "


def test_no_test_sees_the_developers_own_aws_credentials() -> None:
    """Pin the isolation in tests/conftest.py:hide_ambient_aws_credentials.

    Without it, every test that reaches server/lifecycle.py:_aws_session behind a
    fake boto3 passed or failed depending on whether the shell that launched
    pytest had credentials exported -- and bootstrap.sh instructs operators to
    export exactly these. Real credentials in a test process are also a leak
    hazard, not only a flake.
    """

    for name in AMBIENT_AWS_NAMES:
        assert name not in os.environ, f"{name} is visible to tests"


def test_app_preflight_rejects_a_missing_resource_binding() -> None:
    environment = app_environment()
    environment.pop("AURORA_CLUSTER_ID")

    with pytest.raises(AwsAuthConfigurationError, match="AURORA_CLUSTER_ID"):
        validate_app_aws_environment(
            environment,
            session_factory=lambda **_: FakeSession("123456789012"),
        )
