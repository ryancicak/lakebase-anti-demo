"""An Azure installation's ownership scope for Rounds 2 and 3, and the engine's check of it.

An installation lives in one cloud for life, so a scope carries one cloud's identity and refuses
the other's, and the engine proves an Azure artifact owned by subscription and region exactly as
it proves an AWS one by account and region. The AWS path is unchanged: tests/test_safe_change.py
still builds its scopes with only the AWS fields.
"""

from __future__ import annotations

import pytest

from server.safe_change import (
    ArtifactInspection,
    SafeChangeEngine,
    SafeChangeOwnershipScope,
    SafeChangePlan,
    SafeChangeProvider,
    UnsafeCleanupError,
)

SUBSCRIPTION = "00000000-1111-2222-3333-444444444444"
COMMON = {"run_id": "ad-test-001", "owner": "operator@databricks.com",
          "expires_at": "2030-01-01T00:00:00Z"}


def azure_scope(**overrides: str) -> SafeChangeOwnershipScope:
    values = {**COMMON, "cloud": "azure", "azure_subscription_id": SUBSCRIPTION,
              "azure_region": "westus2", **overrides}
    return SafeChangeOwnershipScope(**values)


def test_an_azure_scope_needs_its_subscription_and_region() -> None:
    scope = azure_scope()
    assert (scope.cloud, scope.azure_region, scope.aws_account_id) == ("azure", "westus2", "")
    with pytest.raises(ValueError, match="lowercase GUID"):
        azure_scope(azure_subscription_id="not-a-guid")
    with pytest.raises(ValueError, match="azure_region"):
        azure_scope(azure_region="West US 2")


def test_a_scope_never_carries_the_other_clouds_identity() -> None:
    with pytest.raises(ValueError, match="no AWS account"):
        azure_scope(aws_account_id="123456789012")
    with pytest.raises(ValueError, match="no Azure subscription"):
        SafeChangeOwnershipScope(**COMMON, aws_account_id="123456789012", aws_region="us-west-2",
                                 azure_subscription_id=SUBSCRIPTION)
    with pytest.raises(ValueError, match="cloud must be"):
        SafeChangeOwnershipScope(**COMMON, cloud="gcp")


def test_an_aws_scope_still_validates_exactly_as_before() -> None:
    scope = SafeChangeOwnershipScope(**COMMON, aws_account_id="123456789012",
                                     aws_region="us-west-2")
    assert scope.cloud == "aws"
    with pytest.raises(ValueError, match="exactly 12 digits"):
        SafeChangeOwnershipScope(**COMMON, aws_account_id="1234", aws_region="us-west-2")


def plan(scope: SafeChangeOwnershipScope) -> SafeChangePlan:
    return SafeChangePlan(lane_id="azure_postgres", name="Azure Database for PostgreSQL",
                          provider=SafeChangeProvider.AZURE_POSTGRES, source_id="r2-server",
                          artifact_id="r2-server-copy", scope=scope)


def artifact(**overrides: object) -> ArtifactInspection:
    values: dict[str, object] = {
        "artifact_id": "r2-server-copy", "provider": SafeChangeProvider.AZURE_POSTGRES,
        "source_id": "r2-server", "run_id": "ad-test-001", "owner": "operator@databricks.com",
        "state": "READY", "azure_subscription_id": SUBSCRIPTION, "azure_region": "westus2"}
    values.update(overrides)
    return ArtifactInspection(**values)  # type: ignore[arg-type]


def validate(target: SafeChangePlan, found: ArtifactInspection) -> None:
    # The check reads only its arguments, so it is exercised without building a whole engine.
    SafeChangeEngine._validate_artifact(None, target, found)  # type: ignore[arg-type]


def test_an_azure_artifact_in_the_scopes_subscription_and_region_is_owned() -> None:
    validate(plan(azure_scope()), artifact())


def test_an_azure_artifact_in_another_subscription_or_region_is_refused() -> None:
    with pytest.raises(UnsafeCleanupError, match="Azure subscription"):
        validate(plan(azure_scope()),
                 artifact(azure_subscription_id="99999999-1111-2222-3333-444444444444"))
    with pytest.raises(UnsafeCleanupError, match="Azure region"):
        validate(plan(azure_scope()), artifact(azure_region="eastus2"))


def test_azure_copy_names_are_valid_flexible_server_names() -> None:
    # Azure server names: 3-63 characters, lowercase letters, digits and hyphens.
    import re

    from server.recovery import deterministic_recovery_artifact_id
    from server.safe_change import deterministic_artifact_id

    # The suite's usual placeholder: the repository refuses anything shaped like a real install's
    # run ID, made up or not, and the longest real ones are only a few characters longer.
    run, azure = "ad-test-001-padded-to-real-length", SafeChangeProvider.AZURE_POSTGRES
    for name in (deterministic_artifact_id(run, azure),
                 deterministic_recovery_artifact_id(run, azure)):
        assert re.fullmatch(r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]", name), name


def test_an_azure_plan_never_accepts_an_artifact_proved_only_by_aws_fields() -> None:
    with pytest.raises(UnsafeCleanupError, match="Azure subscription, Azure region"):
        validate(plan(azure_scope()), artifact(azure_subscription_id=None, azure_region=None,
                                               aws_account_id="123456789012",
                                               aws_region="us-west-2"))
