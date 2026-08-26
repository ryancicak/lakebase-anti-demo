from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

import boto3
from botocore.exceptions import BotoCoreError, ClientError

AwsAuthMode = Literal["profile", "environment"]

#: Deliberately no default profile. This used to name one specific internal SSO
#: profile, which is unusable by anyone outside the organisation that issues it
#: and produced a wrong-looking botocore error rather than an explanation. There
#: is nothing to seal here that is not already sealed: `provision` records what
#: `select_setup_auth` resolved as `manifest.aws.profile`, and every runtime
#: caller reads that seal through `validate_runtime_auth`, where the sealed value
#: outranks the environment. So the only fix needed was to stop inventing an
#: answer at the one place that resolves it -- a first provision -- and to say
#: what is missing instead.
_MISSING_PROFILE = (
    "No AWS credential source is configured. Either pass '--aws-profile NAME' "
    "(or export AWS_PROFILE=NAME) for a named profile, or export "
    "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to use ambient keys. Run "
    "'aws configure list-profiles' to see the profiles this machine has. This "
    "is only ever asked on a first provision; afterwards the manifest seals the "
    "answer."
)

_AMBIENT_CREDENTIAL_NAMES = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
)
_CONFLICTING_CREDENTIAL_NAMES = (
    *_AMBIENT_CREDENTIAL_NAMES,
    "AWS_SECURITY_TOKEN",
    "AWS_DEFAULT_PROFILE",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_ROLE_ARN",
    "AWS_ROLE_SESSION_NAME",
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
)
_PROFILE_CONFIGURATION_NAMES = (
    "AWS_SHARED_CREDENTIALS_FILE",
    "AWS_CONFIG_FILE",
    "AWS_SDK_LOAD_CONFIG",
)

#: What a Databricks App runtime cannot start without. These come from the flat
#: `aws.resources` block, which is Round 1's legacy mirror, so each one names a
#: Round 1 resource.
#:
#: `RDS_INSTANCE_ID` and `RDS_SECRET_ARN` used to be listed here and are not,
#: because Round 1 stands no RDS instance up: its lane refuses to enter on engine
#: semantics, Terraform provisions none for it, and
#: `manifest._require_round1_legacy_mirror` forbids those fields from naming
#: another round's box. Demanding them made every v7 seal unstartable. Aurora
#: stays required because Round 1's cluster is real and is the lane that competes
#: there. No coverage is lost: a round that races RDS resolves its own instance
#: from `round_environments[...].rds`, and the seal validates that per round.
APP_AWS_BINDINGS = (
    "AWS_REGION",
    "AWS_EXPECTED_ACCOUNT_ID",
    "AURORA_CLUSTER_ID",
    "AURORA_SECRET_ARN",
)


class AwsAuthConfigurationError(RuntimeError):
    pass


@dataclass(frozen=True)
class AwsAuthSelection:
    mode: AwsAuthMode
    profile: str = ""


def _present(environment: Mapping[str, str], name: str) -> bool:
    return bool(environment.get(name, ""))


def _validate_key_shape(environment: Mapping[str, str]) -> bool:
    access_key = _present(environment, "AWS_ACCESS_KEY_ID")
    secret_key = _present(environment, "AWS_SECRET_ACCESS_KEY")
    session_token = _present(environment, "AWS_SESSION_TOKEN")
    if access_key != secret_key:
        raise AwsAuthConfigurationError(
            "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be provided together"
        )
    if session_token and not access_key:
        raise AwsAuthConfigurationError(
            "AWS_SESSION_TOKEN requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"
        )
    return access_key


def select_setup_auth(
    environment: Mapping[str, str],
    requested_profile: str = "",
) -> AwsAuthSelection:
    """Select one trusted setup credential source without retaining any secret."""

    has_keys = _validate_key_shape(environment)
    profile = (
        requested_profile.strip()
        or environment.get("AWS_PROFILE", "").strip()
        or environment.get("AWS_DEFAULT_PROFILE", "").strip()
    )
    if has_keys and profile:
        raise AwsAuthConfigurationError(
            "AWS_PROFILE and ambient AWS access keys cannot be used together"
        )
    if has_keys:
        return AwsAuthSelection(mode="environment")
    if not profile:
        raise AwsAuthConfigurationError(_MISSING_PROFILE)
    return AwsAuthSelection(mode="profile", profile=profile)


def validate_runtime_auth(
    mode: AwsAuthMode,
    profile: str,
    environment: Mapping[str, str],
) -> AwsAuthSelection:
    has_keys = _validate_key_shape(environment)
    ambient_profile = environment.get("AWS_PROFILE", "").strip()
    default_profile = environment.get("AWS_DEFAULT_PROFILE", "").strip()
    if mode == "environment":
        if ambient_profile or default_profile:
            raise AwsAuthConfigurationError(
                "AWS_PROFILE cannot be set when AWS_AUTH_MODE=environment"
            )
        if not has_keys:
            raise AwsAuthConfigurationError(
                "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required in environment mode"
            )
        return AwsAuthSelection(mode=mode)
    if mode != "profile":
        raise AwsAuthConfigurationError("AWS_AUTH_MODE must be profile or environment")
    selected_profile = profile.strip() or ambient_profile or default_profile
    if not selected_profile:
        raise AwsAuthConfigurationError("AWS_PROFILE is required in profile mode")
    if has_keys:
        raise AwsAuthConfigurationError(
            "AWS_PROFILE and ambient AWS access keys cannot be used together"
        )
    return AwsAuthSelection(mode=mode, profile=selected_profile)


def runtime_auth_from_environment(
    environment: Mapping[str, str],
) -> AwsAuthSelection:
    configured_mode = environment.get("AWS_AUTH_MODE", "").strip()
    if configured_mode not in {"", "profile", "environment"}:
        raise AwsAuthConfigurationError("AWS_AUTH_MODE must be profile or environment")
    if configured_mode == "profile":
        mode: AwsAuthMode = "profile"
    elif configured_mode == "environment":
        mode = "environment"
    elif environment.get("AWS_PROFILE", "").strip() or environment.get(
        "AWS_DEFAULT_PROFILE", ""
    ).strip():
        mode = "profile"
    else:
        mode = "environment"
    return validate_runtime_auth(
        mode,
        environment.get("AWS_PROFILE", ""),
        environment,
    )


def session_arguments(
    mode: AwsAuthMode,
    profile: str,
    region: str,
) -> dict[str, str]:
    arguments = {"region_name": region}
    if mode == "profile":
        arguments["profile_name"] = profile
    return arguments


def selected_subprocess_environment(
    base: Mapping[str, str],
    selection: AwsAuthSelection,
    region: str,
) -> dict[str, str]:
    """Return an environment containing only the selected AWS credential source."""

    environment = dict(base)
    for name in _CONFLICTING_CREDENTIAL_NAMES:
        environment.pop(name, None)
    environment.pop("AWS_PROFILE", None)
    if selection.mode == "profile":
        environment["AWS_PROFILE"] = selection.profile
    else:
        for name in _PROFILE_CONFIGURATION_NAMES:
            environment.pop(name, None)
        for name in _AMBIENT_CREDENTIAL_NAMES:
            value = base.get(name, "")
            if value:
                environment[name] = value
    environment["AWS_REGION"] = region
    environment["AWS_DEFAULT_REGION"] = region
    return environment


def validate_app_aws_environment(
    environment: Mapping[str, str],
    *,
    session_factory=boto3.Session,
) -> None:
    """Fail a Databricks App before orchestration if its private AWS binding is unsafe."""

    configured_mode = environment.get("AWS_AUTH_MODE", "").strip()
    if configured_mode and configured_mode != "environment":
        raise AwsAuthConfigurationError(
            "Databricks App runtime requires AWS_AUTH_MODE=environment"
        )
    auth = validate_runtime_auth("environment", "", environment)
    missing = [name for name in APP_AWS_BINDINGS if not environment.get(name, "")]
    if missing:
        raise AwsAuthConfigurationError(
            "Missing Databricks App AWS binding: " + ", ".join(missing)
        )
    expected_account = environment["AWS_EXPECTED_ACCOUNT_ID"]
    if len(expected_account) != 12 or not expected_account.isdigit():
        raise AwsAuthConfigurationError("AWS_EXPECTED_ACCOUNT_ID must be exactly 12 digits")
    try:
        session = session_factory(
            **session_arguments(auth.mode, auth.profile, environment["AWS_REGION"])
        )
        identity = session.client("sts").get_caller_identity()
    except (BotoCoreError, ClientError) as exc:
        raise AwsAuthConfigurationError(
            "Databricks App AWS credentials failed STS validation"
        ) from exc
    actual_account = str(identity.get("Account") or "")
    if actual_account != expected_account:
        raise AwsAuthConfigurationError(
            f"AWS credentials resolved to account {actual_account or 'UNKNOWN'}, "
            f"expected {expected_account}"
        )
