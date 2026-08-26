from __future__ import annotations

import asyncio
import base64
import gzip
import hashlib
import io
import json
import logging
import re
import secrets
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, Protocol
from urllib.parse import unquote
from uuid import uuid4

import boto3

from .connection_spike import (
    MAX_LAUNCH_SKEW_MS,
    AttemptObservation,
    AttemptProof,
    AttemptStatus,
    ConnectionSpikeArm,
    ConnectionSpikeContract,
    ConnectionSpikeRunResult,
    ConnectionSpikeWitness,
    PublicSetupEvidence,
    RetainedClientWitness,
    SetupLaneObservation,
    SetupLaneStatus,
    SetupPhaseArm,
    SetupStopGateEvidence,
    SharedBarrier,
    arm_setup_phase,
    build_schedule,
    compare_lanes,
    finalize_lane,
)
from .connection_spike_journal import (
    ROUND5_CREATION_JOURNAL_TABLE,
    CreationJournalStore,
    CreationScope,
    FenceGuard,
    JournalEvent,
    JournalReceipt,
    LifecycleState,
    ResourceAdapter,
    ResourceObservation,
    ResourceSpec,
    Round5CreationCoordinator,
)
from .coordination import COORDINATION_TABLE, RING_KEY, validate_ring_key
from .manifest import DemoManifest, load_manifest
from .models import RoundId
from .safe_change import DEFAULT_CANCEL_TEARDOWN_SECONDS, abandon_on_cancel

logger = logging.getLogger(__name__)

RUNNER_PROTOCOL = "connection-spike-v1"
SETUP_RUNNER_PROTOCOL = "connection-spike-setup-v1"
#: The line the runner prints when it refuses, and the shape of the token after
#: it. `runner/connection_spike_runner.py` raises `RunnerContractError` with a
#: fixed snake_case word -- `baseline_auth_hash_invalid`, `setup_lane_invalid`
#: and so on -- chosen by this repository, naming no host, ARN or credential.
#: That makes it the one part of the runner's output that may be repeated.
_RUNNER_ERROR_PREFIX = "RUNNER_ERROR:"
_RUNNER_ERROR_CODE = re.compile(r"[a-z0-9_]{1,64}")
SSM_TIMEOUT_SECONDS = 120.0
#: How long a cancelled setup command is given to confirm it has settled.
#:
#: Ten seconds could not have worked, and a live towel thrown during Round 5
#: setup is what proved it. Settlement is not one call. `CancelCommand` is
#: best-effort and returns before the runner has heard anything; the SSM agent
#: then has to poll the service for the cancellation, deliver SIGTERM, let the
#: runner unwind a setup that awaits its own in-flight worker threads, print
#: `SETUP_SETTLED` and `RUNNER_FLOCK_RELEASED`, and exit -- and only then does
#: the agent report a terminal status and the captured stdout back to
#: `GetCommandInvocation`, which is the only thing this process can read.
#:
#: AWS states a floor for the first step of that chain and it is 30 seconds:
#: botocore models `SendCommand`'s `TimeoutSeconds` with `min=30`, which is the
#: platform saying that under half a minute you may not assume the agent has
#: even picked a command up. `lifecycle.ROUND5_SSM_COMMAND_TIMEOUT_SECONDS`
#: records the same floor for the same reason. A ten-second window therefore
#: expired inside the one step it had no influence over, deterministically,
#: which is why thirty-odd automatic retries all failed at the identical point.
#:
#: Forty-five is that floor plus room for the runner's unwind and for status
#: propagation, and it stays well inside the 120-second boundary the command
#: itself is given -- past which SSM ends the command regardless and there is
#: nothing left to settle. Nothing fatal hangs on it any more: `_settle_commands`
#: treats expiry as a reportable tidy-up failure, never as a reason to skip the
#: deletion that follows.
SETTLEMENT_TIMEOUT_SECONDS = 45.0
SETUP_DEADLINE_SECONDS = 30 * 60.0
PROXY_DELETION_TIMEOUT_SECONDS = 10 * 60.0
RUNNER_PATH = "/opt/lakebase-anti-demo/round5/run_connection_spike.sh"
SETUP_RUNNER_PATH = RUNNER_PATH
TRUST_BUNDLE_PATH = "/opt/lakebase-anti-demo/round5/round5-ca.pem"
RUNNER_ASSETS = (
    "connection_spike_runner.py",
    "run_connection_spike.sh",
    "requirements-round5.txt",
)
#: How long the hop into the sealed runtime role asks for. One hour rather than
#: the role's twelve-hour ceiling because this session is itself the *source* of
#: a second `sts:AssumeRole`, and AWS caps a role-chained session at one hour
#: regardless of what either role's MaxSessionDuration says. Asking for more is
#: not a longer session, it is a `ValidationError` when the caller happens to be
#: the operator's own Identity Center role -- which is exactly the path this
#: whole mechanism exists to admit.
RUNTIME_ROLE_SESSION_SECONDS = 3600

_RUN_ID = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,95}$")
_LANE_ID = re.compile(r"^[a-z][a-z0-9_-]{0,31}$")
_INSTANCE_ID = re.compile(r"^i-[0-9a-f]{8,17}$")
_ROLE_ARN = re.compile(
    r"^arn:(?P<partition>aws(?:-us-gov|-cn)?):iam::(?P<account>[0-9]{12}):"
    r"role/(?P<name>[A-Za-z0-9+=,.@_/-]{1,512})$"
)
_POLICY_ARN = re.compile(
    r"^arn:(?:aws(?:-us-gov|-cn)?):iam::(?P<account>[0-9]{12}):policy/"
    r"[A-Za-z0-9+=,.@_/-]{1,512}$"
)
_SECRET_ARN = re.compile(
    r"^arn:(?:aws(?:-us-gov|-cn)?):secretsmanager:[a-z0-9-]+:[0-9]{12}:secret:.+$"
)
_TERMINAL = {"Success", "Cancelled", "Failed", "TimedOut"}
_COMPETITOR_IDS = {"rds_postgres", "aurora_serverless_v2"}


def _proxy_target_set_matches(
    competitor_id: str,
    target_id: str,
    resource_id: str,
    targets: Sequence[Mapping[str, object]],
    *,
    require_available: bool = False,
) -> bool:
    if not targets:
        return False
    if competitor_id == "rds_postgres":
        bound = (
            len(targets) == 1
            and targets[0].get("Type") == "RDS_INSTANCE"
            and str(targets[0].get("RdsResourceId") or "") == target_id
        )
    else:
        instances = [target for target in targets if target.get("Type") == "RDS_INSTANCE"]
        clusters = [target for target in targets if target.get("Type") == "TRACKED_CLUSTER"]
        bound = (
            len(instances) >= 1
            and len(clusters) == 1
            and len(instances) + len(clusters) == len(targets)
            and all(
                str(target.get("TrackedClusterId") or "") == target_id
                for target in instances
            )
            and str(clusters[0].get("RdsResourceId") or "") == target_id
        )
    if not bound or not require_available:
        return bound
    routable = [target for target in targets if target.get("Type") == "RDS_INSTANCE"]
    return bool(routable) and any(
        str((target.get("TargetHealth") or {}).get("State") or "").upper() == "AVAILABLE"
        for target in routable
    )


class ConnectionSpikeLiveError(RuntimeError):
    """Base error for the sealed Round 5 execution boundary."""


class ConnectionSpikeLiveConfigurationError(ConnectionSpikeLiveError):
    """A runtime value disagrees with the sealed Round 5 contract."""


class ConnectionSpikeLiveOperationError(ConnectionSpikeLiveError):
    """The remote runner did not produce a complete, sanitized proof."""


class ConnectionSpikeCleanupError(ConnectionSpikeLiveOperationError):
    """The exact command did not prove cleanup and flock release."""


def _runner_error_code(output: str) -> str:
    """The runner's own refusal token, or empty when it did not print one.

    Matched against `_RUNNER_ERROR_CODE` rather than repeated verbatim. The
    runner is trusted to choose the word, not to bound it: this output is
    remote text, it reaches a log, and a line that merely *starts* with the
    prefix would otherwise carry whatever followed it. Anything that is not one
    short lowercase identifier is discarded, which fails back to exactly the
    sentence this function was added to improve rather than to something worse.
    """

    for line in output.splitlines():
        if not line.startswith(_RUNNER_ERROR_PREFIX):
            continue
        code = line[len(_RUNNER_ERROR_PREFIX) :].strip()
        if _RUNNER_ERROR_CODE.fullmatch(code):
            return code
    return ""


def _control_role_source_session(
    session_factory: Any,
    *,
    region: str,
    expected_account_id: str,
    runtime_role_arn: str,
    session_name: str,
) -> Any:
    """The session the Round 5 control role is assumed *from*.

    Without a sealed runtime role this is the ambient credential chain, exactly
    as it has always been, and every installation sealed before the runtime role
    existed takes that branch.

    With one, it is the ambient chain hopped once through the runtime role. That
    hop is the entire point of the runtime role: the control role's trust policy
    names one principal, and the two callers that must reach it -- the operator's
    Identity Center role and the deployed app's IAM user -- are not that
    principal and cannot both be. They are both trusted to *become* it.

    The returned session is verified to actually be the sealed role before it is
    handed back, on the same reasoning as the control-role assume it feeds: an
    `sts:AssumeRole` that returns something other than what was asked for is a
    fault worth naming here rather than three calls later as a denial.
    """

    ambient = session_factory(region_name=region)
    if not runtime_role_arn:
        return ambient
    role = _ROLE_ARN.fullmatch(runtime_role_arn)
    if role is None or role.group("account") != expected_account_id:
        raise ConnectionSpikeLiveConfigurationError(
            "Round 5 runtime role is not an IAM role ARN in the sealed account"
        )
    response = ambient.client("sts", region_name=region).assume_role(
        RoleArn=runtime_role_arn,
        RoleSessionName=session_name[:64],
        DurationSeconds=RUNTIME_ROLE_SESSION_SECONDS,
    )
    credentials = response.get("Credentials") or {}
    assumed_arn = str((response.get("AssumedRoleUser") or {}).get("Arn") or "")
    expected_prefix = (
        f"arn:{role.group('partition')}:sts::{expected_account_id}:"
        f"assumed-role/{role.group('name').rsplit('/', 1)[-1]}/"
    )
    if not assumed_arn.startswith(expected_prefix) or any(
        not credentials.get(key)
        for key in ("AccessKeyId", "SecretAccessKey", "SessionToken", "Expiration")
    ):
        raise ConnectionSpikeLiveConfigurationError(
            "STS did not return the sealed Round 5 runtime role"
        )
    return session_factory(
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
        region_name=region,
    )


@dataclass(frozen=True)
class ConnectionSpikeTarget:
    lane_id: str
    secret_arn: str = field(repr=False)
    endpoint_host: str
    credential_host: str
    competitor_id: str = ""
    competitor_target_id: str = ""
    competitor_resource_id: str = ""
    rds_proxy_name: str = ""
    rds_proxy_arn: str = ""
    rds_proxy_role_arn: str = ""
    rds_proxy_max_connections_percent: int = 0
    rds_proxy_borrow_timeout_seconds: int = 0
    database_user: str = ""
    credential_sha256: str = ""

    def __post_init__(self) -> None:
        if _LANE_ID.fullmatch(self.lane_id) is None:
            raise ConnectionSpikeLiveConfigurationError("Round 5 lane ID is invalid")
        if self.secret_arn and _SECRET_ARN.fullmatch(self.secret_arn) is None:
            raise ConnectionSpikeLiveConfigurationError(
                f"Round 5 {self.lane_id} secret binding is not an ARN"
            )
        if not self.secret_arn and self.lane_id != "lakebase":
            raise ConnectionSpikeLiveConfigurationError(
                f"Round 5 {self.lane_id} secret binding is required"
            )
        if self.credential_sha256 and re.fullmatch(r"[0-9a-f]{64}", self.credential_sha256) is None:
            raise ConnectionSpikeLiveConfigurationError(
                f"Round 5 {self.lane_id} credential digest is invalid"
            )
        if not self.endpoint_host or not self.credential_host:
            raise ConnectionSpikeLiveConfigurationError(
                f"Round 5 {self.lane_id} scored and direct endpoint bindings are required"
            )
        source_bindings = (
            bool(self.competitor_id),
            bool(self.competitor_target_id),
            bool(self.competitor_resource_id),
        )
        if len(set(source_bindings)) != 1 or (
            self.competitor_id and self.competitor_id not in _COMPETITOR_IDS
        ):
            raise ConnectionSpikeLiveConfigurationError(
                f"Round 5 {self.lane_id} competitor source bindings must be sealed together"
            )
        if (
            len(
                {
                    bool(self.rds_proxy_name),
                    bool(self.rds_proxy_arn),
                    bool(self.rds_proxy_role_arn),
                    bool(self.rds_proxy_max_connections_percent),
                    bool(self.rds_proxy_borrow_timeout_seconds),
                    bool(self.database_user),
                }
            )
            != 1
        ):
            raise ConnectionSpikeLiveConfigurationError(
                f"Round 5 {self.lane_id} RDS Proxy bindings must be sealed together"
            )
        if self.rds_proxy_name and not self.competitor_id:
            raise ConnectionSpikeLiveConfigurationError(
                f"Round 5 {self.lane_id} RDS Proxy source binding is required"
            )

    def runner_value(self) -> dict[str, str]:
        # Resource identifiers are safe control-plane references. Credentials are
        # resolved only by the isolated runner from its own instance role.
        return {
            "lane_id": self.lane_id,
            "secret_arn": self.secret_arn,
            "endpoint_host": self.endpoint_host,
            "credential_host": self.credential_host,
        }


@dataclass(frozen=True)
class ConnectionSpikeLiveConfig:
    region: str
    expected_account_id: str
    execution_role_arn: str
    runner_instance_id: str
    runner_instance_profile_arn: str
    runner_subnet_id: str
    runner_security_group_id: str
    targets: tuple[ConnectionSpikeTarget, ...]
    runner_instance_type: str = "m6i.large"
    ssm_document_name: str = "AWS-RunShellScript"
    runner_path: str = RUNNER_PATH
    runner_harness_sha256: str = ""
    trust_bundle_path: str = TRUST_BUNDLE_PATH
    trust_bundle_sha256: str = ""
    contract_sha256: str = ""
    command_timeout_seconds: float = SSM_TIMEOUT_SECONDS
    settlement_timeout_seconds: float = SETTLEMENT_TIMEOUT_SECONDS
    poll_interval_seconds: float = 0.5
    role_session_prefix: str = "lakebase-anti-demo-r5"
    #: `manifest.aws.runtime_role_arn`, or empty on every installation sealed
    #: before the runtime role existed. Empty means "assume the control role
    #: directly from the ambient credentials", which is the only behaviour those
    #: installations have ever had.
    runtime_role_arn: str = ""

    def __post_init__(self) -> None:
        role = _ROLE_ARN.fullmatch(self.execution_role_arn)
        if role is None:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 execution role is not an IAM role ARN"
            )
        if (
            len(self.expected_account_id) != 12
            or not self.expected_account_id.isdigit()
            or role.group("account") != self.expected_account_id
        ):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 execution role account does not match the sealed account"
            )
        if _INSTANCE_ID.fullmatch(self.runner_instance_id) is None:
            raise ConnectionSpikeLiveConfigurationError("Round 5 runner instance ID is invalid")
        if (
            not self.runner_instance_profile_arn
            or not self.runner_subnet_id
            or not self.runner_security_group_id
            or self.runner_instance_type != "m6i.large"
        ):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 runner topology bindings are incomplete"
            )
        if not self.region.strip() or not self.ssm_document_name.strip():
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 region and SSM document are required"
            )
        if self.runner_path != RUNNER_PATH:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 runner path does not match the immutable harness contract"
            )
        if self.command_timeout_seconds != SSM_TIMEOUT_SECONDS:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 SSM execution timeout must remain exactly 120 seconds"
            )
        if self.settlement_timeout_seconds != SETTLEMENT_TIMEOUT_SECONDS:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 cleanup settlement must remain exactly "
                f"{SETTLEMENT_TIMEOUT_SECONDS:.0f} seconds"
            )
        if not 0 < self.poll_interval_seconds <= 2:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 polling interval must be greater than zero and at most two seconds"
            )
        if len(self.targets) != 2 or len({target.lane_id for target in self.targets}) != 2:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 requires two distinct, sealed target lanes"
            )
        if self.runner_harness_sha256 and not re.fullmatch(
            r"[0-9a-f]{64}", self.runner_harness_sha256
        ):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 runner harness digest must be lowercase SHA-256"
            )
        if self.trust_bundle_path != TRUST_BUNDLE_PATH or not re.fullmatch(
            r"[0-9a-f]{64}", self.trust_bundle_sha256
        ):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 trust bundle path and SHA-256 must match the sealed contract"
            )
        if self.contract_sha256 and self.contract_sha256 != ConnectionSpikeContract().sha256:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 core contract digest does not match the sealed manifest"
            )


@dataclass(frozen=True)
class ConnectionSpikeLiveProgress:
    phase: str
    status: str
    occurred_at: datetime


@dataclass(frozen=True)
class ConnectionSpikeSetupProgress:
    lane_id: str
    phase: str
    status: str
    occurred_at: datetime
    setup_elapsed_ms: float | None = None


@dataclass(frozen=True)
class ConnectionSpikeSetupConfig:
    """Secret-free inputs for one timed, per-bout Proxy setup."""

    region: str
    expected_account_id: str
    baseline_control_role_arn: str
    runner_instance_id: str
    vpc_id: str
    proxy_subnet_ids: tuple[str, ...]
    lakebase_direct_host: str
    lakebase_pooled_host: str
    competitor_id: Literal["rds_postgres", "aurora_serverless_v2"]
    competitor_target_id: str
    competitor_resource_id: str
    competitor_direct_host: str
    competitor_security_group_id: str
    runner_security_group_id: str
    proxy_service_role_arn: str
    proxy_service_policy_name: str
    aurora_proxy_secret_arn: str
    rds_proxy_secret_arn: str
    deterministic_name_prefix: str
    ownership_tags: tuple[tuple[str, str], ...]
    trust_bundle_path: str
    trust_bundle_sha256: str
    runner_public_key_sha256: str
    baseline_sha256: str
    lakebase_credential_sha256: str
    competitor_credential_sha256: str
    # Migration-only bindings retained for cleanup of pre-simplification journals.
    runner_role_arn: str = ""
    proxy_role_permissions_boundary_arn: str = ""
    secret_name_prefix: str = ""
    competitor_master_secret_arn: str = ""
    runner_path: str = SETUP_RUNNER_PATH
    ssm_document_name: str = "AWS-RunShellScript"
    native_role: str = "anti_demo_burst"
    database_name: str = "anti_demo"
    proxy_max_connections_percent: int = 90
    proxy_borrow_timeout_seconds: int = 120
    command_timeout_seconds: float = SSM_TIMEOUT_SECONDS
    settlement_timeout_seconds: float = SETTLEMENT_TIMEOUT_SECONDS
    deadline_seconds: float = SETUP_DEADLINE_SECONDS
    poll_interval_seconds: float = 0.5
    role_session_prefix: str = "lakebase-anti-demo-r5-setup"
    #: See `ConnectionSpikeLiveConfig.runtime_role_arn`; the same seal, read for
    #: the timed setup half so both halves reach the control role the same way.
    runtime_role_arn: str = ""

    def __post_init__(self) -> None:
        role = _ROLE_ARN.fullmatch(self.baseline_control_role_arn)
        if (
            role is None
            or role.group("account") != self.expected_account_id
            or len(self.expected_account_id) != 12
            or not self.expected_account_id.isdigit()
        ):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 baseline control role is not sealed to the expected account"
            )
        if _INSTANCE_ID.fullmatch(self.runner_instance_id) is None:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 setup runner instance ID is invalid"
            )
        if (
            not self.vpc_id
            or not self.proxy_subnet_ids
            or len(set(self.proxy_subnet_ids)) != len(self.proxy_subnet_ids)
            or not self.runner_security_group_id
            or self.competitor_id not in _COMPETITOR_IDS
            or not self.competitor_target_id
            or not self.competitor_resource_id
            or not self.competitor_direct_host
            or not self.competitor_security_group_id
            or not self.lakebase_direct_host
            or not self.lakebase_pooled_host
        ):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 setup baseline topology is incomplete"
            )
        proxy_role = _ROLE_ARN.fullmatch(self.proxy_service_role_arn)
        if proxy_role is None or proxy_role.group("account") != self.expected_account_id:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 Proxy service role ARN is invalid"
            )
        if re.fullmatch(r"[\w+=,.@-]{1,128}", self.proxy_service_policy_name) is None:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 Proxy service policy name is invalid"
            )
        for secret_arn in (self.aurora_proxy_secret_arn, self.rds_proxy_secret_arn):
            secret = _SECRET_ARN.fullmatch(secret_arn)
            prefix = (
                f"arn:{proxy_role.group('partition')}:secretsmanager:{self.region}:"
                f"{self.expected_account_id}:secret:"
            )
            if secret is None or not secret_arn.startswith(prefix):
                raise ConnectionSpikeLiveConfigurationError(
                    "Round 5 Proxy secret ARN is outside the sealed account or region"
                )
        if self.aurora_proxy_secret_arn == self.rds_proxy_secret_arn:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 Aurora and RDS Proxy secrets must be distinct"
            )
        if self.runner_role_arn:
            runner_role = _ROLE_ARN.fullmatch(self.runner_role_arn)
            if runner_role is None or runner_role.group("account") != self.expected_account_id:
                raise ConnectionSpikeLiveConfigurationError("Round 5 runner role ARN is invalid")
        if self.proxy_role_permissions_boundary_arn:
            boundary = _POLICY_ARN.fullmatch(self.proxy_role_permissions_boundary_arn)
            if boundary is None or boundary.group("account") != self.expected_account_id:
                raise ConnectionSpikeLiveConfigurationError(
                    "Round 5 legacy Proxy role permissions boundary is invalid"
                )
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{1,47}", self.deterministic_name_prefix):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 deterministic name prefix is invalid"
            )
        if self.secret_name_prefix and not re.fullmatch(
            r"[A-Za-z0-9/_+=.@-]{2,128}", self.secret_name_prefix
        ):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 legacy deterministic secret prefix is invalid"
            )
        tag_keys = [key for key, _ in self.ownership_tags]
        if (
            len(tag_keys) != len(set(tag_keys))
            or any(not key or not value for key, value in self.ownership_tags)
            or any(key.startswith("anti-demo:bout-") for key in tag_keys)
        ):
            raise ConnectionSpikeLiveConfigurationError("Round 5 ownership tags are not canonical")
        digests = (
            self.trust_bundle_sha256,
            self.runner_public_key_sha256,
            self.baseline_sha256,
            self.lakebase_credential_sha256,
            self.competitor_credential_sha256,
        )
        if (
            self.trust_bundle_path != TRUST_BUNDLE_PATH
            or (
                self.competitor_master_secret_arn
                and _SECRET_ARN.fullmatch(self.competitor_master_secret_arn) is None
            )
            or any(re.fullmatch(r"[0-9a-f]{64}", value) is None for value in digests)
        ):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 setup runner digests do not match the sealed contract"
            )
        if (
            self.runner_path != SETUP_RUNNER_PATH
            or self.command_timeout_seconds != SSM_TIMEOUT_SECONDS
            or self.settlement_timeout_seconds != SETTLEMENT_TIMEOUT_SECONDS
            or self.deadline_seconds != SETUP_DEADLINE_SECONDS
            or self.proxy_max_connections_percent != 90
            or self.proxy_borrow_timeout_seconds != 120
            or not 0 < self.poll_interval_seconds <= 2
        ):
            raise ConnectionSpikeLiveConfigurationError("Round 5 timed setup constants changed")

    @property
    def proxy_registration(self) -> dict[str, list[str]]:
        key = (
            "DBClusterIdentifiers"
            if self.competitor_id == "aurora_serverless_v2"
            else "DBInstanceIdentifiers"
        )
        return {key: [self.competitor_target_id]}

    @property
    def competitor_credential_id(self) -> Literal["rds", "aurora"]:
        return "aurora" if self.competitor_id == "aurora_serverless_v2" else "rds"

    @property
    def proxy_secret_arn(self) -> str:
        return (
            self.aurora_proxy_secret_arn
            if self.competitor_id == "aurora_serverless_v2"
            else self.rds_proxy_secret_arn
        )


@dataclass(frozen=True)
class ConnectionSpikeSetupNames:
    token: str
    proxy_security_group_name: str
    proxy_name: str
    # Deterministic legacy names are not part of new setup, but allow old
    # journals to be inspected and deleted after an upgrade.
    secret_name: str = ""
    proxy_role_name: str = ""
    proxy_policy_name: str = ""
    runner_policy_name: str = ""


@dataclass(frozen=True)
class ConnectionSpikeSetupLaneStop:
    lane_id: str
    launched_ns: int
    stopped_ns: int
    credential_sha256: str
    endpoint_host: str
    secret_arn: str = field(default="", repr=False)

    @property
    def elapsed_ms(self) -> float:
        return (self.stopped_ns - self.launched_ns) / 1_000_000


@dataclass(frozen=True)
class ConnectionSpikeSetupResult:
    bout_id: str
    arm: SetupPhaseArm
    observations: tuple[SetupLaneObservation, SetupLaneObservation]
    names: ConnectionSpikeSetupNames
    lakebase: ConnectionSpikeSetupLaneStop
    competitor: ConnectionSpikeSetupLaneStop

    @property
    def t0_ns(self) -> int:
        return self.arm.t0_ns

    @property
    def deadline_ns(self) -> int:
        return self.arm.deadline_ns

    @property
    def launch_skew_ms(self) -> float:
        return abs(self.lakebase.launched_ns - self.competitor.launched_ns) / 1_000_000


class ConnectionSpikeSetupJournal(Protocol):
    async def begin_setup(
        self,
        bout_id: str,
        t0_ns: int,
        deadline_ns: int,
        names: ConnectionSpikeSetupNames,
    ) -> None: ...

    async def record_resource(
        self,
        bout_id: str,
        lane_id: str,
        kind: str,
        resource_id: str,
    ) -> None: ...

    async def record_lane_stop(
        self,
        bout_id: str,
        stop: ConnectionSpikeSetupLaneStop,
    ) -> None: ...

    async def finish_setup(self, bout_id: str, status: str) -> None: ...


class _NullSetupJournal:
    async def begin_setup(self, *args: object) -> None:
        return None

    async def record_resource(self, *args: object) -> None:
        return None

    async def record_lane_stop(self, *args: object) -> None:
        return None

    async def finish_setup(self, *args: object) -> None:
        return None


@dataclass(frozen=True)
class _SetupAwsClients:
    ssm: Any
    rds: Any
    ec2: Any
    iam: Any
    secretsmanager: Any


@dataclass
class _SetupResources:
    names: ConnectionSpikeSetupNames
    secret_arn: str = ""
    proxy_role_arn: str = ""
    proxy_security_group_id: str = ""
    rds_security_group_id: str = ""
    proxy_endpoint: str = ""
    security_group_rule_ids: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class _CompetitorSource:
    identifier: str
    resource_id: str
    direct_host: str
    status: str
    vpc_id: str
    security_group_ids: tuple[str, ...]


@dataclass(frozen=True)
class _SetupActiveCommand:
    bout_id: str
    lane_id: str
    action: str
    command_id: str
    ssm: Any


@dataclass(frozen=True)
class _SetupPendingSend:
    """A runner command that has been issued but whose identifier is not known.

    ``SendCommand`` runs in a worker thread, so between the request leaving and
    the identifier arriving there is a window in which SSM may already be
    executing a command this process cannot name. A cancellation landing in that
    window is not exotic -- it is the ordinary shape of Ctrl-C during setup --
    and without this record the ORPHAN RISK line omitted the command entirely,
    which is the difference between "nothing was in flight" and "I do not know".
    """

    bout_id: str
    lane_id: str
    action: str


SetupProgressCallback = Callable[[ConnectionSpikeSetupProgress], Awaitable[None]]
FreshLakebaseHost = Callable[[], Awaitable[str]]
JournalSqlRunner = Callable[[Callable[[Any], Awaitable[Any]]], Awaitable[Any]]


class LakebaseCreationJournalStore:
    """Durable append-only journal using the coordination DB connection path."""

    def __init__(
        self,
        run: JournalSqlRunner,
        *,
        authority_ring_key: str = RING_KEY,
    ) -> None:
        try:
            authority_ring_key = validate_ring_key(authority_ring_key)
        except ValueError as exc:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 journal authority ring key is invalid"
            ) from exc
        self._run = run
        self._authority_ring_key = authority_ring_key

    async def commit(
        self,
        event: JournalEvent,
        *,
        authority_scope: CreationScope | None = None,
    ) -> None:
        authority = authority_scope or CreationScope(
            event.bout_id,
            event.fencing_token,
            event.runtime_seal_sha256,
        )

        async def insert(cursor: Any) -> None:
            await cursor.execute(
                f"""
                INSERT INTO {ROUND5_CREATION_JOURNAL_TABLE} (
                    bout_id, fencing_token, ordinal, resource_kind,
                    deterministic_name, client_token, provider_id, lifecycle_state,
                    metadata, runtime_seal_sha256, intent_at, occurred_at,
                    completed_at, error
                )
                SELECT
                    %s, %s, %s, %s, %s, %s, %s, %s,
                    %s::jsonb, %s, %s, %s, %s, %s
                FROM {COORDINATION_TABLE}
                WHERE ring_key = %s
                  AND session_id = %s
                  AND fencing_token = %s
                  AND lease_id IS NOT NULL
                  AND expires_at > clock_timestamp()
                RETURNING event_id
                """,
                (
                    event.bout_id,
                    event.fencing_token,
                    event.ordinal,
                    event.resource_kind,
                    event.deterministic_name,
                    event.client_token,
                    event.provider_id,
                    event.lifecycle_state.value,
                    json.dumps(event.metadata, sort_keys=True, separators=(",", ":")),
                    event.runtime_seal_sha256,
                    event.intent_at,
                    event.occurred_at,
                    event.completed_at,
                    event.error,
                    self._authority_ring_key,
                    authority.bout_id,
                    authority.fencing_token,
                ),
            )
            if await cursor.fetchone() is None:
                raise ConnectionSpikeLiveOperationError(
                    "Round 5 journal write lost its active lease fence"
                )

        await self._run(insert)

    @staticmethod
    def _event_from_row(row: Sequence[object]) -> JournalEvent:
        return JournalEvent(
            bout_id=str(row[0]),
            fencing_token=int(row[1]),
            ordinal=int(row[2]),
            resource_kind=str(row[3]),
            deterministic_name=(str(row[4]) if row[4] is not None else None),
            client_token=(str(row[5]) if row[5] is not None else None),
            provider_id=(str(row[6]) if row[6] is not None else None),
            lifecycle_state=LifecycleState(str(row[7])),
            metadata=(json.loads(row[8]) if isinstance(row[8], str) else row[8]),
            runtime_seal_sha256=str(row[9]),
            intent_at=row[10],
            occurred_at=row[11],
            completed_at=row[12],
            error=(str(row[13]) if row[13] is not None else None),
        )

    async def events(self, scope: CreationScope) -> Sequence[JournalEvent]:
        async def select(cursor: Any) -> Sequence[JournalEvent]:
            await cursor.execute(
                f"""
                SELECT bout_id, fencing_token, ordinal, resource_kind,
                       deterministic_name, client_token, provider_id, lifecycle_state,
                       metadata, runtime_seal_sha256, intent_at, occurred_at,
                       completed_at, error
                FROM {ROUND5_CREATION_JOURNAL_TABLE}
                WHERE bout_id = %s AND fencing_token = %s
                ORDER BY event_id
                """,
                (scope.bout_id, scope.fencing_token),
            )
            rows = await cursor.fetchall()
            return tuple(self._event_from_row(row) for row in rows)

        return await self._run(select)

    async def scopes(self, bout_id: str) -> Sequence[CreationScope]:
        async def select(cursor: Any) -> Sequence[CreationScope]:
            await cursor.execute(
                f"""
                SELECT bout_id, fencing_token, runtime_seal_sha256
                FROM {ROUND5_CREATION_JOURNAL_TABLE}
                WHERE bout_id = %s
                GROUP BY bout_id, fencing_token, runtime_seal_sha256
                ORDER BY fencing_token DESC
                """,
                (bout_id,),
            )
            return tuple(
                CreationScope(str(row[0]), int(row[1]), str(row[2]))
                for row in await cursor.fetchall()
            )

        return await self._run(select)

    async def unresolved_bout_ids(self) -> Sequence[str]:
        """Return only bouts with journal-authorized resources not yet deleted."""

        async def select(cursor: Any) -> Sequence[str]:
            await cursor.execute(
                f"""
                SELECT DISTINCT bout_id
                FROM (
                    SELECT bout_id, fencing_token, ordinal, lifecycle_state,
                           row_number() OVER (
                               PARTITION BY bout_id, fencing_token, ordinal
                               ORDER BY event_id DESC
                           ) AS newest
                    FROM {ROUND5_CREATION_JOURNAL_TABLE}
                ) AS journal
                WHERE newest = 1 AND lifecycle_state <> 'deleted'
                ORDER BY bout_id
                """
            )
            return tuple(str(row[0]) for row in await cursor.fetchall())

        return await self._run(select)


@dataclass(frozen=True)
class _AwsClients:
    ssm: Any
    rds: Any
    cloudwatch: Any
    ec2: Any


@dataclass(frozen=True)
class _ActiveCommand:
    run_id: str
    command_id: str
    clients: _AwsClients


@dataclass(frozen=True)
class _PendingCommand:
    run_id: str
    send_task: asyncio.Task[str]
    clients: _AwsClients


class SessionFactory(Protocol):
    def __call__(self, **kwargs: Any) -> Any: ...


Sleeper = Callable[[float], Awaitable[None]]
ProgressCallback = Callable[[ConnectionSpikeLiveProgress], Awaitable[None]]


def runner_harness_sha256(root: Path | None = None) -> str:
    asset_root = root or Path(__file__).resolve().parents[1] / "runner"
    digest = hashlib.sha256()
    for name in RUNNER_ASSETS:
        path = asset_root / name
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def connection_spike_config_sha256(config: ConnectionSpikeLiveConfig) -> str:
    value = {
        "region": config.region,
        "expected_account_id": config.expected_account_id,
        "execution_role_arn": config.execution_role_arn,
        "runner_instance_id": config.runner_instance_id,
        "runner_instance_profile_arn": config.runner_instance_profile_arn,
        "runner_subnet_id": config.runner_subnet_id,
        "runner_security_group_id": config.runner_security_group_id,
        "runner_instance_type": config.runner_instance_type,
        "ssm_document_name": config.ssm_document_name,
        "runner_path": config.runner_path,
        "runner_harness_sha256": config.runner_harness_sha256,
        "trust_bundle_path": config.trust_bundle_path,
        "trust_bundle_sha256": config.trust_bundle_sha256,
        "targets": [
            {
                "lane_id": target.lane_id,
                "secret_arn": target.secret_arn,
                "endpoint_host": target.endpoint_host,
                "credential_host": target.credential_host,
                "competitor_id": target.competitor_id,
                "competitor_target_id": target.competitor_target_id,
                "competitor_resource_id": target.competitor_resource_id,
                "rds_proxy_name": target.rds_proxy_name,
                "rds_proxy_arn": target.rds_proxy_arn,
                "rds_proxy_role_arn": target.rds_proxy_role_arn,
                "rds_proxy_max_connections_percent": (target.rds_proxy_max_connections_percent),
                "rds_proxy_borrow_timeout_seconds": (target.rds_proxy_borrow_timeout_seconds),
                "database_user": target.database_user,
            }
            for target in config.targets
        ],
    }
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class _SetupResourceAdapter:
    def __init__(
        self,
        create: Callable[[ResourceSpec], Awaitable[ResourceObservation]],
        inspect: Callable[[ResourceSpec, str | None], Awaitable[ResourceObservation | None]],
        delete: Callable[[ResourceObservation], Awaitable[None]],
    ) -> None:
        self._create = create
        self._inspect = inspect
        self._delete = delete

    async def create(self, spec: ResourceSpec) -> ResourceObservation:
        return await self._create(spec)

    async def inspect(
        self, spec: ResourceSpec, *, provider_id: str | None
    ) -> ResourceObservation | None:
        return await self._inspect(spec, provider_id)

    async def delete(self, resource: ResourceObservation) -> None:
        await self._delete(resource)


class LiveConnectionSpikeSetupOrchestrator:
    """Two setup workflows with a shared T0; never dispatches the scored burst."""

    def __init__(
        self,
        config: ConnectionSpikeSetupConfig,
        *,
        journal: CreationJournalStore,
        fence: FenceGuard,
        fresh_lakebase_host: FreshLakebaseHost,
        session_factory: SessionFactory = boto3.Session,
        sleep: Sleeper = asyncio.sleep,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        cancel_teardown_timeout_seconds: float = DEFAULT_CANCEL_TEARDOWN_SECONDS,
    ) -> None:
        if cancel_teardown_timeout_seconds <= 0:
            raise ValueError("cancel_teardown_timeout_seconds must be positive")
        self.cancel_teardown_timeout_seconds = cancel_teardown_timeout_seconds
        self.config = config
        self._journal = journal
        self._fence = fence
        self._fresh_lakebase_host = fresh_lakebase_host
        self._session_factory = session_factory
        self._sleep = sleep
        self._monotonic_ns = monotonic_ns
        self._lock = asyncio.Lock()
        self._active_commands: dict[str, _SetupActiveCommand] = {}
        self._pending_sends: dict[str, _SetupPendingSend] = {}
        self._coordinators: dict[str, Round5CreationCoordinator] = {}
        self._scopes: dict[str, CreationScope] = {}
        self._receipts: dict[str, JournalReceipt] = {}
        self._results: dict[str, ConnectionSpikeSetupResult] = {}
        self._cleanup_start_lock = asyncio.Lock()
        self._cleanup_tasks: dict[str, asyncio.Task[None]] = {}
        self._proxy_delete_accepted: dict[str, asyncio.Event] = {}

    @staticmethod
    def names_for_bout(
        prefix: str, bout_id: str, secret_prefix: str = "anti-demo-round5"
    ) -> ConnectionSpikeSetupNames:
        LiveConnectionSpikeAdapter._validate_run_id(bout_id)
        token = hashlib.sha256(bout_id.encode()).hexdigest()[:16]
        stem = f"{prefix[:40].rstrip('-')}-{token}"
        return ConnectionSpikeSetupNames(
            token=token,
            secret_name=f"{secret_prefix.rstrip('/')}/{token}",
            proxy_role_name=f"{stem}-role",
            proxy_policy_name=f"{stem}-read",
            runner_policy_name=f"{stem}-runner-secret",
            proxy_security_group_name=f"{stem}-sg",
            proxy_name=f"{stem}-proxy",
        )

    def proxy_name_for_bout(self, bout_id: str) -> str:
        """The RDS Proxy this bout would have created, for naming a leak.

        Exposed so that a caller reporting a cleanup that never converged can
        say *which* resource may still be billing. Derived here rather than at
        the caller on purpose: the name comes out of `names_for_bout` and a
        second copy of that derivation is a second thing to keep in step with
        the resource that actually exists.
        """

        return self.names_for_bout(
            self.config.deterministic_name_prefix,
            bout_id,
            self.config.secret_name_prefix or "anti-demo-round5",
        ).proxy_name

    async def setup(
        self,
        bout_id: str,
        fencing_token: int,
        on_progress: SetupProgressCallback | None = None,
    ) -> ConnectionSpikeSetupResult:
        if self._lock.locked():
            raise ConnectionSpikeLiveOperationError(
                "A Round 5 timed setup is already active in this app replica"
            )
        async with self._lock:
            if bout_id in self._results:
                raise ConnectionSpikeLiveOperationError(
                    "Round 5 setup already completed for this bout"
                )
            names = self.names_for_bout(
                self.config.deterministic_name_prefix,
                bout_id,
                self.config.secret_name_prefix or "anti-demo-round5",
            )
            scope = CreationScope(bout_id, fencing_token, self.config.baseline_sha256)
            clients = await self._assumed_clients(bout_id)
            resources = _SetupResources(
                names,
                secret_arn=self.config.proxy_secret_arn,
                proxy_role_arn=self.config.proxy_service_role_arn,
            )
            coordinator, specs = self._coordinator(scope, clients, resources)
            self._coordinators[bout_id] = coordinator
            self._scopes[bout_id] = scope
            await self._preflight_baseline(scope, clients, coordinator, specs, resources)

            gate = asyncio.Event()
            t0_box: list[int] = []
            lakebase_task = asyncio.create_task(
                self._setup_lakebase(bout_id, clients, gate, t0_box, on_progress)
            )
            competitor_task = asyncio.create_task(
                self._setup_competitor(
                    bout_id,
                    scope,
                    clients,
                    coordinator,
                    specs,
                    resources,
                    gate,
                    t0_box,
                    on_progress,
                )
            )
            t0_ns = self._monotonic_ns()
            arm = arm_setup_phase(("lakebase", "competitor"), t0_ns=t0_ns)
            t0_box.append(t0_ns)
            gate.set()
            try:
                async with asyncio.timeout(self.config.deadline_seconds):
                    # `wait` rather than a bare `gather`, so that a cancellation
                    # is delivered here instead of being queued behind the lanes.
                    # Cancelling a gather cancels its children but leaves the
                    # await parked until each one finishes unwinding, and a lane
                    # inside `_call` unwinds only once the AWS worker thread it
                    # re-awaits returns -- on a wedged endpoint, never. That made
                    # the handler below, and any bound it could apply,
                    # unreachable. `wait` leaves the lanes untouched, so both
                    # handlers still do their own cancelling exactly as before.
                    # The gather that follows only unwraps outcomes: `wait` has
                    # already returned every lane or a failed one, so it cannot
                    # reintroduce the block it was chosen to avoid.
                    await asyncio.wait(
                        (lakebase_task, competitor_task),
                        return_when=asyncio.FIRST_EXCEPTION,
                    )
                    lakebase, competitor = await asyncio.gather(
                        lakebase_task, competitor_task
                    )
                if abs(lakebase.launched_ns - competitor.launched_ns) > 10_000_000:
                    raise ConnectionSpikeLiveOperationError(
                        "Round 5 setup workflows exceeded the 10 ms launch gate"
                    )
                receipt = await coordinator.seal(scope)
                self._receipts[bout_id] = receipt
                observations = (
                    self._setup_observation(lakebase),
                    self._setup_observation(competitor),
                )
                result = ConnectionSpikeSetupResult(
                    bout_id=bout_id,
                    arm=arm,
                    observations=observations,
                    names=names,
                    lakebase=lakebase,
                    competitor=competitor,
                )
                self._results[bout_id] = result
                return result
            except asyncio.CancelledError:
                for task in (lakebase_task, competitor_task):
                    if not task.done():
                        task.cancel()
                # Cancellation must settle the setup runners, but exact provider
                # cleanup continues independently of the cancelled caller.
                #
                # The identifier is a callable so that it is built when the
                # ORPHAN RISK line is written rather than here. At this instant
                # the lanes have only just been told to stop and their
                # `SendCommand` calls may still be in worker threads, so a
                # string computed now can name fewer commands than actually
                # exist -- which is how a report meant to resolve the ambiguity
                # of a cancellation came to be a snapshot of it. Resolving it
                # after the bounded drain spends that budget learning.
                await abandon_on_cancel(
                    lambda: self._abandon_setup(bout_id, (lakebase_task, competitor_task)),
                    identifier=lambda: self._cancelled_setup_identifier(
                        bout_id, specs, resources
                    ),
                    timeout_seconds=self.cancel_teardown_timeout_seconds,
                )
                raise
            except BaseException:
                for task in (lakebase_task, competitor_task):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(lakebase_task, competitor_task, return_exceptions=True)
                await asyncio.shield(self._settle_commands(bout_id))
                await asyncio.shield(coordinator.reconcile_incomplete(scope))
                raise

    async def _abandon_setup(
        self,
        bout_id: str,
        lanes: tuple[asyncio.Task[Any], ...],
    ) -> None:
        """Settle the cancelled lanes and hand the bout to exact cleanup, in order.

        This is the whole of the old cancellation body, moved onto a task of its
        own so that the caller can stop *waiting* for it without stopping *it*.
        Nothing here may be reordered or skipped. The lanes are drained first
        because a lane that is still inside ``_runner_action`` holds an SSM
        command whose runner is connected through the very Proxy that
        :meth:`begin_cleanup` is about to delete, and the runner releases its
        flock only once that command settles. Starting the teardown early would
        turn a delete into a refusal and leave the flock held, which is a worse
        outcome than the wait this method exists to bound.

        The bound belongs outside, not inside, for the same reason: everything
        below reaches AWS through :meth:`_call`, which re-awaits its shielded
        worker thread when cancelled -- deliberately, because a thread that is
        mid-mutation cannot be abandoned safely. An ``asyncio.timeout`` placed
        anywhere in here would therefore not fire until the wedged call
        returned, which is exactly never. Only a caller that abandons its wait
        while this task keeps running can put a ceiling on a cancellation.
        """

        await asyncio.gather(*lanes, return_exceptions=True)
        await self.begin_cleanup(bout_id)

    def _cancelled_setup_identifier(
        self,
        bout_id: str,
        specs: Sequence[ResourceSpec],
        resources: _SetupResources,
    ) -> str:
        """Name every resource a cancelled setup may leave behind.

        The bound is what stops the shutdown hanging; this string is what makes
        stopping it affordable, because an orphan nobody can name is an orphan
        nobody deletes. Deterministic names are read from the same specs the
        coordinator creates and tears down, so the list cannot drift away from
        what actually exists, and provider-assigned identifiers are appended
        whenever setup got far enough to learn them.
        """

        parts = [
            f"{spec.resource_kind} {spec.deterministic_name}"
            for spec in specs
            if spec.deterministic_name
        ]
        parts.append(
            f"proxy target {self.config.competitor_id} {self.config.competitor_target_id}"
        )
        if resources.proxy_security_group_id:
            parts.append(f"observed security group {resources.proxy_security_group_id}")
        if resources.security_group_rule_ids:
            parts.append(
                "observed security group rules "
                + ",".join(resources.security_group_rule_ids)
            )
        if resources.proxy_endpoint:
            parts.append(f"observed proxy endpoint {resources.proxy_endpoint}")
        commands = sorted(
            f"{active.lane_id}:{active.action}={active.command_id}"
            for active in self._active_commands.values()
            if active.bout_id == bout_id
        )
        # Named separately and not merged into the list above, because "a
        # command exists and here is its id" and "a command may exist and its id
        # is unknowable from here" are different instructions to the human who
        # has to clear it: the first is a `cancel-command`, the second is a
        # `list-commands` against the runner over the surrounding minutes.
        unidentified = sorted(
            f"{pending.lane_id}:{pending.action}"
            for pending in self._pending_sends.values()
            if pending.bout_id == bout_id
        )
        if commands:
            parts.append(
                f"in-flight SSM commands on {self.config.runner_instance_id} "
                + ",".join(commands)
            )
        if unidentified:
            parts.append(
                "SSM commands of unknown fate on "
                f"{self.config.runner_instance_id} (SendCommand was in flight, "
                "so a command may exist under an identifier this process never "
                "received) " + ",".join(unidentified)
            )
        return f"Round 5 bout {bout_id} setup [{'; '.join(parts)}]"

    async def begin_cleanup(self, bout_id: str) -> None:
        """Settle setup commands and start exact reverse cleanup once.

        This boundary intentionally returns after cleanup has been started.  Callers
        can wait for the RDS Proxy delete request handoff separately from the much
        slower provider-absence proof.
        """

        starter = asyncio.create_task(
            self._begin_cleanup_once(bout_id),
            name=f"round5-cleanup-start-{bout_id}",
        )
        await asyncio.shield(starter)

    async def _begin_cleanup_once(self, bout_id: str) -> None:
        LiveConnectionSpikeAdapter._validate_run_id(bout_id)
        async with self._cleanup_start_lock:
            if bout_id in self._cleanup_tasks:
                return
            # Awaited for its ordering, never for permission. `_settle_commands`
            # does not raise, so an SSM command that will not confirm can no
            # longer stop the task below from being created -- and that task is
            # the only thing in this process that deletes the RDS Proxy.
            await self._settle_commands(bout_id)
            self._proxy_delete_accepted.setdefault(bout_id, asyncio.Event())
            self._cleanup_tasks[bout_id] = asyncio.create_task(
                self._cleanup_exactly(bout_id),
                name=f"round5-cleanup-{bout_id}",
            )

    async def cleanup(self, bout_id: str) -> None:
        """Start cleanup idempotently and await exact provider absence."""

        await self.begin_cleanup(bout_id)
        await self.wait_for_cleanup_complete(bout_id)

    async def _cleanup_exactly(self, bout_id: str) -> None:
        coordinator = self._coordinators.get(bout_id)
        scope = self._scopes.get(bout_id)
        if coordinator is None or scope is None:
            self._proxy_delete_accepted.setdefault(bout_id, asyncio.Event()).set()
            return
        receipt = self._receipts.get(bout_id)
        report = (
            await coordinator.cleanup(scope, receipt)
            if receipt is not None
            else await coordinator.reconcile_incomplete(scope)
        )
        if not report.complete:
            raise ConnectionSpikeCleanupError(
                "Round 5 per-bout setup cleanup was not ownership-confirmed"
            )
        self._receipts.pop(bout_id, None)
        self._results.pop(bout_id, None)
        self._coordinators.pop(bout_id, None)
        self._scopes.pop(bout_id, None)
        # A successfully completed cleanup with no live Proxy is also a completed
        # handoff (for example, recovery after the Proxy was already absent).
        self._proxy_delete_accepted.setdefault(bout_id, asyncio.Event()).set()

    def proxy_delete_accepted(self, bout_id: str) -> bool:
        """Whether AWS accepted Proxy deletion, or exact cleanup already finished."""

        event = self._proxy_delete_accepted.get(bout_id)
        return event is not None and event.is_set()

    async def wait_for_proxy_delete_accepted(self, bout_id: str) -> None:
        """Wait only for the durable Proxy-delete handoff, not full AWS settling."""

        event = self._proxy_delete_accepted.get(bout_id)
        task = self._cleanup_tasks.get(bout_id)
        if event is None or task is None:
            raise ConnectionSpikeCleanupError("Round 5 cleanup has not been started")
        if event.is_set():
            return
        accepted = asyncio.create_task(event.wait())
        try:
            done, _ = await asyncio.wait(
                {accepted, task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if accepted in done:
                return
            # Cleanup ended before the handoff.  Propagate its exact failure (a
            # successful completion sets the event before returning).
            await asyncio.shield(task)
            if not event.is_set():
                raise ConnectionSpikeCleanupError(
                    "Round 5 cleanup finished without a Proxy delete handoff"
                )
        finally:
            if not accepted.done():
                accepted.cancel()
                await asyncio.gather(accepted, return_exceptions=True)

    async def wait_for_cleanup_complete(self, bout_id: str) -> None:
        """Await the full exact-absence/reverse-cleanup proof."""

        task = self._cleanup_tasks.get(bout_id)
        if task is None:
            raise ConnectionSpikeCleanupError("Round 5 cleanup has not been started")
        await asyncio.shield(task)

    async def cancel_and_settle(self, bout_id: str) -> None:
        LiveConnectionSpikeAdapter._validate_run_id(bout_id)
        await asyncio.shield(self.cleanup(bout_id))

    async def unresolved_bout_ids(self) -> tuple[str, ...]:
        """Read the durable set that must be empty before a fresh Round 5 setup."""

        return tuple(await self._journal.unresolved_bout_ids())

    async def assert_no_unresolved_bouts(
        self,
        new_bout_id: str,
        current_fencing_token: int,
    ) -> None:
        """Fence a read-only pre-create guard for a newly claimed Round 5 bout."""

        LiveConnectionSpikeAdapter._validate_run_id(new_bout_id)
        authority = CreationScope(
            new_bout_id,
            current_fencing_token,
            self.config.baseline_sha256,
        )
        async with self._lock:
            await self._fence.assert_current(authority)
            if await self._journal.unresolved_bout_ids():
                raise ConnectionSpikeCleanupError(
                    "Round 5 setup is blocked until prior cleanup is reconciled"
                )

    async def reconcile_failed_cleanup(
        self,
        bout_id: str,
        current_fencing_token: int,
    ) -> None:
        """Recover persisted old ownership scopes under a fresh active fence."""

        LiveConnectionSpikeAdapter._validate_run_id(bout_id)
        authority = CreationScope(
            bout_id,
            current_fencing_token,
            self.config.baseline_sha256,
        )
        async with self._lock:
            await self._fence.assert_current(authority)
            # Same reasoning as `_begin_cleanup_once`, and it matters more here:
            # this is the path the automatic retry re-enters, so a settlement
            # that could refuse made every attempt fail at an identical point
            # and never reach the reconcile below. It cannot refuse now.
            await self._settle_commands(bout_id)
            clients = await self._assumed_clients(bout_id)
            rds_security_group_id = await self._baseline_rds_security_group(clients)
            scopes = tuple(await self._journal.scopes(bout_id))
            for ownership_scope in scopes:
                if (
                    ownership_scope.bout_id != bout_id
                    or ownership_scope.runtime_seal_sha256 != self.config.baseline_sha256
                ):
                    raise ConnectionSpikeCleanupError(
                        "Round 5 persisted cleanup scope differs from the sealed bout"
                    )
                resources = _SetupResources(
                    self.names_for_bout(
                        self.config.deterministic_name_prefix,
                        bout_id,
                        self.config.secret_name_prefix or "anti-demo-round5",
                    ),
                    secret_arn=self.config.proxy_secret_arn,
                    proxy_role_arn=self.config.proxy_service_role_arn,
                )
                resources.rds_security_group_id = rds_security_group_id
                coordinator, _ = self._coordinator(authority, clients, resources)
                events = tuple(await self._journal.events(ownership_scope))
                await self._restore_resource_bindings(
                    coordinator,
                    events,
                    resources,
                )
                report = await coordinator.reconcile_incomplete(
                    ownership_scope,
                    authority_scope=authority,
                )
                if not report.complete:
                    raise ConnectionSpikeCleanupError(
                        "Round 5 persisted cleanup was not ownership-confirmed"
                    )
            await self._discover_orphaned_addons(
                clients,
                rds_security_group_id,
                include_legacy=True,
            )

    async def _baseline_rds_security_group(self, clients: _SetupAwsClients) -> str:
        source = await self._read_competitor_source(clients)
        if (
            source.identifier != self.config.competitor_target_id
            or source.resource_id != self.config.competitor_resource_id
            or source.direct_host != self.config.competitor_direct_host
            or source.vpc_id != self.config.vpc_id
            or source.security_group_ids != (self.config.competitor_security_group_id,)
        ):
            raise ConnectionSpikeCleanupError(
                "Round 5 cleanup could not bind the sealed competitor security group"
            )
        return self.config.competitor_security_group_id

    async def _read_competitor_source(self, clients: _SetupAwsClients) -> _CompetitorSource:
        if self.config.competitor_id == "rds_postgres":
            response = await self._call(
                clients.rds.describe_db_instances,
                DBInstanceIdentifier=self.config.competitor_target_id,
            )
            values = response.get("DBInstances") or []
            source = values[0] if len(values) == 1 else {}
            return _CompetitorSource(
                identifier=str(source.get("DBInstanceIdentifier") or ""),
                resource_id=str(source.get("DbiResourceId") or ""),
                direct_host=str((source.get("Endpoint") or {}).get("Address") or ""),
                status=str(source.get("DBInstanceStatus") or "").lower(),
                vpc_id=str((source.get("DBSubnetGroup") or {}).get("VpcId") or ""),
                security_group_ids=tuple(
                    str(group.get("VpcSecurityGroupId") or "")
                    for group in source.get("VpcSecurityGroups") or []
                    if group.get("VpcSecurityGroupId")
                ),
            )

        response = await self._call(
            clients.rds.describe_db_clusters,
            DBClusterIdentifier=self.config.competitor_target_id,
        )
        values = response.get("DBClusters") or []
        source = values[0] if len(values) == 1 else {}
        subnet_group_name = str(source.get("DBSubnetGroup") or "")
        subnet_groups: list[Mapping[str, object]] = []
        if subnet_group_name:
            subnet_response = await self._call(
                clients.rds.describe_db_subnet_groups,
                DBSubnetGroupName=subnet_group_name,
            )
            subnet_groups = subnet_response.get("DBSubnetGroups") or []
        subnet_group = subnet_groups[0] if len(subnet_groups) == 1 else {}
        return _CompetitorSource(
            identifier=str(source.get("DBClusterIdentifier") or ""),
            resource_id=str(source.get("DbClusterResourceId") or ""),
            direct_host=str(source.get("Endpoint") or ""),
            status=str(source.get("Status") or "").lower(),
            vpc_id=str(subnet_group.get("VpcId") or ""),
            security_group_ids=tuple(
                str(group.get("VpcSecurityGroupId") or "")
                for group in source.get("VpcSecurityGroups") or []
                if group.get("VpcSecurityGroupId")
            ),
        )

    async def _restore_resource_bindings(
        self,
        coordinator: Round5CreationCoordinator,
        events: Sequence[JournalEvent],
        resources: _SetupResources,
    ) -> None:
        provider_ids = {
            event.resource_kind: event.provider_id
            for event in events
            if event.provider_id is not None
        }
        resources.secret_arn = str(provider_ids.get("proxy_secret") or resources.secret_arn)
        resources.proxy_role_arn = str(
            provider_ids.get("proxy_iam_role") or resources.proxy_role_arn
        )
        resources.proxy_security_group_id = str(provider_ids.get("proxy_security_group") or "")
        by_kind = {
            event.resource_kind: ResourceSpec(
                ordinal=event.ordinal,
                resource_kind=event.resource_kind,
                deterministic_name=event.deterministic_name,
                client_token=event.client_token,
                metadata=event.metadata,
            )
            for event in events
        }
        for kind, attribute in (
            ("proxy_secret", "secret_arn"),
            ("proxy_iam_role", "proxy_role_arn"),
            ("proxy_security_group", "proxy_security_group_id"),
        ):
            if kind not in by_kind or kind in provider_ids:
                continue
            observed = await coordinator._adapters[kind].inspect(
                by_kind[kind],
                provider_id=None,
            )
            if observed is not None:
                setattr(resources, attribute, observed.provider_id)

    async def _setup_lakebase(
        self,
        bout_id: str,
        clients: _SetupAwsClients,
        gate: asyncio.Event,
        t0_box: list[int],
        on_progress: SetupProgressCallback | None,
    ) -> ConnectionSpikeSetupLaneStop:
        await gate.wait()
        launched_ns = self._monotonic_ns()
        if launched_ns - t0_box[0] > 10_000_000:
            raise ConnectionSpikeLiveOperationError(
                "Round 5 Lakebase setup workflow launched after 10 ms"
            )

        async def report(phase: str, status: str = "running") -> None:
            await self._report(
                on_progress,
                "lakebase",
                phase,
                status,
                t0_ns=t0_box[0],
            )

        await report("validating_host")
        host = await self._fresh_lakebase_host()
        if host != self.config.lakebase_pooled_host:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 fresh Lakebase pooled host differs from the sealed baseline"
            )
        await report("verifying_transaction")
        await self._runner_action(
            clients.ssm,
            bout_id=bout_id,
            lane_id="lakebase",
            action="verify",
            endpoint_host=host,
            credential_host=self.config.lakebase_direct_host,
            credential_sha256=self.config.lakebase_credential_sha256,
        )
        stopped_ns = self._monotonic_ns()
        await self._report(
            on_progress,
            "lakebase",
            "setup_stop",
            "verified",
            setup_elapsed_ms=(stopped_ns - t0_box[0]) / 1_000_000,
        )
        return ConnectionSpikeSetupLaneStop(
            lane_id="lakebase",
            launched_ns=launched_ns,
            stopped_ns=stopped_ns,
            credential_sha256=self.config.lakebase_credential_sha256,
            endpoint_host=host,
        )

    async def _setup_competitor(
        self,
        bout_id: str,
        scope: CreationScope,
        clients: _SetupAwsClients,
        coordinator: Round5CreationCoordinator,
        specs: tuple[ResourceSpec, ...],
        resources: _SetupResources,
        gate: asyncio.Event,
        t0_box: list[int],
        on_progress: SetupProgressCallback | None,
    ) -> ConnectionSpikeSetupLaneStop:
        await gate.wait()
        launched_ns = self._monotonic_ns()
        if launched_ns - t0_box[0] > 10_000_000:
            raise ConnectionSpikeLiveOperationError(
                "Round 5 RDS setup workflow launched after 10 ms"
            )

        async def report(phase: str, status: str = "running") -> None:
            await self._report(
                on_progress,
                "competitor",
                phase,
                status,
                t0_ns=t0_box[0],
            )

        phases = {
            "proxy_security_group": "creating_proxy_network",
            "proxy_default_egress": "freezing_proxy_egress",
            "proxy_ingress": "authorizing_proxy_ingress",
            "proxy_egress": "authorizing_proxy_egress",
            "runner_egress": "authorizing_runner_egress",
            "rds_ingress": "authorizing_rds_ingress",
            "rds_proxy": "creating_proxy",
            "proxy_target_group": "freezing_proxy_settings",
            "proxy_target": "registering_proxy_target",
        }
        for spec in specs:
            phase = phases[spec.resource_kind]
            await report(phase)
            await coordinator.create_resource(scope, spec)
        wake_aurora = self.config.competitor_id == "aurora_serverless_v2"
        aurora_proxy_state: Literal["available", "pending_capacity"] | None = None
        await report("waiting_for_proxy_target")
        if wake_aurora:
            aurora_proxy_state = await self._wait_proxy_available(
                clients,
                resources,
                allow_aurora_pending_capacity=True,
            )
        else:
            await self._wait_proxy_available(clients, resources)
        await self._verify_journaled_resources(scope, coordinator, specs)
        if wake_aurora and aurora_proxy_state == "pending_capacity":
            await report("resuming_database")
            await self._runner_action(
                clients.ssm,
                bout_id=bout_id,
                lane_id=self.config.competitor_credential_id,
                action="verify",
                endpoint_host=self.config.competitor_direct_host,
                credential_host=self.config.competitor_direct_host,
                credential_sha256=self.config.competitor_credential_sha256,
            )
            await report("waiting_for_proxy_target")
            await self._wait_proxy_available(clients, resources)
        await report("verifying_topology")
        await self._verify_proxy_topology(clients, resources)
        await report("verifying_transaction")
        await self._runner_action(
            clients.ssm,
            bout_id=bout_id,
            lane_id=self.config.competitor_credential_id,
            action="verify",
            endpoint_host=resources.proxy_endpoint,
            credential_host=self.config.competitor_direct_host,
            credential_sha256=self.config.competitor_credential_sha256,
        )
        stopped_ns = self._monotonic_ns()
        await self._report(
            on_progress,
            "competitor",
            "setup_stop",
            "verified",
            setup_elapsed_ms=(stopped_ns - t0_box[0]) / 1_000_000,
        )
        return ConnectionSpikeSetupLaneStop(
            lane_id="competitor",
            launched_ns=launched_ns,
            stopped_ns=stopped_ns,
            credential_sha256=self.config.competitor_credential_sha256,
            endpoint_host=resources.proxy_endpoint,
            secret_arn=resources.secret_arn,
        )

    def _setup_observation(self, stop: ConnectionSpikeSetupLaneStop) -> SetupLaneObservation:
        if stop.lane_id == "lakebase":
            facts = (
                PublicSetupEvidence("fresh_pooled_path_verified", True),
                PublicSetupEvidence("runner_verify_full_transaction", True),
            )
            gate_id = "lakebase_fresh_pooled_transaction"
        else:
            facts = (
                PublicSetupEvidence("sealed_proxy_auth_verified", True),
                PublicSetupEvidence("proxy_target_state", "AVAILABLE"),
                PublicSetupEvidence("max_connections_percent", 90),
                PublicSetupEvidence("connection_borrow_timeout_seconds", 120),
                PublicSetupEvidence("runner_verify_full_transaction", True),
            )
            gate_id = "rds_proxy_topology_transaction"
        return SetupLaneObservation(
            lane_id=stop.lane_id,
            workflow_launched_ns=stop.launched_ns,
            status=SetupLaneStatus.SUCCEEDED,
            stop_gate_evidence=SetupStopGateEvidence(
                gate_id=gate_id,
                expected=facts,
                observed=facts,
                verified_at_ns=stop.stopped_ns,
            ),
        )

    async def _verify_journaled_resources(
        self,
        scope: CreationScope,
        coordinator: Round5CreationCoordinator,
        specs: Sequence[ResourceSpec],
    ) -> None:
        created = {
            event.ordinal: event
            for event in await self._journal.events(scope)
            if event.lifecycle_state is LifecycleState.CREATED
        }
        adapters = coordinator._adapters
        for spec in specs:
            event = created.get(spec.ordinal)
            if event is None or event.provider_id is None:
                raise ConnectionSpikeLiveOperationError(
                    "Round 5 journal omitted a completed setup mutation"
                )
            observed = await adapters[spec.resource_kind].inspect(
                spec, provider_id=event.provider_id
            )
            if observed is None or observed.provider_id != event.provider_id:
                raise ConnectionSpikeLiveConfigurationError(
                    "Round 5 provider reread differed from the journaled setup mutation"
                )

    async def _assumed_clients(self, bout_id: str) -> _SetupAwsClients:
        def assume() -> _SetupAwsClients:
            suffix = hashlib.sha256(bout_id.encode()).hexdigest()[:16]
            source = _control_role_source_session(
                self._session_factory,
                region=self.config.region,
                expected_account_id=self.config.expected_account_id,
                runtime_role_arn=self.config.runtime_role_arn,
                session_name=f"{self.config.role_session_prefix}-rt-{suffix}",
            )
            sts = source.client("sts", region_name=self.config.region)
            response = sts.assume_role(
                RoleArn=self.config.baseline_control_role_arn,
                RoleSessionName=f"{self.config.role_session_prefix}-{suffix}"[:64],
                DurationSeconds=3600,
            )
            credentials = response.get("Credentials") or {}
            assumed_arn = str((response.get("AssumedRoleUser") or {}).get("Arn") or "")
            role = _ROLE_ARN.fullmatch(self.config.baseline_control_role_arn)
            assert role is not None
            expected = (
                f"arn:{role.group('partition')}:sts::{self.config.expected_account_id}:"
                f"assumed-role/{role.group('name').rsplit('/', 1)[-1]}/"
            )
            required = ("AccessKeyId", "SecretAccessKey", "SessionToken", "Expiration")
            if not assumed_arn.startswith(expected) or any(
                not credentials.get(key) for key in required
            ):
                raise ConnectionSpikeLiveConfigurationError(
                    "STS did not return the sealed Round 5 baseline control role"
                )
            expiration = credentials["Expiration"]
            if isinstance(expiration, datetime):
                if expiration.tzinfo is None:
                    expiration = expiration.replace(tzinfo=UTC)
                if expiration <= datetime.now(UTC) + timedelta(seconds=SETUP_DEADLINE_SECONDS + 60):
                    raise ConnectionSpikeLiveConfigurationError(
                        "Round 5 assumed credentials expire before the setup deadline"
                    )
            assumed = self._session_factory(
                aws_access_key_id=credentials["AccessKeyId"],
                aws_secret_access_key=credentials["SecretAccessKey"],
                aws_session_token=credentials["SessionToken"],
                region_name=self.config.region,
            )
            return _SetupAwsClients(
                ssm=assumed.client("ssm", region_name=self.config.region),
                rds=assumed.client("rds", region_name=self.config.region),
                ec2=assumed.client("ec2", region_name=self.config.region),
                iam=assumed.client("iam", region_name=self.config.region),
                secretsmanager=assumed.client("secretsmanager", region_name=self.config.region),
            )

        return await asyncio.to_thread(assume)

    async def _preflight_baseline(
        self,
        scope: CreationScope,
        clients: _SetupAwsClients,
        coordinator: Round5CreationCoordinator,
        specs: tuple[ResourceSpec, ...],
        resources: _SetupResources,
    ) -> None:
        await self._fence.assert_current(scope)
        source, managed = await asyncio.gather(
            self._read_competitor_source(clients),
            self._call(
                clients.ssm.describe_instance_information,
                Filters=[{"Key": "InstanceIds", "Values": [self.config.runner_instance_id]}],
            ),
        )
        runners = managed.get("InstanceInformationList") or []
        if not source.identifier or len(runners) != 1:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 clean baseline did not resolve exactly once"
            )
        if (
            source.identifier != self.config.competitor_target_id
            or source.resource_id != self.config.competitor_resource_id
            or source.direct_host != self.config.competitor_direct_host
            or source.status != "available"
            or source.vpc_id != self.config.vpc_id
            or len(source.security_group_ids) != 1
            or runners[0].get("InstanceId") != self.config.runner_instance_id
            or runners[0].get("PingStatus") != "Online"
        ):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 source or runner differs from the sealed clean baseline"
            )
        resources.rds_security_group_id = source.security_group_ids[0]
        journal_events = tuple(await self._journal.events(scope))
        for event in journal_events:
            if event.lifecycle_state is not LifecycleState.CREATED or not event.provider_id:
                continue
            if event.resource_kind == "proxy_secret":
                resources.secret_arn = event.provider_id
            elif event.resource_kind == "proxy_iam_role":
                resources.proxy_role_arn = event.provider_id
            elif event.resource_kind == "proxy_security_group":
                resources.proxy_security_group_id = event.provider_id
        if journal_events:
            report = await coordinator.reconcile_incomplete(scope)
            if not report.complete:
                raise ConnectionSpikeCleanupError(
                    "Round 5 incomplete setup journal could not be reconciled"
                )
        resources.secret_arn = self.config.proxy_secret_arn
        resources.proxy_role_arn = self.config.proxy_service_role_arn
        await self._verify_proxy_service_role(clients)
        await self._discover_orphaned_addons(
            clients,
            resources.rds_security_group_id,
            include_legacy=False,
        )
        adapters = coordinator._adapters  # exact pre-T0 discovery; no mutation
        for spec in specs:
            if spec.resource_kind not in {"proxy_security_group", "rds_proxy"}:
                continue
            observed = await adapters[spec.resource_kind].inspect(spec, provider_id=None)
            if observed is not None:
                raise ConnectionSpikeLiveConfigurationError(
                    "Round 5 deterministic per-bout resource already exists"
                )

    async def _verify_proxy_service_role(self, clients: _SetupAwsClients) -> None:
        role_name = self._proxy_service_role_name
        role_result, policy_result = await asyncio.gather(
            self._call(clients.iam.get_role, RoleName=role_name),
            self._call(
                clients.iam.get_role_policy,
                RoleName=role_name,
                PolicyName=self.config.proxy_service_policy_name,
            ),
        )
        role = role_result.get("Role") or {}
        if (
            role.get("RoleName") != role_name
            or role.get("Arn") != self.config.proxy_service_role_arn
            or self._canonical_policy(role.get("AssumeRolePolicyDocument"))
            != self._canonical_policy(self._proxy_trust_policy())
        ):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 Proxy service role identity or trust policy changed"
            )
        if not self._policy_matches(policy_result, self._proxy_service_policy()):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 Proxy service inline policy document changed"
            )

    async def _discover_orphaned_addons(
        self,
        clients: _SetupAwsClients,
        rds_security_group_id: str,
        *,
        include_legacy: bool = True,
    ) -> None:
        base_tags = dict(self.config.ownership_tags)
        iam_base_tags = {
            key: value
            for key, value in base_tags.items()
            if key.casefold() != "owner" or key == "owner"
        }

        def owned(values: Sequence[Mapping[str, object]], *, iam: bool = False) -> bool:
            measured = {str(item.get("Key") or ""): str(item.get("Value") or "") for item in values}
            expected = iam_base_tags if iam else base_tags
            return (
                all(measured.get(key) == value for key, value in expected.items())
                and bool(measured.get("anti-demo-bout-id"))
                and (not iam or {key for key in measured if key.casefold() == "owner"} == {"owner"})
            )

        groups_result, proxies_result, rules_result = await asyncio.gather(
            self._call(
                clients.ec2.describe_security_groups,
                Filters=[{"Name": "vpc-id", "Values": [self.config.vpc_id]}],
            ),
            self._call(clients.rds.describe_db_proxies, MaxRecords=100),
            self._call(
                clients.ec2.describe_security_group_rules,
                MaxResults=1000,
                Filters=[
                    {
                        "Name": "group-id",
                        "Values": [
                            self.config.runner_security_group_id,
                            rds_security_group_id,
                        ],
                    }
                ],
            ),
        )
        secrets_result: Mapping[str, object] = {}
        roles_result: Mapping[str, object] = {}
        policies_result: Mapping[str, object] = {}
        if include_legacy and self.config.secret_name_prefix and self.config.runner_role_arn:
            secrets_result, roles_result, policies_result = await asyncio.gather(
                self._call(
                    clients.secretsmanager.list_secrets,
                    MaxResults=100,
                    IncludePlannedDeletion=True,
                    Filters=[{"Key": "name", "Values": [self.config.secret_name_prefix]}],
                ),
                self._call(clients.iam.list_roles, MaxItems=1000),
                self._call(
                    clients.iam.list_role_policies,
                    RoleName=self._runner_role_name,
                    MaxItems=1000,
                ),
            )
        if any(
            value
            for value in (
                secrets_result.get("NextToken"),
                roles_result.get("IsTruncated"),
                groups_result.get("NextToken"),
                proxies_result.get("Marker"),
                policies_result.get("IsTruncated"),
                rules_result.get("NextToken"),
            )
        ):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 clean-baseline discovery exceeded its bounded page"
            )
        prefix = self.config.deterministic_name_prefix[:40].rstrip("-") + "-"
        leftovers: list[str] = []
        leftovers.extend(
            "secret"
            for value in secrets_result.get("SecretList") or []
            if str(value.get("Name") or "").startswith(
                self.config.secret_name_prefix.rstrip("/") + "/"
            )
        )
        for role in roles_result.get("Roles") or []:
            name = str(role.get("RoleName") or "")
            if not name.startswith(prefix):
                continue
            tags = await self._call(clients.iam.list_role_tags, RoleName=name)
            if tags.get("IsTruncated"):
                raise ConnectionSpikeLiveConfigurationError(
                    "Round 5 role-tag discovery exceeded its bounded page"
                )
            if not owned(tags.get("Tags") or [], iam=True):
                leftovers.append("role_tag_drift")
            else:
                leftovers.append("role")
        leftovers.extend(
            "security_group"
            for value in groups_result.get("SecurityGroups") or []
            if str(value.get("GroupName") or "").startswith(prefix)
        )
        for proxy in proxies_result.get("DBProxies") or []:
            if not str(proxy.get("DBProxyName") or "").startswith(prefix):
                continue
            tags = await self._call(
                clients.rds.list_tags_for_resource,
                ResourceName=str(proxy.get("DBProxyArn") or ""),
            )
            if not owned(tags.get("TagList") or []):
                leftovers.append("proxy_tag_drift")
            else:
                leftovers.append("proxy")
        leftovers.extend(
            "runner_policy"
            for name in policies_result.get("PolicyNames") or []
            if str(name).startswith(prefix) and str(name).endswith("-runner-secret")
        )
        leftovers.extend(
            "security_group_rule"
            for value in rules_result.get("SecurityGroupRules") or []
            if str(value.get("Description") or "").startswith(prefix)
        )
        if leftovers:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 clean baseline contains prior-bout add-ons"
            )

    def _coordinator(
        self,
        scope: CreationScope,
        clients: _SetupAwsClients,
        resources: _SetupResources,
    ) -> tuple[Round5CreationCoordinator, tuple[ResourceSpec, ...]]:
        tags = dict(self.config.ownership_tags)
        tags["anti-demo-bout-id"] = scope.bout_id
        tags["anti-demo:bout-token"] = resources.names.token
        metadata = {
            "tags": tags,
            "baseline_sha256": self.config.baseline_sha256,
            "competitor_id": self.config.competitor_id,
            "competitor_target_id": self.config.competitor_target_id,
            "competitor_resource_id": self.config.competitor_resource_id,
        }
        stem = resources.names.proxy_name
        specs = (
            ResourceSpec(
                1,
                "proxy_security_group",
                resources.names.proxy_security_group_name,
                metadata=metadata,
            ),
            ResourceSpec(2, "proxy_default_egress", f"{stem}-default-egress", metadata=metadata),
            ResourceSpec(3, "proxy_ingress", f"{stem}-proxy-ingress", metadata=metadata),
            ResourceSpec(4, "proxy_egress", f"{stem}-proxy-egress", metadata=metadata),
            ResourceSpec(5, "runner_egress", f"{stem}-runner-egress", metadata=metadata),
            ResourceSpec(6, "rds_ingress", f"{stem}-rds-ingress", metadata=metadata),
            ResourceSpec(7, "rds_proxy", resources.names.proxy_name, metadata=metadata),
            ResourceSpec(8, "proxy_target_group", f"{stem}-target-group", metadata=metadata),
            ResourceSpec(9, "proxy_target", f"{stem}-target", metadata=metadata),
        )
        adapters: dict[str, ResourceAdapter] = {
            "proxy_secret": _SetupResourceAdapter(
                lambda spec: self._create_secret(clients, resources, spec),
                lambda spec, provider_id: self._inspect_secret(clients, spec, provider_id),
                lambda observed: self._delete_secret(clients, observed),
            ),
            "runner_secret_policy": _SetupResourceAdapter(
                lambda spec: self._create_runner_policy(clients, resources, spec),
                lambda spec, provider_id: self._inspect_runner_policy(
                    clients, resources, spec, provider_id
                ),
                lambda observed: self._delete_runner_policy(clients, observed),
            ),
            "runner_credentials": _SetupResourceAdapter(
                lambda spec: self._create_runner_credentials(clients, resources, scope, spec),
                lambda spec, provider_id: self._inspect_transient_action(spec, provider_id),
                self._delete_transient_action,
            ),
            "proxy_iam_role": _SetupResourceAdapter(
                lambda spec: self._create_proxy_role(clients, resources, spec),
                lambda spec, provider_id: self._inspect_proxy_role(clients, spec, provider_id),
                lambda observed: self._delete_proxy_role(clients, observed),
            ),
            "proxy_iam_policy": _SetupResourceAdapter(
                lambda spec: self._create_proxy_policy(clients, resources, spec),
                lambda spec, provider_id: self._inspect_proxy_policy(
                    clients, resources, spec, provider_id
                ),
                lambda observed: self._delete_proxy_policy(clients, resources, observed),
            ),
            "proxy_security_group": _SetupResourceAdapter(
                lambda spec: self._create_proxy_security_group(clients, resources, spec),
                lambda spec, provider_id: self._inspect_proxy_network(clients, spec, provider_id),
                lambda observed: self._delete_proxy_network(clients, resources, observed),
            ),
            "proxy_default_egress": self._default_egress_adapter(clients, resources),
            "proxy_ingress": self._security_rule_adapter(clients, resources, "proxy_ingress"),
            "proxy_egress": self._security_rule_adapter(clients, resources, "proxy_egress"),
            "runner_egress": self._security_rule_adapter(clients, resources, "runner_egress"),
            "rds_ingress": self._security_rule_adapter(clients, resources, "rds_ingress"),
            "rds_proxy": _SetupResourceAdapter(
                lambda spec: self._create_proxy(clients, resources, spec),
                lambda spec, provider_id: self._inspect_proxy(clients, spec, provider_id),
                lambda observed: self._delete_proxy(
                    clients,
                    observed,
                    bout_id=scope.bout_id,
                ),
            ),
            "proxy_target_group": _SetupResourceAdapter(
                lambda spec: self._configure_target_group(clients, resources, spec),
                lambda spec, provider_id: self._inspect_target_group(
                    clients, resources, spec, provider_id
                ),
                lambda observed: self._reset_target_group(clients, resources, observed),
            ),
            "proxy_target": _SetupResourceAdapter(
                lambda spec: self._register_proxy_target(clients, resources, spec),
                lambda spec, provider_id: self._inspect_proxy_target(
                    clients, resources, spec, provider_id
                ),
                lambda observed: self._deregister_proxy_target(clients, resources, observed),
            ),
        }
        return (
            Round5CreationCoordinator(
                journal=self._journal,
                fence=self._fence,
                adapters=adapters,
            ),
            specs,
        )

    @staticmethod
    def _observation(spec: ResourceSpec, provider_id: str) -> ResourceObservation:
        return ResourceObservation(
            resource_kind=spec.resource_kind,
            provider_id=provider_id,
            deterministic_name=spec.deterministic_name,
            client_token=spec.client_token,
            metadata=spec.metadata,
        )

    @staticmethod
    def _tags(spec: ResourceSpec | ResourceObservation) -> list[dict[str, str]]:
        tags = spec.metadata.get("tags")
        if not isinstance(tags, Mapping):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 resource ownership tags are missing"
            )
        if spec.resource_kind == "proxy_iam_role":
            tags = {
                key: value
                for key, value in tags.items()
                if str(key).casefold() != "owner" or str(key) == "owner"
            }
        return [{"Key": str(key), "Value": str(value)} for key, value in sorted(tags.items())]

    @classmethod
    def _require_exact_tags(
        cls, spec: ResourceSpec, values: Sequence[Mapping[str, object]]
    ) -> None:
        expected = {value["Key"]: value["Value"] for value in cls._tags(spec)}
        observed = {str(value.get("Key") or ""): str(value.get("Value") or "") for value in values}
        if observed != expected:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 per-bout resource ownership tags changed"
            )

    async def _create_secret(
        self, clients: _SetupAwsClients, resources: _SetupResources, spec: ResourceSpec
    ) -> ResourceObservation:
        response = await self._call(
            clients.secretsmanager.create_secret,
            Name=resources.names.secret_name,
            Description="Ephemeral Round 5 per-bout RDS Proxy credential container",
            Tags=self._tags(spec),
        )
        arn = str(response.get("ARN") or "")
        if _SECRET_ARN.fullmatch(arn) is None or not arn.startswith(
            f"arn:aws:secretsmanager:{self.config.region}:{self.config.expected_account_id}:secret:"
        ):
            raise ConnectionSpikeLiveOperationError(
                "Secrets Manager did not return the per-bout secret ARN"
            )
        resources.secret_arn = arn
        return self._observation(spec, arn)

    async def _inspect_secret(
        self, clients: _SetupAwsClients, spec: ResourceSpec, provider_id: str | None
    ) -> ResourceObservation | None:
        try:
            response = await self._call(
                clients.secretsmanager.describe_secret,
                SecretId=provider_id or str(spec.deterministic_name),
            )
        except Exception as exc:
            if self._not_found(exc):
                return None
            raise
        arn = str(response.get("ARN") or "")
        if not arn.startswith(
            f"arn:aws:secretsmanager:{self.config.region}:{self.config.expected_account_id}:secret:"
        ):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 per-bout secret account or region changed"
            )
        self._require_exact_tags(spec, response.get("Tags") or [])
        return self._observation(spec, arn)

    async def _delete_secret(
        self, clients: _SetupAwsClients, observed: ResourceObservation
    ) -> None:
        try:
            current = await self._call(
                clients.secretsmanager.describe_secret,
                SecretId=observed.provider_id,
            )
        except Exception as exc:
            if self._not_found(exc):
                return
            raise
        if current.get("DeletedDate") is None:
            await self._call(
                clients.secretsmanager.delete_secret,
                SecretId=observed.provider_id,
                ForceDeleteWithoutRecovery=True,
            )
        for _ in range(240):
            try:
                await self._call(
                    clients.secretsmanager.describe_secret,
                    SecretId=observed.provider_id,
                )
            except Exception as exc:
                if self._not_found(exc):
                    return
                raise
            await self._sleep(self.config.poll_interval_seconds)
        raise ConnectionSpikeCleanupError("Secrets Manager deletion did not settle")

    @property
    def _runner_role_name(self) -> str:
        match = _ROLE_ARN.fullmatch(self.config.runner_role_arn)
        assert match is not None
        return match.group("name").rsplit("/", 1)[-1]

    @property
    def _proxy_service_role_name(self) -> str:
        match = _ROLE_ARN.fullmatch(self.config.proxy_service_role_arn)
        assert match is not None
        return match.group("name").rsplit("/", 1)[-1]

    @staticmethod
    def _policy_matches(response: Mapping[str, object], expected: Mapping[str, object]) -> bool:
        measured = response.get("PolicyDocument")
        return LiveConnectionSpikeSetupOrchestrator._canonical_policy(measured) == (
            LiveConnectionSpikeSetupOrchestrator._canonical_policy(expected)
        )

    @staticmethod
    def _canonical_policy(value: object) -> str:
        if isinstance(value, str):
            try:
                value = json.loads(unquote(value))
            except (json.JSONDecodeError, TypeError):
                return ""
        if not isinstance(value, Mapping):
            return ""

        def normalize(item: object) -> object:
            if isinstance(item, Mapping):
                return {str(key): normalize(child) for key, child in item.items()}
            if isinstance(item, list | tuple):
                children = [normalize(child) for child in item]
                return sorted(
                    children,
                    key=lambda child: json.dumps(
                        child, sort_keys=True, separators=(",", ":")
                    ),
                )
            return item

        return json.dumps(normalize(value), sort_keys=True, separators=(",", ":"))

    def _proxy_service_policy(self) -> dict[str, object]:
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "secretsmanager:DescribeSecret",
                        "secretsmanager:GetSecretValue",
                    ],
                    "Resource": [
                        self.config.aurora_proxy_secret_arn,
                        self.config.rds_proxy_secret_arn,
                    ],
                }
            ],
        }

    def _runner_secret_policy(self, secret_arn: str) -> dict[str, object]:
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "secretsmanager:PutSecretValue",
                        "secretsmanager:DescribeSecret",
                    ],
                    "Resource": secret_arn,
                }
            ],
        }

    async def _create_runner_policy(
        self, clients: _SetupAwsClients, resources: _SetupResources, spec: ResourceSpec
    ) -> ResourceObservation:
        policy = self._runner_secret_policy(resources.secret_arn)
        await self._call(
            clients.iam.put_role_policy,
            RoleName=self._runner_role_name,
            PolicyName=resources.names.runner_policy_name,
            PolicyDocument=json.dumps(policy, separators=(",", ":")),
        )
        return self._observation(
            spec,
            f"{self.config.runner_role_arn}:policy/{resources.names.runner_policy_name}",
        )

    async def _inspect_runner_policy(
        self,
        clients: _SetupAwsClients,
        resources: _SetupResources,
        spec: ResourceSpec,
        provider_id: str | None,
    ) -> ResourceObservation | None:
        try:
            response = await self._call(
                clients.iam.get_role_policy,
                RoleName=self._runner_role_name,
                PolicyName=str(spec.deterministic_name),
            )
        except Exception as exc:
            if self._not_found(exc):
                return None
            raise
        if not self._policy_matches(response, self._runner_secret_policy(resources.secret_arn)):
            raise ConnectionSpikeLiveConfigurationError("Round 5 runner secret policy changed")
        return self._observation(
            spec,
            provider_id or f"{self.config.runner_role_arn}:policy/{spec.deterministic_name}",
        )

    async def _delete_runner_policy(
        self, clients: _SetupAwsClients, observed: ResourceObservation
    ) -> None:
        await self._call(
            clients.iam.delete_role_policy,
            RoleName=self._runner_role_name,
            PolicyName=str(observed.deterministic_name),
        )

    async def _create_runner_credentials(
        self,
        clients: _SetupAwsClients,
        resources: _SetupResources,
        scope: CreationScope,
        spec: ResourceSpec,
    ) -> ResourceObservation:
        propagation_deadline = asyncio.get_running_loop().time() + 60.0
        while True:
            await self._fence.assert_current(scope)
            try:
                await self._runner_action(
                    clients.ssm,
                    bout_id=scope.bout_id,
                    lane_id=self.config.competitor_credential_id,
                    action="reassert_rds_credentials",
                    endpoint_host=self.config.competitor_direct_host,
                    credential_host=self.config.competitor_direct_host,
                    credential_sha256=self.config.competitor_credential_sha256,
                    master_secret_arn=self.config.competitor_master_secret_arn,
                    destination_secret_arn=resources.secret_arn,
                )
                break
            except ConnectionSpikeLiveOperationError:
                if asyncio.get_running_loop().time() >= propagation_deadline:
                    raise
                await self._sleep(self.config.poll_interval_seconds)
        await self._verify_secret_current(clients, resources)
        return self._observation(spec, f"ssm:{scope.bout_id}:runner-credentials")

    async def _inspect_transient_action(
        self, spec: ResourceSpec, provider_id: str | None
    ) -> ResourceObservation | None:
        del spec, provider_id
        return None

    async def _delete_transient_action(self, observed: ResourceObservation) -> None:
        del observed

    async def _create_proxy_role(
        self, clients: _SetupAwsClients, resources: _SetupResources, spec: ResourceSpec
    ) -> ResourceObservation:
        trust = self._proxy_trust_policy()
        response = await self._call(
            clients.iam.create_role,
            RoleName=resources.names.proxy_role_name,
            AssumeRolePolicyDocument=json.dumps(trust, separators=(",", ":")),
            PermissionsBoundary=self.config.proxy_role_permissions_boundary_arn,
            Tags=self._tags(spec),
        )
        arn = str((response.get("Role") or {}).get("Arn") or "")
        if _ROLE_ARN.fullmatch(arn) is None:
            raise ConnectionSpikeLiveOperationError("IAM did not return the Proxy role ARN")
        resources.proxy_role_arn = arn
        return self._observation(spec, arn)

    @staticmethod
    def _proxy_trust_policy() -> dict[str, object]:
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "rds.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }

    async def _create_proxy_policy(
        self, clients: _SetupAwsClients, resources: _SetupResources, spec: ResourceSpec
    ) -> ResourceObservation:
        policy = self._proxy_secret_policy(resources.secret_arn)
        await self._call(
            clients.iam.put_role_policy,
            RoleName=resources.names.proxy_role_name,
            PolicyName=resources.names.proxy_policy_name,
            PolicyDocument=json.dumps(policy, separators=(",", ":")),
        )
        return self._observation(
            spec, f"{resources.proxy_role_arn}:policy/{resources.names.proxy_policy_name}"
        )

    @staticmethod
    def _proxy_secret_policy(secret_arn: str) -> dict[str, object]:
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "secretsmanager:GetSecretValue",
                    "Resource": secret_arn,
                }
            ],
        }

    @classmethod
    def _proxy_cleanup_policy_matches(
        cls, response: Mapping[str, object], secret_arn: str
    ) -> bool:
        legacy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "secretsmanager:GetSecretValue",
                    "Resource": secret_arn,
                    "Condition": {
                        "StringEquals": {"secretsmanager:VersionStage": "AWSCURRENT"}
                    },
                }
            ],
        }
        return cls._policy_matches(response, cls._proxy_secret_policy(secret_arn)) or (
            cls._policy_matches(response, legacy)
        )

    async def _inspect_proxy_policy(
        self,
        clients: _SetupAwsClients,
        resources: _SetupResources,
        spec: ResourceSpec,
        provider_id: str | None,
    ) -> ResourceObservation | None:
        try:
            response = await self._call(
                clients.iam.get_role_policy,
                RoleName=resources.names.proxy_role_name,
                PolicyName=resources.names.proxy_policy_name,
            )
        except Exception as exc:
            if self._not_found(exc):
                return None
            raise
        if not self._proxy_cleanup_policy_matches(response, resources.secret_arn):
            raise ConnectionSpikeLiveConfigurationError("Round 5 Proxy secret policy changed")
        return self._observation(
            spec,
            provider_id or f"{resources.proxy_role_arn}:policy/{resources.names.proxy_policy_name}",
        )

    async def _delete_proxy_policy(
        self,
        clients: _SetupAwsClients,
        resources: _SetupResources,
        observed: ResourceObservation,
    ) -> None:
        del observed
        await self._call(
            clients.iam.delete_role_policy,
            RoleName=resources.names.proxy_role_name,
            PolicyName=resources.names.proxy_policy_name,
        )

    async def _inspect_proxy_role(
        self, clients: _SetupAwsClients, spec: ResourceSpec, provider_id: str | None
    ) -> ResourceObservation | None:
        del provider_id
        try:
            response = await self._call(clients.iam.get_role, RoleName=str(spec.deterministic_name))
        except Exception as exc:
            if self._not_found(exc):
                return None
            raise
        role = response.get("Role") or {}
        self._require_exact_tags(spec, role.get("Tags") or [])
        arn = str(role.get("Arn") or "")
        arn_match = _ROLE_ARN.fullmatch(arn)
        if (
            role.get("RoleName") != spec.deterministic_name
            or arn_match is None
            or arn_match.group("account") != self.config.expected_account_id
            or (role.get("PermissionsBoundary") or {}).get("PermissionsBoundaryArn")
            != self.config.proxy_role_permissions_boundary_arn
            or self._canonical_policy(role.get("AssumeRolePolicyDocument"))
            != self._canonical_policy(self._proxy_trust_policy())
        ):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 per-bout Proxy role identity or complete trust policy changed"
            )
        return self._observation(spec, arn)

    async def _delete_proxy_role(
        self, clients: _SetupAwsClients, observed: ResourceObservation
    ) -> None:
        role_name = observed.deterministic_name or ""
        await self._call(clients.iam.delete_role, RoleName=role_name)

    async def _create_proxy_security_group(
        self, clients: _SetupAwsClients, resources: _SetupResources, spec: ResourceSpec
    ) -> ResourceObservation:
        response = await self._call(
            clients.ec2.create_security_group,
            GroupName=resources.names.proxy_security_group_name,
            Description="Ephemeral Round 5 per-bout RDS Proxy network",
            VpcId=self.config.vpc_id,
            TagSpecifications=[{"ResourceType": "security-group", "Tags": self._tags(spec)}],
        )
        group_id = str(response.get("GroupId") or "")
        if not group_id:
            raise ConnectionSpikeLiveOperationError(
                "EC2 did not return the per-bout Proxy security group"
            )
        resources.proxy_security_group_id = group_id
        return self._observation(spec, group_id)

    def _default_egress_adapter(
        self, clients: _SetupAwsClients, resources: _SetupResources
    ) -> ResourceAdapter:
        async def create(spec: ResourceSpec) -> ResourceObservation:
            response = await self._call(
                clients.ec2.describe_security_groups,
                GroupIds=[resources.proxy_security_group_id],
            )
            groups = response.get("SecurityGroups") or []
            permissions = groups[0].get("IpPermissionsEgress") or [] if len(groups) == 1 else []
            if not permissions or any(
                permission.get("IpProtocol") != "-1"
                or permission.get("UserIdGroupPairs")
                or permission.get("PrefixListIds")
                or any(
                    value.get("CidrIp") != "0.0.0.0/0" for value in permission.get("IpRanges") or []
                )
                or any(
                    value.get("CidrIpv6") != "::/0" for value in permission.get("Ipv6Ranges") or []
                )
                for permission in permissions
            ):
                raise ConnectionSpikeLiveConfigurationError(
                    "Round 5 new Proxy security group had non-default egress"
                )
            await self._call(
                clients.ec2.revoke_security_group_egress,
                GroupId=resources.proxy_security_group_id,
                IpPermissions=permissions,
            )
            return self._observation(spec, f"{resources.proxy_security_group_id}:default-egress")

        async def inspect(
            spec: ResourceSpec, provider_id: str | None
        ) -> ResourceObservation | None:
            try:
                response = await self._call(
                    clients.ec2.describe_security_groups,
                    GroupIds=[resources.proxy_security_group_id],
                )
            except Exception as exc:
                if self._not_found(exc):
                    return None
                raise
            group = (response.get("SecurityGroups") or [{}])[0]
            default_egress_exists = any(
                permission.get("IpProtocol") == "-1"
                and not permission.get("UserIdGroupPairs")
                and not permission.get("PrefixListIds")
                and (
                    any(
                        value.get("CidrIp") == "0.0.0.0/0"
                        for value in permission.get("IpRanges") or []
                    )
                    or any(
                        value.get("CidrIpv6") == "::/0"
                        for value in permission.get("Ipv6Ranges") or []
                    )
                )
                for permission in group.get("IpPermissionsEgress") or []
            )
            return (
                None
                if default_egress_exists
                else self._observation(
                    spec, provider_id or f"{resources.proxy_security_group_id}:default-egress"
                )
            )

        async def delete(observed: ResourceObservation) -> None:
            await self._call(
                clients.ec2.authorize_security_group_egress,
                GroupId=resources.proxy_security_group_id,
                IpPermissions=[{"IpProtocol": "-1", "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}],
                TagSpecifications=[
                    {"ResourceType": "security-group-rule", "Tags": self._tags(observed)}
                ],
            )

        return _SetupResourceAdapter(create, inspect, delete)

    def _security_rule_adapter(
        self,
        clients: _SetupAwsClients,
        resources: _SetupResources,
        kind: str,
    ) -> ResourceAdapter:
        def binding() -> tuple[Any, Any, str, str, bool]:
            bindings = {
                "proxy_ingress": (
                    clients.ec2.authorize_security_group_ingress,
                    clients.ec2.revoke_security_group_ingress,
                    resources.proxy_security_group_id,
                    self.config.runner_security_group_id,
                    False,
                ),
                "proxy_egress": (
                    clients.ec2.authorize_security_group_egress,
                    clients.ec2.revoke_security_group_egress,
                    resources.proxy_security_group_id,
                    resources.rds_security_group_id,
                    True,
                ),
                "runner_egress": (
                    clients.ec2.authorize_security_group_egress,
                    clients.ec2.revoke_security_group_egress,
                    self.config.runner_security_group_id,
                    resources.proxy_security_group_id,
                    True,
                ),
                "rds_ingress": (
                    clients.ec2.authorize_security_group_ingress,
                    clients.ec2.revoke_security_group_ingress,
                    resources.rds_security_group_id,
                    resources.proxy_security_group_id,
                    False,
                ),
            }
            return bindings[kind]

        async def create(spec: ResourceSpec) -> ResourceObservation:
            operation, _, group_id, peer_id, _ = binding()
            response = await self._call(
                operation,
                GroupId=group_id,
                IpPermissions=[
                    {
                        "IpProtocol": "tcp",
                        "FromPort": 5432,
                        "ToPort": 5432,
                        "UserIdGroupPairs": [
                            {
                                "GroupId": peer_id,
                                "Description": str(spec.deterministic_name),
                            }
                        ],
                    }
                ],
                TagSpecifications=[
                    {"ResourceType": "security-group-rule", "Tags": self._tags(spec)}
                ],
            )
            rule_ids = [
                str(value.get("SecurityGroupRuleId") or "")
                for value in response.get("SecurityGroupRules") or []
                if value.get("SecurityGroupRuleId")
            ]
            if len(rule_ids) != 1:
                raise ConnectionSpikeLiveOperationError(
                    "EC2 did not return exactly one per-bout security-group rule"
                )
            return self._observation(spec, rule_ids[0])

        async def inspect(
            spec: ResourceSpec, provider_id: str | None
        ) -> ResourceObservation | None:
            _, _, group_id, peer_id, is_egress = binding()
            try:
                response = await self._call(
                    clients.ec2.describe_security_group_rules,
                    **(
                        {"SecurityGroupRuleIds": [provider_id]}
                        if provider_id is not None
                        else {"Filters": [{"Name": "group-id", "Values": [group_id]}]}
                    ),
                )
            except Exception as exc:
                if self._not_found(exc):
                    return None
                raise
            candidates = [
                rule
                for rule in response.get("SecurityGroupRules") or []
                if rule.get("GroupId") == group_id
                and rule.get("IsEgress") is is_egress
                and rule.get("ReferencedGroupInfo", {}).get("GroupId") == peer_id
                and rule.get("IpProtocol") == "tcp"
                and rule.get("FromPort") == 5432
                and rule.get("ToPort") == 5432
                and rule.get("Description") == spec.deterministic_name
            ]
            if not candidates:
                if provider_id is not None and response.get("SecurityGroupRules"):
                    raise ConnectionSpikeLiveConfigurationError(
                        "Round 5 per-bout security-group rule identity changed"
                    )
                return None
            if len(candidates) != 1:
                raise ConnectionSpikeLiveConfigurationError(
                    "Round 5 per-bout security-group rule identity is ambiguous"
                )
            rule = candidates[0]
            rule_id = str(rule.get("SecurityGroupRuleId") or "")
            if provider_id is not None and rule_id != provider_id:
                raise ConnectionSpikeLiveConfigurationError(
                    "Round 5 per-bout security-group rule identity changed"
                )
            self._require_exact_tags(spec, rule.get("Tags") or [])
            return self._observation(spec, rule_id)

        async def delete(observed: ResourceObservation) -> None:
            _, revoke, group_id, _, _ = binding()
            await self._call(
                revoke,
                GroupId=group_id,
                SecurityGroupRuleIds=[observed.provider_id],
            )

        return _SetupResourceAdapter(create, inspect, delete)

    async def _inspect_proxy_network(
        self, clients: _SetupAwsClients, spec: ResourceSpec, provider_id: str | None
    ) -> ResourceObservation | None:
        kwargs = (
            {"GroupIds": [provider_id]}
            if provider_id
            else {
                "Filters": [
                    {"Name": "group-name", "Values": [str(spec.deterministic_name)]},
                    {"Name": "vpc-id", "Values": [self.config.vpc_id]},
                ]
            }
        )
        try:
            response = await self._call(clients.ec2.describe_security_groups, **kwargs)
        except Exception as exc:
            if self._not_found(exc):
                return None
            raise
        groups = response.get("SecurityGroups") or []
        if not groups:
            return None
        if len(groups) != 1:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 deterministic Proxy security group is ambiguous"
            )
        self._require_exact_tags(spec, groups[0].get("Tags") or [])
        if (
            groups[0].get("GroupName") != spec.deterministic_name
            or groups[0].get("VpcId") != self.config.vpc_id
        ):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 per-bout Proxy security group identity changed"
            )
        return self._observation(spec, str(groups[0].get("GroupId") or ""))

    async def _delete_proxy_network(
        self,
        clients: _SetupAwsClients,
        resources: _SetupResources,
        observed: ResourceObservation,
    ) -> None:
        del resources
        await self._call(clients.ec2.delete_security_group, GroupId=observed.provider_id)

    async def _create_proxy(
        self, clients: _SetupAwsClients, resources: _SetupResources, spec: ResourceSpec
    ) -> ResourceObservation:
        await self._call(
            clients.rds.create_db_proxy,
            DBProxyName=resources.names.proxy_name,
            EngineFamily="POSTGRESQL",
            Auth=[
                {
                    "AuthScheme": "SECRETS",
                    "SecretArn": resources.secret_arn,
                    "IAMAuth": "DISABLED",
                    "ClientPasswordAuthType": "POSTGRES_SCRAM_SHA_256",
                }
            ],
            RoleArn=resources.proxy_role_arn,
            VpcSubnetIds=list(self.config.proxy_subnet_ids),
            VpcSecurityGroupIds=[resources.proxy_security_group_id],
            RequireTLS=True,
            Tags=self._tags(spec),
        )
        response = await self._call(
            clients.rds.describe_db_proxies, DBProxyName=resources.names.proxy_name
        )
        proxies = response.get("DBProxies") or []
        if len(proxies) != 1:
            raise ConnectionSpikeLiveOperationError("RDS did not return the created per-bout Proxy")
        proxy = proxies[0]
        resources.proxy_endpoint = str(proxy.get("Endpoint") or "")
        proxy_arn = str(proxy.get("DBProxyArn") or "")
        if not proxy_arn.startswith(
            f"arn:aws:rds:{self.config.region}:{self.config.expected_account_id}:db-proxy:"
        ):
            raise ConnectionSpikeLiveOperationError(
                "RDS returned a per-bout Proxy outside the sealed account or region"
            )
        return self._observation(spec, proxy_arn)

    async def _configure_target_group(
        self, clients: _SetupAwsClients, resources: _SetupResources, spec: ResourceSpec
    ) -> ResourceObservation:
        response = await self._call(
            clients.rds.describe_db_proxy_target_groups,
            DBProxyName=resources.names.proxy_name,
        )
        groups = response.get("TargetGroups") or []
        group = groups[0] if len(groups) == 1 else {}
        target_group_arn = str(group.get("TargetGroupArn") or "")
        if (
            group.get("TargetGroupName") != "default"
            or not target_group_arn.startswith(
                f"arn:aws:rds:{self.config.region}:"
                f"{self.config.expected_account_id}:target-group:"
            )
        ):
            raise ConnectionSpikeLiveOperationError(
                "RDS did not return the exact per-bout Proxy target group"
            )
        await self._call(
            clients.rds.add_tags_to_resource,
            ResourceName=target_group_arn,
            Tags=self._tags(spec),
        )
        tags = await self._call(
            clients.rds.list_tags_for_resource,
            ResourceName=target_group_arn,
        )
        self._require_exact_tags(spec, tags.get("TagList") or [])
        await self._call(
            clients.rds.modify_db_proxy_target_group,
            DBProxyName=resources.names.proxy_name,
            TargetGroupName="default",
            ConnectionPoolConfig={
                "MaxConnectionsPercent": self.config.proxy_max_connections_percent,
                "ConnectionBorrowTimeout": self.config.proxy_borrow_timeout_seconds,
            },
        )
        return self._observation(spec, f"{resources.names.proxy_name}:default")

    async def _inspect_target_group(
        self,
        clients: _SetupAwsClients,
        resources: _SetupResources,
        spec: ResourceSpec,
        provider_id: str | None,
    ) -> ResourceObservation | None:
        try:
            response = await self._call(
                clients.rds.describe_db_proxy_target_groups,
                DBProxyName=resources.names.proxy_name,
            )
        except Exception as exc:
            if self._error_code(exc) == "DBProxyNotFoundFault":
                return None
            raise
        groups = response.get("TargetGroups") or []
        if not groups:
            return None
        group = groups[0] if len(groups) == 1 else {}
        target_group_arn = str(group.get("TargetGroupArn") or "")
        if (
            group.get("TargetGroupName") != "default"
            or not target_group_arn.startswith(
                f"arn:aws:rds:{self.config.region}:"
                f"{self.config.expected_account_id}:target-group:"
            )
        ):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 per-bout Proxy target-group identity changed"
            )
        tags = await self._call(
            clients.rds.list_tags_for_resource,
            ResourceName=target_group_arn,
        )
        self._require_exact_tags(spec, tags.get("TagList") or [])
        pool = group.get("ConnectionPoolConfig") or {}
        if (
            pool.get("MaxConnectionsPercent") == 90
            and pool.get("ConnectionBorrowTimeout") == 120
        ):
            return self._observation(spec, provider_id or f"{resources.names.proxy_name}:default")
        return None

    async def _reset_target_group(
        self,
        clients: _SetupAwsClients,
        resources: _SetupResources,
        observed: ResourceObservation,
    ) -> None:
        del observed
        await self._call(
            clients.rds.modify_db_proxy_target_group,
            DBProxyName=resources.names.proxy_name,
            TargetGroupName="default",
            ConnectionPoolConfig={
                "MaxConnectionsPercent": 100,
                "MaxIdleConnectionsPercent": 50,
                "ConnectionBorrowTimeout": 120,
            },
        )

    async def _register_proxy_target(
        self, clients: _SetupAwsClients, resources: _SetupResources, spec: ResourceSpec
    ) -> ResourceObservation:
        await self._call(
            clients.rds.register_db_proxy_targets,
            DBProxyName=resources.names.proxy_name,
            **self.config.proxy_registration,
        )
        return self._observation(spec, self.config.competitor_resource_id)

    def _proxy_targets_match(self, targets: Sequence[Mapping[str, object]]) -> bool:
        return _proxy_target_set_matches(
            self.config.competitor_id,
            self.config.competitor_target_id,
            self.config.competitor_resource_id,
            targets,
        )

    def _proxy_targets_available(self, targets: Sequence[Mapping[str, object]]) -> bool:
        return _proxy_target_set_matches(
            self.config.competitor_id,
            self.config.competitor_target_id,
            self.config.competitor_resource_id,
            targets,
            require_available=True,
        )

    def _aurora_targets_pending_capacity(
        self, targets: Sequence[Mapping[str, object]]
    ) -> bool:
        if (
            self.config.competitor_id != "aurora_serverless_v2"
            or not self._proxy_targets_match(targets)
        ):
            return False
        routable = [target for target in targets if target.get("Type") == "RDS_INSTANCE"]
        health = [target.get("TargetHealth") or {} for target in routable]
        pending = [
            value
            for value in health
            if str(value.get("State") or "").upper() == "UNAVAILABLE"
            and str(value.get("Reason") or "").upper() == "PENDING_PROXY_CAPACITY"
        ]
        return bool(pending) and all(not value or value in pending for value in health)

    async def _inspect_proxy_target(
        self,
        clients: _SetupAwsClients,
        resources: _SetupResources,
        spec: ResourceSpec,
        provider_id: str | None,
    ) -> ResourceObservation | None:
        try:
            response = await self._call(
                clients.rds.describe_db_proxy_targets,
                DBProxyName=resources.names.proxy_name,
            )
        except Exception as exc:
            if self._error_code(exc) == "DBProxyNotFoundFault":
                return None
            raise
        targets = response.get("Targets") or []
        if not targets:
            return None
        if not self._proxy_targets_match(targets):
            raise ConnectionSpikeLiveConfigurationError("Round 5 per-bout Proxy target set changed")
        return self._observation(spec, provider_id or self.config.competitor_resource_id)

    async def _deregister_proxy_target(
        self,
        clients: _SetupAwsClients,
        resources: _SetupResources,
        observed: ResourceObservation,
    ) -> None:
        del observed
        await self._call(
            clients.rds.deregister_db_proxy_targets,
            DBProxyName=resources.names.proxy_name,
            **self.config.proxy_registration,
        )

    async def _inspect_proxy(
        self, clients: _SetupAwsClients, spec: ResourceSpec, provider_id: str | None
    ) -> ResourceObservation | None:
        del provider_id
        try:
            response = await self._call(
                clients.rds.describe_db_proxies,
                DBProxyName=str(spec.deterministic_name),
            )
        except Exception as exc:
            if self._not_found(exc):
                return None
            raise
        proxies = response.get("DBProxies") or []
        if not proxies:
            return None
        if len(proxies) != 1:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 deterministic RDS Proxy is ambiguous"
            )
        proxy = proxies[0]
        proxy_arn = str(proxy.get("DBProxyArn") or "")
        if not proxy_arn.startswith(
            f"arn:aws:rds:{self.config.region}:{self.config.expected_account_id}:db-proxy:"
        ):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 per-bout Proxy account or region changed"
            )
        try:
            tags = await self._call(
                clients.rds.list_tags_for_resource,
                ResourceName=proxy_arn,
            )
        except Exception as exc:
            if self._error_code(exc) == "DBProxyNotFoundFault":
                return None
            raise
        self._require_exact_tags(spec, tags.get("TagList") or [])
        if proxy.get("DBProxyName") != spec.deterministic_name:
            raise ConnectionSpikeLiveConfigurationError("Round 5 per-bout Proxy identity changed")
        return self._observation(spec, proxy_arn)

    async def _delete_proxy(
        self,
        clients: _SetupAwsClients,
        observed: ResourceObservation,
        *,
        bout_id: str | None = None,
    ) -> None:
        name = str(observed.deterministic_name or "")
        await self._call(clients.rds.delete_db_proxy, DBProxyName=name)
        # Round5CreationCoordinator durably commits DELETE_INTENT before invoking
        # this adapter.  Reaching this line therefore proves both the durable
        # intent and AWS API acceptance, without waiting minutes for absence.
        if bout_id is not None:
            self._proxy_delete_accepted.setdefault(bout_id, asyncio.Event()).set()
        try:
            # Fires between polls, but not while parked inside `_call`, whose
            # cancellation handler re-awaits the shielded worker thread. So on a
            # wedged endpoint this deadline is unreachable and the real ceiling
            # is `abandon_on_cancel` in the caller. Load-bearing only for a
            # responsive-but-slow Proxy delete; do not read it as protection
            # against a hang, and do not remove the outer bound believing it is.
            async with asyncio.timeout(PROXY_DELETION_TIMEOUT_SECONDS):
                while True:
                    if (
                        await self._inspect_proxy(
                            clients,
                            ResourceSpec(
                                ordinal=12,
                                resource_kind="rds_proxy",
                                deterministic_name=name,
                                metadata=observed.metadata,
                            ),
                            observed.provider_id,
                        )
                        is None
                    ):
                        return
                    await self._sleep(self.config.poll_interval_seconds)
        except TimeoutError as exc:
            raise ConnectionSpikeCleanupError("RDS Proxy deletion did not settle") from exc

    async def _wait_proxy_available(
        self,
        clients: _SetupAwsClients,
        resources: _SetupResources,
        *,
        allow_aurora_pending_capacity: bool = False,
    ) -> Literal["available", "pending_capacity"]:
        while True:
            proxies, targets = await asyncio.gather(
                self._call(
                    clients.rds.describe_db_proxies,
                    DBProxyName=resources.names.proxy_name,
                ),
                self._call(
                    clients.rds.describe_db_proxy_targets,
                    DBProxyName=resources.names.proxy_name,
                ),
            )
            proxy_values = proxies.get("DBProxies") or []
            target_values = targets.get("Targets") or []
            if any(
                str((target.get("TargetHealth") or {}).get("Reason") or "").upper()
                == "AUTH_FAILURE"
                for target in target_values
            ):
                raise ConnectionSpikeLiveOperationError(
                    "Round 5 RDS Proxy target credential registration failed"
                )
            if (
                len(proxy_values) == 1
                and str(proxy_values[0].get("Status") or "").lower() == "available"
            ):
                if self._proxy_targets_available(target_values):
                    return "available"
                if (
                    allow_aurora_pending_capacity
                    and self._aurora_targets_pending_capacity(target_values)
                ):
                    return "pending_capacity"
            await self._sleep(self.config.poll_interval_seconds)

    async def _verify_proxy_topology(
        self, clients: _SetupAwsClients, resources: _SetupResources
    ) -> None:
        (
            proxy_result,
            target_result,
            group_result,
            source,
            network_result,
        ) = await asyncio.gather(
            self._call(
                clients.rds.describe_db_proxies,
                DBProxyName=resources.names.proxy_name,
            ),
            self._call(
                clients.rds.describe_db_proxy_targets,
                DBProxyName=resources.names.proxy_name,
            ),
            self._call(
                clients.rds.describe_db_proxy_target_groups,
                DBProxyName=resources.names.proxy_name,
            ),
            self._read_competitor_source(clients),
            self._call(
                clients.ec2.describe_security_groups,
                GroupIds=[resources.proxy_security_group_id],
            ),
        )
        proxies = proxy_result.get("DBProxies") or []
        targets = target_result.get("Targets") or []
        groups = group_result.get("TargetGroups") or []
        networks = network_result.get("SecurityGroups") or []
        proxy = proxies[0] if len(proxies) == 1 else {}
        auth = proxy.get("Auth") or []
        pool = (groups[0].get("ConnectionPoolConfig") or {}) if len(groups) == 1 else {}
        network = networks[0] if len(networks) == 1 else {}
        stem = resources.names.proxy_name
        ingress = self._security_group_permissions(network.get("IpPermissions") or [])
        egress = self._security_group_permissions(network.get("IpPermissionsEgress") or [])
        if (
            proxy.get("DBProxyName") != resources.names.proxy_name
            or str(proxy.get("Status") or "").lower() != "available"
            or proxy.get("Endpoint") != resources.proxy_endpoint
            or proxy.get("RoleArn") != resources.proxy_role_arn
            or proxy.get("VpcId") != self.config.vpc_id
            or set(proxy.get("VpcSubnetIds") or []) != set(self.config.proxy_subnet_ids)
            or set(proxy.get("VpcSecurityGroupIds") or []) != {resources.proxy_security_group_id}
            or proxy.get("RequireTLS") is not True
            or len(auth) != 1
            or auth[0].get("SecretArn") != resources.secret_arn
            or auth[0].get("IAMAuth") != "DISABLED"
            or auth[0].get("ClientPasswordAuthType") != "POSTGRES_SCRAM_SHA_256"
            or len(groups) != 1
            or groups[0].get("TargetGroupName") != "default"
            or pool.get("MaxConnectionsPercent") != 90
            or pool.get("ConnectionBorrowTimeout") != 120
            or not self._proxy_targets_available(targets)
            or source.identifier != self.config.competitor_target_id
            or source.resource_id != self.config.competitor_resource_id
            or source.direct_host != self.config.competitor_direct_host
            or source.status != "available"
            or source.vpc_id != self.config.vpc_id
            or source.security_group_ids != (resources.rds_security_group_id,)
            or len(networks) != 1
            or network.get("GroupId") != resources.proxy_security_group_id
            or ingress
            != (
                (
                    "tcp",
                    5432,
                    5432,
                    self.config.runner_security_group_id,
                    f"{stem}-proxy-ingress",
                ),
            )
            or egress
            != (
                (
                    "tcp",
                    5432,
                    5432,
                    resources.rds_security_group_id,
                    f"{stem}-proxy-egress",
                ),
            )
        ):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 RDS Proxy topology reread did not exactly match the timed setup"
            )

    @staticmethod
    def _security_group_permissions(
        permissions: Sequence[Mapping[str, object]],
    ) -> tuple[tuple[object, ...], ...]:
        values: list[tuple[object, ...]] = []
        for permission in permissions:
            if (
                permission.get("IpRanges")
                or permission.get("Ipv6Ranges")
                or permission.get("PrefixListIds")
            ):
                values.append(("unexpected_non_group_rule",))
            pairs = permission.get("UserIdGroupPairs") or []
            if not pairs:
                values.append(("unexpected_empty_rule",))
            for pair in pairs:
                values.append(
                    (
                        permission.get("IpProtocol"),
                        permission.get("FromPort"),
                        permission.get("ToPort"),
                        pair.get("GroupId"),
                        pair.get("Description"),
                    )
                )
        return tuple(values)

    async def _verify_secret_current(
        self, clients: _SetupAwsClients, resources: _SetupResources
    ) -> None:
        response = await self._call(
            clients.secretsmanager.describe_secret,
            SecretId=resources.secret_arn,
        )
        stages = response.get("VersionIdsToStages") or {}
        current = [
            version_id for version_id, values in stages.items() if "AWSCURRENT" in (values or [])
        ]
        if response.get("ARN") != resources.secret_arn or len(current) != 1:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 per-bout Proxy secret does not have exactly one AWSCURRENT version"
            )

    async def _runner_action(
        self,
        ssm: Any,
        *,
        bout_id: str,
        lane_id: str,
        action: str,
        endpoint_host: str,
        credential_host: str,
        credential_sha256: str,
        master_secret_arn: str = "",
        destination_secret_arn: str = "",
    ) -> None:
        nonce = hashlib.sha256(f"{bout_id}\0{lane_id}\0{action}".encode()).hexdigest()
        request: dict[str, object] = {
            "protocol": SETUP_RUNNER_PROTOCOL,
            "action": action,
            "nonce": nonce,
            "bout_id": bout_id,
            "lane_id": lane_id,
            "endpoint_host": endpoint_host,
            "credential_host": credential_host,
            "port": 5432,
            "dbname": self.config.database_name,
            "username": self.config.native_role,
            "trust_bundle_path": self.config.trust_bundle_path,
            "trust_bundle_sha256": self.config.trust_bundle_sha256,
            "credential_sha256": credential_sha256,
        }
        if action == "reassert_rds_credentials":
            request["master_secret_arn"] = master_secret_arn
            request["destination_secret_arn"] = destination_secret_arn
        encoded = base64.urlsafe_b64encode(
            gzip.compress(
                json.dumps(request, sort_keys=True, separators=(",", ":")).encode(),
                mtime=0,
            )
        ).decode()
        command = f"{self.config.runner_path} {encoded}"
        if len(command.encode()) > 24_000:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 setup request exceeds the SSM command limit"
            )
        send_kwargs = {
            "InstanceIds": [self.config.runner_instance_id],
            "DocumentName": self.config.ssm_document_name,
            "TimeoutSeconds": int(self.config.command_timeout_seconds),
            "Parameters": {
                "commands": [command],
                "executionTimeout": [str(int(self.config.command_timeout_seconds))],
            },
            "CloudWatchOutputConfig": {"CloudWatchOutputEnabled": False},
        }
        key = f"{lane_id}:{action}"
        # Recorded before the request leaves, never after. Once `send_command`
        # is in a worker thread this process has already, possibly, created a
        # command on the runner, and a cancellation arriving before the
        # identifier comes back must still be able to say so.
        self._pending_sends[key] = _SetupPendingSend(bout_id, lane_id, action)
        send_task = asyncio.create_task(asyncio.to_thread(ssm.send_command, **send_kwargs))
        cancelled_during_send = False
        try:
            response = await asyncio.shield(send_task)
        except asyncio.CancelledError:
            cancelled_during_send = True
            response = await asyncio.shield(send_task)
        command_id = str((response.get("Command") or {}).get("CommandId") or "")
        if not command_id:
            # The pending record is deliberately left in place: a send that
            # answered without an identifier may still have started a command,
            # and this is precisely the case that cannot be verified.
            raise ConnectionSpikeLiveOperationError(
                "SSM did not return a Round 5 setup command identifier"
            )
        active = _SetupActiveCommand(bout_id, lane_id, action, command_id, ssm)
        self._active_commands[key] = active
        self._pending_sends.pop(key, None)
        if cancelled_during_send:
            try:
                await asyncio.shield(self._cancel_setup_command(active, nonce))
            finally:
                self._active_commands.pop(key, None)
            raise asyncio.CancelledError
        # Whether the command is known not to be in flight any more. Reaching a
        # terminal SSM status proves that; so does `_cancel_setup_command`
        # returning, which only happens once the runner has reported
        # `SETUP_SETTLED` and released its flock. Nothing else does, and the
        # registry is what names an in-flight command in the ORPHAN RISK line,
        # so forgetting the entry on an *unconfirmed* settlement would drop the
        # one case worth reporting.
        settled = False
        try:
            async with asyncio.timeout(self.config.command_timeout_seconds):
                while True:
                    invocation = await self._setup_invocation(active)
                    if invocation.get("Status") in _TERMINAL:
                        break
                    await self._sleep(self.config.poll_interval_seconds)
            settled = True
            output = str(invocation.get("StandardOutputContent") or "")
            self._validate_setup_output(
                output,
                bout_id=bout_id,
                lane_id=lane_id,
                action=action,
                nonce=nonce,
            )
            if invocation.get("Status") != "Success":
                raise ConnectionSpikeLiveOperationError(
                    "Round 5 setup runner command did not succeed after settlement"
                )
        except asyncio.CancelledError:
            await asyncio.shield(self._cancel_setup_command(active, nonce))
            settled = True
            raise
        except TimeoutError as exc:
            await self._cancel_setup_command(active, nonce)
            settled = True
            raise ConnectionSpikeLiveOperationError(
                "Round 5 setup runner exceeded its 120-second command boundary"
            ) from exc
        finally:
            if settled and self._active_commands.get(key) == active:
                self._active_commands.pop(key, None)

    async def _setup_invocation(self, active: _SetupActiveCommand) -> Mapping[str, object]:
        try:
            return await self._call(
                active.ssm.get_command_invocation,
                CommandId=active.command_id,
                InstanceId=self.config.runner_instance_id,
            )
        except Exception as exc:
            if self._error_code(exc) == "InvocationDoesNotExist":
                return {"Status": "Pending"}
            raise

    async def _cancel_setup_command(self, active: _SetupActiveCommand, nonce: str) -> None:
        await self._call(
            active.ssm.cancel_command,
            CommandId=active.command_id,
            InstanceIds=[self.config.runner_instance_id],
        )
        # Same caveat as the Proxy deletion deadline: this bounds the polling
        # below, but the `_call` above re-awaits its worker thread on
        # cancellation, so a wedged SSM endpoint never arrives here. The
        # effective ceiling is `abandon_on_cancel` in the caller. Unlike the
        # burst adapter's `_cancel_and_settle`, moving the call inside would not
        # help: `_call`'s re-await, not the placement, is what defeats it.
        async with asyncio.timeout(self.config.settlement_timeout_seconds):
            while True:
                invocation = await self._setup_invocation(active)
                output = str(invocation.get("StandardOutputContent") or "")
                if (
                    invocation.get("Status") in _TERMINAL
                    and f"SETUP_SETTLED:{nonce}" in output
                    and f"RUNNER_FLOCK_RELEASED:{active.bout_id}" in output
                ):
                    return
                await self._sleep(self.config.poll_interval_seconds)

    async def _settle_commands(self, bout_id: str) -> tuple[str, ...]:
        """Drain this bout's in-flight setup commands, and never refuse over them.

        Returns the commands that did not confirm, as ``lane:action=id``.

        **THIS MAY NOT RAISE, AND THAT IS THE WHOLE POINT OF IT.** It used to,
        and a towel thrown during Round 5 setup showed what that costs: every
        caller runs immediately before the reverse cleanup that deletes the RDS
        Proxy, so a settlement that raised took the deletion with it. The
        automatic retry then re-entered at the same line and expired the same
        way roughly every forty seconds, thirty-odd times, while the Proxy
        stayed ``available`` and billing and nothing the operator could read
        said so.

        The ordering settlement protects is real and is kept: a runner still
        inside ``_runner_action`` holds a connection through the very Proxy
        about to be deleted, and it releases its flock only once its command
        settles, so draining first is the better sequence when draining works.
        What changed is the ranking when it does not. An unsettled command is a
        tidy-up problem -- it holds a flock, it costs nothing, and SSM ends it
        at its own ``command_timeout_seconds`` boundary whatever this process
        does. A surviving RDS Proxy is a money problem. The money problem may
        not be blocked by the tidy-up problem, so settlement gets its window,
        and whatever it fails to settle is named and stepped over.
        """

        commands = [
            active for active in self._active_commands.values() if active.bout_id == bout_id
        ]
        if not commands:
            return ()
        outcomes = await asyncio.gather(
            *(
                self._cancel_setup_command(
                    active,
                    hashlib.sha256(
                        f"{bout_id}\0{active.lane_id}\0{active.action}".encode()
                    ).hexdigest(),
                )
                for active in commands
            ),
            # Includes a child's own `CancelledError`. A lane that was cancelled
            # out from under the drain is exactly a command whose fate is now
            # unknown, which is the case this reports; it is not a cancellation
            # of *this* coroutine, and turning it into one would re-create the
            # abort being removed. A cancellation delivered to this task still
            # propagates, because `gather` re-raises that one.
            return_exceptions=True,
        )
        unsettled = tuple(
            f"{active.lane_id}:{active.action}={active.command_id}"
            for active, outcome in zip(commands, outcomes, strict=True)
            if isinstance(outcome, BaseException)
        )
        if unsettled:
            logger.error(
                "ROUND 5 SSM COMMANDS DID NOT CONFIRM SETTLEMENT within %.0fs and "
                "cleanup is going on to delete the RDS Proxy anyway bout=%s "
                "commands=%s. Deleting the Proxy is worth more than draining "
                "these: SSM ends them at their own %.0fs boundary, and a Proxy "
                "left up bills until somebody deletes it. Clear any command that "
                "outlives that boundary by hand.",
                self.config.settlement_timeout_seconds,
                bout_id,
                ",".join(unsettled),
                self.config.command_timeout_seconds,
            )
        return unsettled

    @staticmethod
    def _validate_setup_output(
        output: str,
        *,
        bout_id: str,
        lane_id: str,
        action: str,
        nonce: str,
    ) -> None:
        if len(output.encode()) > 24_000:
            raise ConnectionSpikeLiveOperationError(
                "Round 5 setup runner output exceeded its bounded contract"
            )
        prefix = "SETUP_RESULT:"
        values = [line[len(prefix) :] for line in output.splitlines() if line.startswith(prefix)]
        try:
            value = json.loads(values[0]) if len(values) == 1 else None
        except json.JSONDecodeError as exc:
            raise ConnectionSpikeLiveOperationError(
                "Round 5 setup runner returned malformed output"
            ) from exc
        expected = {
            "protocol": SETUP_RUNNER_PROTOCOL,
            "action": action,
            "bout_id": bout_id,
            "lane_id": lane_id,
            "nonce": nonce,
            "status": "verified",
        }
        if (
            value != expected
            or f"SETUP_SETTLED:{nonce}" not in output
            or f"RUNNER_FLOCK_RELEASED:{bout_id}" not in output
        ):
            # THE RUNNER'S OWN WORD, NOT JUST "did not return evidence". When
            # the runner refuses it has already said why, on stdout, in one
            # token. Dropping it cost two seven-minute bouts on 2026-08-24:
            # both died on `baseline_auth_hash_invalid` -- a seal naming
            # credentials the runner had already replaced -- and every surface
            # an operator could reach said only
            # `ConnectionSpikeLiveOperationError`. Recovering that one word
            # afterwards took a CloudTrail and SSM excavation.
            code = _runner_error_code(output)
            raise ConnectionSpikeLiveOperationError(
                "Round 5 setup runner did not return exact sanitized evidence"
                + (f": the runner refused with {code}" if code else "")
            )
        lowered = output.lower()
        if any(
            field in lowered for field in ("password", "secretstring", "accesskey", "sessiontoken")
        ):
            raise ConnectionSpikeLiveOperationError(
                "Round 5 setup runner output contained a forbidden credential field"
            )

    async def _call(self, operation: Callable[..., Any], **kwargs: object) -> Any:
        task = asyncio.create_task(asyncio.to_thread(operation, **kwargs))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            await asyncio.shield(task)
            raise

    @staticmethod
    def _error_code(exc: BaseException) -> str:
        response = getattr(exc, "response", None)
        if isinstance(response, Mapping):
            error = response.get("Error")
            if isinstance(error, Mapping):
                return str(error.get("Code") or "")
        return ""

    @classmethod
    def _not_found(cls, exc: BaseException) -> bool:
        return cls._error_code(exc) in {
            "DBProxyNotFoundFault",
            "InvalidGroup.NotFound",
            "InvalidSecurityGroupRuleId.NotFound",
            "NoSuchEntity",
            "ResourceNotFoundException",
        }

    async def _report(
        self,
        callback: SetupProgressCallback | None,
        lane_id: str,
        phase: str,
        status: str,
        *,
        t0_ns: int | None = None,
        setup_elapsed_ms: float | None = None,
    ) -> None:
        if callback is not None:
            if setup_elapsed_ms is None and t0_ns is not None:
                setup_elapsed_ms = max(0.0, (self._monotonic_ns() - t0_ns) / 1_000_000)
            await callback(
                ConnectionSpikeSetupProgress(
                    lane_id=lane_id,
                    phase=phase,
                    status=status,
                    occurred_at=datetime.now(UTC),
                    setup_elapsed_ms=setup_elapsed_ms,
                )
            )


class LiveConnectionSpikeAdapter:
    """One-command SSM boundary backed only by an assumed execution role."""

    def __init__(
        self,
        config: ConnectionSpikeLiveConfig,
        *,
        session_factory: SessionFactory = boto3.Session,
        sleep: Sleeper = asyncio.sleep,
        cancel_teardown_timeout_seconds: float = DEFAULT_CANCEL_TEARDOWN_SECONDS,
    ) -> None:
        if cancel_teardown_timeout_seconds <= 0:
            raise ValueError("cancel_teardown_timeout_seconds must be positive")
        self.config = config
        self._session_factory = session_factory
        self._sleep = sleep
        self.cancel_teardown_timeout_seconds = cancel_teardown_timeout_seconds
        self._active: _ActiveCommand | None = None
        self._pending: _PendingCommand | None = None
        self._dispatch_lock = asyncio.Lock()

    def _cancelled_burst_identifier(self, active: _ActiveCommand) -> str:
        """Name what a cancelled burst may leave holding something.

        Nothing named here is a billable AWS resource. The Proxy and the
        security groups belong to setup, which now tears itself down under its
        own bound, so this path leaks no spend. What can survive is *held
        state*: an SSM command still executing on the runner, the flock that
        command releases only once it settles, and the leases the bout holds
        meanwhile. Those block the next bout rather than costing money, and
        they need naming for the same reason a leaked Proxy does -- an operator
        who cannot name a thing cannot clear it.
        """

        return (
            f"Round 5 bout {active.run_id} burst ["
            f"in-flight SSM command {active.command_id} on "
            f"runner instance {self.config.runner_instance_id}; "
            f"runner flock for {active.run_id}; "
            f"ring and scoped Round 5 leases held for {active.run_id}"
            "]"
        )

    async def check(self) -> None:
        clients = await self._assumed_clients("preflight")
        await self._preflight_runner(clients)
        await self._preflight_targets(clients.rds)

    async def execute(
        self,
        run_id: str,
        schedule: Sequence[Mapping[str, object]],
        *,
        targets: Sequence[ConnectionSpikeTarget] | None = None,
    ) -> dict[str, object]:
        if self._dispatch_lock.locked():
            raise ConnectionSpikeLiveOperationError(
                "A Round 5 runner command is already active in this app replica"
            )
        await self._dispatch_lock.acquire()
        try:
            return await self._execute_reserved(run_id, schedule, targets=targets)
        finally:
            self._dispatch_lock.release()

    async def _execute_reserved(
        self,
        run_id: str,
        schedule: Sequence[Mapping[str, object]],
        *,
        targets: Sequence[ConnectionSpikeTarget] | None = None,
    ) -> dict[str, object]:
        self._validate_run_id(run_id)
        request = self._runner_request(run_id, schedule, targets=targets)
        clients = await self._assumed_clients(run_id)
        await self._preflight_runner(clients)
        await self._preflight_targets(clients.rds, targets=targets)
        started_at = datetime.now(UTC)
        send_task = asyncio.create_task(self._send_command(clients.ssm, request))
        pending = _PendingCommand(run_id=run_id, send_task=send_task, clients=clients)
        self._pending = pending
        try:
            command_id = await asyncio.shield(send_task)
        except asyncio.CancelledError:
            command_id = await asyncio.shield(send_task)
            active = _ActiveCommand(
                run_id=run_id,
                command_id=command_id,
                clients=clients,
            )
            self._active = active
            if self._pending == pending:
                self._pending = None
            # Bounds the wait, never the settlement: `_cancel_and_settle` runs
            # on its own task and keeps going after this returns, so a slow
            # runner still gets its cancel issued and its flock released.
            await abandon_on_cancel(
                lambda: self._cancel_and_settle(active),
                identifier=self._cancelled_burst_identifier(active),
                timeout_seconds=self.cancel_teardown_timeout_seconds,
            )
            self._active = None
            raise
        except Exception:
            if self._pending == pending:
                self._pending = None
            raise
        active = _ActiveCommand(run_id=run_id, command_id=command_id, clients=clients)
        self._active = active
        if self._pending == pending:
            self._pending = None
        try:
            invocation = await self._wait_for_terminal(active)
            output = str(invocation.get("StandardOutputContent") or "")
            self._require_settlement(run_id, output)
            if invocation.get("Status") != "Success":
                raise ConnectionSpikeLiveOperationError(
                    "Round 5 runner command did not succeed after cleanup"
                )
            result = self._parse_runner_output(run_id, output)
            witness = await self._cloudwatch_witness(
                clients.cloudwatch,
                started_at,
                datetime.now(UTC),
            )
            result["cloudwatch_witness"] = witness
            return result
        except asyncio.CancelledError:
            await abandon_on_cancel(
                lambda: self._cancel_and_settle(active),
                identifier=self._cancelled_burst_identifier(active),
                timeout_seconds=self.cancel_teardown_timeout_seconds,
            )
            raise
        except TimeoutError as exc:
            await self._cancel_and_settle(active)
            raise ConnectionSpikeLiveOperationError(
                "Round 5 SSM command exceeded its 120-second boundary"
            ) from exc
        finally:
            if self._active == active:
                self._active = None
            if self._pending == pending:
                self._pending = None

    async def cancel(self, run_id: str) -> None:
        self._validate_run_id(run_id)
        active = self._active
        if active is None:
            pending = self._pending
            if pending is None:
                return
            if pending.run_id != run_id:
                raise ConnectionSpikeCleanupError(
                    "Refusing to cancel a Round 5 command owned by another run"
                )
            command_id = await asyncio.shield(pending.send_task)
            active = _ActiveCommand(
                run_id=run_id,
                command_id=command_id,
                clients=pending.clients,
            )
            self._active = active
            if self._pending == pending:
                self._pending = None
        if active.run_id != run_id:
            raise ConnectionSpikeCleanupError(
                "Refusing to cancel a Round 5 command owned by another run"
            )
        await self._cancel_and_settle(active)

    async def _assumed_clients(self, run_id: str) -> _AwsClients:
        def assume() -> _AwsClients:
            # Deliberately omit profile_name and all credential kwargs. The only
            # source session is the ambient credential chain -- or, where this
            # installation seals a runtime role, that chain hopped once through
            # it. Never a named profile either way.
            suffix = re.sub(r"[^A-Za-z0-9+=,.@_-]", "-", run_id)[-24:]
            source = _control_role_source_session(
                self._session_factory,
                region=self.config.region,
                expected_account_id=self.config.expected_account_id,
                runtime_role_arn=self.config.runtime_role_arn,
                session_name=f"{self.config.role_session_prefix}-rt-{suffix}",
            )
            sts = source.client("sts", region_name=self.config.region)
            response = sts.assume_role(
                RoleArn=self.config.execution_role_arn,
                RoleSessionName=f"{self.config.role_session_prefix}-{suffix}"[:64],
                DurationSeconds=900,
            )
            credentials = response.get("Credentials") or {}
            assumed_user = response.get("AssumedRoleUser") or {}
            assumed_arn = str(assumed_user.get("Arn") or "")
            role_match = _ROLE_ARN.fullmatch(self.config.execution_role_arn)
            if role_match is None:
                raise ConnectionSpikeLiveConfigurationError(
                    "Round 5 execution role binding is invalid"
                )
            role_name = role_match.group("name").rsplit("/", 1)[-1]
            expected_prefix = (
                f"arn:{role_match.group('partition')}:sts::{self.config.expected_account_id}:"
                f"assumed-role/{role_name}/"
            )
            required = ("AccessKeyId", "SecretAccessKey", "SessionToken", "Expiration")
            if not assumed_arn.startswith(expected_prefix) or any(
                not credentials.get(name) for name in required
            ):
                raise ConnectionSpikeLiveConfigurationError(
                    "STS did not return the sealed Round 5 assumed role"
                )
            expiration = credentials["Expiration"]
            if isinstance(expiration, datetime):
                if expiration.tzinfo is None:
                    expiration = expiration.replace(tzinfo=UTC)
                if expiration <= datetime.now(UTC) + timedelta(seconds=150):
                    raise ConnectionSpikeLiveConfigurationError(
                        "Round 5 assumed credentials expire before the bounded run can settle"
                    )
            assumed = self._session_factory(
                aws_access_key_id=credentials["AccessKeyId"],
                aws_secret_access_key=credentials["SecretAccessKey"],
                aws_session_token=credentials["SessionToken"],
                region_name=self.config.region,
            )
            return _AwsClients(
                ssm=assumed.client("ssm", region_name=self.config.region),
                rds=assumed.client("rds", region_name=self.config.region),
                cloudwatch=assumed.client("cloudwatch", region_name=self.config.region),
                ec2=assumed.client("ec2", region_name=self.config.region),
            )

        return await asyncio.to_thread(assume)

    async def _preflight_runner(self, clients: _AwsClients) -> None:
        response, managed, groups = await asyncio.gather(
            asyncio.to_thread(
                clients.ec2.describe_instances,
                InstanceIds=[self.config.runner_instance_id],
            ),
            asyncio.to_thread(
                clients.ssm.describe_instance_information,
                Filters=[
                    {
                        "Key": "InstanceIds",
                        "Values": [self.config.runner_instance_id],
                    }
                ],
            ),
            asyncio.to_thread(
                clients.ec2.describe_security_groups,
                GroupIds=[self.config.runner_security_group_id],
            ),
        )
        instances = [
            instance
            for reservation in response.get("Reservations") or []
            for instance in reservation.get("Instances") or []
        ]
        managed_instances = managed.get("InstanceInformationList") or []
        security_groups = groups.get("SecurityGroups") or []
        if len(instances) != 1 or len(managed_instances) != 1 or len(security_groups) != 1:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 runner topology did not resolve exactly once"
            )
        instance = instances[0]
        metadata = instance.get("MetadataOptions") or {}
        profile = instance.get("IamInstanceProfile") or {}
        group_ids = {value.get("GroupId") for value in instance.get("SecurityGroups") or []}
        managed_instance = managed_instances[0]
        security_group = security_groups[0]
        if (
            instance.get("InstanceId") != self.config.runner_instance_id
            or (instance.get("State") or {}).get("Name") != "running"
            or instance.get("InstanceType") != self.config.runner_instance_type
            or instance.get("SubnetId") != self.config.runner_subnet_id
            or profile.get("Arn") != self.config.runner_instance_profile_arn
            or group_ids != {self.config.runner_security_group_id}
            or not instance.get("PublicIpAddress")
            or metadata.get("HttpTokens") != "required"
            or managed_instance.get("InstanceId") != self.config.runner_instance_id
            or managed_instance.get("PingStatus") != "Online"
            or managed_instance.get("PlatformType") != "Linux"
            or security_group.get("GroupId") != self.config.runner_security_group_id
            or bool(security_group.get("IpPermissions"))
        ):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 runner topology differs from the sealed contract"
            )

    async def _preflight_targets(
        self,
        rds: Any,
        *,
        targets: Sequence[ConnectionSpikeTarget] | None = None,
    ) -> None:
        for target in targets or self.config.targets:
            if not target.rds_proxy_name:
                continue
            response = await asyncio.to_thread(
                rds.describe_db_proxies,
                DBProxyName=target.rds_proxy_name,
            )
            if target.competitor_id == "aurora_serverless_v2":
                source_response = await asyncio.to_thread(
                    rds.describe_db_clusters,
                    DBClusterIdentifier=target.competitor_target_id,
                )
                source_values = source_response.get("DBClusters") or []
                source = source_values[0] if len(source_values) == 1 else {}
                source_identifier = source.get("DBClusterIdentifier")
                source_resource_id = source.get("DbClusterResourceId")
                source_direct_host = source.get("Endpoint")
            else:
                source_response = await asyncio.to_thread(
                    rds.describe_db_instances,
                    DBInstanceIdentifier=target.competitor_target_id,
                )
                source_values = source_response.get("DBInstances") or []
                source = source_values[0] if len(source_values) == 1 else {}
                source_identifier = source.get("DBInstanceIdentifier")
                source_resource_id = source.get("DbiResourceId")
                source_direct_host = (source.get("Endpoint") or {}).get("Address")
            targets = await asyncio.to_thread(
                rds.describe_db_proxy_targets,
                DBProxyName=target.rds_proxy_name,
            )
            target_groups = await asyncio.to_thread(
                rds.describe_db_proxy_target_groups,
                DBProxyName=target.rds_proxy_name,
            )
            proxies = response.get("DBProxies") or []
            groups = target_groups.get("TargetGroups") or []
            proxy_targets = targets.get("Targets") or []
            target_available = _proxy_target_set_matches(
                target.competitor_id,
                target.competitor_target_id,
                target.competitor_resource_id,
                proxy_targets,
                require_available=True,
            )
            if (
                len(proxies) != 1
                or proxies[0].get("DBProxyName") != target.rds_proxy_name
                or proxies[0].get("DBProxyArn") != target.rds_proxy_arn
                or str(proxies[0].get("Status") or "").lower() != "available"
                or proxies[0].get("Endpoint") != target.endpoint_host
                or proxies[0].get("RoleArn") != target.rds_proxy_role_arn
                or proxies[0].get("RequireTLS") is not True
                or len(proxies[0].get("Auth") or []) != 1
                or (proxies[0].get("Auth") or [{}])[0].get("SecretArn") != target.secret_arn
                or (proxies[0].get("Auth") or [{}])[0].get("IAMAuth") != "DISABLED"
                or (proxies[0].get("Auth") or [{}])[0].get("UserName") != target.database_user
                or (proxies[0].get("Auth") or [{}])[0].get("ClientPasswordAuthType")
                != "POSTGRES_SCRAM_SHA_256"
                or not target_available
                or len(groups) != 1
                or groups[0].get("TargetGroupName") != "default"
                or (groups[0].get("ConnectionPoolConfig") or {}).get("MaxConnectionsPercent")
                != target.rds_proxy_max_connections_percent
                or (groups[0].get("ConnectionPoolConfig") or {}).get("ConnectionBorrowTimeout")
                != target.rds_proxy_borrow_timeout_seconds
                or source_identifier != target.competitor_target_id
                or source_resource_id != target.competitor_resource_id
                or source_direct_host != target.credential_host
            ):
                raise ConnectionSpikeLiveConfigurationError(
                    f"Round 5 {target.lane_id} RDS Proxy binding changed"
                )

    def _runner_request(
        self,
        run_id: str,
        schedule: Sequence[Mapping[str, object]],
        *,
        targets: Sequence[ConnectionSpikeTarget] | None = None,
    ) -> dict[str, object]:
        effective_targets = tuple(targets or self.config.targets)
        value: dict[str, object] = {
            "protocol": RUNNER_PROTOCOL,
            "run_id": run_id,
            "schedule": [dict(item) for item in schedule],
            "targets": [target.runner_value() for target in effective_targets],
            "trust_bundle_path": self.config.trust_bundle_path,
            "trust_bundle_sha256": self.config.trust_bundle_sha256,
        }
        if all(target.credential_sha256 for target in effective_targets):
            by_lane = {target.lane_id: target for target in effective_targets}
            competitor = by_lane["competitor"]
            value["baseline_auth"] = {
                "lakebase": {
                    "credential_sha256": by_lane["lakebase"].credential_sha256,
                },
                "competitor": {
                    "credential_id": (
                        "aurora" if competitor.competitor_id == "aurora_serverless_v2" else "rds"
                    ),
                    "credential_sha256": competitor.credential_sha256,
                },
            }
        try:
            encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        except (TypeError, ValueError) as exc:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 schedule is not JSON serializable"
            ) from exc
        if len(encoded) > 512_000:
            raise ConnectionSpikeLiveConfigurationError("Round 5 runner request is too large")
        return value

    async def _send_command(self, ssm: Any, request: Mapping[str, object]) -> str:
        encoded = base64.urlsafe_b64encode(
            gzip.compress(
                json.dumps(
                    request,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode(),
                mtime=0,
            )
        ).decode()
        command = f"{self.config.runner_path} {encoded}"
        if len(command.encode("utf-8")) > 24_000:
            raise ConnectionSpikeLiveConfigurationError(
                "Compressed Round 5 runner request exceeds the SSM command limit"
            )
        response = await asyncio.to_thread(
            ssm.send_command,
            InstanceIds=[self.config.runner_instance_id],
            DocumentName=self.config.ssm_document_name,
            TimeoutSeconds=int(self.config.command_timeout_seconds),
            Parameters={
                "commands": [command],
                "executionTimeout": [str(int(self.config.command_timeout_seconds))],
            },
            CloudWatchOutputConfig={"CloudWatchOutputEnabled": False},
        )
        command_id = str((response.get("Command") or {}).get("CommandId") or "")
        if not command_id:
            raise ConnectionSpikeLiveOperationError(
                "SSM did not return a Round 5 command identifier"
            )
        return command_id

    async def _wait_for_terminal(self, active: _ActiveCommand) -> Mapping[str, object]:
        async with asyncio.timeout(self.config.command_timeout_seconds):
            while True:
                invocation = await self._get_invocation(active)
                if invocation.get("Status") in _TERMINAL:
                    return invocation
                await self._sleep(self.config.poll_interval_seconds)

    async def _get_invocation(self, active: _ActiveCommand) -> Mapping[str, object]:
        try:
            return await asyncio.to_thread(
                active.clients.ssm.get_command_invocation,
                CommandId=active.command_id,
                InstanceId=self.config.runner_instance_id,
            )
        except Exception as exc:
            code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
            if code == "InvocationDoesNotExist":
                return {"Status": "Pending"}
            raise ConnectionSpikeLiveOperationError("SSM command status could not be read") from exc

    async def _cancel_and_settle(self, active: _ActiveCommand) -> None:
        try:
            async with asyncio.timeout(self.config.settlement_timeout_seconds):
                # Issued inside the bound, not before it. `cancel_command`
                # reaches AWS on a worker thread, and an unreachable endpoint
                # parks this await for as long as that thread runs. Outside the
                # deadline it was the one call the deadline could not cover, so
                # a wedged cancel never reached the polling the timeout guarded
                # and the whole block read as protection while providing none.
                await asyncio.to_thread(
                    active.clients.ssm.cancel_command,
                    CommandId=active.command_id,
                    InstanceIds=[self.config.runner_instance_id],
                )
                while True:
                    invocation = await self._get_invocation(active)
                    output = str(invocation.get("StandardOutputContent") or "")
                    if invocation.get("Status") in _TERMINAL and self._settled(
                        active.run_id, output
                    ):
                        return
                    await self._sleep(min(self.config.poll_interval_seconds, 0.25))
        except TimeoutError as exc:
            raise ConnectionSpikeCleanupError(
                "Round 5 runner cancellation did not confirm cleanup and flock "
                f"release within {self.config.settlement_timeout_seconds:.0f}s"
            ) from exc

    @staticmethod
    def _settled(run_id: str, output: str) -> bool:
        return (
            f"CLEANUP_CONFIRMED:{run_id}" in output and f"RUNNER_FLOCK_RELEASED:{run_id}" in output
        )

    def _require_settlement(self, run_id: str, output: str) -> None:
        if not self._settled(run_id, output):
            raise ConnectionSpikeCleanupError(
                "Round 5 runner omitted cleanup or flock-release evidence"
            )

    def _parse_runner_output(self, run_id: str, output: str) -> dict[str, object]:
        prefix = "RESULT_GZIP_BASE64:"
        candidates = [
            line.removeprefix(prefix) for line in output.splitlines() if line.startswith(prefix)
        ]
        if len(candidates) != 1:
            raise ConnectionSpikeLiveOperationError(
                "Round 5 runner did not return exactly one result payload"
            )
        try:
            compressed = base64.urlsafe_b64decode(candidates[0])
            with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as archive:
                encoded = archive.read(512_001)
            if len(encoded) > 512_000:
                raise ValueError("expanded result is too large")
            result = json.loads(encoded)
        except (ValueError, OSError, json.JSONDecodeError) as exc:
            raise ConnectionSpikeLiveOperationError(
                "Round 5 runner returned malformed result JSON"
            ) from exc
        if not isinstance(result, dict):
            raise ConnectionSpikeLiveOperationError(
                "Round 5 runner returned an unexpected result shape"
            )
        if result.get("protocol") != RUNNER_PROTOCOL or result.get("run_id") != run_id:
            raise ConnectionSpikeLiveOperationError(
                "Round 5 runner result does not match the active run"
            )
        result_lane_ids = {
            str(lane.get("lane_id")) for lane in result.get("lanes", []) if isinstance(lane, dict)
        }
        if result_lane_ids != {target.lane_id for target in self.config.targets}:
            raise ConnectionSpikeLiveOperationError("Round 5 runner result omitted a sealed lane")
        forbidden = ("password", "secret_access_key", "session_token", "access_key_id")
        flattened = json.dumps(result, sort_keys=True).lower()
        if any(name in flattened for name in forbidden):
            raise ConnectionSpikeLiveOperationError(
                "Round 5 runner result contained a forbidden credential field"
            )
        return result

    async def _cloudwatch_witness(
        self,
        cloudwatch: Any,
        started_at: datetime,
        ended_at: datetime,
    ) -> dict[str, object]:
        witness: dict[str, object] = {}
        for target in self.config.targets:
            if not target.rds_proxy_name:
                continue
            response = await asyncio.to_thread(
                cloudwatch.get_metric_statistics,
                Namespace="AWS/RDS",
                MetricName="DatabaseConnections",
                Dimensions=[{"Name": "ProxyName", "Value": target.rds_proxy_name}],
                StartTime=started_at - timedelta(seconds=60),
                EndTime=ended_at + timedelta(seconds=60),
                Period=60,
                Statistics=["Maximum"],
            )
            maxima = [
                float(point["Maximum"])
                for point in response.get("Datapoints", [])
                if "Maximum" in point
            ]
            witness[target.lane_id] = {
                "metric": "DatabaseConnections",
                "maximum": max(maxima) if maxima else None,
                "sample_count": len(maxima),
            }
        return witness

    @staticmethod
    def _validate_run_id(run_id: str) -> None:
        if _RUN_ID.fullmatch(run_id) is None:
            raise ConnectionSpikeLiveConfigurationError("Round 5 run ID is invalid")


def _competitor_manifest_bindings(
    manifest: DemoManifest,
    competitor_id: str,
) -> tuple[str, str, str, str, str, str]:
    if competitor_id not in _COMPETITOR_IDS:
        raise ConnectionSpikeLiveConfigurationError(
            "Round 5 selected AWS competitor is unsupported"
        )
    resources = manifest.require_round5_resources()
    round5_environment = (
        manifest.round_environment(RoundId.SURVIVE_CONNECTION_SPIKE)
        if getattr(manifest, "round_environments", None) is not None
        else None
    )
    if competitor_id == "aurora_serverless_v2":
        environment = (
            round5_environment.aurora if round5_environment is not None else None
        )
        bindings = (
            resources.aurora_cluster_id,
            resources.aurora_cluster_resource_id,
            resources.aurora_direct_host,
            resources.aurora_credential_sha256,
            resources.aurora_proxy_secret_arn,
            (
                environment.security_group_id
                if environment is not None
                else manifest.aws.resources.security_group_id
            ),
        )
    else:
        environment = round5_environment.rds if round5_environment is not None else None
        bindings = (
            (
                environment.instance_id
                if environment is not None
                else manifest.aws.resources.rds_instance_id
            ),
            resources.rds_resource_id,
            resources.rds_direct_host,
            resources.rds_credential_sha256,
            resources.rds_proxy_secret_arn,
            (
                environment.security_group_id
                if environment is not None
                else manifest.aws.resources.rds_security_group_id
            ),
        )
    if any(not isinstance(value, str) or not value for value in bindings):
        raise ConnectionSpikeLiveConfigurationError(
            "Round 5 selected AWS competitor baseline is not completely sealed"
        )
    return bindings


def _warn_if_expired(manifest: DemoManifest) -> None:
    """Report a passed TTL without deciding that Round 5 must stop.

    Same reasoning as the Round 2/3 builder: expiry is a provision-time
    wall-clock value that says nothing about whether the Round 5 sources are
    healthy.  Refusing here was worse than in Round 2, because `app.py` builds
    the Round 5 engine inside `except (RuntimeError, ValueError): return None`,
    so the refusal was swallowed and the round simply vanished from a running
    installation with no diagnosis anywhere.  The sealed-digest, completeness
    and readiness checks below are real signals and still refuse.
    """

    expiry_warning = manifest.expiry_warning()
    if expiry_warning is not None:
        print(f"WARN  {expiry_warning}", flush=True)


def connection_spike_live_config_from_manifest(
    manifest: DemoManifest,
    competitor_id: str,
) -> ConnectionSpikeLiveConfig:
    _warn_if_expired(manifest)
    resources = manifest.require_round5_resources()
    target_id, resource_id, direct_host, credential_sha256, proxy_secret_arn, _ = (
        _competitor_manifest_bindings(manifest, competitor_id)
    )
    config = ConnectionSpikeLiveConfig(
        region=manifest.aws.region,
        expected_account_id=manifest.aws.account_id,
        execution_role_arn=resources.control_role_arn,
        runner_instance_id=resources.runner_instance_id,
        runner_instance_profile_arn=resources.runner_instance_profile_arn,
        runner_subnet_id=resources.runner_subnet_id,
        runner_security_group_id=resources.runner_security_group_id,
        targets=(
            ConnectionSpikeTarget(
                lane_id="lakebase",
                secret_arn="",
                endpoint_host=resources.lakebase_pooled_host,
                credential_host=resources.lakebase_direct_host,
                credential_sha256=resources.lakebase_credential_sha256,
            ),
            ConnectionSpikeTarget(
                lane_id="competitor",
                # The endpoint is replaced after timed setup; credentials remain
                # bound to the selected, stable, sealed Proxy secret.
                secret_arn=proxy_secret_arn,
                endpoint_host=direct_host,
                credential_host=direct_host,
                competitor_id=competitor_id,
                competitor_target_id=target_id,
                competitor_resource_id=resource_id,
                credential_sha256=credential_sha256,
            ),
        ),
        ssm_document_name=resources.ssm_document_name,
        runner_path=resources.runner_path,
        runner_harness_sha256=resources.runner_harness_sha256,
        trust_bundle_path=resources.trust_bundle_path,
        trust_bundle_sha256=resources.trust_bundle_sha256,
        contract_sha256=resources.contract_sha256,
        runtime_role_arn=getattr(manifest.aws, "runtime_role_arn", None) or "",
    )
    expected_config = hashlib.sha256(
        json.dumps(
            {
                "baseline_sha256": resources.baseline_sha256,
                "contract_sha256": resources.contract_sha256,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    if expected_config != resources.config_sha256:
        raise ConnectionSpikeLiveConfigurationError(
            "Round 5 runtime configuration does not match its sealed digest"
        )
    return config


def connection_spike_setup_config_from_manifest(
    manifest: DemoManifest,
    competitor_id: str,
) -> ConnectionSpikeSetupConfig:
    _warn_if_expired(manifest)
    resources = manifest.require_round5_resources()
    (
        target_id,
        resource_id,
        direct_host,
        credential_sha256,
        _proxy_secret_arn,
        competitor_security_group_id,
    ) = _competitor_manifest_bindings(manifest, competitor_id)
    return ConnectionSpikeSetupConfig(
        region=manifest.aws.region,
        expected_account_id=manifest.aws.account_id,
        baseline_control_role_arn=resources.control_role_arn,
        runner_instance_id=resources.runner_instance_id,
        vpc_id=resources.vpc_id,
        proxy_subnet_ids=tuple(resources.proxy_subnet_ids),
        lakebase_direct_host=resources.lakebase_direct_host,
        lakebase_pooled_host=resources.lakebase_pooled_host,
        competitor_id=competitor_id,
        competitor_target_id=target_id,
        competitor_resource_id=resource_id,
        competitor_direct_host=direct_host,
        competitor_security_group_id=competitor_security_group_id,
        runner_security_group_id=resources.runner_security_group_id,
        proxy_service_role_arn=resources.proxy_service_role_arn,
        proxy_service_policy_name=resources.proxy_service_policy_name,
        aurora_proxy_secret_arn=resources.aurora_proxy_secret_arn,
        rds_proxy_secret_arn=resources.rds_proxy_secret_arn,
        deterministic_name_prefix=resources.bout_name_prefix,
        ownership_tags=tuple(sorted(resources.ownership_tags.as_aws_tags().items())),
        runtime_role_arn=getattr(manifest.aws, "runtime_role_arn", None) or "",
        trust_bundle_path=resources.trust_bundle_path,
        trust_bundle_sha256=resources.trust_bundle_sha256,
        runner_public_key_sha256=resources.runner_public_key_sha256,
        baseline_sha256=resources.baseline_sha256,
        lakebase_credential_sha256=resources.lakebase_credential_sha256,
        competitor_credential_sha256=credential_sha256,
        runner_role_arn=getattr(resources, "runner_role_arn", ""),
        proxy_role_permissions_boundary_arn=getattr(
            resources, "per_bout_role_boundary_arn", ""
        ),
        secret_name_prefix=getattr(resources, "secret_name_prefix", ""),
        competitor_master_secret_arn=getattr(
            resources,
            "aurora_master_secret_arn"
            if competitor_id == "aurora_serverless_v2"
            else "rds_master_secret_arn",
            "",
        )
        or "",
        runner_path=resources.runner_path,
        ssm_document_name=resources.ssm_document_name,
        native_role=resources.native_role,
        database_name=manifest.databricks.database,
        proxy_max_connections_percent=(
            resources.frozen_constants.rds_proxy_max_connections_percent
        ),
        proxy_borrow_timeout_seconds=(resources.frozen_constants.rds_proxy_borrow_timeout_seconds),
    )


def _serialize_schedule(arm: ConnectionSpikeArm) -> tuple[dict[str, object], ...]:
    return tuple(
        {
            "lane_id": attempt.lane_id,
            "kind": attempt.kind.value,
            "ordinal": attempt.ordinal,
            "worker_slot": attempt.worker_slot,
            "proof": {
                "row_uuid": str(attempt.proof.row_uuid),
                "value": attempt.proof.value,
                "attempt_id": str(attempt.proof.attempt_id),
            },
            "scheduled_at_ns": attempt.scheduled_at_ns,
        }
        for attempt in arm.schedule.attempts
    )


def _proof(value: object) -> AttemptProof | None:
    if not isinstance(value, Mapping):
        return None
    try:
        from uuid import UUID

        return AttemptProof(
            row_uuid=UUID(str(value["row_uuid"])),
            value=str(value["value"]),
            attempt_id=UUID(str(value["attempt_id"])),
        )
    except (KeyError, TypeError, ValueError):
        return None


def _finalize_raw_result(
    arm: ConnectionSpikeArm,
    raw: Mapping[str, object],
) -> ConnectionSpikeRunResult:
    try:
        release_ns = int(raw["release_ns"])
        first_launch = {
            str(key): int(value) for key, value in dict(raw["first_launch_ns_by_lane"]).items()
        }
        raw_lanes = {
            str(value["lane_id"]): value for value in raw["lanes"] if isinstance(value, Mapping)
        }
    except (KeyError, TypeError, ValueError) as exc:
        raise ConnectionSpikeLiveOperationError("Round 5 runner evidence is incomplete") from exc
    barrier = SharedBarrier(
        release_ns=release_ns,
        first_launch_ns_by_lane=first_launch,
    )
    lanes = {}
    for lane_id in arm.schedule.lane_ids:
        lane = raw_lanes.get(lane_id)
        if lane is None:
            raise ConnectionSpikeLiveOperationError("Round 5 runner evidence omitted a lane")
        observations = []
        scheduled = {str(item.attempt_id): item for item in arm.schedule.lane_attempts(lane_id)}
        for value in lane.get("observations", []):
            if not isinstance(value, Mapping):
                raise ConnectionSpikeLiveOperationError(
                    "Round 5 runner returned a malformed observation"
                )
            attempt_id = str(value.get("attempt_id") or "")
            attempt = scheduled.get(attempt_id)
            if attempt is None:
                raise ConnectionSpikeLiveOperationError(
                    "Round 5 runner returned an unowned observation"
                )
            status = AttemptStatus(str(value.get("status") or "error"))
            started_ns = int(value["started_ns"])
            completed_ns = int(value["completed_ns"])
            observations.append(
                AttemptObservation(
                    attempt_id=attempt.attempt_id,
                    status=status,
                    started_ns=started_ns,
                    completed_ns=completed_ns,
                    response=(
                        attempt.proof
                        if value.get("exact") is True
                        else _proof(value.get("response"))
                    ),
                    committed=(
                        attempt.proof
                        if value.get("exact") is True
                        else _proof(value.get("committed"))
                    ),
                    error=str(value["error"]) if value.get("error") else None,
                )
            )
        raw_witness = lane.get("witness")
        if not isinstance(raw_witness, Mapping):
            raise ConnectionSpikeLiveOperationError(
                "Round 5 runner omitted direct witness evidence"
            )
        clients = tuple(
            RetainedClientWitness(
                client_id=str(client["client_id"]),
                retained=client.get("retained") is True,
                verified=client.get("verified") is True,
                backend_pid=int(client["backend_pid"]),
            )
            for client in raw_witness.get("clients", [])
            if isinstance(client, Mapping)
        )
        witness = ConnectionSpikeWitness(
            clients=clients,
            peak_backend_sessions=int(raw_witness.get("peak_backend_sessions", -1)),
        )
        lanes[lane_id] = finalize_lane(
            arm.schedule,
            lane_id,
            observations,
            witness,
            barrier,
            cleanup_verified=True,
            fairness_verified=barrier.launch_skew_ms <= MAX_LAUNCH_SKEW_MS,
            contracts_verified=raw.get("contracts_verified") is True,
        )
    values = list(lanes.values())
    return ConnectionSpikeRunResult(
        contract_sha256=arm.contract_sha256,
        lanes=lanes,
        comparison=compare_lanes(values[0], values[1]),
    )


class LiveConnectionSpikeEngine:
    """Manager-facing lifecycle wrapper; scoring remains in the pure Round 5 core."""

    def __init__(
        self,
        adapter: LiveConnectionSpikeAdapter,
        *,
        setup_orchestrator: LiveConnectionSpikeSetupOrchestrator | None = None,
        run_id_factory: Callable[[], str] = lambda: f"r5-{uuid4().hex}",
    ) -> None:
        self._adapter = adapter
        self._setup_orchestrator = setup_orchestrator
        self._run_id_factory = run_id_factory
        self._armed: ConnectionSpikeArm | None = None
        self._active_run_id: str | None = None
        self._setup_result: ConnectionSpikeSetupResult | None = None
        self._setup_bout_id: str | None = None
        self._setup_task: asyncio.Task[Any] | None = None
        self._cleanup_bout_id: str | None = None
        self._cleanup_start_lock = asyncio.Lock()

    @property
    def has_timed_setup(self) -> bool:
        return self._setup_orchestrator is not None

    async def setup(
        self,
        bout_id: str,
        fencing_token: int,
        on_progress: SetupProgressCallback | None = None,
    ) -> ConnectionSpikeSetupResult:
        if self._setup_orchestrator is None:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 timed setup orchestration is not configured"
            )
        self._setup_bout_id = bout_id
        setup_task = asyncio.current_task()
        assert setup_task is not None
        self._setup_task = setup_task
        try:
            result = await self._setup_orchestrator.setup(
                bout_id,
                fencing_token,
                on_progress,
            )
            self._setup_result = result
            return result
        finally:
            if self._setup_task is setup_task:
                self._setup_task = None

    async def check(self) -> ConnectionSpikeArm:
        if self._setup_orchestrator is None:
            await self._adapter.check()
        elif self._setup_result is None:
            raise ConnectionSpikeLiveOperationError(
                "Round 5 burst cannot arm before both timed setup stops"
            )
        contract = ConnectionSpikeContract()
        arm = ConnectionSpikeArm(
            arm_id=secrets.token_urlsafe(18),
            contract_sha256=contract.sha256,
            # The remote runner owns the monotonic clock domain. Zero proves all
            # immutable work was constructed before its positive release stamp.
            schedule=build_schedule(
                tuple(target.lane_id for target in self._adapter.config.targets),
                scheduled_at_ns=0,
            ),
        )
        self._armed = arm
        return arm

    async def run(
        self,
        arm: ConnectionSpikeArm,
        on_progress: ProgressCallback | None = None,
    ) -> ConnectionSpikeRunResult:
        if arm is not self._armed:
            raise ConnectionSpikeLiveOperationError(
                "Round 5 arm is stale or belongs to another run"
            )
        run_id = self._run_id_factory()
        self._active_run_id = run_id
        try:
            await self._report(on_progress, "dispatching", "Dispatching the isolated runner")
            targets = self._runtime_targets()
            raw = await self._adapter.execute(
                run_id,
                _serialize_schedule(arm),
                targets=targets,
            )
            result = _finalize_raw_result(arm, raw)
            await self._report(on_progress, "verified", "Runner evidence verified")
            return result
        finally:
            self._active_run_id = None

    async def stop_and_begin_cleanup(self, arm: ConnectionSpikeArm) -> None:
        """Settle active commands and start Round 5 cleanup idempotently.

        The method does not wait for AWS to prove every resource absent.  That
        slow proof remains available through :meth:`wait_for_cleanup_complete`.
        """

        if arm is not self._armed:
            raise ConnectionSpikeCleanupError("Round 5 cleanup arm is stale")
        starter = asyncio.create_task(
            self._stop_and_begin_cleanup_once(),
            name="round5-engine-cleanup-start",
        )
        await asyncio.shield(starter)

    async def _stop_and_begin_cleanup_once(self) -> None:
        async with self._cleanup_start_lock:
            if self._cleanup_bout_id is not None:
                return
            if self._active_run_id is not None:
                await self._adapter.cancel(self._active_run_id)
            setup = self._setup_result
            if setup is None or self._setup_orchestrator is None:
                return
            await self._setup_orchestrator.begin_cleanup(setup.bout_id)
            self._cleanup_bout_id = setup.bout_id
            self._setup_result = None

    async def stop_setup_and_begin_cleanup(self, bout_id: str) -> None:
        """Attach a cancelled/incomplete timed setup to background cleanup."""

        LiveConnectionSpikeAdapter._validate_run_id(bout_id)
        if bout_id not in {self._setup_bout_id, self._cleanup_bout_id}:
            raise ConnectionSpikeCleanupError("Round 5 setup cleanup bout is stale")
        starter = asyncio.create_task(
            self._stop_setup_and_begin_cleanup_once(bout_id),
            name=f"round5-engine-setup-cleanup-start-{bout_id}",
        )
        await asyncio.shield(starter)

    async def _stop_setup_and_begin_cleanup_once(self, bout_id: str) -> None:
        setup_task = self._setup_task
        if setup_task is not None and setup_task is not asyncio.current_task():
            setup_task.cancel()
            await asyncio.gather(setup_task, return_exceptions=True)
        async with self._cleanup_start_lock:
            if self._cleanup_bout_id is not None:
                if self._cleanup_bout_id != bout_id:
                    raise ConnectionSpikeCleanupError(
                        "Another Round 5 cleanup is already active"
                    )
                return
            if self._setup_orchestrator is None:
                raise ConnectionSpikeCleanupError(
                    "Round 5 timed setup orchestration is not configured"
                )
            await self._setup_orchestrator.begin_cleanup(bout_id)
            self._cleanup_bout_id = bout_id
            if self._setup_result is not None and self._setup_result.bout_id == bout_id:
                self._setup_result = None

    def proxy_delete_accepted(self) -> bool:
        """Whether the slow cleanup crossed its durable Proxy-delete handoff."""

        return (
            self._cleanup_bout_id is not None
            and self._setup_orchestrator is not None
            and self._setup_orchestrator.proxy_delete_accepted(self._cleanup_bout_id)
        )

    def proxy_name_for_bout(self, bout_id: str) -> str:
        """Name the Proxy a bout would have created, or "" when it cannot be named.

        Only ever used to make a leak findable, so it answers with a blank
        rather than an exception on every path where the answer is not known.
        A reporting call that raises turns a money warning into a second fault,
        and "" is reported honestly by the caller as a Proxy it cannot name.
        """

        if self._setup_orchestrator is None:
            return ""
        try:
            return self._setup_orchestrator.proxy_name_for_bout(bout_id)
        except Exception:
            return ""

    async def wait_for_proxy_delete_accepted(self) -> None:
        """Await Proxy delete API acceptance without awaiting full AWS absence."""

        if self._cleanup_bout_id is None or self._setup_orchestrator is None:
            raise ConnectionSpikeCleanupError("Round 5 cleanup has not been started")
        await self._setup_orchestrator.wait_for_proxy_delete_accepted(
            self._cleanup_bout_id
        )

    async def wait_for_cleanup_complete(self) -> None:
        """Await full exact absence and reverse cleanup in the background task."""

        if self._cleanup_bout_id is None or self._setup_orchestrator is None:
            raise ConnectionSpikeCleanupError("Round 5 cleanup has not been started")
        await self._setup_orchestrator.wait_for_cleanup_complete(self._cleanup_bout_id)

    async def cancel_and_cleanup(self, arm: ConnectionSpikeArm) -> None:
        """Compatibility boundary that starts and then fully awaits cleanup."""

        await self.stop_and_begin_cleanup(arm)
        if self._cleanup_bout_id is not None:
            await self.wait_for_cleanup_complete()

    async def cancel_setup_and_settle(self, bout_id: str) -> None:
        """Compatibility boundary that fully awaits incomplete-setup cleanup."""

        await self.stop_setup_and_begin_cleanup(bout_id)
        await self.wait_for_cleanup_complete()

    async def reconcile_failed_cleanup(
        self,
        bout_id: str,
        current_fencing_token: int,
    ) -> None:
        if self._setup_orchestrator is None:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 timed setup orchestration is not configured"
            )
        await self._setup_orchestrator.reconcile_failed_cleanup(
            bout_id,
            current_fencing_token,
        )
        if self._setup_result is not None and self._setup_result.bout_id == bout_id:
            self._setup_result = None

    async def unresolved_bout_ids(self) -> tuple[str, ...]:
        """Return durable unresolved Round 5 bout IDs without mutating them."""

        if self._setup_orchestrator is None:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 timed setup orchestration is not configured"
            )
        return await self._setup_orchestrator.unresolved_bout_ids()

    async def assert_no_unresolved_bouts(
        self,
        new_bout_id: str,
        current_fencing_token: int,
    ) -> None:
        """Block fresh setup while any prior Round 5 journal remains unresolved."""

        if self._setup_orchestrator is None:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 timed setup orchestration is not configured"
            )
        await self._setup_orchestrator.assert_no_unresolved_bouts(
            new_bout_id,
            current_fencing_token,
        )

    def _runtime_targets(self) -> tuple[ConnectionSpikeTarget, ...] | None:
        setup = self._setup_result
        if setup is None:
            return None
        configured = {target.lane_id: target for target in self._adapter.config.targets}
        lakebase = configured["lakebase"]
        competitor = configured["competitor"]
        return (
            ConnectionSpikeTarget(
                lane_id="lakebase",
                secret_arn="",
                endpoint_host=setup.lakebase.endpoint_host,
                credential_host=lakebase.credential_host,
                credential_sha256=setup.lakebase.credential_sha256,
            ),
            ConnectionSpikeTarget(
                lane_id="competitor",
                secret_arn=setup.competitor.secret_arn,
                endpoint_host=setup.competitor.endpoint_host,
                credential_host=competitor.credential_host,
                competitor_id=competitor.competitor_id,
                competitor_target_id=competitor.competitor_target_id,
                competitor_resource_id=competitor.competitor_resource_id,
                credential_sha256=setup.competitor.credential_sha256,
            ),
        )

    @staticmethod
    async def _report(
        callback: ProgressCallback | None,
        phase: str,
        status: str,
    ) -> None:
        if callback is None:
            return
        await callback(
            ConnectionSpikeLiveProgress(
                phase=phase,
                status=status,
                occurred_at=datetime.now(UTC),
            )
        )


def build_connection_spike_live_engine(
    manifest: DemoManifest | None = None,
    *,
    competitor_id: str,
    session_factory: SessionFactory = boto3.Session,
    journal: CreationJournalStore | None = None,
    fence: FenceGuard | None = None,
    fresh_lakebase_host: FreshLakebaseHost | None = None,
) -> LiveConnectionSpikeEngine:
    effective_manifest = manifest or load_manifest()
    config = connection_spike_live_config_from_manifest(effective_manifest, competitor_id)
    if config.runner_harness_sha256 and config.runner_harness_sha256 != runner_harness_sha256():
        raise ConnectionSpikeLiveConfigurationError(
            "Installed Round 5 runner assets do not match the sealed harness digest"
        )
    if journal is None or fence is None or fresh_lakebase_host is None:
        raise ConnectionSpikeLiveConfigurationError(
            "Round 5 live engine requires the durable journal, active fence, and fresh host reader"
        )
    setup = LiveConnectionSpikeSetupOrchestrator(
        connection_spike_setup_config_from_manifest(effective_manifest, competitor_id),
        journal=journal,
        fence=fence,
        fresh_lakebase_host=fresh_lakebase_host,
        session_factory=session_factory,
    )
    return LiveConnectionSpikeEngine(
        LiveConnectionSpikeAdapter(config, session_factory=session_factory),
        setup_orchestrator=setup,
    )
