from __future__ import annotations

import asyncio
import base64
import gzip
import hashlib
import io
import json
import logging
import math
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
from botocore.config import Config
from botocore.exceptions import (
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    ReadTimeoutError,
)

# Round 5 V4 retains the setup phase -- provisioning the
# RDS Proxy an AWS lane needs and Lakebase does not -- is unchanged and still lives
# in `.connection_spike`. Both modules export `ConnectionSpikeArm`, `finalize_lane`
# and `compare_lanes`, so the v2 names are aliased rather than shadowing the setup
# phase's while that removal is staged.
from .connection_fanin import (
    ADVISORY_TELEMETRY_CODES,
    FANIN_PROTOCOL,
    FANIN_SCHEMA_VERSION,
    PROGRESS_PREFIX,
    RUNTIME_LANE_IDS,
    SAFETY_EVIDENCE_VERSION,
    WORKER_COUNT,
    CapacityPreflight,
    ConnectionSpikeLaneResult,
    FanInError,
    FanInProgress,
    evaluate_capacity_preflight,
    fanin_config_sha256,
    fanin_generator_sha256,
    fanin_preflight_request,
)
from .connection_fanin import (
    RUN_TIMEOUT_SECONDS as FANIN_RUN_TIMEOUT_SECONDS,
)
from .connection_fanin import (
    RUNNER_INSTANCE_TYPE as FANIN_RUNNER_INSTANCE_TYPE,
)
from .connection_fanin import TRUST_BUNDLE_PATH as FANIN_TRUST_BUNDLE_PATH
from .connection_fanin import ConnectionSpikeArm as FanInArm
from .connection_fanin import ConnectionSpikeContract as FanInContract
from .connection_fanin import ConnectionSpikeRunResult as FanInRunResult
from .connection_fanin import (
    capacity_model_sha256 as fanin_capacity_model_sha256,
)
from .connection_fanin import compare_lanes as compare_fanin_lanes
from .connection_fanin import finalize_lane as finalize_fanin_lane
from .connection_spike import (
    AttemptProof,
    ConnectionSpikeContract,
    PublicSetupEvidence,
    SetupLaneObservation,
    SetupLaneStatus,
    SetupPhaseArm,
    SetupStopGateEvidence,
    arm_setup_phase,
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
from .models import CompetitorId, RoundId
from .round5_control import (
    ROUND5_ARM_STAGE_DEADLINE_SECONDS,
    Round5ControlBinding,
    Round5ControlEvent,
    Round5ControlKind,
    Round5ResidentTransport,
    canonical_request_sha256,
)
from .safe_change import DEFAULT_CANCEL_TEARDOWN_SECONDS, abandon_on_cancel

logger = logging.getLogger(__name__)

_AWS_CLIENT_CONFIG = Config(
    connect_timeout=5,
    read_timeout=30,
    retries={"mode": "standard", "max_attempts": 4},
)


def _dispatch_timeout_seconds(request: Mapping[str, object], default: float) -> float:
    """How long this particular dispatch is given, chosen by what it is.

    A fan-in dispatch -- preflight or bout -- is bounded by the runner's own budget and
    needs the longer window. Everything else keeps the 120-second contract it shares with
    the setup phase. Read from the request rather than from adapter state so a caller
    cannot get the window wrong by sending one protocol while configured for another.
    """

    if request.get("protocol") == FANIN_PROTOCOL:
        return FANIN_SSM_TIMEOUT_SECONDS
    return default


SETUP_RUNNER_PROTOCOL = "connection-spike-setup-v1"
#: The line the runner prints when it refuses, and the shape of the token after
#: it. `runner/connection_spike_runner.py` raises `RunnerContractError` with a
#: fixed snake_case word -- `baseline_auth_hash_invalid`, `setup_lane_invalid`
#: and so on -- chosen by this repository, naming no host, ARN or credential.
#: That makes it the one part of the runner's output that may be repeated.
_RUNNER_ERROR_PREFIX = "RUNNER_ERROR:"
_RUNNER_ERROR_CODE = re.compile(r"[a-z0-9_]{1,64}")
SSM_TIMEOUT_SECONDS = 120.0
#: The same number, named for the agreement it is half of.
#:
#: `runner.connection_spike_runner.SSM_COMMAND_TIMEOUT_SECONDS` must equal this, and
#: the runner's own `SETUP_VERIFY_DEADLINE_SECONDS` must fall strictly inside it. If
#: the runner were allowed to verify right up to this boundary, SSM would time the
#: command out while the runner still held a transaction open -- and the failure
#: would arrive as "the command did not complete", which says nothing about the
#: transaction that was actually mid-flight. Kept as a distinct name from
#: `SSM_TIMEOUT_SECONDS` because it is a cross-boundary contract rather than a local
#: preference: changing it means changing the runner in the same commit.
SETUP_SSM_TIMEOUT_SECONDS = SSM_TIMEOUT_SECONDS
#: The window a fan-in dispatch gets, which cannot be the 120-second one above.
#:
#: That number is an agreement with the setup phase about how long a runner may hold a
#: transaction open. A fan-in bout is a different kind of work with a different bound:
#: the runner budgets itself `RUN_TIMEOUT_SECONDS` for a full ramp to 10,000 clients per
#: lane, a hold, and sampling, and ends the bout itself when it expires. Derived from
#: that constant rather than restated, so the two cannot drift into an SSM timeout that
#: kills a bout the runner was still measuring -- a failure that arrives as "the command
#: did not complete" and says nothing about the 10,000 clients that were up at the time.
#:
#: The margin covers what SSM adds around the script: agent pickup, and the status and
#: stdout propagation the server can only read afterwards.
FANIN_SSM_MARGIN_SECONDS = 60.0
FANIN_SSM_TIMEOUT_SECONDS = FANIN_RUN_TIMEOUT_SECONDS + FANIN_SSM_MARGIN_SECONDS
DISPATCH_CAPSULE_SAFETY_SECONDS = FANIN_SSM_TIMEOUT_SECONDS + 60
DISPATCH_CAPSULE_REFRESH_LEAD_SECONDS = 60
DISPATCH_CAPSULE_REFRESH_INTERVAL_SECONDS = 30
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
    # The fan-in protocol split the harness into three files, and the installer only
    # ever copied one. A module missing from this tuple is not a soft failure: the
    # runner dies on ModuleNotFoundError inside an SSM command, and the operator sees
    # "Round 5 runner configuration command failed" with no mention of an import.
    "round5_fanin.py",
    "external_io.py",
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
            and all(str(target.get("TrackedClusterId") or "") == target_id for target in instances)
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
    #: The digest of the credential a second role uses to watch this lane's pool from
    #: its own direct connection. Multiplexing is what Round 5 claims, and it is proved
    #: by observing the backend session count while 10,000 clients are held, so the
    #: fan-in request names one per lane and the runner refuses a request without it.
    #: Sealed at install time and not re-minted per bout, which is why it lives on the
    #: lane binding beside the client digest rather than on a bout's setup result.
    observer_credential_sha256: str = ""

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
        if (
            self.observer_credential_sha256
            and re.fullmatch(r"[0-9a-f]{64}", self.observer_credential_sha256) is None
        ):
            raise ConnectionSpikeLiveConfigurationError(
                f"Round 5 {self.lane_id} observer credential digest is invalid"
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
    runner_instance_type: str = FANIN_RUNNER_INSTANCE_TYPE
    ssm_document_name: str = "AWS-RunShellScript"
    runner_path: str = RUNNER_PATH
    resident_control_queue_url: str = ""
    resident_control_secret_arn: str = ""
    resident_installation_id: str = ""
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
        if (
            self.resident_control_secret_arn
            and _SECRET_ARN.fullmatch(self.resident_control_secret_arn) is None
        ):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 resident control secret ARN is invalid"
            )
        if self.resident_control_queue_url and not self.resident_installation_id:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 resident installation identity is missing"
            )
        if _INSTANCE_ID.fullmatch(self.runner_instance_id) is None:
            raise ConnectionSpikeLiveConfigurationError("Round 5 runner instance ID is invalid")
        if (
            not self.runner_instance_profile_arn
            or not self.runner_subnet_id
            or not self.runner_security_group_id
            or self.runner_instance_type != FANIN_RUNNER_INSTANCE_TYPE
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
        if self.resident_control_queue_url and not self.resident_control_queue_url.endswith(
            ".fifo"
        ):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 resident FIFO control queue URL is missing"
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
    competitor_runner_instance_id: str
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
    proxy_security_group_id: str
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
        if (
            _INSTANCE_ID.fullmatch(self.runner_instance_id) is None
            or _INSTANCE_ID.fullmatch(self.competitor_runner_instance_id) is None
            or self.runner_instance_id == self.competitor_runner_instance_id
        ):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 setup requires two distinct physical runner instance IDs"
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
            or not self.proxy_security_group_id
            or not self.lakebase_direct_host
            or not self.lakebase_pooled_host
        ):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 setup baseline topology is incomplete"
            )
        proxy_role = _ROLE_ARN.fullmatch(self.proxy_service_role_arn)
        if proxy_role is None or proxy_role.group("account") != self.expected_account_id:
            raise ConnectionSpikeLiveConfigurationError("Round 5 Proxy service role ARN is invalid")
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
    # Real CreateDBProxy request-boundary stamp (competitor only). None for lanes
    # that issue no setup-phase request the contract can score.
    create_db_proxy_requested_ns: int | None = None

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


#: Called the moment one lane's setup verifies, with that lane's stop.
#:
#: The stop carries the endpoint and credential digest that lane's ramp needs, so a caller can
#: start the lane's 10,000 immediately instead of waiting for the other lane's setup. Awaited inside
#: the lane's own task, so a failure to start a ramp fails the lane it belongs to.
SetupLaneReadyCallback = Callable[[ConnectionSpikeSetupLaneStop], Awaitable[None]]
#: Called after CreateDBProxy returns the provider-assigned endpoint but before
#: the exact Proxy gate.  The resident may parse the exact late-bound request
#: and prepare workers, but its release gate remains closed.
SetupLaneStageCallback = Callable[[ConnectionSpikeSetupLaneStop], Awaitable[None]]


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
    expires_at: datetime


@dataclass
class _SetupResources:
    names: ConnectionSpikeSetupNames
    secret_arn: str = ""
    proxy_role_arn: str = ""
    proxy_security_group_id: str = ""
    rds_security_group_id: str = ""
    proxy_endpoint: str = ""
    # Monotonic stamp taken at the CreateDBProxy request boundary (before the SDK
    # call leaves this process), so the setup contract can score the real
    # bell -> CreateDBProxy request latency instead of the workflow_launched
    # lower bound.
    proxy_create_requested_ns: int | None = None
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
    runner_instance_id: str
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


@dataclass(frozen=True)
class ConnectionSpikeWarmSetupContext:
    clients: _SetupAwsClients
    rds_security_group_id: str
    proxy_security_group_id: str
    observed_at: datetime


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
    expires_at: datetime


@dataclass(frozen=True)
class _ActiveCommand:
    run_id: str
    command_id: str
    clients: _AwsClients
    job_id: str | None = None


@dataclass(frozen=True)
class _PendingCommand:
    run_id: str
    send_task: asyncio.Task[str]
    clients: _AwsClients
    job_id: str | None = None


class SessionFactory(Protocol):
    def __call__(self, **kwargs: Any) -> Any: ...


Sleeper = Callable[[float], Awaitable[None]]
ProgressCallback = Callable[[ConnectionSpikeLiveProgress], Awaitable[None]]


def runner_asset_sha256s(root: Path | None = None) -> dict[str, str]:
    """Return the source digest of each file in the installed runner contract."""
    asset_root = root or Path(__file__).resolve().parents[1] / "runner"
    return {
        name: hashlib.sha256((asset_root / name).read_bytes()).hexdigest() for name in RUNNER_ASSETS
    }


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
        "resident_control_queue_url": config.resident_control_queue_url,
        "resident_control_secret_arn": config.resident_control_secret_arn,
        "resident_installation_id": config.resident_installation_id,
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
        #: Bouts whose untimed preparation has already run, mapped to the fencing token it ran
        #: under. Keyed by token and not just by bout so a re-armed bout under a new fence
        #: prepares again rather than trusting work done for a fence that has since been lost.
        self._prepared: dict[str, int] = {}
        #: What that preparation discovered, kept for the bout that will use it.
        #:
        #: `_preflight_baseline` writes onto the resources it is handed -- the database's security
        #: group, the sealed secret and proxy role, anything an interrupted bout left journalled --
        #: so preparing against one object and then building the bout on another authorized the
        #: per-bout rules against an empty source group id. Reusing the prepared object is what
        #: makes skipping the preflight safe.
        self._prepared_resources: dict[str, _SetupResources] = {}
        self._prepared_specs: dict[str, tuple[ResourceSpec, ...]] = {}
        self._resources_by_bout: dict[str, _SetupResources] = {}
        self._warm_context: ConnectionSpikeWarmSetupContext | None = None
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

    async def prepare(self, bout_id: str, fencing_token: int) -> None:
        """Bind the current warm context to a bout using coordination only."""

        async with self._lock:
            if bout_id in self._results:
                raise ConnectionSpikeLiveOperationError(
                    "Round 5 setup already completed for this bout"
                )
            if self._prepared.get(bout_id) == fencing_token:
                return
            warm = self._warm_context
            if warm is None or warm.clients.expires_at <= datetime.now(UTC) + timedelta(
                seconds=SETUP_DEADLINE_SECONDS + 60
            ):
                raise ConnectionSpikeLiveOperationError(
                    "Round 5 automatic warm launch context is unavailable"
                )
            names = self.names_for_bout(
                self.config.deterministic_name_prefix,
                bout_id,
                self.config.secret_name_prefix or "anti-demo-round5",
            )
            scope = CreationScope(bout_id, fencing_token, self.config.baseline_sha256)
            await self._fence.assert_current(scope)
            resources = _SetupResources(
                names,
                secret_arn=self.config.proxy_secret_arn,
                proxy_role_arn=self.config.proxy_service_role_arn,
                proxy_security_group_id=warm.proxy_security_group_id,
                rds_security_group_id=warm.rds_security_group_id,
            )
            self._require_rule_bindings(resources)
            coordinator, specs = self._coordinator(scope, warm.clients, resources)
            self._coordinators[bout_id] = coordinator
            self._scopes[bout_id] = scope
            self._prepared[bout_id] = fencing_token
            self._prepared_resources[bout_id] = resources
            self._prepared_specs[bout_id] = specs
            self._resources_by_bout[bout_id] = resources

    async def warm(self, generation: int) -> ConnectionSpikeWarmSetupContext:
        """Perform every slow setup prerequisite before a session can claim."""

        clients = await self._assumed_clients(f"warm-{generation}")
        source, lakebase_managed, competitor_managed = await asyncio.gather(
            self._read_competitor_source(clients),
            self._call(
                clients.ssm.describe_instance_information,
                Filters=[{"Key": "InstanceIds", "Values": [self.config.runner_instance_id]}],
            ),
            self._call(
                clients.ssm.describe_instance_information,
                Filters=[
                    {
                        "Key": "InstanceIds",
                        "Values": [self.config.competitor_runner_instance_id],
                    }
                ],
            ),
        )
        lakebase_runners = lakebase_managed.get("InstanceInformationList") or []
        competitor_runners = competitor_managed.get("InstanceInformationList") or []
        if (
            source.identifier != self.config.competitor_target_id
            or source.resource_id != self.config.competitor_resource_id
            or source.direct_host != self.config.competitor_direct_host
            or source.status != "available"
            or source.vpc_id != self.config.vpc_id
            or source.security_group_ids != (self.config.competitor_security_group_id,)
            or len(lakebase_runners) != 1
            or lakebase_runners[0].get("InstanceId") != self.config.runner_instance_id
            or lakebase_runners[0].get("PingStatus") != "Online"
            or len(competitor_runners) != 1
            or competitor_runners[0].get("InstanceId") != self.config.competitor_runner_instance_id
            or competitor_runners[0].get("PingStatus") != "Online"
        ):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 warm source or physical runner identity changed"
            )
        await self._verify_proxy_service_role(clients)
        await self._verify_static_proxy_network(clients)
        await self._discover_orphaned_addons(
            clients,
            self.config.competitor_security_group_id,
            include_legacy=False,
        )
        context = ConnectionSpikeWarmSetupContext(
            clients=clients,
            rds_security_group_id=self.config.competitor_security_group_id,
            proxy_security_group_id=self.config.proxy_security_group_id,
            observed_at=datetime.now(UTC),
        )
        self._warm_context = context
        return context

    async def _verify_static_proxy_network(self, clients: _SetupAwsClients) -> None:
        groups = await self._call(
            clients.ec2.describe_security_groups,
            GroupIds=[self.config.proxy_security_group_id],
        )
        values = groups.get("SecurityGroups") or []
        if len(values) != 1:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 static Proxy network fixture did not resolve exactly once"
            )
        group = values[0]
        ingress = group.get("IpPermissions") or []
        egress = group.get("IpPermissionsEgress") or []

        def exact_rule(
            rules: Sequence[Mapping[str, object]],
            peer_key: str,
            peer_id: str,
        ) -> bool:
            if len(rules) != 1:
                return False
            rule = rules[0]
            peers = rule.get(peer_key) or []
            return (
                rule.get("IpProtocol") == "tcp"
                and rule.get("FromPort") == 5432
                and rule.get("ToPort") == 5432
                and len(peers) == 1
                and peers[0].get("GroupId") == peer_id
            )

        if (
            group.get("GroupId") != self.config.proxy_security_group_id
            or not exact_rule(
                ingress,
                "UserIdGroupPairs",
                self.config.runner_security_group_id,
            )
            or not exact_rule(
                egress,
                "UserIdGroupPairs",
                self.config.competitor_security_group_id,
            )
        ):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 static Proxy network fixture differs from the warm contract"
            )

    def _require_rule_bindings(self, resources: _SetupResources) -> None:
        """Refuse now if a per-bout security-group rule would name an empty source group.

        Four rules are authorized during the timed setup and each references two groups. The
        per-bout proxy group does not exist yet, so it cannot be checked here; the other two can,
        and they are the two that were empty when a live bout died seconds after the bell on
        `AuthorizeSecurityGroupEgress ... Source group ID missing`.

        Named by binding rather than by AWS operation. The provider's own message sends an operator
        to look at EC2 for a fault that is in this process.
        """

        missing = [
            name
            for name, value in (
                ("the competitor database's security group", resources.rds_security_group_id),
                ("the sealed runner security group", self.config.runner_security_group_id),
            )
            if not str(value or "").strip()
        ]
        if missing:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 cannot arm: the per-bout security-group rules would name no source group "
                f"for {', and '.join(missing)}. Nothing was created and no clock was started."
            )

    async def setup(
        self,
        bout_id: str,
        fencing_token: int,
        on_progress: SetupProgressCallback | None = None,
        on_lane_ready: SetupLaneReadyCallback | None = None,
        on_lane_stage: SetupLaneStageCallback | None = None,
        *,
        t0_ns: int | None = None,
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
            prepared = self._prepared_resources.pop(bout_id, None)
            scope = self._scopes.get(bout_id)
            coordinator = self._coordinators.get(bout_id)
            specs = self._prepared_specs.get(bout_id)
            warm = self._warm_context
            if (
                self._prepared.get(bout_id) != fencing_token
                or prepared is None
                or scope is None
                or coordinator is None
                or specs is None
                or warm is None
                or warm.clients.expires_at <= datetime.now(UTC)
            ):
                raise ConnectionSpikeLiveOperationError(
                    "Round 5 has no current warm launch context; the bell is blocked "
                    "while automatic warming repairs it"
                )
            resources = prepared
            clients = warm.clients

            # Journal-before-AWS durability for the timed CreateDBProxy mutation
            # is satisfied HERE, before the authoritative comparison T0 is
            # captured and before the shared gate releases. The intent commit is
            # ~3 coordination-store round trips (fence assert + duplicate-ordinal
            # read + durable intent write) that used to sit *after* T0 on the
            # timed path and structurally blew the 100 ms bell-relative
            # create_db_proxy_window (live proxy CREATE_INTENT durable wall was
            # ~163 ms after T0). Pre-committing it before T0 keeps the reference
            # (bell/T0) and the 100 ms budget intact while removing every awaited
            # journal/fence op from the post-gate path: the first awaited call the
            # competitor lane makes after the gate releases is the direct boto3
            # CreateDBProxy request itself. This is not a pre-created Proxy -- no
            # AWS mutation happens before T0, only the durable coordination write
            # -- and it introduces no new orphan class because the intent is still
            # journalled within this same ``setup()``/bell invocation.
            proxy_spec = next(
                (spec for spec in specs if spec.resource_kind == "rds_proxy"), None
            )
            if proxy_spec is None:
                raise ConnectionSpikeLiveOperationError(
                    "Round 5 competitor specs omitted the timed CreateDBProxy mutation"
                )
            proxy_intent = await coordinator.precommit_intent(scope, proxy_spec)

            gate = asyncio.Event()
            t0_box: list[int] = []
            # Two-party launch-stamp barrier: both lane tasks capture
            # workflow_launched_ns immediately after the gate releases and rendezvous
            # here before either runs a downstream callback. Without it, whichever
            # task the loop resumes first would run its synchronous ``on_lane_ready``
            # prefix (or absorb a GC pause) before the sibling could stamp, inflating
            # the inter-lane skew metric even though the barrier release was fair.
            launch_barrier = asyncio.Barrier(2)
            lakebase_task = asyncio.create_task(
                self._setup_lakebase(
                    bout_id, clients, gate, t0_box, on_progress, on_lane_ready, launch_barrier
                )
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
                    on_lane_ready,
                    on_lane_stage,
                    launch_barrier,
                    proxy_intent,
                )
            )
            comparison_t0_ns = self._monotonic_ns() if t0_ns is None else t0_ns
            arm = arm_setup_phase(
                ("lakebase", "competitor"),
                t0_ns=comparison_t0_ns,
            )
            t0_box.append(comparison_t0_ns)
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
                    lakebase, competitor = await asyncio.gather(lakebase_task, competitor_task)
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
                    names=resources.names,
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
                    identifier=lambda: self._cancelled_setup_identifier(bout_id, specs, resources),
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
        parts.append(f"proxy target {self.config.competitor_id} {self.config.competitor_target_id}")
        if resources.proxy_security_group_id:
            parts.append(f"observed security group {resources.proxy_security_group_id}")
        if resources.security_group_rule_ids:
            parts.append(
                "observed security group rules " + ",".join(resources.security_group_rule_ids)
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
                f"in-flight SSM commands on {self.config.runner_instance_id} " + ",".join(commands)
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
            scope = self._scopes.get(bout_id)
            resources = getattr(self, "_resources_by_bout", {}).get(bout_id)
            if scope is not None and resources is not None:
                cleanup_clients = await self._assumed_clients(
                    f"cleanup-{bout_id}",
                    minimum_lifetime_seconds=45 * 60 + 60,
                )
                coordinator, specs = self._coordinator(
                    scope,
                    cleanup_clients,
                    resources,
                )
                self._coordinators[bout_id] = coordinator
                self._prepared_specs[bout_id] = specs
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
        getattr(self, "_resources_by_bout", {}).pop(bout_id, None)
        getattr(self, "_prepared_specs", {}).pop(bout_id, None)
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

    async def prove_bout_absent(self, bout_id: str) -> None:
        """Prove a journal-free inherited claim left no provider resource."""

        LiveConnectionSpikeAdapter._validate_run_id(bout_id)
        if bout_id in await self._journal.unresolved_bout_ids():
            raise ConnectionSpikeCleanupError("Round 5 inherited claim still has journal debt")
        clients = await self._assumed_clients(
            f"cleanup-{bout_id}",
            minimum_lifetime_seconds=45 * 60 + 60,
        )
        names = self.names_for_bout(
            self.config.deterministic_name_prefix,
            bout_id,
            self.config.secret_name_prefix or "anti-demo-round5",
        )
        try:
            response = await self._call(
                clients.rds.describe_db_proxies,
                DBProxyName=names.proxy_name,
            )
        except Exception as exc:
            if self._error_code(exc) != "DBProxyNotFoundFault":
                raise
            response = {"DBProxies": []}
        if response.get("DBProxies"):
            raise ConnectionSpikeCleanupError("Round 5 inherited claim still owns an RDS Proxy")
        await self._discover_orphaned_addons(
            clients,
            self.config.competitor_security_group_id,
            include_legacy=False,
        )

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
        on_lane_ready: SetupLaneReadyCallback | None = None,
        launch_barrier: asyncio.Barrier | None = None,
    ) -> ConnectionSpikeSetupLaneStop:
        await gate.wait()
        launched_ns = self._monotonic_ns()
        # Rendezvous so the sibling lane stamps its own launch before either lane
        # runs downstream work; keeps the inter-lane skew metric a pure function of
        # the shared gate release, not of any post-launch synchronous prefix.
        if launch_barrier is not None:
            await launch_barrier.wait()

        async def report(phase: str, status: str = "running") -> None:
            await self._report(
                on_progress,
                "lakebase",
                phase,
                status,
                t0_ns=t0_box[0],
            )

        stopped_ns = self._monotonic_ns()
        stop = ConnectionSpikeSetupLaneStop(
            lane_id="lakebase",
            launched_ns=launched_ns,
            stopped_ns=stopped_ns,
            credential_sha256=self.config.lakebase_credential_sha256,
            endpoint_host=self.config.lakebase_pooled_host,
        )
        # Ready now, not when the other lane is. Lakebase verifies its included pool in seconds
        # and has no reason to wait on a Proxy build.
        if on_lane_ready is not None:
            await on_lane_ready(stop)
        await self._report(
            on_progress,
            "lakebase",
            "setup_stop",
            "verified",
            setup_elapsed_ms=(stopped_ns - t0_box[0]) / 1_000_000,
        )
        return stop

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
        on_lane_ready: SetupLaneReadyCallback | None = None,
        on_lane_stage: SetupLaneStageCallback | None = None,
        launch_barrier: asyncio.Barrier | None = None,
        proxy_intent: JournalEvent | None = None,
    ) -> ConnectionSpikeSetupLaneStop:
        await gate.wait()
        launched_ns = self._monotonic_ns()
        # Rendezvous so both lanes stamp workflow_launched_ns before either runs a
        # downstream callback (see _setup_lakebase). Nothing awaited between the
        # gate release and this stamp/barrier, so the CreateDBProxy request below
        # is the first awaited call after launch.
        if launch_barrier is not None:
            await launch_barrier.wait()

        async def report(phase: str, status: str = "running") -> None:
            await self._report(
                on_progress,
                "competitor",
                phase,
                status,
                t0_ns=t0_box[0],
            )

        phases = {
            "rds_proxy": "creating_proxy",
            "proxy_target_group": "freezing_proxy_settings",
            "proxy_target": "registering_proxy_target",
        }
        for spec in specs:
            phase = phases[spec.resource_kind]
            if spec.resource_kind == "rds_proxy":
                # The CreateDBProxy CREATE_INTENT was durably pre-committed before
                # the bell T0 (see setup()). No journal/fence/progress/log write
                # lies between the gate release and this first timed AWS mutation:
                # complete_prestaged issues the direct boto3 CreateDBProxy request
                # with no awaited coordination I/O in front of it, so the request
                # boundary lands inside the 100 ms bell-relative window. The
                # CREATED completion it commits afterwards is off the timed path.
                if proxy_intent is None:
                    raise ConnectionSpikeLiveOperationError(
                        "Round 5 CreateDBProxy intent was not pre-staged before the bell"
                    )
                await coordinator.complete_prestaged(scope, spec, intent=proxy_intent)
                await report(phase)
            else:
                await report(phase)
                await coordinator.create_resource(scope, spec)
        if on_lane_stage is not None:
            if not resources.proxy_endpoint:
                raise ConnectionSpikeLiveOperationError(
                    "RDS did not publish the exact per-bout Proxy endpoint"
                )
            # Bind only after CreateDBProxy has returned its endpoint.  The
            # resident parses and prepares this exact request while AWS is
            # still making the Proxy usable; its RELEASE remains durably held
            # until the topology gate below passes.
            await on_lane_stage(
                ConnectionSpikeSetupLaneStop(
                    lane_id="competitor",
                    launched_ns=launched_ns,
                    stopped_ns=self._monotonic_ns(),
                    credential_sha256=self.config.competitor_credential_sha256,
                    endpoint_host=resources.proxy_endpoint,
                    secret_arn=resources.secret_arn,
                )
            )
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
        stopped_ns = self._monotonic_ns()
        stop = ConnectionSpikeSetupLaneStop(
            lane_id="competitor",
            launched_ns=launched_ns,
            stopped_ns=stopped_ns,
            credential_sha256=self.config.competitor_credential_sha256,
            endpoint_host=resources.proxy_endpoint,
            secret_arn=resources.secret_arn,
            create_db_proxy_requested_ns=getattr(
                resources, "proxy_create_requested_ns", None
            ),
        )
        # Ready the instant the Proxy verifies, so its 10,000 starts then rather than after
        # some other lane finishes something unrelated to it.
        if on_lane_ready is not None:
            await on_lane_ready(stop)
        await self._report(
            on_progress,
            "competitor",
            "setup_stop",
            "verified",
            setup_elapsed_ms=(stopped_ns - t0_box[0]) / 1_000_000,
        )
        return stop

    def _setup_observation(self, stop: ConnectionSpikeSetupLaneStop) -> SetupLaneObservation:
        if stop.lane_id == "lakebase":
            facts = (
                PublicSetupEvidence("warm_launch_capsule_current", True),
                # Boolean gate fact, not a host: it asserts that the pooled path
                # bound exactly.  It must NOT contain the substring "endpoint",
                # because the public projection's sensitive-key denylist redacts
                # any key carrying "endpoint"/"host"/"arn"; a collision there
                # once nulled this lane's entire public gate and downgraded a
                # genuinely verified Lakebase setup to unverified.
                PublicSetupEvidence("pooled_path_binding_exact", True),
            )
            gate_id = "lakebase_dispatch_eligibility"
        else:
            facts = (
                PublicSetupEvidence("sealed_proxy_auth_verified", True),
                PublicSetupEvidence("proxy_target_state", "AVAILABLE"),
                PublicSetupEvidence("max_connections_percent", 90),
                PublicSetupEvidence("connection_borrow_timeout_seconds", 120),
                PublicSetupEvidence("static_network_fixture_exact", True),
            )
            gate_id = "rds_proxy_exact_control_plane"
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
            create_db_proxy_requested_ns=stop.create_db_proxy_requested_ns,
            # The AWS competitor always issues CreateDBProxy, so its observation
            # must carry the request stamp; a missing stamp fails closed. Lakebase
            # issues no setup-phase request and leaves this False.
            requires_create_db_proxy_stamp=(stop.lane_id == "competitor"),
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

    async def _assumed_clients(
        self,
        bout_id: str,
        *,
        minimum_lifetime_seconds: int = SETUP_DEADLINE_SECONDS + 60,
    ) -> _SetupAwsClients:
        def assume() -> _SetupAwsClients:
            suffix = hashlib.sha256(bout_id.encode()).hexdigest()[:16]
            source = _control_role_source_session(
                self._session_factory,
                region=self.config.region,
                expected_account_id=self.config.expected_account_id,
                runtime_role_arn=self.config.runtime_role_arn,
                session_name=f"{self.config.role_session_prefix}-rt-{suffix}",
            )
            sts = source.client(
                "sts",
                region_name=self.config.region,
                config=_AWS_CLIENT_CONFIG,
            )
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
            if not isinstance(expiration, datetime):
                raise ConnectionSpikeLiveConfigurationError(
                    "STS omitted the Round 5 setup credential expiration"
                )
            if expiration.tzinfo is None:
                expiration = expiration.replace(tzinfo=UTC)
            if expiration <= datetime.now(UTC) + timedelta(seconds=minimum_lifetime_seconds):
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
                ssm=assumed.client(
                    "ssm",
                    region_name=self.config.region,
                    config=_AWS_CLIENT_CONFIG,
                ),
                rds=assumed.client(
                    "rds",
                    region_name=self.config.region,
                    config=_AWS_CLIENT_CONFIG,
                ),
                ec2=assumed.client(
                    "ec2",
                    region_name=self.config.region,
                    config=_AWS_CLIENT_CONFIG,
                ),
                iam=assumed.client(
                    "iam",
                    region_name=self.config.region,
                    config=_AWS_CLIENT_CONFIG,
                ),
                secretsmanager=assumed.client(
                    "secretsmanager",
                    region_name=self.config.region,
                    config=_AWS_CLIENT_CONFIG,
                ),
                expires_at=expiration,
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
        source, lakebase_managed, competitor_managed = await asyncio.gather(
            self._read_competitor_source(clients),
            self._call(
                clients.ssm.describe_instance_information,
                Filters=[{"Key": "InstanceIds", "Values": [self.config.runner_instance_id]}],
            ),
            self._call(
                clients.ssm.describe_instance_information,
                Filters=[
                    {
                        "Key": "InstanceIds",
                        "Values": [self.config.competitor_runner_instance_id],
                    }
                ],
            ),
        )
        lakebase_runners = lakebase_managed.get("InstanceInformationList") or []
        competitor_runners = competitor_managed.get("InstanceInformationList") or []
        if not source.identifier or len(lakebase_runners) != 1 or len(competitor_runners) != 1:
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
            or lakebase_runners[0].get("InstanceId") != self.config.runner_instance_id
            or lakebase_runners[0].get("PingStatus") != "Online"
            or competitor_runners[0].get("InstanceId") != self.config.competitor_runner_instance_id
            or competitor_runners[0].get("PingStatus") != "Online"
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
            # Static least-privilege network fixtures, role and secret were
            # verified by warming. CreateDBProxy is therefore the first timed
            # AWS mutation.
            ResourceSpec(1, "rds_proxy", resources.names.proxy_name, metadata=metadata),
            ResourceSpec(2, "proxy_target_group", f"{stem}-target-group", metadata=metadata),
            ResourceSpec(3, "proxy_target", f"{stem}-target", metadata=metadata),
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
                    key=lambda child: json.dumps(child, sort_keys=True, separators=(",", ":")),
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
    def _proxy_cleanup_policy_matches(cls, response: Mapping[str, object], secret_arn: str) -> bool:
        legacy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "secretsmanager:GetSecretValue",
                    "Resource": secret_arn,
                    "Condition": {"StringEquals": {"secretsmanager:VersionStage": "AWSCURRENT"}},
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
        # Stamp the request boundary before the SDK call leaves this process. This
        # is the real, scored bell -> CreateDBProxy latency; workflow_launched_ns
        # is only a lower bound taken right after gate.wait().
        resources.proxy_create_requested_ns = self._monotonic_ns()
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
        if group.get("TargetGroupName") != "default" or not target_group_arn.startswith(
            f"arn:aws:rds:{self.config.region}:{self.config.expected_account_id}:target-group:"
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
        if group.get("TargetGroupName") != "default" or not target_group_arn.startswith(
            f"arn:aws:rds:{self.config.region}:{self.config.expected_account_id}:target-group:"
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
        if pool.get("MaxConnectionsPercent") == 90 and pool.get("ConnectionBorrowTimeout") == 120:
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

    def _aurora_targets_pending_capacity(self, targets: Sequence[Mapping[str, object]]) -> bool:
        if self.config.competitor_id != "aurora_serverless_v2" or not self._proxy_targets_match(
            targets
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
                if allow_aurora_pending_capacity and self._aurora_targets_pending_capacity(
                    target_values
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
        ingress = self._security_group_permissions(network.get("IpPermissions") or [])
        egress = self._security_group_permissions(network.get("IpPermissionsEgress") or [])
        checks = {
            "proxy_identity": (
                proxy.get("DBProxyName") == resources.names.proxy_name
                and str(proxy.get("Status") or "").lower() == "available"
                and proxy.get("Endpoint") == resources.proxy_endpoint
            ),
            "proxy_engine_role": (
                proxy.get("RoleArn") == resources.proxy_role_arn
                and proxy.get("EngineFamily") == "POSTGRESQL"
            ),
            "proxy_vpc": (
                proxy.get("VpcId") == self.config.vpc_id
                and set(proxy.get("VpcSubnetIds") or []) == set(self.config.proxy_subnet_ids)
                and set(proxy.get("VpcSecurityGroupIds") or [])
                == {resources.proxy_security_group_id}
            ),
            "proxy_tls_auth": (
                proxy.get("RequireTLS") is True
                and len(auth) == 1
                and auth[0].get("AuthScheme") == "SECRETS"
                and auth[0].get("SecretArn") == resources.secret_arn
                and auth[0].get("IAMAuth") == "DISABLED"
                and auth[0].get("UserName")
                in {
                    None,
                    self.config.native_role,
                }
                and auth[0].get("ClientPasswordAuthType") == "POSTGRES_SCRAM_SHA_256"
            ),
            "target_group": (
                len(groups) == 1
                and groups[0].get("TargetGroupName") == "default"
                and pool.get("MaxConnectionsPercent") == 90
                and pool.get("ConnectionBorrowTimeout") == 120
            ),
            "target_exact": self._proxy_targets_available(targets),
            "source_exact": (
                source.identifier == self.config.competitor_target_id
                and source.resource_id == self.config.competitor_resource_id
                and source.direct_host == self.config.competitor_direct_host
                and source.status == "available"
                and source.vpc_id == self.config.vpc_id
                and source.security_group_ids == (resources.rds_security_group_id,)
            ),
            "network_ingress": (
                len(networks) == 1
                and network.get("GroupId") == resources.proxy_security_group_id
                and ingress
                == (
                    (
                        "tcp",
                        5432,
                        5432,
                        self.config.runner_security_group_id,
                        "PostgreSQL from the sealed Round 5 physical runners",
                    ),
                )
            ),
            "network_egress": (
                len(networks) == 1
                and network.get("GroupId") == resources.proxy_security_group_id
                and egress
                == (
                    (
                        "tcp",
                        5432,
                        5432,
                        resources.rds_security_group_id,
                        "PostgreSQL to the exact sealed Round 5 source",
                    ),
                )
            ),
        }
        failed = [name for name, passed in checks.items() if not passed]
        if failed:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 exact Proxy control gate failed: " + ",".join(failed)
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
        runner_instance_id = (
            self.config.runner_instance_id
            if lane_id == "lakebase"
            else self.config.competitor_runner_instance_id
        )
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
            "InstanceIds": [runner_instance_id],
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
        active = _SetupActiveCommand(
            bout_id,
            lane_id,
            action,
            command_id,
            runner_instance_id,
            ssm,
        )
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
                InstanceId=active.runner_instance_id,
            )
        except Exception as exc:
            if self._error_code(exc) == "InvocationDoesNotExist":
                return {"Status": "Pending"}
            raise

    async def _cancel_setup_command(self, active: _SetupActiveCommand, nonce: str) -> None:
        await self._call(
            active.ssm.cancel_command,
            CommandId=active.command_id,
            InstanceIds=[active.runner_instance_id],
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
        resident_transport: Round5ResidentTransport | None = None,
    ) -> None:
        if cancel_teardown_timeout_seconds <= 0:
            raise ValueError("cancel_teardown_timeout_seconds must be positive")
        self.config = config
        self._session_factory = session_factory
        self._sleep = sleep
        self.cancel_teardown_timeout_seconds = cancel_teardown_timeout_seconds
        self._active: _ActiveCommand | None = None
        self._pending: _PendingCommand | None = None
        # A command identity remains here until remote socket cleanup and the
        # runner flock release are both observed.  Task cancellation is only a
        # local fact and must never erase the job provider cleanup is waiting on.
        self._settlement_debt: dict[str, _ActiveCommand] = {}
        self._dispatch_lock = asyncio.Lock()
        self._capsule_refresh_lock = asyncio.Lock()
        self._prepared_clients: _AwsClients | None = None
        self._prepared_boot_id = ""
        self._prepared_capacity: CapacityPreflight | None = None
        self._resident_transport = resident_transport
        self._resident_process_boot_id = ""
        self._resident_process_pid = 0
        self._resident_warm_attempt_token = ""
        self._resident_settlement_debt: dict[str, Round5ControlBinding] = {}
        self._resident_release_events: dict[str, Round5ControlEvent] = {}

    @property
    def prepared_boot_id(self) -> str:
        return self._prepared_boot_id

    @property
    def prepared_capacity(self) -> CapacityPreflight | None:
        return self._prepared_capacity

    @property
    def prepared_expires_at(self) -> datetime | None:
        return self._prepared_clients.expires_at if self._prepared_clients is not None else None

    def settlement_pending(self, run_id: str) -> bool:
        return run_id in self._settlement_debt or run_id in self._resident_settlement_debt

    async def _settle_debt(self, active: _ActiveCommand) -> None:
        self._settlement_debt[active.run_id] = active
        await self._cancel_and_settle(active)
        if self._settlement_debt.get(active.run_id) == active:
            self._settlement_debt.pop(active.run_id, None)

    @staticmethod
    def _validated_progress(
        payload: Mapping[str, object],
        *,
        sequence: int,
    ) -> FanInProgress:
        lane_id = str(payload.get("lane_id") or "")
        phase = str(payload.get("phase") or "")
        if (
            payload.get("protocol") != FANIN_PROTOCOL
            or payload.get("schema_version") != FANIN_SCHEMA_VERSION
            or lane_id not in RUNTIME_LANE_IDS
            or phase not in {"ramping", "holding"}
        ):
            raise ConnectionSpikeLiveOperationError(
                "Round 5 progress did not match the semantic fan-in contract"
            )
        counts: dict[str, int] = {}
        for name, maximum in (
            ("initiated_clients", 10_000),
            ("authenticated_clients", 10_000),
            ("held_clients", 10_000),
            ("peak_held_clients", 10_000),
            ("terminal_failures", 10_000),
            ("sampled_queries_succeeded", 64),
            ("sampled_queries_failed", 64),
        ):
            raw = payload.get(name, 0)
            if isinstance(raw, bool) or not isinstance(raw, int) or not 0 <= raw <= maximum:
                raise ConnectionSpikeLiveOperationError(
                    "Round 5 progress carried an invalid bounded counter"
                )
            counts[name] = raw
        target = payload.get("time_to_target_ms")
        target_valid = (
            isinstance(target, (int, float))
            and not isinstance(target, bool)
            and math.isfinite(float(target))
            and float(target) >= 0
        )
        exact = (
            counts["initiated_clients"] == 10_000
            and counts["authenticated_clients"] == 10_000
            and counts["held_clients"] == 10_000
            and counts["terminal_failures"] == 0
        )
        if (
            counts["peak_held_clients"] < counts["held_clients"]
            or (target is not None and (not exact or not target_valid))
            or (phase == "holding" and (not exact or not target_valid))
        ):
            raise ConnectionSpikeLiveOperationError(
                "Round 5 progress advanced beyond its aggregate barrier proof"
            )
        milestone_times = {
            name: payload.get(name)
            for name in (
                "first_socket_initiated_ms",
                "first_client_authenticated_ms",
            )
        }
        if any(
            value is not None
            and (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or float(value) < 0
            )
            for value in milestone_times.values()
        ) or (
            milestone_times["first_socket_initiated_ms"] is not None
            and milestone_times["first_client_authenticated_ms"] is not None
            and float(milestone_times["first_client_authenticated_ms"])
            < float(milestone_times["first_socket_initiated_ms"])
        ):
            raise ConnectionSpikeLiveOperationError(
                "Round 5 progress milestone chronology is invalid"
            )
        fields = {name: value for name, value in payload.items() if name in FanInProgress.__slots__}
        fields["sequence"] = sequence
        return FanInProgress(**fields)

    @staticmethod
    def _record_progress_identity(
        payload: Mapping[str, object],
        *,
        sequence: int,
        seen_digests: dict[int, str],
    ) -> None:
        canonical = dict(payload)
        canonical["sequence"] = sequence
        digest = hashlib.sha256(
            json.dumps(
                canonical,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        previous = seen_digests.get(sequence)
        if previous is not None and previous != digest:
            raise ConnectionSpikeLiveOperationError(
                "Round 5 equal progress revision changed payload"
            )
        seen_digests[sequence] = digest

    @staticmethod
    def _progress_from_output(
        output: str,
        *,
        after_sequence: int,
        seen_digests: dict[int, str] | None = None,
    ) -> tuple[list[FanInProgress], int]:
        """Read the runner's progress lines out of one SSM output block.

        SSM has no stream: each poll returns the whole of stdout so far, so the same
        lines arrive again on every poll for the life of the bout. `after_sequence`
        is what makes that idempotent -- the caller passes back the last sequence it
        has already shown, and gets only what is new. The returned sequence is the
        highest *seen*, not the highest returned, so a poll that finds nothing new
        still reports where the stream is and cannot rewind the caller's cursor.

        A gap is refused rather than skipped. The runner numbers these consecutively
        and stops printing when it runs out of budget, so it cannot skip one; a gap
        therefore means output was lost or spliced, and silently continuing would
        show the room a ramp with a hole in it and call it a measurement. Lines that
        are not progress lines are ignored: the same stdout legitimately carries the
        runner's own refusals and settlement tokens.
        """

        parsed: list[FanInProgress] = []
        identities = seen_digests if seen_digests is not None else {}
        last_seen = after_sequence
        expected: int | None = None
        wire_fields = {slot for slot in FanInProgress.__slots__}

        for line in output.splitlines():
            stripped = line.strip()
            if not stripped.startswith(PROGRESS_PREFIX):
                continue
            body = stripped[len(PROGRESS_PREFIX) :]
            try:
                payload = json.loads(body)
            except ValueError as exc:
                raise ConnectionSpikeLiveOperationError(
                    "Round 5 progress line was not valid JSON"
                ) from exc
            if not isinstance(payload, Mapping):
                raise ConnectionSpikeLiveOperationError(
                    "Round 5 progress line was not a JSON object"
                )
            try:
                sequence = int(payload["sequence"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ConnectionSpikeLiveOperationError(
                    "Round 5 progress line carried no usable sequence"
                ) from exc
            if expected is not None and sequence != expected:
                raise ConnectionSpikeLiveOperationError(
                    f"Round 5 progress sequence jumped from {expected - 1} to "
                    f"{sequence}; output was lost rather than merely delayed"
                )
            expected = sequence + 1
            last_seen = max(last_seen, sequence)
            LiveConnectionSpikeAdapter._record_progress_identity(
                payload,
                sequence=sequence,
                seen_digests=identities,
            )
            if sequence <= after_sequence:
                continue
            # Only the declared fields, so a field added to the runner's wire
            # cannot reach this dataclass as an unexpected keyword and turn a
            # newer runner into a crash on this side.
            fields = {name: value for name, value in payload.items() if name in wire_fields}
            parsed.append(
                LiveConnectionSpikeAdapter._validated_progress(
                    fields,
                    sequence=sequence,
                )
            )

        return parsed, last_seen

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
        self._prepared_clients = clients

    async def refresh_launch_context(self, context_id: str) -> datetime:
        """Rotate the ephemeral dispatch context while the old one stays usable."""

        clients = await self._assumed_clients(context_id)
        await self._preflight_runner(clients)
        self._prepared_clients = clients
        return clients.expires_at

    async def ensure_dispatch_capsule(
        self,
        context_id: str,
        *,
        refresh_lead_seconds: float = DISPATCH_CAPSULE_REFRESH_LEAD_SECONDS,
    ) -> datetime:
        """Retain the full dispatch safety window without rerunning capacity."""

        async with self._capsule_refresh_lock:
            now = datetime.now(UTC)
            clients = self._prepared_clients
            refresh_before = now + timedelta(
                seconds=DISPATCH_CAPSULE_SAFETY_SECONDS + refresh_lead_seconds
            )
            if clients is not None and clients.expires_at > refresh_before:
                return clients.expires_at
            refreshed = await self._assumed_clients(context_id)
            if refreshed.expires_at <= now + timedelta(seconds=DISPATCH_CAPSULE_SAFETY_SECONDS):
                raise ConnectionSpikeLiveOperationError(
                    "Round 5 refreshed dispatch capsule cannot retain its safety margin"
                )
            self._prepared_clients = refreshed
            return refreshed.expires_at

    async def execute(
        self,
        run_id: str,
        request: Mapping[str, object],
        *,
        targets: Sequence[ConnectionSpikeTarget] | None = None,
    ) -> dict[str, object]:
        """Dispatch one complete runner request and return its verified payload.

        The request arrives built. An adapter that assembled it here could only ever
        send the one protocol its builder knew, which is how Round 5 came to dispatch a
        bounded schedule to a runner whose result the finaliser read as a fan-in bout.
        """

        if self._dispatch_lock.locked() or self._settlement_debt:
            raise ConnectionSpikeLiveOperationError(
                "A Round 5 runner command or unsettled prior job is already active "
                "in this app replica"
            )
        await self._dispatch_lock.acquire()
        try:
            return await self._execute_reserved(run_id, request, targets=targets)
        finally:
            self._dispatch_lock.release()

    async def execute_prepared(
        self,
        run_id: str,
        request: Mapping[str, object],
        *,
        targets: Sequence[ConnectionSpikeTarget] | None = None,
        on_progress: Callable[[FanInProgress], Awaitable[None]] | None = None,
        resident_binding: Round5ControlBinding | None = None,
    ) -> dict[str, object]:
        """Use the staged resident generation; never start post-bell SSM."""

        clients = self._prepared_clients
        if clients is None or clients.expires_at <= datetime.now(UTC) + timedelta(
            seconds=DISPATCH_CAPSULE_SAFETY_SECONDS
        ):
            raise ConnectionSpikeLiveOperationError(
                "Round 5 dispatch capsule is absent or inside its safety margin"
            )
        transport = self._resident_transport
        if transport is None:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 resident runner transport is not configured"
            )
        lane_ids = self._requested_lane_ids(request)
        generation = request.get("resident_generation")
        if (
            len(lane_ids) != 1
            or isinstance(generation, bool)
            or not isinstance(generation, int)
            or generation <= 0
        ):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 resident request omitted one lane or its generation"
            )
        lane_id = next(iter(lane_ids))
        if (
            resident_binding is None
            or resident_binding.job_id != run_id
            or resident_binding.lane_id != lane_id
            or resident_binding.generation != generation
            or canonical_request_sha256(request) != resident_binding.request_sha256
        ):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 resident request binding is incomplete"
            )
        if self._dispatch_lock.locked() or self._settlement_debt:
            raise ConnectionSpikeLiveOperationError(
                "A Round 5 runner command or unsettled prior job is already active "
                "in this app replica"
            )
        await self._dispatch_lock.acquire()
        self._resident_settlement_debt[run_id] = resident_binding
        try:
            if lane_id == "competitor":
                staged = getattr(self, "_resident_release_events", {}).get(run_id)
                if staged is None:
                    await transport.stage_and_release(
                        binding=resident_binding,
                        request=request,
                    )
                else:
                    if (
                        staged.binding != resident_binding
                        or staged.kind != Round5ControlKind.RELEASE
                    ):
                        raise ConnectionSpikeLiveConfigurationError(
                            "Round 5 staged competitor release binding changed"
                        )
                    # Synchronous permission edge: all durable I/O and resident
                    # PREPARED work completed before the exact Proxy gate.
                    transport.dispatcher.allow_release(staged.event_id)

            async def report(value: Mapping[str, object]) -> None:
                if on_progress is None:
                    return
                sequence = value.get("sequence")
                if isinstance(sequence, bool) or not isinstance(sequence, int):
                    raise ConnectionSpikeLiveOperationError(
                        "Resident progress omitted its sequence"
                    )
                await on_progress(self._validated_progress(value, sequence=sequence))

            result = await transport.result(resident_binding, on_progress=report)
            self._resident_settlement_debt.pop(run_id, None)
            getattr(self, "_resident_release_events", {}).pop(run_id, None)
            return result
        finally:
            self._dispatch_lock.release()

    async def stage_prepared_release(
        self,
        *,
        binding: Round5ControlBinding,
        request: Mapping[str, object],
    ) -> None:
        """Prepare one exact resident request without opening its release gate."""

        transport = self._resident_transport
        if transport is None:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 resident runner transport is not configured"
            )
        existing = self._resident_release_events.get(binding.job_id)
        if existing is not None:
            if existing.binding != binding:
                raise ConnectionSpikeLiveConfigurationError(
                    "Round 5 staged competitor binding changed"
                )
            return
        # Record settlement debt before the first delivery await.  A STAGE may
        # be accepted remotely even when this caller loses its acknowledgement.
        self._resident_settlement_debt[binding.job_id] = binding
        event = await transport.stage_for_release(
            binding=binding,
            request=request,
        )
        self._resident_release_events[binding.job_id] = event

    async def stage_resident_generation(
        self,
        *,
        generation: int,
        lane_id: str,
        warm_attempt_token: str,
        request_template: Mapping[str, object],
    ) -> Mapping[str, object]:
        transport = self._resident_transport
        if transport is None:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 resident runner transport is not configured"
            )
        generation_job = hashlib.sha256(
            f"round5-resident\0{generation}\0{warm_attempt_token}\0{lane_id}".encode()
        ).hexdigest()
        request_sha256 = canonical_request_sha256(request_template)
        binding = Round5ControlBinding(
            installation_id=self.config.resident_installation_id,
            lane_id=lane_id,
            generation=generation,
            warm_attempt_token=warm_attempt_token,
            claim_id=None,
            bout_id=None,
            bell_id=None,
            fence=0,
            job_id=generation_job,
            runner_boot_id=self._prepared_boot_id,
            runner_process_boot_id="unattested",
            runner_harness_sha256=self.config.runner_harness_sha256,
            request_sha256=request_sha256,
        )
        readiness_not_before = datetime.now(UTC)
        await transport.preload(
            binding=binding,
            request=request_template,
        )
        readiness = await transport.wait_agent_ready(
            binding,
            not_before=readiness_not_before,
        )
        self._resident_process_boot_id = str(readiness["runner_process_boot_id"])
        self._resident_process_pid = int(readiness["process_pid"])
        self._resident_warm_attempt_token = warm_attempt_token
        return readiness

    async def _execute_reserved(
        self,
        run_id: str,
        request: Mapping[str, object],
        *,
        targets: Sequence[ConnectionSpikeTarget] | None = None,
        prepared_clients: _AwsClients | None = None,
        on_progress: Callable[[FanInProgress], Awaitable[None]] | None = None,
    ) -> dict[str, object]:
        self._validate_run_id(run_id)
        self._validate_request(run_id, request)
        is_preflight = request.get("action") == "preflight"
        timeout_seconds = _dispatch_timeout_seconds(request, self.config.command_timeout_seconds)
        clients = prepared_clients or await self._assumed_clients(run_id)
        if prepared_clients is None:
            await self._preflight_runner(clients)
            await self._preflight_targets(clients.rds, targets=targets)
        started_at = datetime.now(UTC)
        send_task = asyncio.create_task(
            self._send_command(clients.ssm, request, timeout_seconds=timeout_seconds)
        )
        job_id = str(request.get("job_id")) if request.get("action") == "run_lane_v3" else None
        pending = _PendingCommand(
            run_id=run_id,
            send_task=send_task,
            clients=clients,
            job_id=job_id,
        )
        self._pending = pending
        try:
            command_id = await asyncio.shield(send_task)
        except asyncio.CancelledError:
            command_id = await asyncio.shield(send_task)
            active = _ActiveCommand(
                run_id=run_id,
                command_id=command_id,
                clients=clients,
                job_id=job_id,
            )
            self._active = active
            if self._pending == pending:
                self._pending = None
            # Bounds the wait, never the settlement: `_cancel_and_settle` runs
            # on its own task and keeps going after this returns, so a slow
            # runner still gets its cancel issued and its flock released.
            self._settlement_debt[run_id] = active
            await abandon_on_cancel(
                lambda: self._settle_debt(active),
                identifier=self._cancelled_burst_identifier(active),
                timeout_seconds=self.cancel_teardown_timeout_seconds,
            )
            self._active = None
            raise
        except Exception:
            if self._pending == pending:
                self._pending = None
            raise
        active = _ActiveCommand(
            run_id=run_id,
            command_id=command_id,
            clients=clients,
            job_id=job_id,
        )
        self._active = active
        if self._pending == pending:
            self._pending = None
        try:
            invocation = await self._wait_for_terminal(
                active,
                timeout_seconds=timeout_seconds,
                on_progress=on_progress,
            )
            output = str(invocation.get("StandardOutputContent") or "")
            if invocation.get("Status") != "Success":
                self._require_settlement(run_id, output)
                code = _runner_error_code(output)
                raise ConnectionSpikeLiveOperationError(
                    "Round 5 runner command did not succeed after cleanup"
                    + (f": the runner refused with {code}" if code else "")
                )
            if is_preflight:
                self._require_settlement(run_id, output)
                # No CloudWatch witness. The witness corroborates a bout's backend
                # session count against a second source; a preflight opens no
                # connection, so there is nothing for it to corroborate and asking
                # would add a metric read that only ever returns nothing.
                parsed = self._parse_preflight_output(run_id, output)
                self._prepared_clients = clients
                return parsed
            if job_id is not None:
                job_status = await self._query_job_status(
                    clients.ssm,
                    job_id=job_id,
                )
                encoded_result = await self._query_job_result(
                    clients.ssm,
                    job_id=job_id,
                    status=job_status,
                )
                output = (
                    f"CLEANUP_CONFIRMED:{run_id}\n"
                    f"RESULT_GZIP_BASE64:{encoded_result}\n"
                    f"RUNNER_FLOCK_RELEASED:{run_id}\n"
                )
            else:
                self._require_settlement(run_id, output)
            result = self._parse_runner_output(
                run_id,
                output,
                expected_lane_ids=self._requested_lane_ids(request),
            )
            witness = await self._cloudwatch_witness(
                clients.cloudwatch,
                started_at,
                datetime.now(UTC),
            )
            result["cloudwatch_witness"] = witness
            return result
        except asyncio.CancelledError:
            self._settlement_debt[run_id] = active
            await abandon_on_cancel(
                lambda: self._settle_debt(active),
                identifier=self._cancelled_burst_identifier(active),
                timeout_seconds=self.cancel_teardown_timeout_seconds,
            )
            raise
        except TimeoutError as exc:
            await self._settle_debt(active)
            raise ConnectionSpikeLiveOperationError(
                f"Round 5 SSM command exceeded its {timeout_seconds:.0f}-second boundary"
            ) from exc
        except Exception:
            # A local observer/parser fault does not mean the remote job
            # stopped. Settle the exact logical job first, then preserve the
            # original exception as the bout's primary failure.
            try:
                await self._settle_debt(active)
            except Exception:
                logger.error(
                    "Round 5 active job settlement failed after a primary adapter error",
                    exc_info=True,
                )
            raise
        finally:
            if self._active == active:
                self._active = None
            if self._pending == pending:
                self._pending = None

    async def cancel(self, run_id: str) -> None:
        self._validate_run_id(run_id)
        active = self._active
        if active is None:
            active = self._settlement_debt.get(run_id)
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
                job_id=pending.job_id,
            )
            self._active = active
            if self._pending == pending:
                self._pending = None
        if active.run_id != run_id:
            raise ConnectionSpikeCleanupError(
                "Refusing to cancel a Round 5 command owned by another run"
            )
        await self._settle_debt(active)

    async def cancel_resident(
        self,
        *,
        binding: Round5ControlBinding,
    ) -> None:
        transport = self._resident_transport
        if transport is None:
            raise ConnectionSpikeCleanupError(
                "Resident runner cancellation transport is unavailable"
            )
        await transport.cancel(binding=binding, await_settlement=True)
        self._resident_settlement_debt.pop(binding.job_id, None)
        self._resident_release_events.pop(binding.job_id, None)

    async def cancel_job(self, job_id: str) -> None:
        """Cancel and observe one durable logical job during restart cleanup."""

        self._validate_run_id(job_id)
        transport = self._resident_transport
        if transport is None:
            raise ConnectionSpikeCleanupError("Resident runner registry is unavailable")
        await transport.settle_registry_job(job_id)

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
            sts = source.client(
                "sts",
                region_name=self.config.region,
                config=_AWS_CLIENT_CONFIG,
            )
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
            if not isinstance(expiration, datetime):
                raise ConnectionSpikeLiveConfigurationError(
                    "STS omitted the Round 5 dispatch credential expiration"
                )
            if expiration.tzinfo is None:
                expiration = expiration.replace(tzinfo=UTC)
            if expiration <= datetime.now(UTC) + timedelta(seconds=FANIN_SSM_TIMEOUT_SECONDS + 60):
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
                ssm=assumed.client(
                    "ssm",
                    region_name=self.config.region,
                    config=_AWS_CLIENT_CONFIG,
                ),
                rds=assumed.client(
                    "rds",
                    region_name=self.config.region,
                    config=_AWS_CLIENT_CONFIG,
                ),
                cloudwatch=assumed.client(
                    "cloudwatch",
                    region_name=self.config.region,
                    config=_AWS_CLIENT_CONFIG,
                ),
                ec2=assumed.client(
                    "ec2",
                    region_name=self.config.region,
                    config=_AWS_CLIENT_CONFIG,
                ),
                expires_at=expiration,
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

    def _validate_request(self, run_id: str, request: Mapping[str, object]) -> None:
        """Refuse a request this adapter must not send, before it costs a dispatch.

        The run id is checked against the request rather than trusted alongside it: the
        adapter tracks the active command by run id and the runner echoes it back in the
        payload, so a request naming a different one would produce a result this adapter
        correctly rejects as belonging to another run, after paying for the bout.
        """

        if request.get("protocol") != FANIN_PROTOCOL:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 dispatches only the fan-in protocol"
            )
        if request.get("run_id") != run_id:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 runner request names a different run than the dispatch"
            )
        try:
            encoded = json.dumps(request, sort_keys=True, separators=(",", ":")).encode()
        except (TypeError, ValueError) as exc:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 runner request is not JSON serializable"
            ) from exc
        if len(encoded) > 512_000:
            raise ConnectionSpikeLiveConfigurationError("Round 5 runner request is too large")

    async def _send_command(
        self,
        ssm: Any,
        request: Mapping[str, object],
        *,
        timeout_seconds: float,
    ) -> str:
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
        send_kwargs = {
            "InstanceIds": [self.config.runner_instance_id],
            "DocumentName": self.config.ssm_document_name,
            "TimeoutSeconds": int(timeout_seconds),
            "Parameters": {
                "commands": [command],
                "executionTimeout": [str(int(timeout_seconds))],
            },
            "CloudWatchOutputConfig": {"CloudWatchOutputEnabled": False},
        }
        try:
            response = await asyncio.to_thread(ssm.send_command, **send_kwargs)
        except (
            TimeoutError,
            OSError,
            ConnectTimeoutError,
            ReadTimeoutError,
            EndpointConnectionError,
            ConnectionClosedError,
        ):
            if request.get("action") != "run_lane_v3":
                raise
            # SendCommand has no client token. Query the runner's durable
            # logical registry, then resend the identical job. Whether the
            # first acknowledgement was lost before or after execution, the
            # atomic job claim makes this an attach rather than a second fan-in.
            await self._query_job_status(
                ssm,
                job_id=str(request["job_id"]),
            )
            response = await asyncio.to_thread(ssm.send_command, **send_kwargs)
        command_id = str((response.get("Command") or {}).get("CommandId") or "")
        if not command_id:
            raise ConnectionSpikeLiveOperationError(
                "SSM did not return a Round 5 command identifier"
            )
        return command_id

    async def _query_job_status(
        self,
        ssm: Any,
        *,
        job_id: str,
    ) -> Mapping[str, object]:
        command_id = await self._send_job_control(
            ssm,
            action="job_status",
            job_id=job_id,
        )
        async with asyncio.timeout(60):
            while True:
                try:
                    invocation = await asyncio.to_thread(
                        ssm.get_command_invocation,
                        CommandId=command_id,
                        InstanceId=self.config.runner_instance_id,
                    )
                except Exception as exc:
                    if self._error_code(exc) == "InvocationDoesNotExist":
                        await self._sleep(0.1)
                        continue
                    raise
                if invocation.get("Status") not in _TERMINAL:
                    await self._sleep(0.1)
                    continue
                output = str(invocation.get("StandardOutputContent") or "")
                rows = [
                    line.removeprefix("JOB_STATUS:")
                    for line in output.splitlines()
                    if line.startswith("JOB_STATUS:")
                ]
                if invocation.get("Status") != "Success" or len(rows) != 1:
                    raise ConnectionSpikeLiveOperationError(
                        "Round 5 logical job status could not be verified"
                    )
                try:
                    status = json.loads(rows[0])
                except json.JSONDecodeError as exc:
                    raise ConnectionSpikeLiveOperationError(
                        "Round 5 logical job status was malformed"
                    ) from exc
                if (
                    not isinstance(status, Mapping)
                    or status.get("protocol") != "round5-job-v3"
                    or status.get("job_id") != job_id
                ):
                    raise ConnectionSpikeLiveOperationError(
                        "Round 5 logical job status named another job"
                    )
                return status

    async def _query_job_result(
        self,
        ssm: Any,
        *,
        job_id: str,
        status: Mapping[str, object],
    ) -> str:
        chunk_count = int(status.get("result_chunks") or 0)
        result_size = int(status.get("result_size") or 0)
        expected_sha256 = str(status.get("result_sha256") or "")
        if (
            status.get("state") != "completed"
            or status.get("settled") is not True
            or status.get("result_available") is not True
            or not 1 <= chunk_count <= 4
            or not 1 <= result_size <= 23_500
            or re.fullmatch(r"[0-9a-f]{64}", expected_sha256) is None
        ):
            raise ConnectionSpikeLiveOperationError(
                "Round 5 durable runner result metadata is invalid"
            )
        chunks: list[str] = []
        for chunk_index in range(chunk_count):
            command_id = await self._send_job_control(
                ssm,
                action="job_result",
                job_id=job_id,
                chunk_index=chunk_index,
            )
            async with asyncio.timeout(60):
                while True:
                    try:
                        invocation = await asyncio.to_thread(
                            ssm.get_command_invocation,
                            CommandId=command_id,
                            InstanceId=self.config.runner_instance_id,
                        )
                    except Exception as exc:
                        if self._error_code(exc) == "InvocationDoesNotExist":
                            await self._sleep(0.1)
                            continue
                        raise
                    if invocation.get("Status") not in _TERMINAL:
                        await self._sleep(0.1)
                        continue
                    output = str(invocation.get("StandardOutputContent") or "")
                    rows = [
                        line.removeprefix("JOB_RESULT:")
                        for line in output.splitlines()
                        if line.startswith("JOB_RESULT:")
                    ]
                    if invocation.get("Status") != "Success" or len(rows) != 1:
                        raise ConnectionSpikeLiveOperationError(
                            "Round 5 durable runner result chunk was unavailable"
                        )
                    try:
                        document = json.loads(rows[0])
                    except json.JSONDecodeError as exc:
                        raise ConnectionSpikeLiveOperationError(
                            "Round 5 durable runner result chunk was malformed"
                        ) from exc
                    if (
                        not isinstance(document, Mapping)
                        or document.get("protocol") != "round5-job-v3"
                        or document.get("job_id") != job_id
                        or document.get("chunk_index") != chunk_index
                        or document.get("chunk_count") != chunk_count
                        or document.get("result_sha256") != expected_sha256
                        or not isinstance(document.get("payload"), str)
                    ):
                        raise ConnectionSpikeLiveOperationError(
                            "Round 5 durable runner result chunk identity changed"
                        )
                    chunks.append(str(document["payload"]))
                    break
        encoded = "".join(chunks)
        if (
            len(encoded) != result_size
            or hashlib.sha256(encoded.encode("ascii")).hexdigest() != expected_sha256
        ):
            raise ConnectionSpikeLiveOperationError("Round 5 durable runner result digest changed")
        return encoded

    async def _wait_for_terminal(
        self,
        active: _ActiveCommand,
        *,
        timeout_seconds: float | None = None,
        on_progress: Callable[[FanInProgress], Awaitable[None]] | None = None,
    ) -> Mapping[str, object]:
        sequence = 0
        progress_digests: dict[int, str] = {}
        next_registry_poll = 0.0
        async with asyncio.timeout(
            self.config.command_timeout_seconds if timeout_seconds is None else timeout_seconds
        ):
            while True:
                invocation = await self._get_invocation(active)
                if on_progress is not None:
                    updates, sequence = self._progress_from_output(
                        str(invocation.get("StandardOutputContent") or ""),
                        after_sequence=sequence,
                        seen_digests=progress_digests,
                    )
                    for update in updates:
                        await on_progress(update)
                    now = time.monotonic()
                    if (
                        active.job_id is not None
                        and invocation.get("Status") not in _TERMINAL
                        and now >= next_registry_poll
                    ):
                        status = await self._query_job_status(
                            active.clients.ssm,
                            job_id=active.job_id,
                        )
                        latest = status.get("latest_progress")
                        if isinstance(latest, Mapping):
                            latest_sequence = int(latest.get("sequence") or 0)
                            if latest_sequence <= 0:
                                raise ConnectionSpikeLiveOperationError(
                                    "Round 5 durable progress carried no usable sequence"
                                )
                            self._record_progress_identity(
                                latest,
                                sequence=latest_sequence,
                                seen_digests=progress_digests,
                            )
                            if latest_sequence > sequence:
                                await on_progress(
                                    self._validated_progress(
                                        latest,
                                        sequence=latest_sequence,
                                    )
                                )
                                sequence = latest_sequence
                        next_registry_poll = now + 2.0
                if invocation.get("Status") in _TERMINAL:
                    return invocation
                await self._sleep(self.config.poll_interval_seconds)

    @staticmethod
    def _error_code(exc: BaseException) -> str:
        response = getattr(exc, "response", None)
        if isinstance(response, Mapping):
            error = response.get("Error")
            if isinstance(error, Mapping):
                return str(error.get("Code") or "")
        return ""

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
                # Cancellation addresses the logical job, not whichever SSM
                # acknowledgement happened to reach this process.
                if active.job_id is not None:
                    await self._send_job_control(
                        active.clients.ssm,
                        action="cancel_job",
                        job_id=active.job_id,
                    )
                else:
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

    async def _send_job_control(
        self,
        ssm: Any,
        *,
        action: Literal["cancel_job", "job_status", "job_result"],
        job_id: str,
        chunk_index: int | None = None,
    ) -> str:
        request = {
            "protocol": "round5-job-v3",
            "schema_version": 3,
            "action": action,
            "job_id": job_id,
        }
        if action == "job_result":
            if chunk_index is None or chunk_index < 0:
                raise ConnectionSpikeLiveConfigurationError("Round 5 result chunk index is invalid")
            request["chunk_index"] = chunk_index
        encoded = base64.urlsafe_b64encode(
            gzip.compress(
                json.dumps(request, sort_keys=True, separators=(",", ":")).encode(),
                mtime=0,
            )
        ).decode()
        response = await asyncio.to_thread(
            ssm.send_command,
            InstanceIds=[self.config.runner_instance_id],
            DocumentName=self.config.ssm_document_name,
            TimeoutSeconds=60,
            Parameters={
                "commands": [f"{self.config.runner_path} {encoded}"],
                "executionTimeout": ["60"],
            },
            CloudWatchOutputConfig={"CloudWatchOutputEnabled": False},
        )
        command_id = str((response.get("Command") or {}).get("CommandId") or "")
        if not command_id:
            raise ConnectionSpikeCleanupError(
                "Round 5 logical job cancellation was not acknowledged"
            )
        return command_id

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

    @staticmethod
    def _requested_lane_ids(request: Mapping[str, object]) -> frozenset[str]:
        """The lanes this request told the runner to run.

        Each dispatch carries one lane now, so the result is checked against what was asked for
        rather than against every sealed lane. A runner that drops a lane it was told to run is
        still refused; a runner that does not return a lane nobody asked for is not.
        """

        targets = request.get("targets")
        if not isinstance(targets, Sequence) or isinstance(targets, (str, bytes)):
            return frozenset()
        return frozenset(
            str(target.get("lane_id") or "")
            for target in targets
            if isinstance(target, Mapping) and target.get("lane_id")
        )

    def _parse_runner_output(
        self,
        run_id: str,
        output: str,
        *,
        expected_lane_ids: frozenset[str] | None = None,
    ) -> dict[str, object]:
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
        if result.get("protocol") != FANIN_PROTOCOL or result.get("run_id") != run_id:
            raise ConnectionSpikeLiveOperationError(
                "Round 5 runner result does not match the active run"
            )
        result_lane_ids = {
            str(lane.get("lane_id")) for lane in result.get("lanes", []) if isinstance(lane, dict)
        }
        expected = (
            expected_lane_ids
            if expected_lane_ids is not None
            else frozenset(target.lane_id for target in self.config.targets)
        )
        if result_lane_ids != expected:
            raise ConnectionSpikeLiveOperationError(
                "Round 5 runner result lanes do not match the dispatch: expected "
                f"{', '.join(sorted(expected))}, got {', '.join(sorted(result_lane_ids)) or 'none'}"
            )
        # The structural scan, not a substring sweep over flattened JSON. This payload
        # legitimately contains the word "password" in `auth_method`:
        # `tls-cleartext-password` is one of the protocol's two supported methods and is a
        # label, not a secret. A flattened match refused every successful bout on that
        # basis, after paying for it.
        if _runner_result_has_forbidden_credential(result):
            raise ConnectionSpikeLiveOperationError(
                "Round 5 runner result contained credential material and was discarded"
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

    def _parse_preflight_output(self, run_id: str, output: str) -> dict[str, object]:
        """Read the runner's measured capacity, and re-derive the verdict here.

        The runner reports both what it measured and whether that is sufficient. Only
        the measurements are taken. Re-evaluating them through the server's own copy of
        the capacity model means a runner cannot report itself adequate for 10,000
        clients per lane on a projection this side does not make, and a disagreement
        surfaces as a model digest mismatch, naming the drift rather than hiding it
        behind a bout that fails at 6,000 clients for no stated reason.
        """

        prefix = "PREFLIGHT_RESULT:"
        candidates = [
            line.removeprefix(prefix) for line in output.splitlines() if line.startswith(prefix)
        ]
        if len(candidates) != 1:
            code = _runner_error_code(output)
            raise ConnectionSpikeLiveOperationError(
                "Round 5 runner did not return exactly one capacity preflight"
                + (f": the runner refused with {code}" if code else "")
            )
        try:
            raw = json.loads(candidates[0])
        except json.JSONDecodeError as exc:
            raise ConnectionSpikeLiveOperationError(
                "Round 5 runner returned malformed capacity preflight JSON"
            ) from exc
        if not isinstance(raw, Mapping):
            raise ConnectionSpikeLiveOperationError(
                "Round 5 runner returned an unexpected capacity preflight shape"
            )
        expected_seal = {
            "protocol": FANIN_PROTOCOL,
            "schema_version": FANIN_SCHEMA_VERSION,
            "action": "preflight",
            "safety_evidence_version": SAFETY_EVIDENCE_VERSION,
            "contract_sha256": FanInContract().sha256,
            "config_sha256": fanin_config_sha256(),
            "capacity_model_sha256": fanin_capacity_model_sha256(),
            "generator_sha256": fanin_generator_sha256(),
            "runner_harness_sha256": self.config.runner_harness_sha256,
        }
        if any(raw.get(name) != expected for name, expected in expected_seal.items()):
            raise ConnectionSpikeLiveOperationError(
                "Round 5 capacity preflight provenance does not match the sealed protocol"
            )
        assets = raw.get("runner_asset_sha256s")
        if (
            not isinstance(assets, Mapping)
            or set(assets) != set(RUNNER_ASSETS)
            or any(
                not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None
                for value in assets.values()
            )
            or assets.get("round5_fanin.py") != raw.get("generator_sha256")
        ):
            raise ConnectionSpikeLiveOperationError(
                "Round 5 capacity preflight omitted complete five-file harness evidence"
            )
        shard = raw.get("shard_process_preflight")
        if (
            not isinstance(shard, Mapping)
            or shard.get("worker_count") != WORKER_COUNT
            or shard.get("unique_processes") != WORKER_COUNT
            or shard.get("unique_cpus") != WORKER_COUNT
            or shard.get("sufficient") is not True
        ):
            raise ConnectionSpikeLiveOperationError(
                "Round 5 capacity preflight omitted exact shard evidence"
            )
        advisories = raw.get("telemetry_advisories")
        if not isinstance(advisories, list) or any(
            not isinstance(value, str) or value not in ADVISORY_TELEMETRY_CODES
            for value in advisories
        ):
            raise ConnectionSpikeLiveOperationError(
                "Round 5 capacity preflight carried unknown advisory evidence"
            )
        return dict(raw)

    @staticmethod
    def _capacity_from_preflight(raw: Mapping[str, object]) -> CapacityPreflight:
        """Project the runner's measurements through the server's capacity model."""

        def integer(name: str) -> int:
            value = raw.get(name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise ConnectionSpikeLiveOperationError(
                    f"Round 5 capacity preflight omitted the measured {name}"
                )
            return value

        def number(name: str) -> float:
            value = raw.get(name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ConnectionSpikeLiveOperationError(
                    f"Round 5 capacity preflight omitted the measured {name}"
                )
            return float(value)

        boot_id = str(raw.get("boot_id") or "")
        if not boot_id or len(boot_id) > 64 or _RUN_ID.fullmatch(boot_id) is None:
            raise ConnectionSpikeLiveOperationError(
                "Round 5 capacity preflight omitted its boot identity"
            )
        try:
            return evaluate_capacity_preflight(
                instance_type=str(raw.get("instance_type") or ""),
                cpu_count=integer("cpu_count"),
                physical_memory_bytes=integer("physical_memory_bytes"),
                available_memory_bytes=integer("available_memory_bytes"),
                baseline_rss_bytes=integer("baseline_rss_bytes"),
                fd_soft_limit=integer("fd_soft_limit"),
                fd_hard_limit=integer("fd_hard_limit"),
                open_fds=integer("open_fds"),
                ephemeral_port_first=integer("ephemeral_port_first"),
                ephemeral_port_last=integer("ephemeral_port_last"),
                event_loop_p99_ms=number("event_loop_p99_ms"),
                cpu_calibration_ms=number("cpu_calibration_ms"),
                event_loop_microbatch_p99_ms=number("event_loop_microbatch_p99_ms"),
                event_loop_selector_fanout_peak_ms=number("event_loop_selector_fanout_peak_ms"),
                event_loop_selector_fanout_baseline_peak_ms=number(
                    "event_loop_selector_fanout_baseline_peak_ms"
                ),
                event_loop_selector_fanout_peak_deferred=integer(
                    "event_loop_selector_fanout_peak_deferred"
                ),
                boot_id=boot_id,
                runner_harness_sha256=str(raw.get("runner_harness_sha256") or ""),
                runner_asset_sha256s=dict(raw.get("runner_asset_sha256s") or {}),
            )
        except ValueError as exc:
            raise ConnectionSpikeLiveOperationError(
                f"Round 5 capacity preflight measurements are not usable: {exc}"
            ) from exc

    async def preflight_capacity(
        self,
        run_id: str,
        *,
        contract_sha256: str,
        config_sha256: str,
        generator_sha256: str,
        capacity_model_sha256: str,
    ) -> CapacityPreflight:
        """Measure on the runner whether 10,000 clients per lane can be held at all."""

        raw = await self.execute(
            run_id,
            fanin_preflight_request(
                run_id=run_id,
                runner_instance_type=self.config.runner_instance_type,
                contract_sha256=contract_sha256,
                config_sha256=config_sha256,
                generator_sha256=generator_sha256,
                capacity_model_sha256=capacity_model_sha256,
                runner_harness_sha256=self.config.runner_harness_sha256,
            ),
        )
        boot_id = str(raw.get("boot_id") or "")
        if not boot_id or len(boot_id) > 128:
            raise ConnectionSpikeLiveOperationError(
                "Round 5 capacity receipt omitted the runner boot identity"
            )
        capacity = self._capacity_from_preflight(raw)
        if (
            raw.get("sufficient") is not capacity.sufficient
            or raw.get("protocol_failures") != list(capacity.protocol_failures)
            or raw.get("hard_safety_failures") != list(capacity.hard_safety_failures)
            or raw.get("failures") != list(capacity.failures)
        ):
            raise ConnectionSpikeLiveOperationError(
                "Round 5 runner preflight verdict does not match re-derived evidence"
            )
        self._prepared_boot_id = boot_id
        self._prepared_capacity = capacity
        return capacity

    @staticmethod
    def _validate_run_id(run_id: str) -> None:
        if _RUN_ID.fullmatch(run_id) is None:
            raise ConnectionSpikeLiveConfigurationError("Round 5 run ID is invalid")


def _competitor_observer_digest(manifest: DemoManifest, competitor_id: str) -> str:
    """The sealed observer credential digest for the selected competitor lane.

    Returned empty for a seal minted before observer credentials were sealed. That is a
    truthful state rather than a broken one: the installation keeps serving every other
    round, and Round 5 refuses by name when the request would have to omit it.
    """

    resources = manifest.require_round5_resources()
    name = (
        "aurora_observer_credential_sha256"
        if competitor_id == "aurora_serverless_v2"
        else "rds_observer_credential_sha256"
    )
    return str(getattr(resources, name, "") or "")


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
        environment = round5_environment.aurora if round5_environment is not None else None
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
    *,
    runner_lane: Literal["lakebase", "competitor"] = "lakebase",
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
        runner_instance_id=(
            resources.runner_instance_id
            if runner_lane == "lakebase"
            else str(resources.competitor_runner_instance_id or "")
        ),
        runner_instance_profile_arn=(
            resources.runner_instance_profile_arn
            if runner_lane == "lakebase"
            else str(resources.competitor_runner_instance_profile_arn or "")
        ),
        runner_subnet_id=resources.runner_subnet_id,
        runner_security_group_id=(
            resources.runner_security_group_id
            if runner_lane == "lakebase"
            else str(resources.competitor_runner_security_group_id or "")
        ),
        targets=(
            ConnectionSpikeTarget(
                lane_id="lakebase",
                secret_arn="",
                endpoint_host=resources.lakebase_pooled_host,
                credential_host=resources.lakebase_direct_host,
                credential_sha256=resources.lakebase_credential_sha256,
                observer_credential_sha256=(resources.lakebase_observer_credential_sha256 or ""),
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
                observer_credential_sha256=_competitor_observer_digest(manifest, competitor_id),
            ),
        ),
        ssm_document_name=resources.ssm_document_name,
        runner_path=resources.runner_path,
        resident_control_queue_url=(
            str(resources.lakebase_control_queue_url)
            if runner_lane == "lakebase"
            else str(resources.competitor_control_queue_url)
        ),
        resident_control_secret_arn=str(
            resources.runner_control_secret_arn
            if runner_lane == "lakebase"
            else resources.competitor_runner_control_secret_arn
        ),
        resident_installation_id=(
            getattr(manifest, "installation_id", None)
            or getattr(manifest, "run_id", "test-installation")
        ),
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
        competitor_runner_instance_id=str(resources.competitor_runner_instance_id or ""),
        vpc_id=resources.vpc_id,
        proxy_subnet_ids=tuple(resources.proxy_subnet_ids),
        lakebase_direct_host=resources.lakebase_direct_host,
        lakebase_pooled_host=resources.lakebase_pooled_host,
        competitor_id=competitor_id,
        competitor_target_id=target_id,
        competitor_resource_id=resource_id,
        competitor_direct_host=direct_host,
        competitor_security_group_id=competitor_security_group_id,
        runner_security_group_id=str(resources.competitor_runner_security_group_id or ""),
        proxy_security_group_id=str(
            resources.aurora_proxy_security_group_id
            if competitor_id == "aurora_serverless_v2"
            else resources.rds_proxy_security_group_id
        ),
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
        proxy_role_permissions_boundary_arn=getattr(resources, "per_bout_role_boundary_arn", ""),
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


#: Keys whose values are method *labels* rather than credential material.
#:
#: `auth_method` legitimately carries `tls-cleartext-password`: that is the name of
#: a PostgreSQL authentication method, and Round 5 reports it because which method
#: the server negotiated is part of what the round proves. A scanner that flagged
#: the substring `password` anywhere would refuse every honest result from the
#: cleartext-password lane, so the exemption is by key and the key is named here
#: rather than inferred from the value.
_CREDENTIAL_LABEL_KEYS = frozenset({"auth_method", "auth_methods"})

#: What leaking credential material actually looks like in this payload: an
#: assignment, not a word. A connection string, a libpq keyword pair or an env dump
#: all take the form `name=value`, and it is the value after `=` that must never
#: reach a receipt or a log. Matching the assignment rather than the bare noun is
#: what lets `auth_method` above stay readable while still refusing `password=...`.
_FORBIDDEN_CREDENTIAL_ASSIGNMENT = re.compile(
    r"(?:password|passwd|pgpassword|secret|secret_access_key|session_token|token)"
    r"\s*[=:]\s*\S",
    re.IGNORECASE,
)
#: A key whose *name* is credential material. Needed alongside the assignment pattern above,
#: which only sees strings: a bare secret arriving as `{"session_token": "AQoDX..."}` carries
#: no `=` and would otherwise pass. Matched as a whole word against the key so
#: `credential_sha256` and `master_secret_arn` stay readable -- a digest is evidence and an ARN
#: is a control-plane reference, and refusing either would refuse every honest payload.
_FORBIDDEN_CREDENTIAL_KEYS = frozenset(
    {
        "password",
        "passwd",
        "pgpassword",
        "secret",
        "secret_access_key",
        "session_token",
        "access_key_id",
        "token",
        "secretstring",
    }
)


def _runner_result_has_forbidden_credential(raw: object) -> bool:
    """Does this runner payload carry credential material anywhere inside it?

    Round 5's result crosses a boundary the rest of the round does not: it comes
    back from an EC2 instance over SSM, is folded into a receipt, and receipts are
    pasted into notes and issues. The runner is written not to emit secrets, but
    "the runner is careful" is not a property this side can verify, so the payload
    is scanned before anything is retained.

    Walks the whole structure rather than a known set of fields, because the leak
    that matters is the one nobody predicted -- a diagnostic added to the runner in
    a hurry, under a key this file has never heard of. Keys are matched by name
    only to *exempt* label fields; nothing is trusted because of where it sits.

    Returns True when the payload must be refused, so a caller that cannot decide
    treats it as unsafe.
    """

    if isinstance(raw, Mapping):
        for key, value in raw.items():
            name = str(key).casefold()
            if isinstance(value, str) and str(key) in _CREDENTIAL_LABEL_KEYS:
                continue
            # The key alone is enough. A value under this name is credential material
            # whether or not it happens to look like an assignment.
            if name in _FORBIDDEN_CREDENTIAL_KEYS:
                return True
            if _runner_result_has_forbidden_credential(value):
                return True
        return False
    if isinstance(raw, (list, tuple, set, frozenset)):
        return any(_runner_result_has_forbidden_credential(item) for item in raw)
    if isinstance(raw, str):
        return _FORBIDDEN_CREDENTIAL_ASSIGNMENT.search(raw) is not None
    return False


def _require_sealed_payload(
    arm: FanInArm,
    raw: Mapping[str, object],
    *,
    lane_id: str | None = None,
) -> None:
    """Refuse a payload produced by a different contract, config or generator than was armed.

    Reported all at once, and as staleness rather than as malformed evidence, because the
    operator's next move differs: redeploy the runner, not debug the round. A stale generator
    that still answers is more dangerous than one that fails, since its numbers look exactly
    like a measurement.
    """

    preflight = arm.preflights.get(lane_id, arm.preflight) if lane_id else arm.preflight
    expected_seal = {
        "schema_version": FANIN_SCHEMA_VERSION,
        "protocol": FANIN_PROTOCOL,
        "contract_sha256": arm.contract_sha256,
        "config_sha256": arm.config_sha256,
        "generator_sha256": arm.generator_sha256,
        "capacity_model_sha256": arm.capacity_model_sha256,
        "runner_harness_sha256": preflight.runner_harness_sha256,
        "runner_boot_id": preflight.boot_id,
    }
    stale: list[str] = []
    for name, expected in expected_seal.items():
        observed = raw.get(name)
        # `bool` is a subclass of `int`, so True would otherwise satisfy a schema_version of 1.
        # Compared by type as well as value because a runner that reported `true` there has not
        # reported a version at all.
        if isinstance(expected, int) and not isinstance(expected, bool):
            matches = (
                isinstance(observed, int)
                and not isinstance(observed, bool)
                and observed == expected
            )
        else:
            matches = isinstance(observed, str) and observed == expected
        if not matches:
            stale.append(name)
    if stale:
        raise ConnectionSpikeLiveOperationError(
            "Round 5 runner evidence is stale: "
            f"{', '.join(stale)} does not match the sealed arm, so this payload was "
            "produced by a different contract, config or generator than the one armed"
        )
    assets = raw.get("runner_asset_sha256s")
    if (
        not isinstance(assets, Mapping)
        or set(assets) != set(RUNNER_ASSETS)
        or any(
            not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None
            for value in assets.values()
        )
        or assets.get("round5_fanin.py") != arm.generator_sha256
    ):
        raise ConnectionSpikeLiveOperationError(
            "Round 5 result omitted complete five-file harness evidence"
        )


def _finalize_raw_result(
    arm: FanInArm,
    raw: Mapping[str, object],
    *,
    cleanup_verified: bool = True,
) -> FanInRunResult:
    """Turn one runner payload into a scored v2 result, or refuse it.

    Refusal is the common case worth designing for. This payload crossed SSM from a
    machine that may be running an older generator than the arm was sealed against,
    and a stale generator that still answers is more dangerous than one that fails:
    its numbers look exactly like a measurement. So every digest in the seal is
    compared before any lane is read, and a mismatch is reported as staleness rather
    than as malformed evidence, because the operator's next move differs -- redeploy
    the runner, not debug the round.

    `cleanup_verified` defaults to True because the adapter has already refused any
    command that did not print `SETUP_SETTLED` and `RUNNER_FLOCK_RELEASED` by the
    time a payload reaches here. A caller that has not run that gate must pass what
    it actually observed; the default is not a claim this function can make.
    """

    _require_sealed_payload(arm, raw)

    # Diagnostics are not scored, but their absence is still a refusal: the runtime
    # gates (event-loop p99, selector amplification, CPU capacity) are what
    # distinguish 10,000 real clients from 10,000 numbers, and a payload that
    # carries no diagnostics cannot be checked against them at all.
    diagnostics = raw.get("runtime_diagnostics")
    if not isinstance(diagnostics, Mapping):
        raise ConnectionSpikeLiveOperationError(
            "Round 5 runner returned no runtime diagnostics, so the runtime gates "
            "this protocol depends on cannot be evaluated"
        )

    raw_lanes_value = raw.get("lanes")
    if not isinstance(raw_lanes_value, Sequence) or isinstance(raw_lanes_value, (str, bytes)):
        raise ConnectionSpikeLiveOperationError("Round 5 runner evidence carried no lanes")
    raw_lanes: dict[str, Mapping[str, object]] = {}
    for value in raw_lanes_value:
        if not isinstance(value, Mapping):
            raise ConnectionSpikeLiveOperationError("Round 5 runner returned a malformed lane")
        lane_id = str(value.get("lane_id") or "")
        if not lane_id:
            raise ConnectionSpikeLiveOperationError("Round 5 runner returned an unnamed lane")
        if lane_id in raw_lanes:
            raise ConnectionSpikeLiveOperationError(f"Round 5 runner returned lane {lane_id} twice")
        raw_lanes[lane_id] = value
    if len(raw_lanes) != len(RUNTIME_LANE_IDS):
        raise ConnectionSpikeLiveOperationError(
            f"Round 5 is a two-lane round; the runner returned {len(raw_lanes)} lane(s)"
        )

    lanes: dict[str, ConnectionSpikeLaneResult] = {}
    # Sorted so the left/right labels on the comparison are a property of the
    # installation rather than of dict ordering in the runner's JSON.
    for lane_id in sorted(raw_lanes):
        try:
            lanes[lane_id] = finalize_fanin_lane(
                raw_lanes[lane_id],
                expected_lane_id=lane_id,
                expected_config_sha256=arm.config_sha256,
                expected_generator_sha256=arm.generator_sha256,
                expected_capacity_model_sha256=arm.capacity_model_sha256,
                cleanup_verified=cleanup_verified,
            )
        except FanInError as exc:
            # The contract's refusal token is the whole diagnosis and is safe to
            # repeat: it is a fixed snake_case word chosen by this repository, never
            # a host, ARN or credential.
            raise ConnectionSpikeLiveOperationError(
                f"Round 5 lane {lane_id} failed the fan-in contract: {exc}"
            ) from exc

    left_id, right_id = sorted(lanes)
    return FanInRunResult(
        schema_version=FANIN_SCHEMA_VERSION,
        protocol=FANIN_PROTOCOL,
        contract_sha256=arm.contract_sha256,
        config_sha256=arm.config_sha256,
        generator_sha256=arm.generator_sha256,
        capacity_model_sha256=arm.capacity_model_sha256,
        lanes=lanes,
        # None when either lane did not verify. That is not an error: a lane that
        # held 9,999 clients has failed this protocol, and the round shows it
        # failing rather than comparing it.
        comparison=compare_fanin_lanes(lanes[left_id], lanes[right_id]),
        runtime_diagnostics=dict(diagnostics),
    )


def _finalize_lane_payload(
    arm: FanInArm,
    raw: Mapping[str, object],
    *,
    lane_id: str,
    cleanup_verified: bool = True,
) -> ConnectionSpikeLaneResult:
    """Verify one dispatch's seal and score the single lane it carries.

    Each lane is now dispatched on its own, so each payload is sealed on its own and is checked
    on its own. A runner that changed underneath the second dispatch is caught there rather than
    inherited from the first.
    """

    _require_sealed_payload(arm, raw, lane_id=lane_id)
    diagnostics = raw.get("runtime_diagnostics")
    if not isinstance(diagnostics, Mapping):
        raise ConnectionSpikeLiveOperationError(
            f"Round 5 lane {lane_id} returned no runtime diagnostics, so the runtime gates "
            "this protocol depends on cannot be evaluated"
        )
    raw_lanes_value = raw.get("lanes")
    if not isinstance(raw_lanes_value, Sequence) or isinstance(raw_lanes_value, (str, bytes)):
        raise ConnectionSpikeLiveOperationError(f"Round 5 lane {lane_id} evidence carried no lanes")
    payloads = [
        value
        for value in raw_lanes_value
        if isinstance(value, Mapping) and str(value.get("lane_id") or "") == lane_id
    ]
    if len(payloads) != 1:
        raise ConnectionSpikeLiveOperationError(
            f"Round 5 expected exactly one {lane_id} lane in this dispatch, got {len(payloads)}"
        )
    try:
        return finalize_fanin_lane(
            payloads[0],
            expected_lane_id=lane_id,
            expected_config_sha256=arm.config_sha256,
            expected_generator_sha256=arm.generator_sha256,
            expected_capacity_model_sha256=arm.capacity_model_sha256,
            cleanup_verified=cleanup_verified,
        )
    except FanInError as exc:
        # The contract's refusal token is the whole diagnosis and is safe to repeat: a fixed
        # snake_case word chosen by this repository, never a host, ARN or credential.
        raise ConnectionSpikeLiveOperationError(
            f"Round 5 lane {lane_id} failed the fan-in contract: {exc}"
        ) from exc


def _merge_lane_results(
    arm: FanInArm,
    lanes: Mapping[str, ConnectionSpikeLaneResult],
    diagnostics: Mapping[str, object],
) -> FanInRunResult:
    """One scored result from lanes that ran separately.

    A scored Round 5 still needs both lanes; that requirement simply lives here now, because the
    lanes no longer arrive in one payload. Each lane's time to 10,000 was measured from its own
    start, which is the point: neither lane's number is held hostage to the other's setup.
    """

    if len(lanes) != len(RUNTIME_LANE_IDS):
        raise ConnectionSpikeLiveOperationError(
            f"Round 5 is a two-lane comparison; {len(lanes)} lane(s) completed"
        )
    return FanInRunResult(
        schema_version=FANIN_SCHEMA_VERSION,
        protocol=FANIN_PROTOCOL,
        contract_sha256=arm.contract_sha256,
        config_sha256=arm.config_sha256,
        generator_sha256=arm.generator_sha256,
        capacity_model_sha256=arm.capacity_model_sha256,
        lanes=dict(lanes),
        # Physical runner monotonic clocks are never compared. The manager
        # builds the v3 result from one server-authoritative bell origin.
        comparison=None,
        # Kept per lane, because each lane ran its own dispatch and its own runtime gates.
        runtime_diagnostics={"by_lane": dict(diagnostics)},
    )


@dataclass(frozen=True)
class LiveRound5WarmEngineReceipt:
    arm: FanInArm
    setup_context: ConnectionSpikeWarmSetupContext
    runner_boot_ids: Mapping[str, str]
    runner_process_boot_ids: Mapping[str, str]
    warm_attempt_token: str
    dispatch_expires_at: Mapping[str, datetime]


class LiveConnectionSpikeEngine:
    """Manager-facing lifecycle wrapper; scoring remains in the pure Round 5 core."""

    def __init__(
        self,
        adapter: LiveConnectionSpikeAdapter,
        *,
        lane_adapters: Mapping[str, LiveConnectionSpikeAdapter] | None = None,
        setup_orchestrator: LiveConnectionSpikeSetupOrchestrator | None = None,
        run_id_factory: Callable[[], str] = lambda: f"r5-{uuid4().hex}",
    ) -> None:
        self._adapter = adapter
        self._lane_adapters = dict(
            lane_adapters
            or {
                "lakebase": adapter,
                "competitor": adapter,
            }
        )
        if set(self._lane_adapters) != {"lakebase", "competitor"}:
            raise ValueError("Round 5 requires one adapter per lane")
        self._setup_orchestrator = setup_orchestrator
        self._run_id_factory = run_id_factory
        self._armed: FanInArm | None = None
        self._active_run_ids: dict[str, str] = {}
        self._setup_result: ConnectionSpikeSetupResult | None = None
        self._setup_bout_id: str | None = None
        self._setup_task: asyncio.Task[Any] | None = None
        self._cleanup_bout_id: str | None = None
        self._cleanup_start_lock = asyncio.Lock()
        #: Ramps started by a lane's own setup stop, awaited by `run`. Populated during the setup
        #: phase, which is the point: Lakebase's ten thousand is held while the AWS path is still
        #: building its Proxy.
        self._lane_bursts: dict[str, asyncio.Task[ConnectionSpikeLaneResult]] = {}
        self._lane_stops: dict[str, ConnectionSpikeSetupLaneStop] = {}
        self._lane_progress_callback: ProgressCallback | None = None
        self._lane_result_callback: ProgressCallback | None = None
        self._fatal_lane_error: BaseException | None = None
        self._capsule_refresh_error: BaseException | None = None
        self._job_ids: dict[str, str] = {}
        self._bell_t0_ns: int | None = None
        self._warm_generation = 0
        self._warm_attempt_token = ""
        self._bound_claim: object | None = None
        self._resident_bindings: dict[str, Round5ControlBinding] = {}
        self._durable_lakebase_release: Round5ControlEvent | None = None

    @property
    def has_timed_setup(self) -> bool:
        return self._setup_orchestrator is not None

    def bind_claim(self, claim: object) -> None:
        lakebase = str(getattr(claim, "lakebase_job_id", ""))
        competitor = str(getattr(claim, "competitor_job_id", ""))
        if any(re.fullmatch(r"[0-9a-f]{64}", value) is None for value in (lakebase, competitor)):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 claim omitted deterministic lane job identities"
            )
        self._job_ids = {
            "lakebase": lakebase,
            "competitor": competitor,
        }
        generation = getattr(claim, "capsule_generation", 0)
        if isinstance(generation, bool) or not isinstance(generation, int) or generation <= 0:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 claim omitted its resident generation"
            )
        self._warm_generation = generation
        warm_attempt_token = str(getattr(claim, "warm_attempt_token", ""))
        if not warm_attempt_token or (
            self._warm_attempt_token and warm_attempt_token != self._warm_attempt_token
        ):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 claim does not match the resident warm attempt"
            )
        self._warm_attempt_token = warm_attempt_token
        self._bound_claim = claim

    def bind_bell(self, context: Any) -> None:
        t0_ns = context.t0_monotonic_ns
        if isinstance(t0_ns, bool) or not isinstance(t0_ns, int) or t0_ns <= 0:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 bell context omitted its server origin"
            )
        self._bell_t0_ns = t0_ns
        release = self._durable_lakebase_release
        if (
            release is None
            or release.binding.bell_id != context.bell_id
            or release.binding.claim_id != context.claim_id
        ):
            raise ConnectionSpikeLiveConfigurationError(
                "Lakebase durable release is unavailable after bell acceptance"
            )
        transport = self._lane_adapters["lakebase"]._resident_transport
        if transport is None:
            raise ConnectionSpikeLiveConfigurationError("Round 5 resident transport is unavailable")
        # The atomic transaction made RELEASE durable.  Only this process-local
        # edge, after T0 exists, permits the dispatcher to publish it.
        transport.dispatcher.allow_release(release.event_id)

    def lakebase_release_event(self, bell_at: datetime) -> Round5ControlEvent:
        binding = self._resident_bindings.get("lakebase")
        if binding is None:
            raise ConnectionSpikeLiveConfigurationError(
                "Lakebase resident request was not staged before bell"
            )
        event = Round5ControlEvent.create(
            binding=binding,
            sequence=2,
            kind=Round5ControlKind.RELEASE,
            created_at=bell_at,
            payload={"bell_id": binding.bell_id},
        )
        self._durable_lakebase_release = event
        return event

    async def warm(
        self,
        generation: int,
        warm_attempt_token: str,
    ) -> LiveRound5WarmEngineReceipt:
        """Prepare setup and both physical runner capsules backstage."""

        if self._setup_orchestrator is None:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 automatic warm setup is not configured"
            )
        setup_context, arm = await asyncio.gather(
            self._setup_orchestrator.warm(generation),
            self.check(),
        )
        self._warm_generation = generation
        self._warm_attempt_token = warm_attempt_token
        targets_by_lane = {target.lane_id: target for target in self._adapter.config.targets}
        await asyncio.gather(
            *(
                adapter.stage_resident_generation(
                    generation=generation,
                    lane_id=lane_id,
                    warm_attempt_token=warm_attempt_token,
                    request_template=self._fanin_request(
                        hashlib.sha256(
                            (
                                f"round5-resident\0{generation}\0{warm_attempt_token}\0{lane_id}"
                            ).encode()
                        ).hexdigest(),
                        arm,
                        (targets_by_lane[lane_id],),
                        lane_ids=(lane_id,),
                    ),
                )
                for lane_id, adapter in self._lane_adapters.items()
            )
        )
        expirations = {
            lane_id: adapter.prepared_expires_at for lane_id, adapter in self._lane_adapters.items()
        }
        if any(value is None for value in expirations.values()):
            raise ConnectionSpikeLiveOperationError(
                "Round 5 warm dispatch credentials were not retained"
            )
        return LiveRound5WarmEngineReceipt(
            arm=arm,
            setup_context=setup_context,
            runner_boot_ids={
                lane_id: adapter.prepared_boot_id
                for lane_id, adapter in self._lane_adapters.items()
            },
            runner_process_boot_ids={
                lane_id: adapter._resident_process_boot_id
                for lane_id, adapter in self._lane_adapters.items()
            },
            warm_attempt_token=warm_attempt_token,
            dispatch_expires_at={
                lane_id: value for lane_id, value in expirations.items() if value is not None
            },
        )

    async def refresh_warm(self, generation: int) -> LiveRound5WarmEngineReceipt:
        """Rotate launch credentials without rerunning the capacity benchmark."""

        if self._setup_orchestrator is None or self._armed is None:
            raise ConnectionSpikeLiveOperationError("Round 5 cannot refresh before a complete warm")
        setup_context, *dispatch_expirations = await asyncio.gather(
            self._setup_orchestrator.warm(generation),
            *(
                adapter.refresh_launch_context(f"warm-{generation}-{lane_id}-{uuid4().hex[:8]}")
                for lane_id, adapter in self._lane_adapters.items()
            ),
        )
        return LiveRound5WarmEngineReceipt(
            arm=self._armed,
            setup_context=setup_context,
            runner_boot_ids={
                lane_id: adapter.prepared_boot_id
                for lane_id, adapter in self._lane_adapters.items()
            },
            runner_process_boot_ids={
                lane_id: adapter._resident_process_boot_id
                for lane_id, adapter in self._lane_adapters.items()
            },
            warm_attempt_token=self._warm_attempt_token,
            dispatch_expires_at=dict(zip(self._lane_adapters, dispatch_expirations, strict=True)),
        )

    async def validate_ready_provenance(
        self,
        shared_receipt: object,
        warm_attempt_token: str,
    ) -> bool:
        """Re-read physical boot and five-file harness evidence off the request path."""

        # Capacity is benchmarked once while WARMING. READY validation must not
        # continuously dispatch that expensive SSM preflight; the resident
        # heartbeat below proves that the same boot, process, and loaded harness
        # are still serving the prepared generation.
        arm = self._armed
        if arm is None:
            return False
        expected = {
            "lakebase": shared_receipt.lakebase_runner,
            "competitor": shared_receipt.competitor_runner,
        }
        static_current = all(
            lane_id in arm.preflights
            and arm.preflights[lane_id].boot_id == receipt.boot_id
            and self._lane_adapters[lane_id]._resident_process_boot_id == receipt.process_boot_id
            and self._lane_adapters[lane_id]._resident_process_pid == receipt.process_pid
            and arm.preflights[lane_id].runner_harness_sha256 == receipt.loaded_harness_sha256
            and arm.preflights[lane_id].model_sha256 == receipt.capacity_model_sha256
            for lane_id, receipt in expected.items()
        )
        if not static_current:
            return False
        checks = []
        for lane_id, receipt in expected.items():
            transport = self._lane_adapters[lane_id]._resident_transport
            if transport is None:
                return False
            checks.append(
                transport.resident_is_current(
                    installation_id=(self._lane_adapters[lane_id].config.resident_installation_id),
                    lane_id=lane_id,
                    warm_attempt_token=warm_attempt_token,
                    runner_boot_id=receipt.boot_id,
                    process_boot_id=receipt.process_boot_id,
                    process_pid=receipt.process_pid,
                    harness_sha256=receipt.loaded_harness_sha256,
                    now=datetime.now(UTC),
                )
            )
        return all(await asyncio.gather(*checks))

    async def warm_with_physical_runners_from(
        self,
        source: LiveConnectionSpikeEngine,
        generation: int,
    ) -> LiveRound5WarmEngineReceipt:
        """Warm another target variant without benchmarking the runners again."""

        if self._setup_orchestrator is None or source._armed is None:
            raise ConnectionSpikeLiveOperationError(
                "Round 5 shared physical runner receipt is unavailable"
            )
        self._lane_adapters = source._lane_adapters
        self._armed = source._armed
        self._warm_generation = generation
        self._warm_attempt_token = source._warm_attempt_token
        setup_context = await self._setup_orchestrator.warm(generation)
        expirations = {
            lane_id: adapter.prepared_expires_at for lane_id, adapter in self._lane_adapters.items()
        }
        if any(value is None for value in expirations.values()):
            raise ConnectionSpikeLiveOperationError(
                "Round 5 shared dispatch credentials are unavailable"
            )
        return LiveRound5WarmEngineReceipt(
            arm=self._armed,
            setup_context=setup_context,
            runner_boot_ids={
                lane_id: adapter.prepared_boot_id
                for lane_id, adapter in self._lane_adapters.items()
            },
            runner_process_boot_ids={
                lane_id: adapter._resident_process_boot_id
                for lane_id, adapter in self._lane_adapters.items()
            },
            warm_attempt_token=self._warm_attempt_token,
            dispatch_expires_at={
                lane_id: value for lane_id, value in expirations.items() if value is not None
            },
        )

    async def prepare(self, bout_id: str, fencing_token: int) -> None:
        """Run the untimed preparation now, so the bell starts the clock.

        Silent when this installation has no timed setup to prepare for, because then there is
        no untimed phase to move: the round arms and rings in one step.
        """

        if self._setup_orchestrator is None:
            return
        await self._setup_orchestrator.prepare(bout_id, fencing_token)
        claim = self._bound_claim
        arm = self._armed
        if claim is None or arm is None:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 resident claim was not bound before ARM"
            )
        lakebase_target = next(
            target for target in self._adapter.config.targets if target.lane_id == "lakebase"
        )
        request = self._fanin_request(
            self._job_ids["lakebase"],
            arm,
            (lakebase_target,),
            lane_ids=("lakebase",),
        )
        binding = self._resident_binding("lakebase", request)
        transport = self._lane_adapters["lakebase"]._resident_transport
        if transport is None:
            raise ConnectionSpikeLiveConfigurationError("Round 5 resident transport is unavailable")
        # Record ownership of the resident stage BEFORE enqueuing it.  ``stage``
        # publishes the STAGE control event (which boots/prepares the resident
        # agent) and then blocks on ``wait_prepared``; if that wait fails or the
        # process dies mid-stage, a later cancellation must still be able to find
        # this binding to cancel and settle the resident.  Recording it only
        # after ``stage`` returns would strand a prepared resident with no owner.
        self._resident_bindings["lakebase"] = binding
        # ARM is O(1)-shaped against a READY warm slot: automatic backstage warm
        # (LiveConnectionSpikeEngine.warm -> stage_resident_generation) has
        # already staged this lane's resident pool and benchmarked capacity, so
        # this call is a fast rebind of the warm pool, not a cold per-bout
        # PREPARED build. Bound it to the ARM budget rather than the 720s
        # bout-execution deadline: if the warm pool is not actually PREPARED the
        # ARM fails fast with a clear "ring was not warm" error instead of
        # blocking the operator for up to twelve minutes. The slow cold staging
        # stays backstage, where the contract requires it.
        try:
            async with asyncio.timeout(ROUND5_ARM_STAGE_DEADLINE_SECONDS):
                await transport.stage(
                    binding=binding,
                    request=request,
                )
        except TimeoutError as exc:
            raise ConnectionSpikeLiveOperationError(
                "Round 5 ARM exceeded its bounded staging budget "
                f"({ROUND5_ARM_STAGE_DEADLINE_SECONDS:.0f}s): the Lakebase resident pool "
                "was not already warm/PREPARED. ARM must be a fast rebind against a READY "
                "warm slot; a cold per-bout stage here means the ring was not actually warm."
            ) from exc

    async def setup(
        self,
        bout_id: str,
        fencing_token: int,
        on_progress: SetupProgressCallback | None = None,
        on_lane_progress: ProgressCallback | None = None,
        on_lane_result: ProgressCallback | None = None,
    ) -> ConnectionSpikeSetupResult:
        if self._setup_orchestrator is None:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 timed setup orchestration is not configured"
            )
        self._setup_bout_id = bout_id
        self._lane_bursts = {}
        self._lane_stops = {}
        self._lane_progress_callback = on_lane_progress
        self._lane_result_callback = on_lane_result
        self._fatal_lane_error = None
        self._capsule_refresh_error = None
        setup_task = asyncio.current_task()
        assert setup_task is not None
        self._setup_task = setup_task
        capsule_stop = asyncio.Event()
        capsule_task = asyncio.create_task(
            self._keep_competitor_capsule_fresh(bout_id, capsule_stop),
            name=f"round5-competitor-capsule-{bout_id}",
        )

        def stop_setup_on_capsule_failure(done: asyncio.Task[None]) -> None:
            if done.cancelled():
                return
            failure = done.exception()
            if failure is None:
                return
            self._capsule_refresh_error = failure
            if not setup_task.done():
                setup_task.cancel()

        capsule_task.add_done_callback(stop_setup_on_capsule_failure)
        try:
            try:
                result = await (
                    self._setup_orchestrator.setup(
                        bout_id,
                        fencing_token,
                        on_progress,
                        self._start_lane_burst,
                        self._stage_competitor_burst,
                        t0_ns=self._bell_t0_ns,
                    )
                    if self._bell_t0_ns is not None
                    else self._setup_orchestrator.setup(
                        bout_id,
                        fencing_token,
                        on_progress,
                        self._start_lane_burst,
                        self._stage_competitor_burst,
                    )
                )
            except asyncio.CancelledError:
                fatal = self._fatal_lane_error or self._capsule_refresh_error
                if fatal is not None:
                    raise fatal from None
                raise
            self._setup_result = result
            return result
        finally:
            capsule_stop.set()
            capsule_task.cancel()
            await asyncio.gather(capsule_task, return_exceptions=True)
            if self._setup_task is setup_task:
                self._setup_task = None

    async def _keep_competitor_capsule_fresh(
        self,
        bout_id: str,
        stop: asyncio.Event,
    ) -> None:
        adapter = self._lane_adapters["competitor"]
        sequence = 0
        while not stop.is_set():
            await adapter.ensure_dispatch_capsule(
                f"{bout_id}-proxy-wait-{sequence}",
            )
            sequence += 1
            try:
                await asyncio.wait_for(
                    stop.wait(),
                    timeout=DISPATCH_CAPSULE_REFRESH_INTERVAL_SECONDS,
                )
            except TimeoutError:
                continue

    async def _stage_competitor_burst(
        self,
        stop: ConnectionSpikeSetupLaneStop,
    ) -> None:
        """Prepare the exact late-bound Proxy request before its ready gate."""

        if stop.lane_id != "competitor":
            raise ConnectionSpikeLiveConfigurationError(
                "Only the competitor lane may stage behind the Proxy gate"
            )
        arm = self._armed
        if arm is None:
            raise ConnectionSpikeLiveOperationError("Round 5 warm capacity receipt is unavailable")
        if stop.lane_id in self._resident_bindings:
            raise ConnectionSpikeLiveOperationError("Round 5 competitor lane was already staged")
        target = self._runtime_target_for(stop)
        if target is None:
            raise ConnectionSpikeLiveOperationError("Round 5 competitor warm binding is incomplete")
        run_id = self._job_ids.get(stop.lane_id) or self._run_id_factory()
        request = self._fanin_request(
            run_id,
            arm,
            (target,),
            lane_ids=(stop.lane_id,),
        )
        binding = self._resident_binding(stop.lane_id, request)
        self._resident_bindings[stop.lane_id] = binding
        self._active_run_ids[stop.lane_id] = run_id
        try:
            await self._lane_adapters[stop.lane_id].stage_prepared_release(
                binding=binding,
                request=request,
            )
        except BaseException:
            # Keep the active id and binding: STAGE may have crossed the
            # delivery boundary, so cleanup must cancel this exact job.
            raise

    async def _start_lane_burst(self, stop: ConnectionSpikeSetupLaneStop) -> None:
        """Launch this lane's 10,000 now that its own setup has verified.

        The other lane is not consulted. That is the whole change: Lakebase has no reason to wait
        on an RDS Proxy build, and a round that makes it wait shows nothing for eleven minutes.

        Launched as a task rather than awaited, so the setup phase is not blocked by a ramp and the
        other lane keeps building. `run` awaits these.
        """

        arm = self._armed
        if arm is None:
            raise ConnectionSpikeLiveOperationError("Round 5 warm capacity receipt is unavailable")
        if stop.lane_id in self._lane_bursts:
            raise ConnectionSpikeLiveOperationError(
                f"Round 5 {stop.lane_id} lane was already dispatched"
            )
        self._lane_stops[stop.lane_id] = stop
        target = self._runtime_target_for(stop)
        if target is None:
            raise ConnectionSpikeLiveOperationError(
                f"Round 5 {stop.lane_id} warm binding is incomplete"
            )
        run_id = self._job_ids.get(stop.lane_id) or self._run_id_factory()
        launched = asyncio.create_task(
            self._dispatch_lane(run_id, arm, target),
            name=f"round5-burst-{stop.lane_id}",
        )
        self._lane_bursts[stop.lane_id] = launched

        def stop_sibling_on_failure(done: asyncio.Task[ConnectionSpikeLaneResult]) -> None:
            if done.cancelled():
                return
            try:
                failure = done.exception()
            except asyncio.CancelledError:
                return
            if failure is None:
                return
            self._fatal_lane_error = failure
            for lane_id, sibling in self._lane_bursts.items():
                if lane_id != stop.lane_id and not sibling.done():
                    sibling.cancel()
            setup_task = self._setup_task
            if setup_task is not None and not setup_task.done():
                setup_task.cancel()

        launched.add_done_callback(stop_sibling_on_failure)

    def _runtime_target_for(
        self, stop: ConnectionSpikeSetupLaneStop
    ) -> ConnectionSpikeTarget | None:
        """This lane's binding for the ramp: the endpoint setup produced, sealed credentials.

        The observer digest and the direct host come from the configured lane rather than the stop,
        because they are sealed once at install time and are the same in every bout; the stop
        carries what this bout changed, which is the endpoint the clients connect to.
        """

        configured = {target.lane_id: target for target in self._adapter.config.targets}
        lane = configured.get(stop.lane_id)
        if lane is None:
            return None
        try:
            return self._bind_lane(stop, lane)
        except (AttributeError, ConnectionSpikeLiveConfigurationError) as exc:
            raise ConnectionSpikeLiveConfigurationError(
                f"Round 5 {stop.lane_id} warm binding is incomplete"
            ) from exc

    @staticmethod
    def _bind_lane(
        stop: ConnectionSpikeSetupLaneStop,
        lane: ConnectionSpikeTarget,
    ) -> ConnectionSpikeTarget:
        return ConnectionSpikeTarget(
            lane_id=stop.lane_id,
            secret_arn=stop.secret_arn,
            endpoint_host=stop.endpoint_host,
            credential_host=lane.credential_host,
            competitor_id=lane.competitor_id,
            competitor_target_id=lane.competitor_target_id,
            competitor_resource_id=lane.competitor_resource_id,
            credential_sha256=stop.credential_sha256,
            observer_credential_sha256=lane.observer_credential_sha256,
        )

    async def _dispatch_lane(
        self,
        run_id: str,
        arm: FanInArm,
        target: ConnectionSpikeTarget,
    ) -> ConnectionSpikeLaneResult:
        """One lane's ramp, hold and sampling, scored on arrival."""

        adapter = self._lane_adapters[target.lane_id]
        self._active_run_ids[target.lane_id] = run_id
        try:

            async def report(progress: FanInProgress) -> None:
                callback = self._lane_progress_callback
                if callback is not None:
                    await callback(progress)

            request = self._fanin_request(
                run_id,
                arm,
                (target,),
                lane_ids=(target.lane_id,),
            )
            binding = self._resident_bindings.get(target.lane_id)
            if self._bound_claim is not None:
                if binding is None:
                    binding = self._resident_binding(target.lane_id, request)
                    self._resident_bindings[target.lane_id] = binding
                elif binding.request_sha256 != canonical_request_sha256(request):
                    raise ConnectionSpikeLiveConfigurationError(
                        "Round 5 staged request changed before release"
                    )
            execute_kwargs: dict[str, object] = {
                "targets": (target,),
                "on_progress": report,
            }
            if binding is not None:
                execute_kwargs["resident_binding"] = binding
            raw = await adapter.execute_prepared(
                run_id,
                request,
                **execute_kwargs,
            )
            result = _finalize_lane_payload(arm, raw, lane_id=target.lane_id)
            callback = self._lane_result_callback
            if callback is not None:
                await callback(result)
            return result
        finally:
            if self._active_run_ids.get(
                target.lane_id
            ) == run_id and not adapter.settlement_pending(run_id):
                self._active_run_ids.pop(target.lane_id, None)

    async def check(self) -> FanInArm:
        """Arm one fan-in bout, refusing before a runner that cannot hold it.

        The arm is not a formality. It fixes the four digests the runner compares against
        the files it is about to execute, and it records the capacity measured on that
        machine, so a bout can only be scored against the contract that was armed for it.
        """

        if self._setup_orchestrator is None:
            await self._adapter.check()
        # No setup-result requirement. The capacity preflight measures this runner's memory,
        # file descriptors, ephemeral ports and event loop, none of which depend on a lane, so
        # the answer is the same before the bell as after it. Requiring both setup clocks first
        # put an SSM round trip after the bell, inside the dead period the round is judged on,
        # and made a runner that cannot hold 10,000 clients refuse only after paying for an
        # eleven-minute Proxy build. `run` still refuses without the endpoints setup produces.
        contract = FanInContract()
        config_sha256 = fanin_config_sha256()
        generator_sha256 = fanin_generator_sha256()
        capacity_model_sha256 = fanin_capacity_model_sha256()
        preflights = await asyncio.gather(
            *(
                adapter.preflight_capacity(
                    self._run_id_factory(),
                    contract_sha256=contract.sha256,
                    config_sha256=config_sha256,
                    generator_sha256=generator_sha256,
                    capacity_model_sha256=capacity_model_sha256,
                )
                for adapter in self._lane_adapters.values()
            )
        )
        failed = [
            f"{lane_id}:{','.join(preflight.failures)}"
            for (lane_id, _adapter), preflight in zip(
                self._lane_adapters.items(),
                preflights,
                strict=True,
            )
            if not preflight.sufficient
        ]
        if failed:
            # Named failures, because each one sends the operator somewhere different: a
            # small instance shape, a file-descriptor limit, an event loop already under
            # pressure. A bare "insufficient" would send them to all three.
            raise ConnectionSpikeLiveOperationError(
                "Round 5 runner cannot hold 10,000 clients per lane: " + ", ".join(failed)
            )
        preflight = preflights[0]
        arm = FanInArm(
            arm_id=secrets.token_urlsafe(18),
            contract_sha256=contract.sha256,
            config_sha256=config_sha256,
            generator_sha256=generator_sha256,
            capacity_model_sha256=capacity_model_sha256,
            preflight=preflight,
            preflights={
                lane_id: lane_preflight
                for (lane_id, _adapter), lane_preflight in zip(
                    self._lane_adapters.items(),
                    preflights,
                    strict=True,
                )
            },
        )
        self._armed = arm
        return arm

    def _fanin_request(
        self,
        run_id: str,
        arm: FanInArm,
        targets: Sequence[ConnectionSpikeTarget],
        *,
        lane_ids: Sequence[str] = (),
    ) -> dict[str, object]:
        """Build one lane's bout request, refusing a lane that cannot be proved.

        `lane_ids` selects which of the bound lanes this dispatch runs. One lane at a time is
        the normal case: two lanes ramping in the same process put both their handshake batches
        in one event-loop turn, so each got half the budget and neither reached 10,000.
        """

        by_lane = {target.lane_id: target for target in targets}
        selected = tuple(lane_ids) or tuple(sorted(by_lane))
        missing = [lane_id for lane_id in selected if lane_id not in by_lane]
        if missing:
            raise ConnectionSpikeLiveConfigurationError(
                f"Round 5 has no binding for {', '.join(sorted(missing))}"
            )
        lanes = [by_lane[lane_id] for lane_id in selected]
        unproved = [
            lane.lane_id
            for lane in lanes
            if not lane.credential_sha256 or not lane.observer_credential_sha256
        ]
        if unproved:
            # Round 5's claim is that one pooled endpoint multiplexes 10,000 clients onto
            # a small backend session count. The observer credential is how that is
            # watched, from a second role on its own direct connection, so a lane without
            # one could report 10,000 clients with nothing checking the sessions behind
            # them. Refused here by name rather than left to the runner, whose token for
            # it -- `baseline_auth_invalid` -- says nothing about which lane or why.
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 cannot prove multiplexing for "
                + ", ".join(unproved)
                + ": the seal names no client or observer credential digest, which an "
                "installation sealed before the fan-in protocol will not have. Re-run "
                "setup to reseal Round 5."
            )
        # Built here rather than through `fanin_run_request`, whose signature names both lanes
        # and therefore cannot express one. The shape is identical; only the lanes present
        # differ, and the runner validates that `baseline_auth` names exactly the lanes the
        # targets name.
        baseline_auth: dict[str, dict[str, str]] = {}
        for lane in lanes:
            entry = {
                "credential_sha256": lane.credential_sha256,
                "observer_credential_sha256": lane.observer_credential_sha256,
            }
            if lane.lane_id != "lakebase":
                # Asymmetric on purpose and enforced by the runner: Aurora and RDS are two
                # separately sealed credentials, so the competitor lane has to say which one it
                # is holding, and the Lakebase lane has exactly one so saying is over-specifying.
                entry["credential_id"] = (
                    "aurora" if lane.competitor_id == "aurora_serverless_v2" else "rds"
                )
            baseline_auth[lane.lane_id] = entry
        request: dict[str, object] = {
            "protocol": FANIN_PROTOCOL,
            "schema_version": FANIN_SCHEMA_VERSION,
            "action": "run_lane_v3",
            "run_id": run_id,
            "job_id": run_id,
            "resident_generation": self._warm_generation,
            "runner_instance_type": self._adapter.config.runner_instance_type,
            "contract_sha256": arm.contract_sha256,
            "config_sha256": arm.config_sha256,
            "generator_sha256": arm.generator_sha256,
            "capacity_model_sha256": arm.capacity_model_sha256,
            "runner_harness_sha256": (
                arm.preflights.get(lanes[0].lane_id, arm.preflight).runner_harness_sha256
            ),
            "capacity_receipt": (arm.preflights.get(lanes[0].lane_id, arm.preflight).receipt),
            "trust_bundle_path": FANIN_TRUST_BUNDLE_PATH,
            "trust_bundle_sha256": self._adapter.config.trust_bundle_sha256,
            "baseline_auth": baseline_auth,
            "targets": [lane.runner_value() for lane in lanes],
        }
        request["prepared_request_digest"] = hashlib.sha256(
            json.dumps(
                request,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        return request

    def _resident_binding(
        self,
        lane_id: str,
        request: Mapping[str, object],
    ) -> Round5ControlBinding:
        claim = self._bound_claim
        adapter = self._lane_adapters[lane_id]
        if claim is None:
            raise ConnectionSpikeLiveConfigurationError("Round 5 resident claim is unavailable")
        return Round5ControlBinding(
            installation_id=adapter.config.resident_installation_id,
            lane_id=lane_id,
            generation=self._warm_generation,
            warm_attempt_token=self._warm_attempt_token,
            claim_id=str(claim.claim_id),
            bout_id=str(claim.bout_id),
            bell_id=str(claim.bell_id),
            fence=int(claim.bout_fence),
            job_id=str(request["job_id"]),
            runner_boot_id=adapter.prepared_boot_id,
            runner_process_boot_id=adapter._resident_process_boot_id,
            runner_harness_sha256=adapter.config.runner_harness_sha256,
            request_sha256=canonical_request_sha256(request),
        )

    async def run(
        self,
        arm: FanInArm,
        on_progress: ProgressCallback | None = None,
    ) -> FanInRunResult:
        if arm is not self._armed:
            raise ConnectionSpikeLiveOperationError(
                "Round 5 arm is stale or belongs to another run"
            )
        if self._setup_orchestrator is not None and self._setup_result is None:
            # The precondition moved here from arming, where it did not belong. A bout needs the
            # endpoints timed setup produces; measuring the runner never did.
            raise ConnectionSpikeLiveOperationError(
                "Round 5 burst cannot run before both timed setup stops"
            )
        targets = self._runtime_targets()
        effective = tuple(targets if targets is not None else self._adapter.config.targets)
        lane_ids = tuple(sorted(target.lane_id for target in effective))
        merged: dict[str, ConnectionSpikeLaneResult] = {}
        diagnostics: dict[str, object] = {}
        try:
            launched = {lane_id: self._lane_bursts.pop(lane_id, None) for lane_id in lane_ids}
            missing = [lane_id for lane_id, task in launched.items() if task is None]
            if missing:
                raise ConnectionSpikeLiveOperationError(
                    f"Round 5 {', '.join(missing)} was not dispatched at its eligibility edge; "
                    "no after-bell fallback is permitted"
                )
            tasks = {task for task in launched.values() if task is not None}
            await self._report(
                on_progress,
                "verifying",
                "Supervising both independently dispatched physical runners",
            )
            done, pending = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_EXCEPTION,
            )
            failed = next(
                (
                    task.exception()
                    for task in done
                    if not task.cancelled() and task.exception() is not None
                ),
                None,
            )
            if failed is not None:
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                cancellation_results = await asyncio.gather(
                    *(
                        self._cancel_resident_lane(lane_id, run_id)
                        for lane_id, run_id in tuple(self._active_run_ids.items())
                    ),
                    return_exceptions=True,
                )
                if any(isinstance(result, BaseException) for result in cancellation_results):
                    raise ConnectionSpikeCleanupError(
                        "Round 5 sibling jobs did not settle after a lane failure"
                    ) from failed
                raise failed
            if pending:
                await asyncio.gather(*pending)
            for lane_id, task in launched.items():
                assert task is not None
                merged[lane_id] = task.result()
            result = _merge_lane_results(arm, merged, diagnostics)
            await self._report(on_progress, "verified", "Runner evidence verified")
            return result
        finally:
            for lane_id, run_id in tuple(self._active_run_ids.items()):
                if not self._lane_adapters[lane_id].settlement_pending(run_id):
                    self._active_run_ids.pop(lane_id, None)

    async def stop_and_begin_cleanup(self, arm: FanInArm) -> None:
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

    async def _cancel_resident_lane(self, lane_id: str, run_id: str) -> None:
        adapter = self._lane_adapters[lane_id]
        binding = self._resident_bindings.get(lane_id)
        if binding is not None:
            await adapter.cancel_resident(binding=binding)
            return
        await adapter.cancel_resident(
            generation=self._warm_generation,
            lane_id=lane_id,
            job_id=run_id,
        )

    async def _stop_and_begin_cleanup_once(self) -> None:
        async with self._cleanup_start_lock:
            if self._cleanup_bout_id is not None:
                return
            for lane_id, run_id in tuple(self._active_run_ids.items()):
                await self._cancel_resident_lane(lane_id, run_id)
                if self._active_run_ids.get(lane_id) == run_id:
                    self._active_run_ids.pop(lane_id, None)
            if self._active_run_ids:
                raise ConnectionSpikeCleanupError(
                    "Round 5 active jobs did not settle before provider cleanup"
                )
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
        bursts = tuple(self._lane_bursts.values())
        for burst in bursts:
            if not burst.done():
                burst.cancel()
        if bursts:
            await asyncio.gather(*bursts, return_exceptions=True)
        for lane_id, run_id in tuple(self._active_run_ids.items()):
            await self._cancel_resident_lane(lane_id, run_id)
            if self._active_run_ids.get(lane_id) == run_id:
                self._active_run_ids.pop(lane_id, None)
        if self._active_run_ids:
            raise ConnectionSpikeCleanupError(
                "Round 5 active jobs did not settle before provider cleanup"
            )
        async with self._cleanup_start_lock:
            if self._cleanup_bout_id is not None:
                if self._cleanup_bout_id != bout_id:
                    raise ConnectionSpikeCleanupError("Another Round 5 cleanup is already active")
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
        await self._setup_orchestrator.wait_for_proxy_delete_accepted(self._cleanup_bout_id)

    async def wait_for_cleanup_complete(self) -> None:
        """Await full exact absence and reverse cleanup in the background task."""

        if self._cleanup_bout_id is None or self._setup_orchestrator is None:
            raise ConnectionSpikeCleanupError("Round 5 cleanup has not been started")
        await self._setup_orchestrator.wait_for_cleanup_complete(self._cleanup_bout_id)

    async def cancel_and_cleanup(self, arm: FanInArm) -> None:
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

    async def reconcile_claim(self, claim: Any) -> None:
        """Settle both logical jobs and prove exact provider absence on restart."""

        self.bind_claim(claim)
        settlement_results = await asyncio.gather(
            self._lane_adapters["lakebase"].cancel_job(self._job_ids["lakebase"]),
            self._lane_adapters["competitor"].cancel_job(self._job_ids["competitor"]),
            return_exceptions=True,
        )
        if any(isinstance(result, BaseException) for result in settlement_results):
            raise ConnectionSpikeCleanupError(
                "Round 5 restart reconciliation could not prove both logical jobs settled"
            )
        bout_id = str(claim.bout_id)
        unresolved = await self.unresolved_bout_ids()
        if bout_id not in unresolved:
            if self._setup_orchestrator is None:
                raise ConnectionSpikeCleanupError("Round 5 setup orchestrator is unavailable")
            await self._setup_orchestrator.prove_bout_absent(bout_id)
            return
        await self.reconcile_failed_cleanup(bout_id, int(claim.bout_fence))

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
                # From the configured lane, not from the setup result. Timed setup
                # replaces the endpoint a lane is scored against; the observer role and
                # its credential are sealed once at install time and are the same in
                # every bout, so a setup result has nothing to say about them.
                observer_credential_sha256=lakebase.observer_credential_sha256,
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
                observer_credential_sha256=competitor.observer_credential_sha256,
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
    resident_transport: Round5ResidentTransport | None = None,
) -> LiveConnectionSpikeEngine:
    effective_manifest = manifest or load_manifest()
    lakebase_config = connection_spike_live_config_from_manifest(
        effective_manifest,
        competitor_id,
        runner_lane="lakebase",
    )
    competitor_config = connection_spike_live_config_from_manifest(
        effective_manifest,
        competitor_id,
        runner_lane="competitor",
    )
    if (
        lakebase_config.runner_harness_sha256
        and lakebase_config.runner_harness_sha256 != runner_harness_sha256()
    ):
        raise ConnectionSpikeLiveConfigurationError(
            "Installed Round 5 runner assets do not match the sealed harness digest"
        )
    if (
        journal is None
        or fence is None
        or fresh_lakebase_host is None
        or resident_transport is None
    ):
        raise ConnectionSpikeLiveConfigurationError(
            "Round 5 live engine requires journal, fence, fresh host, and resident transport"
        )
    setup = LiveConnectionSpikeSetupOrchestrator(
        connection_spike_setup_config_from_manifest(effective_manifest, competitor_id),
        journal=journal,
        fence=fence,
        fresh_lakebase_host=fresh_lakebase_host,
        session_factory=session_factory,
    )
    lakebase_adapter = LiveConnectionSpikeAdapter(
        lakebase_config,
        session_factory=session_factory,
        resident_transport=resident_transport,
    )
    competitor_adapter = LiveConnectionSpikeAdapter(
        competitor_config,
        session_factory=session_factory,
        resident_transport=resident_transport,
    )
    return LiveConnectionSpikeEngine(
        lakebase_adapter,
        lane_adapters={
            "lakebase": lakebase_adapter,
            "competitor": competitor_adapter,
        },
        setup_orchestrator=setup,
    )


class LiveRound5WarmProvider:
    """Materialize both target variants and rotate their ephemeral capsules."""

    def __init__(
        self,
        manifest: DemoManifest,
        engine_factory: Callable[[CompetitorId], LiveConnectionSpikeEngine],
    ) -> None:
        self._manifest = manifest
        self._engine_factory = engine_factory
        self._engines: dict[str, LiveConnectionSpikeEngine] = {}
        self._receipts: dict[str, LiveRound5WarmEngineReceipt] = {}
        self._credential_generation = 0

    async def reconcile(self, slot: object) -> bool:
        from .round5_warm import (
            BlockedWarmError,
            RetryableWarmError,
            Round5Variant,
            Round5WarmState,
        )

        if (
            slot.state not in {Round5WarmState.RUNNING, Round5WarmState.CLEANING}
            or slot.claim is None
        ):
            return False
        competitor_id = (
            CompetitorId.AURORA_SERVERLESS_V2
            if slot.claim.selected_variant == Round5Variant.AURORA
            else CompetitorId.RDS_POSTGRES
        )
        try:
            engine = self._engine_factory(competitor_id)
            await engine.reconcile_claim(slot.claim)
            return True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if self._retryable(exc):
                raise RetryableWarmError("cleanup_reconcile_retryable") from exc
            raise BlockedWarmError("cleanup_reconcile_blocked") from exc

    async def validate_ready(self, slot: object, capsule: object) -> bool:
        del capsule
        if slot.shared_receipt is None or not self._engines:
            return False
        engine = self._engines.get("aurora_serverless_v2")
        if engine is None:
            return False
        try:
            current = await engine.validate_ready_provenance(
                slot.shared_receipt,
                slot.warm_attempt_token or "",
            )
            if current:
                for variant_engine in self._engines.values():
                    variant_engine._armed = engine._armed
            return current
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            from .round5_warm import RetryableWarmError

            if self._retryable(exc):
                raise RetryableWarmError("runner_provenance_probe_retryable") from exc
            return False

    @staticmethod
    def _retryable(error: BaseException) -> bool:
        if isinstance(
            error,
            (
                TimeoutError,
                ConnectionError,
                ConnectTimeoutError,
                ReadTimeoutError,
                EndpointConnectionError,
                ConnectionClosedError,
            ),
        ):
            return True
        response = getattr(error, "response", None)
        if not isinstance(response, Mapping):
            return False
        code = str((response.get("Error") or {}).get("Code") or "")
        status = int((response.get("ResponseMetadata") or {}).get("HTTPStatusCode") or 0)
        return (
            status >= 500
            or code.startswith("Throttl")
            or code
            in {
                "RequestLimitExceeded",
                "ServiceUnavailable",
                "InternalFailure",
                "PriorRequestNotComplete",
            }
        )

    async def prepare(
        self,
        *,
        generation: int,
        coordinator_fence: int,
        process_epoch: str,
        broker_epoch: str,
        warm_attempt_token: str,
    ) -> object:
        from .round5_warm import (
            BlockedWarmError,
            RetryableWarmError,
            Round5RunnerReceipt,
            Round5SharedReceipt,
            Round5Variant,
            Round5WarmPreparation,
        )

        del process_epoch
        variants = {
            Round5Variant.AURORA: CompetitorId.AURORA_SERVERLESS_V2,
            Round5Variant.RDS: CompetitorId.RDS_POSTGRES,
        }
        try:
            engines = {
                variant: self._engine_factory(competitor_id)
                for variant, competitor_id in variants.items()
            }
            aurora_receipt = await engines[Round5Variant.AURORA].warm(
                generation,
                warm_attempt_token,
            )
            rds_receipt = await engines[Round5Variant.RDS].warm_with_physical_runners_from(
                engines[Round5Variant.AURORA],
                generation,
            )
            warmed = (aurora_receipt, rds_receipt)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # Observability: the warm-slot public status carries only the fixed,
            # secret-free code (``warm_baseline_invalid`` / ``warm_provider_retryable``)
            # and the underlying cause was previously swallowed, so a blocked warm
            # could not be diagnosed without a redeploy. Name the cause type and its
            # (internal, non-secret) message at WARNING so an operator can see WHY the
            # baseline was rejected. Provider ARNs/secrets never appear in these
            # internal ConnectionSpikeLive* messages.
            if self._retryable(exc):
                logger.warning(
                    "round5_warm_provider_retryable generation=%s cause=%s: %s",
                    generation,
                    type(exc).__name__,
                    exc,
                )
                raise RetryableWarmError("warm_provider_retryable") from exc
            # A baseline mismatch is RETRYABLE, not a terminal block. The overnight
            # outage was exactly this: a transient baseline failure (eventual
            # consistency / a briefly-reaped-then-restored fixture) raised
            # BlockedWarmError, and a BLOCKED slot is never re-attempted by the
            # same process -- so Round 5 stayed UNAVAILABLE for hours even though a
            # baseline replay passed minutes later. Per the contract, only TYPED
            # permanent anti-cheat/config defects block; a generic baseline failure
            # retries with capped backoff until it self-heals. This never publishes
            # READY on a bad baseline (publish_ready still OBSERVES proxy absence,
            # runner identity, and network fixtures); it only keeps trying. The
            # cause is logged above/below so an operator can see a persistent one.
            logger.warning(
                "round5_warm_baseline_retryable generation=%s cause=%s: %s",
                generation,
                type(exc).__name__,
                exc,
            )
            raise RetryableWarmError("warm_baseline_invalid") from exc
        receipts = dict(zip(engines, warmed, strict=True))
        self._engines = {variants[variant].value: engine for variant, engine in engines.items()}
        self._receipts = {variants[variant].value: receipt for variant, receipt in receipts.items()}
        self._credential_generation += 1

        resources = self._manifest.require_round5_resources()
        now = datetime.now(UTC)
        runner_expiration = now + timedelta(minutes=45)
        reference = receipts[Round5Variant.AURORA]
        other = receipts[Round5Variant.RDS]
        if (
            reference.runner_boot_ids != other.runner_boot_ids
            or reference.runner_process_boot_ids != other.runner_process_boot_ids
            or reference.warm_attempt_token != warm_attempt_token
            or other.warm_attempt_token != warm_attempt_token
        ):
            raise BlockedWarmError("runner_boot_identity_changed")
        capacity_digest = fanin_capacity_model_sha256()
        physical_harnesses = {
            lane_id: reference.arm.preflights[lane_id].runner_harness_sha256
            for lane_id in ("lakebase", "competitor")
        }
        if (
            any(
                other.arm.preflights[lane_id].runner_harness_sha256 != physical_harnesses[lane_id]
                for lane_id in physical_harnesses
            )
            or len(set(physical_harnesses.values())) != 1
            or next(iter(physical_harnesses.values())) != resources.runner_harness_sha256
        ):
            raise BlockedWarmError("runner_harness_identity_changed")
        image_digest = next(iter(physical_harnesses.values()))
        runners = {
            "lakebase": Round5RunnerReceipt(
                lane_id="lakebase",
                instance_id=resources.runner_instance_id,
                boot_id=reference.runner_boot_ids["lakebase"],
                process_boot_id=reference.runner_process_boot_ids["lakebase"],
                process_pid=self._engines["aurora_serverless_v2"]
                ._lane_adapters["lakebase"]
                ._resident_process_pid,
                instance_type=FANIN_RUNNER_INSTANCE_TYPE,
                image_sha256=image_digest,
                loaded_harness_sha256=physical_harnesses["lakebase"],
                capacity_model_sha256=capacity_digest,
                expires_at=runner_expiration,
            ),
            "competitor": Round5RunnerReceipt(
                lane_id="competitor",
                instance_id=str(resources.competitor_runner_instance_id),
                boot_id=reference.runner_boot_ids["competitor"],
                process_boot_id=reference.runner_process_boot_ids["competitor"],
                process_pid=self._engines["aurora_serverless_v2"]
                ._lane_adapters["competitor"]
                ._resident_process_pid,
                instance_type=FANIN_RUNNER_INSTANCE_TYPE,
                image_sha256=image_digest,
                loaded_harness_sha256=physical_harnesses["competitor"],
                capacity_model_sha256=capacity_digest,
                expires_at=runner_expiration,
            ),
        }
        static_network_digest = hashlib.sha256(
            json.dumps(
                {
                    "aurora": resources.aurora_proxy_security_group_id,
                    "rds": resources.rds_proxy_security_group_id,
                    "lakebase_runner": resources.runner_security_group_id,
                    "competitor_runner": (resources.competitor_runner_security_group_id),
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        shared = Round5SharedReceipt(
            source_sha256=resources.baseline_sha256,
            config_sha256=resources.config_sha256,
            runner_image_sha256=image_digest,
            fanin_contract_sha256=FanInContract().sha256,
            capacity_model_sha256=capacity_digest,
            lakebase_binding_sha256=hashlib.sha256(
                (resources.lakebase_direct_host + "\0" + resources.lakebase_pooled_host).encode()
            ).hexdigest(),
            static_network_fixture_sha256=static_network_digest,
            lakebase_runner=runners["lakebase"],
            competitor_runner=runners["competitor"],
        )
        public_variants = {
            variant: self._variant_receipt(
                variant,
                receipts[variant],
                expires_at=runner_expiration,
            )
            for variant in variants
        }
        capsule = self._capsule(
            generation=generation,
            coordinator_fence=coordinator_fence,
            broker_epoch=broker_epoch,
            engines=engines,
            receipts=receipts,
        )
        return Round5WarmPreparation(
            shared_receipt=shared,
            variants=public_variants,
            capsule=capsule,
        )

    async def refresh_capsule(
        self,
        slot: object,
        previous: object,
    ) -> object:
        from .round5_warm import (
            BlockedWarmError,
            RetryableWarmError,
            Round5Variant,
        )

        del previous
        engines = {
            Round5Variant.AURORA: self._engines.get("aurora_serverless_v2"),
            Round5Variant.RDS: self._engines.get("rds_postgres"),
        }
        if any(engine is None for engine in engines.values()):
            raise BlockedWarmError("launch_capsule_missing")
        try:
            aurora_engine = engines[Round5Variant.AURORA]
            rds_engine = engines[Round5Variant.RDS]
            assert aurora_engine is not None and rds_engine is not None
            aurora_receipt = await aurora_engine.refresh_warm(slot.generation)
            rds_receipt = await rds_engine.warm_with_physical_runners_from(
                aurora_engine,
                slot.generation,
            )
            refreshed = (aurora_receipt, rds_receipt)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if self._retryable(exc):
                raise RetryableWarmError("credential_refresh_retryable") from exc
            raise BlockedWarmError("credential_refresh_failed") from exc
        receipts = dict(zip(engines, refreshed, strict=True))
        self._credential_generation += 1
        return self._capsule(
            generation=slot.generation,
            coordinator_fence=slot.coordinator_fence,
            broker_epoch=f"broker-{uuid4().hex}",
            engines=engines,
            receipts=receipts,
        )

    def _variant_receipt(
        self,
        variant: object,
        receipt: LiveRound5WarmEngineReceipt,
        *,
        expires_at: datetime,
    ) -> object:
        from .round5_warm import Round5Variant, Round5VariantReceipt

        resources = self._manifest.require_round5_resources()
        aurora = variant == Round5Variant.AURORA
        values = {
            "target": (
                resources.aurora_cluster_resource_id if aurora else resources.rds_resource_id
            ),
            "source": (resources.aurora_direct_host if aurora else resources.rds_direct_host),
            "secret": (
                resources.aurora_proxy_secret_arn if aurora else resources.rds_proxy_secret_arn
            ),
            "role": resources.proxy_service_role_arn,
            "auth": "POSTGRES_SCRAM_SHA_256:DISABLED",
            "tls": "require_tls:true",
            "sg": (
                resources.aurora_proxy_security_group_id
                if aurora
                else resources.rds_proxy_security_group_id
            ),
            "subnets": ",".join(sorted(resources.proxy_subnet_ids)),
            "vpc": resources.vpc_id,
            "request": (
                self._engines[
                    "aurora_serverless_v2" if aurora else "rds_postgres"
                ]._adapter.config.contract_sha256
            ),
        }

        def hashed(name: str) -> str:
            return hashlib.sha256(str(values[name]).encode()).hexdigest()

        return Round5VariantReceipt(
            variant=variant,
            target_sha256=hashed("target"),
            source_sha256=hashed("source"),
            secret_ref_sha256=hashed("secret"),
            role_sha256=hashed("role"),
            auth_sha256=hashed("auth"),
            tls_sha256=hashed("tls"),
            security_group_sha256=hashed("sg"),
            subnet_sha256=hashed("subnets"),
            vpc_sha256=hashed("vpc"),
            proxy_absent=True,
            proxy_absence_observed_at=receipt.setup_context.observed_at,
            request_template_sha256=hashed("request"),
            expires_at=expires_at,
        )

    def _capsule(
        self,
        *,
        generation: int,
        coordinator_fence: int,
        broker_epoch: str,
        engines: Mapping[object, LiveConnectionSpikeEngine | None],
        receipts: Mapping[object, LiveRound5WarmEngineReceipt],
    ) -> object:
        from .round5_warm import Round5LaunchCapsule, Round5Variant

        if any(engine is None for engine in engines.values()):
            raise ValueError("Round 5 capsule requires both variant engines")
        control_expires = min(
            receipt.setup_context.clients.expires_at for receipt in receipts.values()
        )
        dispatch_expires = {
            lane_id: min(receipt.dispatch_expires_at[lane_id] for receipt in receipts.values())
            for lane_id in ("lakebase", "competitor")
        }
        expires_at = min(control_expires, *dispatch_expires.values())
        renew_by = min(
            control_expires - timedelta(seconds=SETUP_DEADLINE_SECONDS + 60),
            *(
                value - timedelta(seconds=FANIN_SSM_TIMEOUT_SECONDS + 60)
                for value in dispatch_expires.values()
            ),
        )
        warm_attempt_tokens = {receipt.warm_attempt_token for receipt in receipts.values()}
        if len(warm_attempt_tokens) != 1:
            raise ValueError("Round 5 capsule warm attempts disagree")
        return Round5LaunchCapsule(
            generation=generation,
            coordinator_fence=coordinator_fence,
            credential_generation=self._credential_generation,
            broker_epoch=broker_epoch,
            runner_contexts={
                "lakebase": receipts[Round5Variant.AURORA].dispatch_expires_at["lakebase"],
                "competitor": receipts[Round5Variant.AURORA].dispatch_expires_at["competitor"],
            },
            aws_control_contexts={
                variant: receipt.setup_context for variant, receipt in receipts.items()
            },
            lakebase_context=receipts[Round5Variant.AURORA].setup_context,
            variant_contexts={
                variant: engine for variant, engine in engines.items() if engine is not None
            },
            control_expires_at=control_expires,
            dispatch_expires_at=dispatch_expires,
            expires_at=expires_at,
            renew_by=renew_by,
            warm_attempt_token=next(iter(warm_attempt_tokens)),
        )
