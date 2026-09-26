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
from concurrent.futures import ThreadPoolExecutor
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
    ROUND5_ABANDONED_ARM_SETTLEMENT_SECONDS,
    ROUND5_ARM_STAGE_DEADLINE_SECONDS,
    Round5ControlBinding,
    Round5ControlEvent,
    Round5ControlKind,
    Round5ResidentBindingChangedError,
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
# Swarm defect #4: how many bounded describe confirmations prove that an
# ambiguous ``DBProxyAlreadyExistsFault`` was eventual consistency (our exact
# Proxy is genuinely absent) before the create is retried.  A single describe is
# not proof of absence.
PROXY_CREATE_ABSENCE_CONFIRMATIONS = 3
# Swarm defect #5: how many consecutive bounded NotFound describes prove a
# per-bout Proxy is *definitively* gone during cleanup.  One NotFound is unknown
# (control-plane eventual consistency after DeleteDBProxy acceptance), never
# clean; cleanup debt is held until this many confirmations agree.
PROXY_DELETE_ABSENCE_CONFIRMATIONS = 3
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


class ConnectionSpikeLiveTransientError(ConnectionSpikeLiveError):
    """A live dependency is temporarily unable to prove the sealed contract."""


class ConnectionSpikeLiveSourceUnavailableError(ConnectionSpikeLiveTransientError):
    """The sealed competitor source matches identity but is not yet available.

    A *transient* condition -- the source is backing-up / modifying /
    failing-over / rebooting -- never identity drift.  It self-heals on a bounded
    retry and, if it persists, escalates to a SELF-VERIFIABLE block (the DB is
    busy, not misconfigured); it must never latch ``warm_baseline_invalid``.
    """


class ConnectionSpikeLiveSourceUnresolvedError(ConnectionSpikeLiveSourceUnavailableError):
    """A source describe did not resolve to exactly one matching row.

    Swarm finding req #4 (identity inversion).  An EMPTY describe is retried
    boundedly under provider eventual consistency, but -- unlike a busy-but-present
    source -- if it stays unresolved past the transient budget it is NOT a
    self-verifiable "the DB will recover" condition: the sealed source cannot be
    found at all, which needs operator attention.  It therefore escalates to a
    TERMINAL block (``warm_source_unresolved_persistent``, absent from
    ``SELF_VERIFIABLE_BLOCK_CODES``) instead of rechecking forever.  A DUPLICATE
    describe (more than one matching row) never reaches here: it is a terminal
    configuration fault raised immediately at read time.
    """


class ConnectionSpikeLiveOperationError(ConnectionSpikeLiveError):
    """The remote runner did not produce a complete, sanitized proof."""


def _require_warm_runner_online(
    runners: Sequence[Mapping[str, object]],
    *,
    expected_instance_id: str,
    lane_id: str,
) -> None:
    """Distinguish immutable runner drift from temporary SSM availability."""

    if len(runners) > 1 or (
        len(runners) == 1 and runners[0].get("InstanceId") != expected_instance_id
    ):
        raise ConnectionSpikeLiveConfigurationError(
            f"Round 5 {lane_id} physical runner identity changed"
        )
    if not runners:
        raise ConnectionSpikeLiveTransientError(
            f"Round 5 {lane_id} runner is temporarily absent from SSM "
            "(observed_count=0)"
        )
    ping_status = runners[0].get("PingStatus")
    if ping_status == "ConnectionLost":
        raise ConnectionSpikeLiveTransientError(
            f"Round 5 {lane_id} runner is temporarily unavailable in SSM "
            "(ping_status=ConnectionLost)"
        )
    if ping_status != "Online":
        raise ConnectionSpikeLiveConfigurationError(
            f"Round 5 {lane_id} runner has invalid SSM ping status "
            f"(ping_status={ping_status!r})"
        )


def _require_warm_source_identity(
    source: _CompetitorSource,
    *,
    expected_identifier: str,
    expected_resource_id: str,
    expected_direct_host: str,
    expected_vpc_id: str,
    expected_security_group_id: str | None,
) -> None:
    """Split immutable source identity (terminal) from availability (transient).

    A true identifier / resource-id / host / VPC / security-group mismatch is
    permanent drift from the sealed contract and raises
    ``ConnectionSpikeLiveConfigurationError`` (``warm_baseline_invalid``, operator
    attention).  Matching identity with a non-``available`` status -- or a
    describe that momentarily returned no matching row -- is a transient provider
    condition and raises ``ConnectionSpikeLiveSourceUnavailableError`` for a
    bounded, self-healing retry.  An empty describe is treated as ambiguous
    provider state (retry boundedly), never silently accepted as a valid identity.

    ``expected_security_group_id`` is compared exactly when provided (the warm
    contract seals exactly one group); pass ``None`` to require only that exactly
    one group is attached (the preflight path derives it from the source itself).
    """

    if not source.identifier:
        # Empty/absent describe: retried boundedly under eventual consistency, but
        # (req #4) it escalates to a TERMINAL block if it persists rather than
        # rechecking forever -- an unfindable sealed source needs operator
        # attention, unlike a present-but-busy one.  A duplicate describe never
        # reaches here (it is raised as a terminal configuration fault at read).
        raise ConnectionSpikeLiveSourceUnresolvedError(
            "Round 5 warm source did not resolve to exactly one matching row"
        )
    identity_changed = (
        source.identifier != expected_identifier
        or source.resource_id != expected_resource_id
        or source.direct_host != expected_direct_host
        or source.vpc_id != expected_vpc_id
    )
    if expected_security_group_id is not None:
        identity_changed = identity_changed or (
            source.security_group_ids != (expected_security_group_id,)
        )
    else:
        identity_changed = identity_changed or (len(source.security_group_ids) != 1)
    if identity_changed:
        raise ConnectionSpikeLiveConfigurationError(
            "Round 5 warm source identity changed"
        )
    if source.status != "available":
        raise ConnectionSpikeLiveSourceUnavailableError(
            "Round 5 warm source matches identity but is not available yet "
            f"(status={source.status!r})"
        )


class ConnectionSpikeCleanupError(ConnectionSpikeLiveOperationError):
    """The exact command did not prove cleanup and flock release."""

    def __init__(
        self,
        message: str,
        *,
        stage: str | None = None,
        reason_code: str | None = None,
        lane: str | None = None,
        job_id: str | None = None,
        failures: Sequence[ConnectionSpikeCleanupError] = (),
    ) -> None:
        super().__init__(message)
        self.stage = stage
        self.reason_code = reason_code
        self.lane = lane
        self.job_id = job_id
        self.failures = tuple(failures)

    def underlying_causes(self) -> tuple[BaseException, ...]:
        """Independent safe-to-enumerate failures for redacted diagnostics."""

        return self.failures


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
    proxy_arn: str = ""
    # Monotonic stamp taken at the CreateDBProxy request boundary INSIDE the
    # dedicated worker (immediately before the SDK call leaves this process), so
    # the setup contract can score the real bell -> CreateDBProxy request latency
    # instead of the workflow_launched lower bound.
    proxy_create_requested_ns: int | None = None
    # Monotonic stamp taken on the event loop just before the CreateDBProxy call
    # is submitted to its dedicated executor. The gap
    # ``proxy_create_requested_ns - proxy_create_submitted_ns`` is the worker
    # scheduling delay that a shared default executor would otherwise hide.
    proxy_create_submitted_ns: int | None = None
    security_group_rule_ids: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class _BellCapability:
    """A process-local, non-durable proof that this replica accepted the bell.

    Swarm defect #2.  The single timed CreateDBProxy mutation must not perform any
    remote fence I/O after the bell (that would sit on the T0 -> boto3 path), yet
    it must still refuse to run for a stale owner.  This capability is minted on
    the bell path, keyed to the exact bout and fence, with a monotonic expiry
    covering the legitimate bout window.  ``_create_proxy`` checks it -- a pure,
    synchronous, in-process lookup with no await -- immediately before submitting
    the mutation.  A create whose bell was accepted more than a bout-deadline ago,
    or under a different fence, is refused locally without touching AWS.
    """

    bout_id: str
    fencing_token: int
    minted_ns: int
    expires_ns: int


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

    async def admitted_lifecycle_states(self) -> frozenset[str]:
        """Return the ``lifecycle_state`` values the LIVE CHECK constraint admits.

        Swarm req #3: readiness uses this to refuse ring readiness (and arm) when
        the deployed journal CHECK cannot admit a state the arm/bell/cleanup path
        emits -- the exact failure mode (code emits a state the live CHECK forbids)
        that let the 2026-09-23 arm regression ship 'green' against fake journals.
        Reads ``pg_get_constraintdef`` from the catalog (no table lock).
        """

        schema, _, table = ROUND5_CREATION_JOURNAL_TABLE.partition(".")

        async def select(cursor: Any) -> str:
            await cursor.execute(
                """
                SELECT pg_get_constraintdef(c.oid)
                FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE n.nspname = %s
                  AND t.relname = %s
                  AND c.contype = 'c'
                  AND pg_get_constraintdef(c.oid) LIKE '%%lifecycle_state%%'
                """,
                (schema, table),
            )
            rows = await cursor.fetchall()
            return " ".join(str(row[0]) for row in rows)

        definition = await self._run(select)
        return frozenset(re.findall(r"'([a-z_]+)'", definition))

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
        #: The proxy CREATE_INTENT committed on the bell path (swarm defect #1,
        #: Approach A: precommit_launch_intent), keyed by bout.  ``setup()`` consumes
        #: it so no journal write is awaited between the authoritative T0 and the
        #: boto3 CreateDBProxy request.
        self._promoted_proxy_intents: dict[str, JournalEvent] = {}
        #: Process-local bell capabilities (swarm defect #2), keyed by bout.  Minted
        #: on the bell path; checked -- with no await -- immediately before the
        #: timed CreateDBProxy mutation so a stale owner cannot mutate.
        self._bell_capabilities: dict[str, _BellCapability] = {}
        #: A single reused worker for the timed CreateDBProxy dispatch (swarm
        #: defect #3 + req #7).  Created ONCE and warmed before the scored window,
        #: never inside it -- spawning a ThreadPoolExecutor (thread create + start)
        #: between T0 and the boto3 request would add uncontrolled latency to the
        #: scored bell -> CreateDBProxy delta.
        self._createproxy_executor: ThreadPoolExecutor | None = None

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
            # Swarm defect #1 (Approach A): ARM writes NOTHING to the creation
            # journal.  The timed CreateDBProxy CREATE_INTENT is made durable on the
            # BELL path (``precommit_launch_intent``), before the authoritative T0
            # and before Lakebase is released, so Lakebase can never be released
            # while the competitor's create record does not yet exist -- and no
            # journal write is charged against the bell-relative window.  Only
            # already-allowed lifecycle states are ever written, so no schema
            # migration is required (this replaces the reverted PENDING_LAUNCH
            # pre-state that the live CHECK constraint rejected).
            self._coordinators[bout_id] = coordinator
            self._scopes[bout_id] = scope
            self._prepared[bout_id] = fencing_token
            self._prepared_resources[bout_id] = resources
            self._prepared_specs[bout_id] = specs
            self._resources_by_bout[bout_id] = resources

    async def precommit_launch_intent(self, bout_id: str, fencing_token: int) -> JournalEvent:
        """Durably commit the timed CreateDBProxy CREATE_INTENT on the bell path.

        Swarm defect #1 (Approach A).  Invoked on the bell path immediately before
        the authoritative T0 (and before Lakebase is released), so the durable
        create intent exists first and ``setup()`` awaits no coordination I/O
        between T0 and the boto3 CreateDBProxy request.  Writes ``CREATE_INTENT``
        directly -- an already-allowed journal ``lifecycle_state`` -- so no schema
        migration is needed.  Idempotent for a duplicate ``/run``: a second call
        returns the same already-committed intent instead of failing the bell.
        """

        scope = self._scopes.get(bout_id)
        coordinator = self._coordinators.get(bout_id)
        specs = self._prepared_specs.get(bout_id)
        if (
            self._prepared.get(bout_id) != fencing_token
            or scope is None
            or coordinator is None
            or specs is None
        ):
            raise ConnectionSpikeLiveOperationError(
                "Round 5 bell cannot stage a launch that ARM never prepared"
            )
        existing = self._promoted_proxy_intents.get(bout_id)
        if existing is not None:
            # A duplicate /run must re-mint the process-local bell capability under
            # the current token (already validated to equal the prepared fence
            # above) BEFORE returning the cached intent.  Without this the cached
            # path inherited whatever expiry the first mint set, so a legitimate
            # same-fence retry that arrived a whole setup-deadline after the first
            # bell would present an expired capability and _require_bell_capability
            # would refuse the timed CreateDBProxy.  Re-minting keeps the same-owner
            # retry fresh; the fence check above still fails closed on real drift.
            self.arm_bell_capability(bout_id, fencing_token)
            return existing
        proxy_spec = next(
            (spec for spec in specs if spec.resource_kind == "rds_proxy"), None
        )
        if proxy_spec is None:
            raise ConnectionSpikeLiveOperationError(
                "Round 5 competitor specs omitted the timed CreateDBProxy mutation"
            )
        intent = await coordinator.precommit_intent(scope, proxy_spec)
        self._promoted_proxy_intents[bout_id] = intent
        # Mint the local bell capability on the bell path, keyed to this exact
        # bout/fence (swarm defect #2).  A duplicate /run re-promotes idempotently
        # above and re-mints here with a fresh expiry, which is correct: the same
        # in-process owner is still fresh.
        self.arm_bell_capability(bout_id, fencing_token)
        return intent

    def arm_bell_capability(
        self, bout_id: str, fencing_token: int, *, ttl_seconds: float | None = None
    ) -> None:
        """Mint a process-local bell capability for the timed CreateDBProxy.

        Swarm defect #2.  Called on the bell path.  The default TTL is the setup
        deadline: a create that has not fired within a whole bout window of the
        bell is, by definition, a stale owner and must be refused locally.
        """

        ttl = self.config.deadline_seconds if ttl_seconds is None else ttl_seconds
        now = self._monotonic_ns()
        self._bell_capabilities[bout_id] = _BellCapability(
            bout_id=bout_id,
            fencing_token=fencing_token,
            minted_ns=now,
            expires_ns=now + int(ttl * 1_000_000_000),
        )

    def _require_bell_capability(self, bout_id: str, fencing_token: int) -> None:
        """Refuse the timed mutation for a stale owner. Pure local, no await/I/O.

        Deliberately synchronous so it can sit on the T0 -> boto3 path without
        adding any awaited coordination.  Fail-closed on absence, fence drift, or
        expiry.
        """

        capability = self._bell_capabilities.get(bout_id)
        if capability is None:
            raise ConnectionSpikeLiveOperationError(
                "Round 5 timed CreateDBProxy refused: no local bell capability for "
                "this bout; the bell was not accepted by this replica"
            )
        if capability.fencing_token != fencing_token:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 bell capability fence does not match the CreateDBProxy scope"
            )
        if self._monotonic_ns() >= capability.expires_ns:
            raise ConnectionSpikeLiveOperationError(
                "Round 5 local bell capability expired before the timed CreateDBProxy "
                "mutation; refusing a stale owner"
            )

    async def warm(
        self,
        generation: int,
        *,
        cleaned_bout_id: str | None = None,
    ) -> ConnectionSpikeWarmSetupContext:
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
        _require_warm_source_identity(
            source,
            expected_identifier=self.config.competitor_target_id,
            expected_resource_id=self.config.competitor_resource_id,
            expected_direct_host=self.config.competitor_direct_host,
            expected_vpc_id=self.config.vpc_id,
            expected_security_group_id=self.config.competitor_security_group_id,
        )
        _require_warm_runner_online(
            lakebase_runners,
            expected_instance_id=self.config.runner_instance_id,
            lane_id="lakebase",
        )
        _require_warm_runner_online(
            competitor_runners,
            expected_instance_id=self.config.competitor_runner_instance_id,
            lane_id="competitor",
        )
        await self._verify_proxy_service_role(clients)
        await self._verify_static_proxy_network(clients)
        await self._discover_orphaned_addons(
            clients,
            self.config.competitor_security_group_id,
            include_legacy=False,
            bout_id=cleaned_bout_id,
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

            # Req #7: warm the reused CreateDBProxy worker now, before the scored
            # window, so no ThreadPoolExecutor is created between T0 and the boto3
            # request.
            self._ensure_createproxy_executor()

            # Swarm defect #1 (Approach A): the timed CreateDBProxy CREATE_INTENT is
            # made durable on the bell path (``precommit_launch_intent``, before the
            # authoritative T0), NOT here.  ``setup()`` therefore awaits zero
            # journal/fence I/O before releasing the gate -- the first awaited call
            # the competitor lane makes after the gate is the direct boto3
            # CreateDBProxy request itself.  We only consume the already-durable
            # intent below.
            #
            # Fallback: if the bell path did not pre-commit (e.g. the non-atomic
            # in-memory bell store, or a direct setup() invocation), commit it now
            # through the same ``precommit_intent`` path so the semantics are
            # identical.  This still commits before the gate releases, so no orphan
            # class is introduced.
            proxy_spec = next(
                (spec for spec in specs if spec.resource_kind == "rds_proxy"), None
            )
            if proxy_spec is None:
                raise ConnectionSpikeLiveOperationError(
                    "Round 5 competitor specs omitted the timed CreateDBProxy mutation"
                )
            proxy_intent = self._promoted_proxy_intents.pop(bout_id, None)
            if proxy_intent is None:
                proxy_intent = await coordinator.precommit_intent(scope, proxy_spec)
                # Fallback path (in-memory bell store / direct setup): the bell
                # capability was not minted on a separate bell path, so mint it
                # here.  Production always mints at precommit_launch_intent above
                # and takes the pop branch, so this never re-mints a stale owner.
                self.arm_bell_capability(bout_id, fencing_token)

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
            # Process-local setup state is not an absence proof. In particular,
            # CreateDBProxy may have crossed AWS immediately before a restart.
            # Leave the handoff unset and force the durable warm owner through
            # reconcile_failed_cleanup, which reconstructs exact tagged specs
            # from the claim's bout id and fence.
            raise ConnectionSpikeCleanupError(
                "Round 5 cleanup requires durable resource reconstruction",
                stage="cleanup_reconstruction",
                reason_code="cleanup_reconstruction_required",
            )
        receipt = self._receipts.get(bout_id)
        report = (
            await coordinator.cleanup(scope, receipt)
            if receipt is not None
            else await coordinator.reconcile_incomplete(scope)
        )
        if not report.complete:
            proxy_spec = next(
                (
                    spec
                    for spec in self._prepared_specs.get(bout_id, ())
                    if spec.resource_kind == "rds_proxy"
                ),
                None,
            )
            proxy_adapter = coordinator._adapters.get("rds_proxy")
            if proxy_spec is None or proxy_adapter is None:
                raise ConnectionSpikeLiveTransientError(
                    "Round 5 per-bout setup cleanup is waiting on provider absence"
                )
            observed_proxy = await proxy_adapter.inspect(proxy_spec, provider_id=None)
            if observed_proxy is not None:
                await proxy_adapter.delete(observed_proxy)
            remaining_proxy = await proxy_adapter.inspect(proxy_spec, provider_id=None)
            if remaining_proxy is not None:
                raise ConnectionSpikeLiveTransientError(
                    "Round 5 bout-owned proxy is still present"
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
        try:
            unresolved = await self._journal.unresolved_bout_ids()
        except Exception as exc:
            raise ConnectionSpikeCleanupError(
                "Round 5 absence proof could not read cleanup ownership",
                stage="journal_ownership",
                reason_code="journal_ownership_unavailable",
            ) from exc
        if bout_id in unresolved:
            raise ConnectionSpikeCleanupError(
                "Round 5 inherited claim still has journal debt",
                stage="journal_ownership",
                reason_code="current_bout_journal_debt",
            )
        try:
            clients = await self._assumed_clients(
                f"cleanup-{bout_id}",
                minimum_lifetime_seconds=45 * 60 + 60,
            )
        except Exception as exc:
            raise ConnectionSpikeCleanupError(
                "Round 5 absence proof could not acquire provider clients",
                stage="provider_session",
                reason_code="provider_session_unavailable",
            ) from exc
        names = self.names_for_bout(
            self.config.deterministic_name_prefix,
            bout_id,
            self.config.secret_name_prefix or "anti-demo-round5",
        )

        async def proxy_for_bout() -> Mapping[str, object]:
            try:
                return await self._call(
                    clients.rds.describe_db_proxies,
                    DBProxyName=names.proxy_name,
                )
            except Exception as exc:
                if self._error_code(exc) == "DBProxyNotFoundFault":
                    return {"DBProxies": []}
                raise

        async def target_groups_for_bout() -> Mapping[str, object]:
            try:
                return await self._call(
                    clients.rds.describe_db_proxy_target_groups,
                    DBProxyName=names.proxy_name,
                )
            except Exception as exc:
                code = self._error_code(exc)
                if code == "DBProxyNotFoundFault":
                    return {"TargetGroups": []}
                if code == "InvalidDBProxyStateFault":
                    raise ConnectionSpikeLiveTransientError(
                        "Round 5 bout-owned proxy is still deleting"
                    ) from exc
                raise

        # A just-accepted DeleteDBProxy can transiently describe as NotFound and
        # then reappear while RDS finishes deletion.  This journal-free recovery
        # path must therefore apply the same bounded proof as `_delete_proxy`:
        # one empty/NotFound sample is unknown, not clean.  Keep the calls
        # sequential so an enumerable parent fences immediately.  When the
        # parent describe races to NotFound, still perform the named target-group
        # describe; a group returned from that split-brain view is direct
        # evidence that the bout is not absent.
        try:
            confirmations = 0
            while confirmations < PROXY_DELETE_ABSENCE_CONFIRMATIONS:
                response = await proxy_for_bout()
                proxies = response.get("DBProxies") or []
                if proxies:
                    if all(
                        str(proxy.get("Status") or "").lower() == "deleting"
                        for proxy in proxies
                    ):
                        raise ConnectionSpikeLiveTransientError(
                            "Round 5 bout-owned proxy is still deleting"
                        )
                    raise ConnectionSpikeCleanupError(
                        "Round 5 inherited claim still owns an RDS Proxy",
                        stage="exact_provider_absence",
                        reason_code="current_bout_proxy_present",
                    )
                target_groups = await target_groups_for_bout()
                if target_groups.get("TargetGroups"):
                    raise ConnectionSpikeCleanupError(
                        "Round 5 inherited claim still owns an RDS Proxy target group",
                        stage="exact_provider_absence",
                        reason_code="current_bout_target_group_present",
                    )
                confirmations += 1
                if confirmations < PROXY_DELETE_ABSENCE_CONFIRMATIONS:
                    await self._sleep(self.config.poll_interval_seconds)
        except (
            ConnectionSpikeCleanupError,
            ConnectionSpikeLiveConfigurationError,
            ConnectionSpikeLiveTransientError,
        ):
            # A bounded token/marker/truncation overrun is a configuration or
            # inventory fault, not an opaque provider read failure; it must keep
            # its own class so the operator sees "fix the inventory bound", not
            # "the absence probe broke". In-flight DELETING is retryable.
            raise
        except Exception as exc:
            raise ConnectionSpikeCleanupError(
                "Round 5 exact provider absence proof failed",
                stage="exact_provider_absence",
                reason_code="exact_provider_read_failed",
            ) from exc

        # Carry the exact bout identity into the broad clean-baseline scan.  Its
        # named target-group extend is the final race fence; passing None here
        # would silently replace that provider read with an empty synthetic page.
        try:
            await self._discover_orphaned_addons(
                clients,
                self.config.competitor_security_group_id,
                # The old per-bout shape also created a secret, IAM role and
                # runner inline policy. This is still a bout-scoped scan:
                # canonical names or this bout's ownership tags block, while
                # foreign prefix matches are ignored by the classifier below.
                include_legacy=True,
                bout_id=bout_id,
            )
        except (
            ConnectionSpikeCleanupError,
            ConnectionSpikeLiveConfigurationError,
            ConnectionSpikeLiveTransientError,
        ):
            # Same taxonomy as the exact-absence block above: a bounded
            # pagination/role-tag-truncation overrun surfaces as a configuration
            # fault, and current-bout resources surface as a scoped cleanup
            # block. Neither is flattened into scoped_orphan_read_failed.
            # In-flight DELETING stays retryable.
            raise
        except Exception as exc:
            raise ConnectionSpikeCleanupError(
                "Round 5 scoped orphan discovery failed",
                stage="scoped_orphan_discovery",
                reason_code="scoped_orphan_read_failed",
            ) from exc

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
        *,
        cleanup_authority: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        """Recover persisted old ownership scopes under a fresh active fence."""

        LiveConnectionSpikeAdapter._validate_run_id(bout_id)
        authority = CreationScope(
            bout_id,
            current_fencing_token,
            self.config.baseline_sha256,
        )
        async with self._lock:
            reclaim = getattr(self._fence, "reclaim_expired_cleanup", None)
            # Warm-coordinator cleanup already holds CAS authority via
            # ``cleanup_authority``. Calling reclaim while a live manager
            # ``round5_cleanup`` lease is still active fights that owner
            # ("prior cleanup owner is still active") and never reaches
            # provider delete — the gen54 empty-AWS retry loop.
            # Restart-during-cleanup is different: the coordinator CAS is
            # new, the departed process's artifact ring is gone, and journal
            # DELETED commits JOIN that ring. Skipping reclaim then makes
            # "journal write lost its active lease fence" a permanent
            # cleanup_reconcile_blocked after AWS is already absent.
            using_reclaimed_fence = False
            if callable(reclaim) and cleanup_authority is None:
                authority = await reclaim(authority)
                using_reclaimed_fence = True
            elif callable(reclaim) and cleanup_authority is not None:
                artifact_current = False
                try:
                    await self._fence.assert_current(authority)
                    artifact_current = True
                except Exception:
                    artifact_current = False
                if not artifact_current:
                    try:
                        authority = await reclaim(authority)
                        using_reclaimed_fence = True
                    except Exception as exc:
                        text = str(exc)
                        still_live_owner = type(exc).__name__ == "InvalidStateError" and (
                            "still active" in text or "no longer current" in text
                        )
                        if not still_live_owner:
                            raise

            class CleanupFence:
                async def assert_current(inner_self, _scope: CreationScope) -> None:
                    if cleanup_authority is None:
                        await self._fence.assert_current(authority)
                    else:
                        await cleanup_authority()
                        if using_reclaimed_fence:
                            await self._fence.assert_current(authority)

            # CLAIMED -> CLEANING transfers mutation ownership from the bout ring
            # to the durable warm coordinator. After a process restart the bout
            # lease may be gone, so a reconstructed engine must use that current
            # cleanup authority rather than repeatedly refusing the stale ring.
            recovery_fence = CleanupFence()
            await recovery_fence.assert_current(authority)
            # Same reasoning as `_begin_cleanup_once`, and it matters more here:
            # this is the path the automatic retry re-enters, so a settlement
            # that could refuse made every attempt fail at an identical point
            # and never reach the reconcile below. It cannot refuse now.
            await self._settle_commands(bout_id)
            clients = await self._assumed_clients(bout_id)
            rds_security_group_id = await self._baseline_rds_security_group(clients)
            scopes = tuple(await self._journal.scopes(bout_id))
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
            coordinator, specs = self._coordinator(
                authority,
                clients,
                resources,
                fence=recovery_fence,
            )
            incomplete_child_journal = False
            for ownership_scope in scopes:
                if (
                    ownership_scope.bout_id != bout_id
                    or ownership_scope.runtime_seal_sha256 != self.config.baseline_sha256
                ):
                    raise ConnectionSpikeCleanupError(
                        "Round 5 persisted cleanup scope differs from the sealed bout"
                    )
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
                    incomplete_child_journal = True
            # An empty or incomplete child journal is unknown ownership, never
            # proof of absence and never a terminal block. CreateDBProxy may
            # have crossed AWS before CREATE_INTENT became readable, or a
            # child target-group confirm can lag while the tagged parent is
            # still billable. Reconstruct the exact deterministic specs from
            # the durable claim and delete the parent through the normal
            # adapter. A still-present or DELETING parent is in-flight AWS,
            # retried as a transient, not cleanup_reconcile_blocked.
            proxy_spec = next(
                spec for spec in specs if spec.resource_kind == "rds_proxy"
            )
            await recovery_fence.assert_current(authority)
            proxy_adapter = coordinator._adapters["rds_proxy"]
            observed_proxy = await proxy_adapter.inspect(proxy_spec, provider_id=None)
            if observed_proxy is not None:
                await recovery_fence.assert_current(authority)
                await proxy_adapter.delete(observed_proxy)
            remaining_proxy = await proxy_adapter.inspect(proxy_spec, provider_id=None)
            if remaining_proxy is not None:
                raise ConnectionSpikeLiveTransientError(
                    "Round 5 bout-owned proxy is still present"
                )
            if incomplete_child_journal:
                logger.warning(
                    "Round 5 child cleanup journal is incomplete for %s; "
                    "exact parent proxy absence is required",
                    bout_id,
                )
            # Parent absence is not journal absence. Replica-local readiness
            # treats any newest lifecycle_state <> deleted as unresolved debt and
            # keeps the fight card unavailable while the warm slot is already
            # READY. Re-inspect each child now that the parent is gone so
            # inspect-None rows are committed DELETED.
            for ownership_scope in scopes:
                report = await coordinator.reconcile_incomplete(
                    ownership_scope,
                    authority_scope=authority,
                )
                if not report.complete:
                    raise ConnectionSpikeLiveTransientError(
                        "Round 5 cleanup journal is not sealed after parent absence"
                    )
            await self._discover_orphaned_addons(
                clients,
                rds_security_group_id,
                include_legacy=True,
                bout_id=bout_id,
            )
            if using_reclaimed_fence:
                release = getattr(self._fence, "release_cleanup", None)
                if not callable(release):
                    raise ConnectionSpikeCleanupError(
                        "Round 5 reclaimed cleanup fence cannot be released"
                    )
                await release(authority)

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
            if len(values) > 1:
                # Req #4: a duplicate describe is an ambiguous identity, a terminal
                # configuration fault -- never a transient "unavailable" that would
                # retry forever.  (An empty describe collapses to {} below and is
                # handled as bounded->terminal "unresolved".)
                raise ConnectionSpikeLiveConfigurationError(
                    "Round 5 competitor source describe returned more than one "
                    "matching instance"
                )
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
        if len(values) > 1:
            # Req #4: duplicate describe -> terminal ambiguous identity, not a
            # transient that retries forever.
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 competitor source describe returned more than one "
                "matching cluster"
            )
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
        resources.proxy_arn = str(provider_ids.get("rds_proxy") or "")
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
        # Immutable identity drift is terminal (warm_baseline_invalid); a
        # non-available status, an absent source describe, or a temporarily
        # absent/ConnectionLost runner is a transient provider condition that must
        # retry and self-heal instead of latching the warm slot for an operator.
        _require_warm_source_identity(
            source,
            expected_identifier=self.config.competitor_target_id,
            expected_resource_id=self.config.competitor_resource_id,
            expected_direct_host=self.config.competitor_direct_host,
            expected_vpc_id=self.config.vpc_id,
            expected_security_group_id=None,
        )
        _require_warm_runner_online(
            lakebase_runners,
            expected_instance_id=self.config.runner_instance_id,
            lane_id="lakebase",
        )
        _require_warm_runner_online(
            competitor_runners,
            expected_instance_id=self.config.competitor_runner_instance_id,
            lane_id="competitor",
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
        bout_id: str | None = None,
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

        names = (
            self.names_for_bout(
                self.config.deterministic_name_prefix,
                bout_id,
                self.config.secret_name_prefix or "anti-demo-round5",
            )
            if bout_id is not None
            else None
        )

        def belongs_to_bout(
            values: Sequence[Mapping[str, object]], *, iam: bool = False
        ) -> bool:
            if bout_id is None:
                return owned(values, iam=iam)
            measured = {
                str(item.get("Key") or ""): str(item.get("Value") or "")
                for item in values
            }
            expected = iam_base_tags if iam else base_tags
            return (
                all(measured.get(key) == value for key, value in expected.items())
                and measured.get("anti-demo-bout-id") == bout_id
            )

        async def target_groups_for_bout() -> Mapping[str, object]:
            if bout_id is None:
                return {"TargetGroups": []}
            assert names is not None
            try:
                return await self._call(
                    clients.rds.describe_db_proxy_target_groups,
                    DBProxyName=names.proxy_name,
                )
            except Exception as exc:
                code = self._error_code(exc)
                if code == "DBProxyNotFoundFault":
                    return {"TargetGroups": []}
                if code == "InvalidDBProxyStateFault":
                    # AWS refuses TG describe while the parent is DELETING.
                    # That is in-flight exact cleanup, not a dual-authority or
                    # permanent ownership fault.
                    raise ConnectionSpikeLiveTransientError(
                        "Round 5 bout-owned proxy is still deleting"
                    ) from exc
                raise

        groups_result, proxies_result, rules_result, target_groups_result = await asyncio.gather(
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
            target_groups_for_bout(),
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
                target_groups_result.get("Marker"),
                policies_result.get("IsTruncated"),
                rules_result.get("NextToken"),
            )
        ):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 clean-baseline discovery exceeded its bounded page"
            )
        prefix = self.config.deterministic_name_prefix[:40].rstrip("-") + "-"
        leftovers: list[str] = []
        deleting_leftovers: list[str] = []
        # Secrets carry their tags inline in ListSecrets, so a scoped scan blocks
        # on the exact canonical name (missing/partial tags still block) OR on a
        # noncanonical secret stamped with this bout's ownership tags. Unscoped,
        # the installation-wide prefix behavior is preserved.
        for value in secrets_result.get("SecretList") or []:
            name = str(value.get("Name") or "")
            if names is not None:
                if name == names.secret_name or belongs_to_bout(
                    value.get("Tags") or []
                ):
                    leftovers.append("secret")
            elif name.startswith(self.config.secret_name_prefix.rstrip("/") + "/"):
                leftovers.append("secret")
        for role in roles_result.get("Roles") or []:
            name = str(role.get("RoleName") or "")
            is_canonical = names is not None and name == names.proxy_role_name
            # The enumeration net is the installation prefix in both modes; the
            # exact canonical name is always in the net. A prefix role from
            # another bout is fetched, found to be foreign by its ownership tag,
            # and skipped -- it never blocks this scoped proof.
            if not (is_canonical or name.startswith(prefix)):
                continue
            tags = await self._call(clients.iam.list_role_tags, RoleName=name)
            if tags.get("IsTruncated"):
                raise ConnectionSpikeLiveConfigurationError(
                    "Round 5 role-tag discovery exceeded its bounded page"
                )
            role_tags = tags.get("Tags") or []
            if names is not None:
                if is_canonical:
                    # Exact canonical name blocks even with missing/partial tags.
                    leftovers.append(
                        "role" if belongs_to_bout(role_tags, iam=True) else "role_tag_drift"
                    )
                elif belongs_to_bout(role_tags, iam=True):
                    leftovers.append("role")
            elif not owned(role_tags, iam=True):
                leftovers.append("role_tag_drift")
            else:
                leftovers.append("role")
        leftovers.extend(
            "security_group"
            for value in groups_result.get("SecurityGroups") or []
            if (
                (
                    str(value.get("GroupName") or "")
                    == names.proxy_security_group_name
                    or belongs_to_bout(value.get("Tags") or [])
                )
                if names is not None
                else str(value.get("GroupName") or "").startswith(prefix)
            )
        )
        for proxy in proxies_result.get("DBProxies") or []:
            proxy_name = str(proxy.get("DBProxyName") or "")
            is_canonical = names is not None and proxy_name == names.proxy_name
            if not (is_canonical or proxy_name.startswith(prefix)):
                continue
            tags = await self._call(
                clients.rds.list_tags_for_resource,
                ResourceName=str(proxy.get("DBProxyArn") or ""),
            )
            proxy_tags = tags.get("TagList") or []
            leftover_kind = ""
            if names is not None:
                if is_canonical:
                    leftover_kind = (
                        "proxy" if belongs_to_bout(proxy_tags) else "proxy_tag_drift"
                    )
                elif belongs_to_bout(proxy_tags):
                    leftover_kind = "proxy"
            elif not owned(proxy_tags):
                leftover_kind = "proxy_tag_drift"
            else:
                leftover_kind = "proxy"
            if leftover_kind:
                if str(proxy.get("Status") or "").lower() == "deleting":
                    deleting_leftovers.append(leftover_kind)
                else:
                    leftovers.append(leftover_kind)
        leftovers.extend(
            "proxy_target_group"
            for value in target_groups_result.get("TargetGroups") or []
            if value
        )
        leftovers.extend(
            "runner_policy"
            for name in policies_result.get("PolicyNames") or []
            if (
                str(name) == names.runner_policy_name
                if names is not None
                else str(name).startswith(prefix) and str(name).endswith("-runner-secret")
            )
        )
        leftovers.extend(
            "security_group_rule"
            for value in rules_result.get("SecurityGroupRules") or []
            if (
                (
                    str(value.get("Description") or "").startswith(
                        names.proxy_name.removesuffix("-proxy") + "-"
                    )
                    or belongs_to_bout(value.get("Tags") or [])
                )
                if names is not None
                else str(value.get("Description") or "").startswith(prefix)
            )
        )
        if deleting_leftovers and not leftovers:
            raise ConnectionSpikeLiveTransientError(
                "Round 5 bout-owned proxy is still deleting"
            )
        if leftovers:
            if names is None:
                # Pre-T0/setup installation-wide scan: a leftover is a
                # prior-bout add-on and remains a configuration/inventory fault.
                raise ConnectionSpikeLiveConfigurationError(
                    "Round 5 clean baseline contains prior-bout add-ons"
                )
            raise ConnectionSpikeCleanupError(
                "Round 5 scoped absence proof found current-bout add-ons",
                stage="scoped_orphan_discovery",
                reason_code="current_bout_resource_present",
            )

    def _coordinator(
        self,
        scope: CreationScope,
        clients: _SetupAwsClients,
        resources: _SetupResources,
        *,
        fence: FenceGuard | None = None,
    ) -> tuple[Round5CreationCoordinator, tuple[ResourceSpec, ...]]:
        mutation_fence = fence or self._fence

        async def assert_mutation_authority() -> None:
            await mutation_fence.assert_current(scope)

        tags = dict(self.config.ownership_tags)
        tags["anti-demo-bout-id"] = scope.bout_id
        tags["anti-demo:bout-token"] = resources.names.token
        # Swarm defect #2: the bout fence is an immutable operation-identity tag.
        # It is verified EXACTLY on inspect/adopt/cleanup (``_require_exact_tags``),
        # so a proxy created by one generation (fence N) can never be adopted by,
        # or deleted as a replacement for, a later generation (fence N+1): their
        # per-bout Proxies are distinct operations even when they share a
        # bout-derived name.  This is the "never-reused operation identity" the
        # generation-unique-name requirement is really after, expressed as a
        # verified tag rather than only as a name string (which the journal-free
        # absence-proof and reconcile paths must still be able to reconstruct from
        # the bout id alone).
        tags["anti-demo:bout-fence"] = str(scope.fencing_token)
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
                lambda spec: self._create_proxy(clients, resources, spec, scope=scope),
                lambda spec, provider_id: self._inspect_proxy(clients, spec, provider_id),
                lambda observed: self._delete_proxy(
                    clients,
                    observed,
                    bout_id=scope.bout_id,
                    assert_authority=assert_mutation_authority,
                ),
            ),
            "proxy_target_group": _SetupResourceAdapter(
                lambda spec: self._configure_target_group(clients, resources, spec),
                lambda spec, provider_id: self._inspect_target_group(
                    clients, resources, spec, provider_id
                ),
                lambda observed: self._reset_target_group_if_parent_matches(
                    clients,
                    resources,
                    observed,
                    assert_mutation_authority,
                ),
            ),
            "proxy_target": _SetupResourceAdapter(
                lambda spec: self._register_proxy_target(clients, resources, spec),
                lambda spec, provider_id: self._inspect_proxy_target(
                    clients, resources, spec, provider_id
                ),
                lambda observed: self._deregister_proxy_target_if_parent_matches(
                    clients,
                    resources,
                    observed,
                    assert_mutation_authority,
                ),
            ),
        }
        return (
            Round5CreationCoordinator(
                journal=self._journal,
                fence=mutation_fence,
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

    def _ensure_createproxy_executor(self) -> ThreadPoolExecutor:
        """Return the reused CreateDBProxy worker, creating it if absent.

        Warmed before the scored window (see ``setup()``); this creates a single
        long-lived worker once and reuses it across bouts.  It must never be
        called for the first time between T0 and the boto3 request (req #7): thread
        creation there would add uncontrolled latency to the scored delta.
        """

        executor = self._createproxy_executor
        if executor is None:
            executor = ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="r5-createproxy"
            )
            self._createproxy_executor = executor
        return executor

    async def _dispatch_create_db_proxy(
        self,
        clients: _SetupAwsClients,
        resources: _SetupResources,
        create_kwargs: Mapping[str, Any],
    ) -> Any:
        """Issue the single timed CreateDBProxy mutation on the reused worker.

        ``asyncio.to_thread`` (used by ``_call``) shares the event loop's default
        ``ThreadPoolExecutor`` with every other lane and probe.  Under saturation
        the ``create_db_proxy`` submission would queue behind unrelated work while
        a pre-call stamp had already been taken -- so the scored bell ->
        CreateDBProxy latency both understated the delay and the dispatch itself
        was delayed.  A dedicated single-thread executor removes both: the stamp
        is taken INSIDE the worker immediately before the SDK call, and no shared
        queue sits in front of it.  The executor is REUSED (warmed before the
        scored window), never created here inside it (req #7).
        """

        loop = asyncio.get_running_loop()
        executor = self._ensure_createproxy_executor()

        def _invoke() -> Any:
            # Inside the dedicated worker, immediately before the SDK call leaves
            # the process: the true, scored bell -> CreateDBProxy boundary.
            resources.proxy_create_requested_ns = self._monotonic_ns()
            return clients.rds.create_db_proxy(**create_kwargs)

        task = asyncio.ensure_future(loop.run_in_executor(executor, _invoke))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            await asyncio.shield(task)
            raise

    async def _create_proxy(
        self,
        clients: _SetupAwsClients,
        resources: _SetupResources,
        spec: ResourceSpec,
        *,
        scope: CreationScope | None = None,
    ) -> ResourceObservation:
        # Swarm defect #2: refuse a stale owner locally BEFORE any AWS mutation.
        # This is a pure, synchronous, in-process check (no await, no I/O), so it
        # is safe on the T0 -> boto3 path and adds nothing to the scored window.
        if scope is not None:
            self._require_bell_capability(scope.bout_id, scope.fencing_token)
        create_kwargs: dict[str, Any] = dict(
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
        # Submit boundary on the loop; the request boundary is stamped inside the
        # dedicated worker in _dispatch_create_db_proxy immediately before the SDK
        # call, so a saturated default executor can neither hide nor delay it.
        resources.proxy_create_submitted_ns = self._monotonic_ns()
        # Swarm defect #4: the happy path is a single dispatch (no extra awaits, so
        # the scored window is untouched).  Only an ambiguous AlreadyExists takes
        # the adopt/retry recovery below.
        await self._create_or_adopt_db_proxy(clients, resources, spec, create_kwargs)
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
        resources.proxy_arn = proxy_arn
        return self._observation(spec, proxy_arn)

    async def _create_or_adopt_db_proxy(
        self,
        clients: _SetupAwsClients,
        resources: _SetupResources,
        spec: ResourceSpec,
        create_kwargs: Mapping[str, Any],
    ) -> None:
        """Issue CreateDBProxy, resolving an ambiguous AlreadyExists correctly.

        Swarm defect #4.  ``CreateDBProxy`` has no client token, so a retried or
        ambiguously-acknowledged send can surface ``DBProxyAlreadyExistsFault``.
        The only safe responses are:

        * **Adopt** iff inspection proves the existing Proxy is *our exact
          operation* -- ``_inspect_proxy`` verifies account/region ARN prefix,
          exact ownership tags (including the bout fence), and the deterministic
          name.  A Proxy that shares the name but not the identity makes
          ``_inspect_proxy`` raise, which propagates as a terminal refusal.
        * **Retry only after proving absence** -- an AlreadyExists whose exact
          Proxy then inspects as absent is control-plane eventual consistency; it
          is confirmed absent over bounded describes before a single create retry,
          so we never spin against a name a different owner holds.
        """

        try:
            await self._dispatch_create_db_proxy(clients, resources, create_kwargs)
            return
        except Exception as exc:  # noqa: BLE001 - re-raised unless it is AlreadyExists
            if self._error_code(exc) != "DBProxyAlreadyExistsFault":
                raise
            already_exists = exc
        # Ambiguous AlreadyExists: adopt ONLY our exact Proxy.  ``_inspect_proxy``
        # raises on an identity/tag mismatch (a name collision that is not ours),
        # which is exactly the terminal refusal we want.
        observed = await self._inspect_proxy(clients, spec, None)
        if observed is not None:
            return  # exact owned Proxy exists -> adopt; describe extracts endpoint
        # AlreadyExists but our exact Proxy inspects absent: prove absence over
        # bounded describes before one create retry.
        for _ in range(PROXY_CREATE_ABSENCE_CONFIRMATIONS):
            await self._sleep(self.config.poll_interval_seconds)
            observed = await self._inspect_proxy(clients, spec, None)
            if observed is not None:
                return  # it materialized as ours in the meantime -> adopt
        # Proven absent under our exact identity: one bounded create retry.
        try:
            await self._dispatch_create_db_proxy(clients, resources, create_kwargs)
        except Exception as exc:  # noqa: BLE001
            if self._error_code(exc) == "DBProxyAlreadyExistsFault":
                raise ConnectionSpikeLiveOperationError(
                    "Round 5 CreateDBProxy reported AlreadyExists but no Proxy with the "
                    "exact per-bout operation identity could be adopted after proving "
                    "absence; refusing rather than adopting an unowned Proxy"
                ) from already_exists
            raise

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
        pool = group.get("ConnectionPoolConfig") or {}
        if (
            pool.get("MaxConnectionsPercent") == 100
            and pool.get("MaxIdleConnectionsPercent") == 50
            and pool.get("ConnectionBorrowTimeout") == 120
        ):
            # The cleanup mutation is already absent. RDS can drop tags from its
            # provider-owned default target group after the reset; requiring
            # those stale child tags before recognizing the exact sealed
            # baseline wedges every retry at provider inspection. This branch
            # authorizes no mutation: the exact parent/name/account identity and
            # exact baseline settings are sufficient to report our action gone.
            return None
        tags = await self._call(
            clients.rds.list_tags_for_resource,
            ResourceName=target_group_arn,
        )
        self._require_exact_tags(spec, tags.get("TagList") or [])
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

    async def _reset_target_group_if_parent_matches(
        self,
        clients: _SetupAwsClients,
        resources: _SetupResources,
        observed: ResourceObservation,
        assert_authority: Callable[[], Awaitable[None]],
    ) -> None:
        if not await self._cleanup_parent_matches(clients, resources, observed):
            return
        await assert_authority()
        if await self._cleanup_parent_matches(clients, resources, observed):
            await self._reset_target_group(clients, resources, observed)

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

    async def _deregister_proxy_target_if_parent_matches(
        self,
        clients: _SetupAwsClients,
        resources: _SetupResources,
        observed: ResourceObservation,
        assert_authority: Callable[[], Awaitable[None]],
    ) -> None:
        if not await self._cleanup_parent_matches(clients, resources, observed):
            return
        await assert_authority()
        if await self._cleanup_parent_matches(clients, resources, observed):
            await self._deregister_proxy_target(clients, resources, observed)

    async def _cleanup_parent_matches(
        self,
        clients: _SetupAwsClients,
        resources: _SetupResources,
        child: ResourceObservation,
    ) -> bool:
        """Authorize a child mutation only for the exact journaled parent ARN."""

        if not resources.proxy_arn:
            return False
        parent = await self._inspect_proxy(
            clients,
            ResourceSpec(
                ordinal=1,
                resource_kind="rds_proxy",
                deterministic_name=resources.names.proxy_name,
                metadata=child.metadata,
            ),
            resources.proxy_arn,
        )
        return parent is not None

    async def _inspect_proxy(
        self, clients: _SetupAwsClients, spec: ResourceSpec, provider_id: str | None
    ) -> ResourceObservation | None:
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
        # Swarm defect #5: when an expected ARN is supplied (cleanup and delete
        # confirmation), a Proxy that shares the deterministic name but not the
        # ARN is a *different operation* -- a generation-N+1 replacement that took
        # the name after generation N's Proxy was removed (RDS allows only one
        # Proxy per name at a time).  Our exact operation is therefore ABSENT: we
        # return None instead of touching it, so a stale cleanup can never adopt,
        # confirm against, or delete a replacement.  The adopt path passes
        # ``provider_id=None`` and still matches by exact tags + name below.
        if provider_id is not None and proxy_arn != provider_id:
            return None
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
        assert_authority: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        name = str(observed.deterministic_name or "")
        if assert_authority is not None:
            exact = await self._inspect_proxy(
                clients,
                ResourceSpec(
                    ordinal=1,
                    resource_kind="rds_proxy",
                    deterministic_name=name,
                    metadata=observed.metadata,
                ),
                observed.provider_id,
            )
            if exact is None:
                return
            await assert_authority()
            exact = await self._inspect_proxy(
                clients,
                ResourceSpec(
                    ordinal=1,
                    resource_kind="rds_proxy",
                    deterministic_name=name,
                    metadata=observed.metadata,
                ),
                observed.provider_id,
            )
            if exact is None:
                return
        try:
            await self._call(clients.rds.delete_db_proxy, DBProxyName=name)
        except Exception as exc:
            if self._error_code(exc) != "InvalidDBProxyStateFault":
                raise
            # A restart can observe the exact tagged Proxy after AWS has already
            # accepted deletion. RDS rejects a duplicate DeleteDBProxy while the
            # resource is DELETING; that is an accepted in-flight handoff, not a
            # reason to strand cleanup. The exact-name/ARN/tag inspection happened
            # before this adapter call. Re-read the exact operation and accept
            # the duplicate-delete fault only if AWS says that same ARN is
            # DELETING (or it has already disappeared).
            exact = await self._inspect_proxy(
                clients,
                ResourceSpec(
                    ordinal=1,
                    resource_kind="rds_proxy",
                    deterministic_name=name,
                    metadata=observed.metadata,
                ),
                observed.provider_id,
            )
            if exact is not None:
                state = await self._call(
                    clients.rds.describe_db_proxies,
                    DBProxyName=name,
                )
                exact_proxies = [
                    proxy
                    for proxy in state.get("DBProxies") or []
                    if str(proxy.get("DBProxyArn") or "") == observed.provider_id
                ]
                if (
                    len(exact_proxies) != 1
                    or str(exact_proxies[0].get("Status") or "").lower()
                    != "deleting"
                ):
                    raise
            # The absence loop below still must prove the same ARN gone before
            # debt can clear.
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
            #
            # Swarm defect #5: a single NotFound is *unknown*, not clean -- the RDS
            # control plane can momentarily describe a just-accepted delete as gone
            # and then report it again.  Require bounded consecutive confirmations
            # of exact-operation absence before declaring the billable Proxy
            # definitively deleted; any reappearance of our exact ARN resets the
            # count.  Until then the cleanup fence stays held (cleanup debt), and a
            # timeout raises so the debt is reported rather than silently cleared.
            confirmations = 0
            async with asyncio.timeout(PROXY_DELETION_TIMEOUT_SECONDS):
                while True:
                    if assert_authority is not None:
                        await assert_authority()
                    observed_now = await self._inspect_proxy(
                        clients,
                        ResourceSpec(
                            ordinal=12,
                            resource_kind="rds_proxy",
                            deterministic_name=name,
                            metadata=observed.metadata,
                        ),
                        observed.provider_id,
                    )
                    if observed_now is None:
                        confirmations += 1
                        if confirmations >= PROXY_DELETE_ABSENCE_CONFIRMATIONS:
                            return
                    else:
                        confirmations = 0
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
        # The exact resident-generation binding this adapter PRELOADed at its last
        # successful stage. Retained so an attested identity-change re-establishment
        # can durably CANCEL it (retire the old resident job) instead of leaving a
        # wedged same-process resident beating a superseded token. None until staged.
        self._resident_generation_binding: Round5ControlBinding | None = None
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
        # Retain the exact binding this generation PRELOADed, purely for diagnostics
        # and for a claim-BOUND retire path; a claim-less resident-generation PRELOAD
        # is retired by the superseding fresh PRELOAD, not an explicit CANCEL (the
        # control protocol forbids CANCEL on a claim-less binding).
        self._resident_generation_binding = binding
        return readiness

    async def retire_resident_generation(self) -> None:
        """Forget the resident identity this adapter attested (identity-change retire).

        Called during an attested identity-change re-establishment. The resident this
        adapter attested is provably gone/replaced, so drop the retained binding and
        the in-memory attested identity (boot/pid/token) that a stale
        ``validate_ready_provenance`` static check would otherwise read as CURRENT.
        The old resident generation itself is retired by the superseding fresh PRELOAD
        the clean rewarm issues for a new token (a claim-less generation PRELOAD is not
        CANCEL-able under the control protocol), so this method takes no external
        control action and cannot block the recovery.
        """

        self._resident_generation_binding = None
        self._resident_process_boot_id = ""
        self._resident_process_pid = 0
        self._resident_warm_attempt_token = ""

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
        # Record debt before durable CANCEL delivery. If the bounded abandoned-
        # ARM caller stops awaiting settlement, the binding remains visible to
        # settlement_pending and restart reconciliation instead of becoming an
        # unowned resident.
        self._resident_settlement_debt[binding.job_id] = binding
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
        if len(instances) != 1 or len(security_groups) != 1:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 runner topology did not resolve exactly once"
            )
        instance = instances[0]
        metadata = instance.get("MetadataOptions") or {}
        profile = instance.get("IamInstanceProfile") or {}
        group_ids = {value.get("GroupId") for value in instance.get("SecurityGroups") or []}
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
            or security_group.get("GroupId") != self.config.runner_security_group_id
            or bool(security_group.get("IpPermissions"))
        ):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 runner topology differs from the sealed contract"
            )
        _require_warm_runner_online(
            managed_instances,
            expected_instance_id=self.config.runner_instance_id,
            lane_id=str(
                getattr(self.config, "runner_lane", "")
                or (self.config.targets[0].lane_id if self.config.targets else "runner")
            ),
        )
        managed_instance = managed_instances[0]
        if managed_instance.get("PlatformType") != "Linux":
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
        self._cleanup_bout_required = False
        self._cleanup_start_lock = asyncio.Lock()
        #: Ramps started by a lane's own setup stop, awaited by `run`. Populated during the setup
        #: phase, which is the point: Lakebase's ten thousand is held while the AWS path is still
        #: building its Proxy.
        self._lane_bursts: dict[str, asyncio.Task[ConnectionSpikeLaneResult]] = {}
        # ``run()`` pops ``_lane_bursts`` into a local ``launched`` map while it
        # supervises them, so during a bout the burst tasks are no longer reachable
        # through ``_lane_bursts``. Mirror them here so ``cancel_local_round5_run_tasks``
        # can still cancel the ACTUALLY-running bursts on a towel/abandon -- otherwise
        # a burst could dispatch after the slot is fenced CLEANING (Finding A).
        self._run_burst_tasks: set[asyncio.Task[ConnectionSpikeLaneResult]] = set()
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
        # Blocker 2: durable authority guard the warm provider propagates. Consulted
        # immediately before this engine's external mutation boundaries so a stale
        # coordinator fence refuses the mutation even if cancellation was swallowed.
        self.authority_guard: Callable[[], Awaitable[None]] | None = None

    async def _check_authority(self) -> None:
        guard = getattr(self, "authority_guard", None)
        if guard is not None:
            await guard()

    @property
    def has_timed_setup(self) -> bool:
        return self._setup_orchestrator is not None

    def retain_cleaned_bout(self, bout_id: str) -> None:
        """Carry the last exact cleanup proof into this warm-engine instance."""

        LiveConnectionSpikeAdapter._validate_run_id(bout_id)
        if self._cleanup_bout_id not in {None, bout_id}:
            raise ConnectionSpikeCleanupError(
                "Round 5 engine already carries another cleanup bout"
            )
        self._cleanup_bout_id = bout_id
        self._cleanup_bout_required = True

    def _supersede_completed_cleanup_bout(self, bout_id: str) -> None:
        """Reset a retained cleaned-bout id left by a COMPLETED predecessor cleanup.

        Engine identity reuse across generations: the coordinator reuses this warm
        engine, so a finished predecessor cleanup may still hold ``_cleanup_bout_id``
        for lineage. A new claim's cleanup supersedes that predecessor -- mirroring
        ``setup()``, which already clears the retained id when a different bout begins
        -- so a subsequent ``retain_cleaned_bout(bout_id)`` is not refused with "Round
        5 engine already carries another cleanup bout" (the live wedge that looped the
        no-bell converge in ``cleanup_reconcile_blocked``). Same-bout is left intact so
        a re-run stays idempotent. This never permits two CONCURRENT cleanup bouts on
        one engine: the coordinator serializes cleanup per claim (single-owner
        converge), so a differing retained id is always a finished predecessor, never
        an in-flight bout. Cross-replica takeover still uses the durable journal/job
        ids -- this only resets in-process engine identity.
        """

        if self._cleanup_bout_id is not None and self._cleanup_bout_id != bout_id:
            self._cleanup_bout_id = None

    def require_cleaned_bout(self) -> None:
        """Mark this engine as belonging to a post-cleanup warm lineage."""

        self._cleanup_bout_required = True

    def _require_cleaned_bout_for_lineage(self) -> None:
        if self._cleanup_bout_required and self._cleanup_bout_id is None:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 post-cleanup warm omitted its retained cleaned bout"
            )

    async def verify_start_state(self, bout_id: str, fencing_token: int) -> None:
        """Prove the warm/lease revision before any public CHECKING projection.

        Orchestrator ``prepare`` is coordination-only (no AWS, no resident
        stage). A failure here must roll the unstarted claim back to READY.
        """

        if self._setup_orchestrator is None:
            return
        await self._setup_orchestrator.prepare(bout_id, fencing_token)

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

        self._require_cleaned_bout_for_lineage()
        if self._setup_orchestrator is None:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 automatic warm setup is not configured"
            )
        # Authority boundary: refuse before the credential/AWS setup mutation.
        await self._check_authority()
        setup_context, arm = await asyncio.gather(
            self._setup_orchestrator.warm(
                generation,
                cleaned_bout_id=self._cleanup_bout_id,
            ),
            self.check(),
        )
        self._warm_generation = generation
        self._warm_attempt_token = warm_attempt_token
        targets_by_lane = {target.lane_id: target for target in self._adapter.config.targets}
        # Authority boundary: refuse before the runner STAGE/PRELOAD control dispatch.
        await self._check_authority()
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

        self._require_cleaned_bout_for_lineage()
        if self._setup_orchestrator is None or self._armed is None:
            raise ConnectionSpikeLiveOperationError("Round 5 cannot refresh before a complete warm")
        setup_context, *dispatch_expirations = await asyncio.gather(
            self._setup_orchestrator.warm(
                generation,
                cleaned_bout_id=self._cleanup_bout_id,
            ),
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

    async def retire_resident_generation(self) -> None:
        """Retire (durably CANCEL) every lane's staged resident generation.

        Used by the warm provider's attested identity-change re-establishment: the
        resident this generation attests is provably gone/replaced, so drain each
        lane's staged binding before the fresh rewarm re-PRELOADs. The authority guard
        is checked first so a stale owner cannot mutate the resident control plane; a
        lost fence propagates (the coordinator loop defers to the new owner). Each lane
        retire is best-effort (see the adapter) so one lane cannot strand the other.
        """

        await self._check_authority()
        for adapter in self._lane_adapters.values():
            await adapter.retire_resident_generation()

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
            # In-process preflight identity (boot/pid/harness/model) no longer matches
            # the prepared receipt: an ATTESTED runner change. Demote (return False).
            return False
        from .round5_control import ResidentLiveness
        from .round5_warm import RetryableWarmError

        checks = []
        for lane_id, receipt in expected.items():
            transport = self._lane_adapters[lane_id]._resident_transport
            if transport is None:
                # No transport to attest liveness right now -> transient, not an
                # attested identity change. Strike, do not demote.
                raise RetryableWarmError("runner_attestation_transport_absent")
            checks.append(
                transport.resident_liveness(
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
        liveness = await asyncio.gather(*checks)
        # Idle keep-alive contract: only an ATTESTED identity change demotes. A merely
        # STALE (beat aged past the window) or ABSENT (no beat yet) resident is a
        # transient miss -> RetryableWarmError -> the coordinator's strike budget keeps
        # READY. This is the primary fix for the idle "Temporarily Unavailable" flicker.
        if any(state is ResidentLiveness.IDENTITY_CHANGED for state in liveness):
            return False
        if any(
            state in (ResidentLiveness.STALE, ResidentLiveness.ABSENT)
            for state in liveness
        ):
            raise RetryableWarmError("runner_attestation_stale")
        return True

    async def warm_with_physical_runners_from(
        self,
        source: LiveConnectionSpikeEngine,
        generation: int,
    ) -> LiveRound5WarmEngineReceipt:
        """Warm another target variant without benchmarking the runners again."""

        source._require_cleaned_bout_for_lineage()
        if self._setup_orchestrator is None or source._armed is None:
            raise ConnectionSpikeLiveOperationError(
                "Round 5 shared physical runner receipt is unavailable"
            )
        self._lane_adapters = source._lane_adapters
        self._armed = source._armed
        self._warm_generation = generation
        self._warm_attempt_token = source._warm_attempt_token
        setup_context = await self._setup_orchestrator.warm(
            generation,
            cleaned_bout_id=source._cleanup_bout_id,
        )
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

    async def precommit_launch_intent(self, bout_id: str, fencing_token: int) -> None:
        """Durably commit the CreateDBProxy CREATE_INTENT on the bell path.

        Swarm defect #1 (Approach A).  The manager awaits this immediately before
        the authoritative bell transaction (before T0 and before Lakebase is
        released), so the durable competitor create intent always exists first and
        the post-T0 path awaits no coordination I/O.  A no-op when this
        installation has no timed setup.  This is a required bell-seam method on
        the engine, not an optional getattr hook.
        """

        if self._setup_orchestrator is None:
            return
        await self._setup_orchestrator.precommit_launch_intent(bout_id, fencing_token)

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
        if self._cleanup_bout_id == bout_id:
            raise ConnectionSpikeCleanupError(
                "Round 5 cannot restart a bout already under its cleanup fence"
            )
        # The retained ID fenced warm/refresh against the previous bout. Once a
        # different bout starts timed setup, its own cleanup identity supersedes
        # that completed predecessor.
        self._cleanup_bout_id = None
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
            # Mirror the launched bursts so a concurrent towel/abandon can cancel the
            # actually-running burst tasks (see _run_burst_tasks / Finding A).
            self._run_burst_tasks = {
                task for task in launched.values() if task is not None
            }
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
            # Finding A3: on run() cancel/exit (e.g. SIGTERM cancels this task), the
            # launched bursts must NOT be silently orphaned by wiping the set -- an
            # orphan could keep dispatching after teardown. Cancel+await them first,
            # then clear. On the normal success path they are already done (no-op).
            pending_bursts = {
                task for task in self._run_burst_tasks if not task.done()
            }
            for task in pending_bursts:
                task.cancel()
            if pending_bursts:
                await asyncio.gather(*pending_bursts, return_exceptions=True)
            self._run_burst_tasks = set()
            for lane_id, run_id in tuple(self._active_run_ids.items()):
                if not self._lane_adapters[lane_id].settlement_pending(run_id):
                    self._active_run_ids.pop(lane_id, None)

    async def _cancel_local_bursts(self) -> None:
        """Cancel+await EVERY in-process lane burst: both the pre-dispatch bursts still
        parked in ``_lane_bursts`` and the already-dispatched bursts that ``run()``
        popped into its local ``launched`` map (mirrored in ``_run_burst_tasks``).

        Cancelling only ``_lane_bursts`` (which ``run()`` empties as it dispatches)
        left the ACTUALLY-running bursts alive, so a burst could still dispatch after
        the slot was fenced CLEANING / after the per-bout Proxy was deleted
        (Findings A / A2). This is the single chokepoint every cleanup path uses
        before any orchestrator begin_cleanup / Proxy delete.
        """

        # Defensive getattr: some cleanup paths run on engines built via
        # ``object.__new__`` in tests (no __init__), so these attributes may be
        # absent -- a missing burst registry simply means nothing to cancel.
        current = asyncio.current_task()
        lane_bursts = getattr(self, "_lane_bursts", {})
        run_bursts = getattr(self, "_run_burst_tasks", ())
        bursts = {
            task
            for task in (*lane_bursts.values(), *run_bursts)
            if task is not None and task is not current
        }
        for burst in bursts:
            if not burst.done():
                burst.cancel()
        if bursts:
            await asyncio.gather(*bursts, return_exceptions=True)

    async def cancel_local_round5_run_tasks(self) -> None:
        """Cancel in-process setup/burst work without external cleanup mutation."""

        current = asyncio.current_task()
        setup_task = self._setup_task
        if setup_task is not None and setup_task is not current:
            setup_task.cancel()
            await asyncio.gather(setup_task, return_exceptions=True)
        await self._cancel_local_bursts()

    async def _ensure_post_bell_provider_cleanup_started(self, claim: Any) -> None:
        if self._cleanup_bout_id is not None:
            return
        bout_id = str(claim.bout_id)
        # Whether or not timed setup already completed (``_setup_result`` present),
        # post-bell cleanup MUST settle the ARM-staged residents AND cancel any
        # still-running lane bursts -- not only the dispatched ``_active_run_ids``.
        # ``_stop_setup_and_begin_cleanup_once`` performs that exact complete
        # sequence (cancel setup task + cancel bursts + ``_settle_staged_residents``
        # + cancel remaining active runs + begin durable cleanup); the narrower
        # ``_stop_and_begin_cleanup_once`` skipped staged residents and bursts,
        # leaking a prepared resident that quarantines the next warm generation.
        setup = self._setup_result
        if setup is not None:
            await self._stop_setup_and_begin_cleanup_once(setup.bout_id)
            return
        if self._setup_bout_id == bout_id:
            await self._stop_setup_and_begin_cleanup_once(bout_id)

    async def stop_and_begin_cleanup(self, arm: FanInArm) -> None:
        """Settle active commands and start Round 5 cleanup idempotently.

        The method does not wait for AWS to prove every resource absent.  That
        slow proof remains available through :meth:`wait_for_cleanup_complete`.
        """

        if getattr(self, "_round5_cleanup_janitor_owned", False):
            return
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
        # No staged binding for this lane (a dispatched run whose staged binding was
        # already settled and popped by ``_settle_staged_residents``). The real
        # adapter's ``cancel_resident`` is binding-only, so calling it with the
        # legacy ``generation/lane_id/job_id`` kwargs raises TypeError. Settle the
        # durable logical job by id through ``cancel_job`` when the adapter exposes
        # it; only fall back to the legacy resident-cancel kwargs for older test
        # doubles that still implement that signature and lack ``cancel_job``.
        cancel_job = getattr(adapter, "cancel_job", None)
        if callable(cancel_job):
            await cancel_job(run_id)
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
            # Finding A2: cancel+await every in-process burst (both _lane_bursts and the
            # run()-popped _run_burst_tasks) before the orchestrator begin_cleanup /
            # Proxy delete so no burst dispatches after teardown.
            await self._cancel_local_bursts()
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

        if getattr(self, "_round5_cleanup_janitor_owned", False):
            return
        LiveConnectionSpikeAdapter._validate_run_id(bout_id)
        if bout_id not in {self._setup_bout_id, self._cleanup_bout_id}:
            raise ConnectionSpikeCleanupError("Round 5 setup cleanup bout is stale")
        starter = asyncio.create_task(
            self._stop_setup_and_begin_cleanup_once(bout_id),
            name=f"round5-engine-setup-cleanup-start-{bout_id}",
        )
        await asyncio.shield(starter)

    async def _settle_staged_residents(self) -> set[str]:
        """Cancel and settle every resident staged at ARM before a bell.

        ARM stages the Lakebase resident (``prepare`` -> ``transport.stage``)
        before any lane is dispatched, so its binding lives in
        ``_resident_bindings`` and is *not* yet represented in
        ``_active_run_ids``. Every abandon path -- an armed slot whose TTL
        expired, an operator who cancelled the arm, a refused bell -- must settle
        those staged residents. Otherwise the exact prepared job stays resident
        on the runner and the next warm generation collides with it in
        ``wait_agent_ready`` ("resident readiness binding changed"), which the
        warm provider classifies as ``warm_baseline_unexpected`` and latches the
        slot BLOCKED. Cancelling here returns the resident to idle so the next
        ``stage_resident_generation`` re-attests cleanly.

        Returns the set of job ids whose SETTLED observation was deferred past
        the bounded budget. ``transport.cancel`` commits the durable CANCEL
        before awaiting SETTLED, so a deferred job keeps both the engine binding
        and the adapter settlement debt for restart reconciliation to finish --
        it is never an unowned resident -- but the provider janitor (and its RDS
        Proxy delete) is not held for the full 12-minute settlement deadline.
        """

        staged = tuple(self._resident_bindings.items())
        if staged:
            # Blocker 3: authority guard before beginning the settle mutations.
            await self._check_authority()
            # Observable on-success, not just on-timeout: an unstage is the exact
            # action this fix exists to perform, and the original incident was
            # invisible on every surface (the residue was only findable via SSM).
            logger.info(
                "round5_abandoned_arm_resident_settle_begin lanes=%s",
                [lane_id for lane_id, _ in staged],
            )
        deferred_resident_jobs: set[str] = set()
        settled_lanes: list[str] = []
        for lane_id, binding in staged:
            # Blocker 3: authority guard before EACH resident cancel (not just at
            # method entry) so a fence lost between lanes aborts the next mutation.
            await self._check_authority()
            try:
                async with asyncio.timeout(ROUND5_ABANDONED_ARM_SETTLEMENT_SECONDS):
                    await self._lane_adapters[lane_id].cancel_resident(binding=binding)
            except TimeoutError:
                deferred_resident_jobs.add(binding.job_id)
                logger.error(
                    "round5_abandoned_arm_resident_settlement_deferred "
                    "lane=%s job_id=%s budget_seconds=%.0f; a warm attempt in this "
                    "window may block until restart reconciliation settles it",
                    lane_id,
                    binding.job_id,
                    ROUND5_ABANDONED_ARM_SETTLEMENT_SECONDS,
                )
                continue
            if self._resident_bindings.get(lane_id) is binding:
                self._resident_bindings.pop(lane_id, None)
            if self._active_run_ids.get(lane_id) == binding.job_id:
                self._active_run_ids.pop(lane_id, None)
            settled_lanes.append(lane_id)
            logger.info(
                "round5_abandoned_arm_resident_settled lane=%s job_id=%s",
                lane_id,
                binding.job_id,
            )
        if staged:
            logger.info(
                "round5_abandoned_arm_resident_settle_done settled=%s deferred=%d",
                settled_lanes,
                len(deferred_resident_jobs),
            )
        return deferred_resident_jobs

    async def settle_abandoned_arm(self) -> set[str]:
        """Settle residents staged at ARM when a bout is abandoned before setup.

        The pure arm-abandon path: an armed slot that reached ARMED (staging the
        Lakebase resident in ``prepare``) but was never rung, so timed ``setup``
        never ran and there is no per-bout Proxy, security group, or dispatched
        lane to tear down -- only the staged resident to unstage. This is
        deliberately narrower than ``stop_setup_and_begin_cleanup``: it does not
        require ``_setup_bout_id`` (which a pre-bell abandon never set), begins no
        provider cleanup, and is idempotent, so it is safe on every abandon path
        and a no-op once the residents are already settled. Deferred SETTLED
        observations remain owned by ``_resident_settlement_debt`` for restart
        reconciliation, exactly as in the post-bell cleanup path.
        """

        return await self._settle_staged_residents()

    async def _stop_setup_and_begin_cleanup_once(self, bout_id: str) -> None:
        setup_task = self._setup_task
        if setup_task is not None and setup_task is not asyncio.current_task():
            setup_task.cancel()
            await asyncio.gather(setup_task, return_exceptions=True)
        # Finding A2: cancel+await BOTH _lane_bursts AND the run()-popped
        # _run_burst_tasks before any orchestrator begin_cleanup / Proxy delete, so no
        # burst can dispatch after this bout is torn down. (Snapshotting only
        # _lane_bursts missed the actually-running bursts once run() had popped them.)
        await self._cancel_local_bursts()
        # ARM stages the Lakebase resident before any lane is dispatched; settle
        # those bindings first (see _settle_staged_residents) so an abandoned arm
        # cannot leave a prepared job resident that quarantines the next warm
        # generation as resident_job_active.
        deferred_resident_jobs = await self._settle_staged_residents()
        for lane_id, run_id in tuple(self._active_run_ids.items()):
            if run_id in deferred_resident_jobs:
                continue
            await self._cancel_resident_lane(lane_id, run_id)
            if self._active_run_ids.get(lane_id) == run_id:
                self._active_run_ids.pop(lane_id, None)
        blocking_active_jobs = {
            lane_id: run_id
            for lane_id, run_id in self._active_run_ids.items()
            if run_id not in deferred_resident_jobs
        }
        if blocking_active_jobs:
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
        reconstruct = getattr(
            self._setup_orchestrator,
            "reconcile_failed_cleanup",
            None,
        )
        if callable(reconstruct):
            await reconstruct(
                bout_id,
                current_fencing_token,
                cleanup_authority=self._check_authority,
            )
        else:
            # Compatibility for narrow stateless test orchestrators. Every
            # production orchestrator implements reconstructive cleanup.
            await self._setup_orchestrator.prove_bout_absent(bout_id)
        self.retain_cleaned_bout(bout_id)
        if self._setup_result is not None and self._setup_result.bout_id == bout_id:
            self._setup_result = None

    async def reconcile_claim(self, claim: Any) -> None:
        """Settle both logical jobs and prove exact provider absence on restart."""

        # Authority boundary: refuse the job cancel/settle mutation under a lost fence.
        await self._check_authority()
        self.bind_claim(claim)
        # Engine identity reuse across generations (see helper): supersede a completed
        # predecessor's retained cleaned-bout id BEFORE starting this bout's cleanup,
        # so _ensure_post_bell_provider_cleanup_started does not early-return on a stale
        # id and the terminal retain_cleaned_bout is not refused on a reused engine.
        self._supersede_completed_cleanup_bout(str(claim.bout_id))
        await self._ensure_post_bell_provider_cleanup_started(claim)
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
        # Blocker 3: authority guard before the provider-absence/journal mutation.
        await self._check_authority()
        # Always enter reconstructive cleanup. Journal absence is not provider
        # absence: CreateDBProxy may have crossed AWS before the intent/response
        # became durable. The orchestrator rebuilds exact tagged specs from this
        # durable claim and deletes them through the normal adapters.
        await self.reconcile_failed_cleanup(bout_id, int(claim.bout_fence))

    async def reconcile_abandoned_claim(self, claim: Any) -> None:
        """Recover a durable pre-bell claim without trusting an empty journal.

        ARM stages resident logical jobs before timed setup writes any per-bout
        resource journal. A restart can therefore observe CLEANING plus an empty
        journal while a resident is still PREPARED. The claim's durable job IDs
        are the recovery intent: cancel and settle both exact jobs first, then
        prove the (necessarily pre-bell) provider scope absent.
        """

        # Authority boundary: refuse the resident cancel/settle control dispatch
        # under a lost fence before issuing any external mutation.
        await self._check_authority()
        self.bind_claim(claim)
        # Engine identity reuse across generations (see helper): supersede a completed
        # predecessor's retained cleaned-bout id so the terminal retain_cleaned_bout
        # is not refused. This is the exact live no-bell wedge fix.
        self._supersede_completed_cleanup_bout(str(claim.bout_id))
        lane_jobs = (
            ("lakebase", self._job_ids["lakebase"]),
            ("competitor", self._job_ids["competitor"]),
        )
        settlement_results = await asyncio.gather(
            *(
                self._lane_adapters[lane].cancel_job(job_id)
                for lane, job_id in lane_jobs
            ),
            return_exceptions=True,
        )
        failures: list[ConnectionSpikeCleanupError] = []
        causes: list[BaseException] = []
        for (lane, job_id), result in zip(lane_jobs, settlement_results, strict=True):
            if not isinstance(result, BaseException):
                continue
            failure = ConnectionSpikeCleanupError(
                "Round 5 resident job settlement was not proven",
                stage="resident_settlement",
                reason_code="resident_job_not_settled",
                lane=lane,
                job_id=job_id,
            )
            failure.__cause__ = result
            failures.append(failure)
            causes.append(result)
        if len(failures) == 1:
            raise failures[0] from causes[0]
        if failures:
            raise ConnectionSpikeCleanupError(
                "Round 5 abandoned claim could not prove both resident jobs settled",
                stage="resident_settlement",
                reason_code="resident_jobs_not_settled",
                failures=failures,
            ) from BaseExceptionGroup("Round 5 resident settlement failures", causes)
        if self._setup_orchestrator is None:
            raise ConnectionSpikeCleanupError(
                "Round 5 setup orchestrator is unavailable",
                stage="provider_absence",
                reason_code="setup_orchestrator_unavailable",
            )
        bout_id = str(claim.bout_id)
        # Blocker 3: authority guard before the provider-absence proof (the second
        # external mutation phase), so a fence lost after the job cancels aborts here.
        await self._check_authority()
        await self._setup_orchestrator.prove_bout_absent(bout_id)
        self.retain_cleaned_bout(bout_id)

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

    ADOPTED_ENGINE_SAFETY_CAP = 64

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
        self._cleaned_bout_id: str | None = None
        self._requires_cleaned_bout = False
        # Blocker 2: a durable authority guard the coordinator installs. Invoked
        # immediately before every external mutation boundary so a stale owner is
        # refused even if a cancellation was swallowed. None until installed.
        self.authority_guard: Callable[[], Awaitable[None]] | None = None
        # Finding 3: the manager ADOPTS its claimed engine (the one that holds the
        # ARM-staged resident bindings) into the provider, keyed by claim_id, so the
        # provider -- the SOLE cleanup janitor under the coordinator's lease/fence --
        # can cancel those staged residents itself instead of the manager doing it.
        self._adopted_engines: dict[str, LiveConnectionSpikeEngine] = {}
        self._adopted_engine_order: list[str] = []
        self._adopted_engine_cap_exceeded = 0
        self._adopted_engine_high_water = 0
        self._journal_sealed_generation: int | None = None

    async def seal_absent_cleanup_journals(self, generation: int) -> None:
        """Commit DELETED on leftover journal rows whose AWS parent is already gone.

        Replica-local readiness treats newest lifecycle_state <> deleted as
        unresolved debt and keeps the fight card unavailable even when the warm
        slot is READY and proxies are absent.
        """

        if self._journal_sealed_generation == generation:
            return
        engine = self._engine_factory(CompetitorId.RDS_POSTGRES)
        engine.authority_guard = self.authority_guard
        orchestrator = engine._setup_orchestrator
        if orchestrator is None:
            return
        leftover = tuple(await orchestrator.unresolved_bout_ids())
        if not leftover:
            self._journal_sealed_generation = generation
            return
        for bout_id in leftover:
            scopes = tuple(await orchestrator._journal.scopes(bout_id))
            token = max((int(scope.fencing_token) for scope in scopes), default=1)
            # Journal DELETED commits require the artifact cleanup fence, not
            # the warm-coordinator heartbeat. The engine path passes
            # cleanup_authority and therefore skips reclaim.
            await orchestrator.reconcile_failed_cleanup(bout_id, token)
        remaining = tuple(await orchestrator.unresolved_bout_ids())
        if remaining:
            raise ConnectionSpikeLiveTransientError(
                "Round 5 cleanup journal still has unresolved bouts after parent absence"
            )
        self._journal_sealed_generation = generation

    def _mark_engine_cleanup_transferred(
        self, engine: LiveConnectionSpikeEngine | None, *, transferred: bool
    ) -> None:
        if engine is None:
            return
        engine._round5_cleanup_janitor_owned = transferred

    def _release_adopted_entry(self, claim_id: str) -> None:
        key = str(claim_id)
        engine = self._adopted_engines.pop(key, None)
        if key in self._adopted_engine_order:
            self._adopted_engine_order = [cid for cid in self._adopted_engine_order if cid != key]
        self._mark_engine_cleanup_transferred(engine, transferred=False)

    def release_adopted_engine(self, claim_id: str) -> None:
        self._release_adopted_entry(claim_id)

    def release_all_adopted_engines(self) -> None:
        for claim_id in list(self._adopted_engines):
            self._release_adopted_entry(claim_id)

    def transfer_adopted_engine_at_cleaning(self, claim_id: str) -> None:
        engine = self._adopted_engines.get(str(claim_id))
        self._mark_engine_cleanup_transferred(engine, transferred=True)

    def _enforce_adopted_engine_cap(self) -> None:
        """Observability-only soft cap -- it NEVER evicts a live engine.

        Every adopted engine is LIVE for its entire lifetime in the registry: it
        holds the exact ARM-staged resident bindings that the provider (the sole
        cleanup janitor under the coordinator's lease/fence) must cancel during
        convergence. Evicting a live engine would silently discard those bindings
        and re-introduce the original incident -- a rewarm/reconcile running over
        an orphaned staged resident that latches ``warm_baseline_unexpected``
        (terminal BLOCKED). A hard cap that drops active claims is therefore
        unacceptable.

        Entries leave the registry ONLY through the deterministic, durable-state-
        driven lifecycle: released on convergence/abandon success (after the
        durable store confirms the claim finished or advanced), on replacement by
        a newer engine for the same claim, or on ``close``. Correct single-warm-
        slot operation holds at most one live engine at a time, so that lifecycle
        keeps the registry bounded. A sustained breach of the soft cap means
        adopted engines are not being released (a leak); we surface it loudly for
        operators rather than masking a leak by destroying cleanup state.
        """
        held = len(self._adopted_engines)
        if held > self._adopted_engine_high_water:
            self._adopted_engine_high_water = held
        if held > self.ADOPTED_ENGINE_SAFETY_CAP:
            self._adopted_engine_cap_exceeded += 1
            logger.warning(
                "round5_adopted_engine_cap_breached held=%d cap=%d "
                "(NOT evicting: live engines hold staged cleanup bindings; "
                "investigate unreleased adopted engines)",
                held,
                self.ADOPTED_ENGINE_SAFETY_CAP,
            )

    def adopt_claimed_engine(
        self, claim_id: str, engine: LiveConnectionSpikeEngine
    ) -> None:
        key = str(claim_id)
        previous = self._adopted_engines.get(key)
        if previous is not engine:
            self._release_adopted_entry(key)
        self._adopted_engines[key] = engine
        if key not in self._adopted_engine_order:
            self._adopted_engine_order.append(key)
        self._mark_engine_cleanup_transferred(engine, transferred=False)
        self._enforce_adopted_engine_cap()

    async def _check_authority(self) -> None:
        guard = getattr(self, "authority_guard", None)
        if guard is not None:
            await guard()

    def _factory_engine(
        self,
        competitor_id: CompetitorId,
    ) -> LiveConnectionSpikeEngine:
        engine = self._engine_factory(competitor_id)
        if self._requires_cleaned_bout:
            engine.require_cleaned_bout()
        cleaned_bout_id = self._cleaned_bout_id
        if cleaned_bout_id is not None:
            engine.retain_cleaned_bout(cleaned_bout_id)
        # Propagate the durable authority guard to the engine so its own external
        # mutation boundaries (STAGE/PRELOAD, control dispatch, credential/AWS
        # setup) refuse under a lost fence too.
        engine.authority_guard = getattr(self, "authority_guard", None)
        return engine

    async def reconcile(self, slot: object) -> bool:
        from .round5_warm import (
            BlockedWarmError,
            RetryableWarmError,
            Round5Variant,
            Round5WarmState,
            WarmFenceLostError,
        )

        if slot.state not in {Round5WarmState.RUNNING, Round5WarmState.CLEANING}:
            persisted = getattr(slot, "cleaned_bout_id", None)
            self._requires_cleaned_bout = bool(
                getattr(slot, "requires_cleaned_bout", persisted is not None)
            )
            if persisted is not None:
                LiveConnectionSpikeAdapter._validate_run_id(persisted)
            self._cleaned_bout_id = persisted if self._requires_cleaned_bout else None
            return False
        if slot.claim is None:
            return False
        competitor_id = (
            CompetitorId.AURORA_SERVERLESS_V2
            if slot.claim.selected_variant == Round5Variant.AURORA
            else CompetitorId.RDS_POSTGRES
        )
        # Durable authority guard before the cleanup dispatch mutations (job
        # cancel/settle, provider absence proof). Refuses under a lost fence.
        await self._check_authority()
        claim_id = str(slot.claim.claim_id)
        try:
            # Prefer the ADOPTED claimed engine (it holds the ARM-staged resident
            # bindings) so the provider can cancel those residents itself; fall back
            # to a fresh engine only when nothing was adopted (e.g. restart takeover).
            engine = self._adopted_engines.get(claim_id)
            if engine is None:
                engine = self._engine_factory(competitor_id)
            engine.authority_guard = getattr(self, "authority_guard", None)
            if slot.bell_id is None and slot.bell_at_utc is None:
                # The provider is the SOLE janitor for a no-bell abandon: it cancels
                # the ARM-staged residents (on the adopted claimed engine, each under
                # the coordinator's per-mutation authority guard) AND settles the
                # exact durable jobs + proves provider absence.
                settle = getattr(engine, "settle_abandoned_arm", None)
                if callable(settle):
                    await settle()
                await engine.reconcile_abandoned_claim(slot.claim)
            else:
                await engine.reconcile_claim(slot.claim)
            self._cleaned_bout_id = str(slot.claim.bout_id)
            self._requires_cleaned_bout = True
            self._release_adopted_entry(claim_id)
            return True
        except asyncio.CancelledError:
            raise
        except WarmFenceLostError:
            # Authority moved mid-cleanup: propagate so the loop aborts without
            # masking the fence loss as a retryable/blocked provider fault.
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

    async def reestablish(self, slot: object) -> None:
        """Retire the resident + discard stale engines after an attested identity change.

        The coordinator calls this the instant ``validate_ready`` attests a resident
        identity change. The installed ``_engines``/``_receipts`` pin the CHANGED
        identity, so a rewarm over them would re-PRELOAD the stale binding; discard
        them (forcing the next ``prepare`` to build fresh engines and issue a genuinely
        fresh PRELOAD) and RETIRE the old resident generation job on each engine (drain
        a wedged same-process resident). Best-effort per engine so one lane's failure
        cannot strand the recovery; a lost fence propagates so the loop defers to the
        new owner. Idempotent: safe to call on every attested-change beat.
        """

        from .round5_warm import WarmFenceLostError

        del slot
        # Refuse under a lost fence before mutating the resident control plane.
        await self._check_authority()
        for engine in list(self._engines.values()):
            try:
                await engine.retire_resident_generation()
            except asyncio.CancelledError:
                raise
            except WarmFenceLostError:
                raise
            except Exception:
                logger.warning(
                    "round5_reestablish_retire_failed", exc_info=True
                )
        # Discard the stale engines/receipts so the next prepare() builds fresh ones.
        # validate_ready returns False while empty, which is harmless: the slot is
        # already WARMING for the clean rewarm.
        self._engines = {}
        self._receipts = {}

    @staticmethod
    def _retryable(error: BaseException) -> bool:
        if isinstance(
            error,
            (
                ConnectionSpikeLiveTransientError,
                TimeoutError,
                ConnectionError,
                ConnectTimeoutError,
                ReadTimeoutError,
                EndpointConnectionError,
                ConnectionClosedError,
            ),
        ):
            return True
        if type(error).__name__ == "InvalidStateError":
            text = str(error)
            if "still active" in text or "no longer current" in text:
                return True
        current: BaseException | None = error
        seen: set[int] = set()
        while current is not None and id(current) not in seen:
            seen.add(id(current))
            if isinstance(current, ConnectionSpikeLiveTransientError):
                return True
            if isinstance(current, ConnectionSpikeLiveOperationError) and (
                "journal write lost its active lease fence" in str(current)
            ):
                return True
            response = getattr(current, "response", None)
            if isinstance(response, Mapping):
                code = str((response.get("Error") or {}).get("Code") or "")
                status = int(
                    (response.get("ResponseMetadata") or {}).get("HTTPStatusCode") or 0
                )
                if (
                    status >= 500
                    or code.startswith("Throttl")
                    or code
                    in {
                        "RequestLimitExceeded",
                        "ServiceUnavailable",
                        "InternalFailure",
                        "PriorRequestNotComplete",
                        "InvalidDBProxyStateFault",
                    }
                ):
                    return True
            current = current.__cause__
        return False

    async def prepare(
        self,
        *,
        generation: int,
        coordinator_fence: int,
        process_epoch: str,
        broker_epoch: str,
        warm_attempt_token: str,
        requires_cleaned_bout: bool,
    ) -> object:
        from .round5_warm import (
            BlockedWarmError,
            RetryableWarmError,
            Round5Variant,
        )

        del process_epoch
        self._requires_cleaned_bout = requires_cleaned_bout
        if not requires_cleaned_bout:
            self._cleaned_bout_id = None
        variants = {
            Round5Variant.AURORA: CompetitorId.AURORA_SERVERLESS_V2,
            Round5Variant.RDS: CompetitorId.RDS_POSTGRES,
        }
        # Durable authority guard immediately before the warm mutation sequence
        # (setup/credential/AWS + runner STAGE). A stale owner is refused here even
        # if _run_holding_lease's cancellation was swallowed.
        await self._check_authority()
        try:
            engines = {
                variant: self._factory_engine(competitor_id)
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
            # Split at the raise site -- a bare ``except Exception -> retry`` would
            # silently retry a genuine anti-cheat/config defect forever, and a bare
            # ``-> block`` would terminally freeze a transient throttle (the
            # overnight outage). Classify explicitly:
            #   * transient AWS/Lakebase read failures (throttles, timeouts, 5xx,
            #     ping/connection errors) -> RETRYABLE with capped backoff.
            #   * ConnectionSpikeLiveConfigurationError (runner identity change,
            #     an orphaned per-bout Proxy present at warm, or fixture drift) ->
            #     typed permanent BLOCK. These are real defects that must fail
            #     closed for operator attention, never be papered over as READY.
            #   * anything else unexpected -> fail closed rather than assume it is
            #     safe to retry.
            if isinstance(exc, ConnectionSpikeLiveSourceUnresolvedError):
                # Req #4: the sealed source did not resolve to exactly one row (an
                # empty describe).  Retry boundedly for eventual consistency, but
                # escalate to a TERMINAL block if it persists -- an unfindable
                # source is a real problem, not a self-verifiable "the DB is busy"
                # condition, and must not recheck forever.  (Subclass of
                # SourceUnavailable, so this check MUST precede it.)
                logger.warning(
                    "round5_warm_source_unresolved generation=%s cause=%s: %s",
                    generation,
                    type(exc).__name__,
                    exc,
                )
                raise RetryableWarmError("warm_source_unresolved") from exc
            if isinstance(exc, ConnectionSpikeLiveSourceUnavailableError):
                # Identity is intact; only availability is temporarily missing
                # (source backing-up/modifying/failing-over/rebooting). Distinct,
                # self-verifiable transient taxonomy -- retryable like a throttle,
                # but named so an operator can tell a source-availability blip
                # apart from a generic AWS/Lakebase read failure. Never
                # warm_baseline_invalid.
                logger.warning(
                    "round5_warm_source_unavailable generation=%s cause=%s: %s",
                    generation,
                    type(exc).__name__,
                    exc,
                )
                raise RetryableWarmError("warm_source_unavailable") from exc
            if self._retryable(exc):
                logger.warning(
                    "round5_warm_provider_retryable generation=%s cause=%s: %s",
                    generation,
                    type(exc).__name__,
                    exc,
                )
                raise RetryableWarmError("warm_provider_retryable") from exc
            if isinstance(exc, ConnectionSpikeLiveConfigurationError):
                logger.warning(
                    "round5_warm_baseline_invalid generation=%s cause=%s: %s",
                    generation,
                    type(exc).__name__,
                    exc,
                )
                raise BlockedWarmError("warm_baseline_invalid") from exc
            if isinstance(exc, Round5ResidentBindingChangedError):
                # A staged resident from a prior claim/ARM is still draining --
                # an ownership TRANSITION, not a permanent baseline defect. While a
                # cleanup lineage is in force (requires_cleaned_bout, i.e. a claim
                # was just cleaned and its resident may not be fully settled), this
                # is settle-first/RETRYABLE: the coordinator settles the exact
                # residents and re-attempts, and it must NEVER latch the terminal
                # warm_baseline_unexpected block the live no-bell wedge produced.
                # With no claim/cleanup debt to explain it, a binding change is
                # genuine baseline drift and still fails closed (below).
                if requires_cleaned_bout:
                    logger.warning(
                        "round5_warm_resident_binding_transition generation=%s cause=%s: %s",
                        generation,
                        type(exc).__name__,
                        exc,
                    )
                    raise RetryableWarmError(
                        "warm_resident_binding_transition"
                    ) from exc
            logger.warning(
                "round5_warm_baseline_unexpected generation=%s cause=%s: %s",
                generation,
                type(exc).__name__,
                exc,
            )
            raise BlockedWarmError("warm_baseline_unexpected") from exc
        receipts = dict(zip(engines, warmed, strict=True))
        # Fresh-PRELOAD invariant (assert BEFORE installing the engines/receipts):
        # both lane receipts must carry THIS attempt's token and an attested
        # resident process identity that could only come from a PRELOAD ->
        # agent_ready round trip observed this attempt (each warm builds fresh
        # engines whose adapters start unattested). This fails closed with a
        # distinct, non-secret code instead of installing engines and publishing
        # READY on stale evidence -- the exact hazard a skipped or superseded
        # re-PRELOAD would slip past. Raised outside the transient/blocked
        # classifier above so it surfaces as its own terminal code.
        for receipt in warmed:
            boots = receipt.runner_process_boot_ids
            if (
                receipt.warm_attempt_token != warm_attempt_token
                or set(boots) != {"lakebase", "competitor"}
                or any(
                    (not boot) or boot == "unattested" for boot in boots.values()
                )
            ):
                raise BlockedWarmError("warm_ready_without_fresh_preload")
        self._engines = {variants[variant].value: engine for variant, engine in engines.items()}
        self._receipts = {variants[variant].value: receipt for variant, receipt in receipts.items()}
        self._credential_generation += 1

        return self._assemble_preparation(
            generation=generation,
            coordinator_fence=coordinator_fence,
            broker_epoch=broker_epoch,
            warm_attempt_token=warm_attempt_token,
            engines=engines,
            receipts=receipts,
        )

    def _assemble_preparation(
        self,
        *,
        generation: int,
        coordinator_fence: int,
        broker_epoch: str,
        warm_attempt_token: str,
        engines: Mapping[object, LiveConnectionSpikeEngine | None],
        receipts: Mapping[object, LiveRound5WarmEngineReceipt],
    ) -> object:
        """Seal the READY preparation (runner/shared/variant receipts + capsule).

        Shared by the initial warm (``prepare``) and the in-place credential +
        receipt renewal (``refresh_preparation``). The runner IDENTITY (boot id,
        process boot id, image/harness/capacity digests) is immutable and is
        re-asserted here on every renewal -- a mismatch is a TYPED PERMANENT block
        (``runner_boot_identity_changed`` / ``runner_harness_identity_changed``),
        never a silently slid receipt. Only the freshness bound (``expires_at``)
        and the freshly-observed proxy-absence timestamp
        (``receipt.setup_context.observed_at``, carried into each variant receipt)
        advance -- exactly "identity immutable, provenance renewable". So a
        renewal publishes genuinely NEW receipts off a fresh live probe, it does
        not rest READY on a stale/hardcoded absence.
        """

        from .round5_warm import (
            BlockedWarmError,
            Round5RunnerReceipt,
            Round5SharedReceipt,
            Round5Variant,
            Round5WarmPreparation,
        )

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
            for variant in receipts
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

    async def refresh_preparation(self, slot: object, capsule: object) -> object:
        """Renew credentials AND republish fresh receipts off a live probe.

        The keep-alive path: ``refresh_warm`` re-runs the setup orchestrator
        (which re-observes per-bout Proxy ABSENCE and refreshes launch/dispatch
        credentials) and re-reads the resident runner boot identity, so the
        preparation this returns carries a freshly-observed proxy-absence
        timestamp, rotated credentials, and a fresh 45-minute receipt horizon --
        on the SAME immutable runner identity (a change fails closed inside
        ``_assemble_preparation``). This lets READY renew in place across the
        45-minute receipt horizon without a full ``prepare()`` rewarm, while never
        resting on a hardcoded/stale absence.
        """

        from .round5_warm import (
            BlockedWarmError,
            RetryableWarmError,
            Round5Variant,
        )

        del capsule
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
        self._receipts = {
            (
                "aurora_serverless_v2"
                if variant == Round5Variant.AURORA
                else "rds_postgres"
            ): receipt
            for variant, receipt in receipts.items()
        }
        self._credential_generation += 1
        return self._assemble_preparation(
            generation=slot.generation,
            coordinator_fence=slot.coordinator_fence,
            # PRESERVE the slot's broker_epoch across a credential/receipt refresh.
            # broker_epoch is a per-process identity used ONLY by the capsule-belonging
            # checks (_capsule_belongs/_capsule_current); it has NO role in the resident
            # control wire. Minting a fresh random broker_epoch here diverged it from the
            # coordinator's stable self.broker_epoch (what the rewarm path stamps), so
            # after this in-place refresh any later freshness_lost->rewarm published a
            # capsule whose broker_epoch no longer matched the slot -> launch_capsule_missing
            # -> the ~45-min idle rewarm storm. Keeping it stable makes refresh and rewarm
            # produce belonging capsules interchangeably.
            broker_epoch=slot.broker_epoch,
            warm_attempt_token=slot.warm_attempt_token,
            engines=engines,
            receipts=receipts,
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
            # PRESERVE the slot's broker_epoch (see refresh_preparation): a fresh random
            # broker_epoch here diverged from the coordinator's stable self.broker_epoch and
            # broke _capsule_belongs after a post-refresh rewarm. broker_epoch is not on the
            # resident control wire; keeping it stable is correct and storm-free.
            broker_epoch=slot.broker_epoch,
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
