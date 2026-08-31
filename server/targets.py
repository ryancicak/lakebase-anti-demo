from __future__ import annotations

import asyncio
import json
import os
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Protocol

import boto3
import psycopg
from botocore.exceptions import BotoCoreError, ClientError

from .aws_auth import (
    AwsAuthMode,
    AwsAuthSelection,
    runtime_auth_from_environment,
    session_arguments,
)
from .capacity import rds_lane_is_scored
from .cost_model import (
    ACU_SAMPLE_PERIOD_SECONDS,
    acu_sampling_window,
    integrate_acu_seconds,
)
from .models import CompetitorId, RoundId
from .verifier import FatalProbeError, PreparedTarget


class TargetConfigurationError(RuntimeError):
    pass


class TargetNotArmedError(RuntimeError):
    pass


_LAKEBASE_HOST = re.compile(
    r"^[^.]+\.database\.([a-z0-9-]+)\.cloud\.databricks\.com$",
    re.IGNORECASE,
)

#: The one relation every measured lane writes, schema-qualified, exported so the
#: privilege the deployed app is granted is derived from the same expression that
#: issues the SQL. It was a literal in two query strings here and nowhere else,
#: and the grant plan in `lifecycle` had no entry for it at all: the app's
#: principal could connect to a measured Lakebase database and was then refused
#: at the INSERT with SQLSTATE 42501, after the bell, on a round the catalog had
#: already advertised as ready. A name imported from here fails a rename loudly
#: instead of orphaning the grant, which is the correction
#: `_coordination_runtime_grants` already made for the coordination tables.
PROBE_TABLE = "public.anti_demo_probe"

#: Exactly what :meth:`PsycopgPreparedTarget.attempt` performs against
#: :data:`PROBE_TABLE`, in the vocabulary PostgreSQL grants: the upsert needs
#: INSERT and -- because it is `ON CONFLICT DO UPDATE` -- UPDATE, and the nonce
#: read-back needs SELECT. Kept beside the statements rather than beside the
#: grant so that changing the SQL and changing the privilege are the same edit.
PROBE_PRIVILEGES = ("SELECT", "INSERT", "UPDATE")


def lakebase_region_from_host(host: str) -> str:
    match = _LAKEBASE_HOST.fullmatch(host.strip())
    if match is None:
        raise TargetConfigurationError("Lakebase endpoint host has an unrecognized region")
    return match.group(1).lower()


def _required_aws_environment() -> tuple[AwsAuthSelection, str, str]:
    values = {
        "AWS_REGION": os.environ.get("AWS_REGION", ""),
        "AWS_EXPECTED_ACCOUNT_ID": os.environ.get("AWS_EXPECTED_ACCOUNT_ID", ""),
    }
    missing = [name for name, value in values.items() if not value]
    if missing:
        raise TargetConfigurationError(
            f"Missing explicit AWS target binding: {', '.join(missing)}"
        )
    try:
        auth = runtime_auth_from_environment(os.environ)
    except RuntimeError as exc:
        raise TargetConfigurationError(str(exc)) from exc
    return auth, values["AWS_REGION"], values["AWS_EXPECTED_ACCOUNT_ID"]


def _assert_aws_identity(session: boto3.Session, expected_account_id: str) -> None:
    identity = session.client("sts").get_caller_identity()
    actual_account_id = str(identity.get("Account") or "")
    if actual_account_id != expected_account_id:
        raise TargetConfigurationError(
            "AWS credentials resolved to account "
            f"{actual_account_id or 'UNKNOWN'}, expected {expected_account_id}"
        )


def _assert_arn_binding(arn: str, region: str, account_id: str, label: str) -> None:
    parts = arn.split(":", 5)
    if len(parts) != 6 or parts[0] != "arn" or parts[3] != region or parts[4] != account_id:
        raise TargetConfigurationError(
            f"{label} is not bound to expected AWS account {account_id} and region {region}"
        )


@dataclass(frozen=True)
class ConnectionMaterial:
    host: str
    port: int
    database: str
    user: str
    password: str = field(repr=False)


class CredentialProvider(Protocol):
    async def connection_material(self) -> ConnectionMaterial:
        """Resolve credentials without making a PostgreSQL connection."""

    async def assert_armed(
        self,
        *,
        not_before: datetime | None = None,
    ) -> dict[str, object]:
        """Verify the required zero/idle state using control-plane signals only."""


class CommandRunner:
    """One Lakebase control-plane read, over the SDK rather than a subprocess.

    THE NAME IS KEPT AND THE MECHANISM IS NOT. This used to spawn ``databricks
    postgres get-endpoint`` and ``generate-database-credential``, and both are on
    the deployed app's arming path -- ``TargetResolver`` builds a
    :class:`LakebaseCredentialProvider` for every bout, and nothing arms without
    ``assert_armed`` and ``connection_material``. The Apps container has no CLI
    the app installed: it declares ``databricks-sdk`` only, and inherits whatever
    binary the base image ships. Round 2's backstage lane is simply where that
    surfaced first, because the readiness gate runs it before anything is armed.

    The accessor is the one ``pipeline_power.workspace_api`` already exposes, via
    :class:`~server.safe_change_live.DatabricksRestRunner`, so there is one REST
    seam for Lakebase in this codebase rather than three.

    THE STDERR RULE IS UNCHANGED. A refusal's own sentence is not pasted into
    this error; it is chained, and ``manager.operator_diagnosis`` decides how
    much of it may reach a screen. What is new is that there *is* a cause: the
    subprocess version reduced every failure to its last stderr line and a
    non-zero exit told the operator nothing about which of the two it was.
    """

    def __init__(self, *, profile: str = "") -> None:
        from .safe_change_live import DatabricksRestRunner

        self._runner = DatabricksRestRunner(profile=profile)

    async def json(
        self,
        method: str,
        path: str,
        *,
        body: dict[str, Any] | None = None,
        timeout_seconds: float = 120.0,
    ) -> dict[str, Any]:
        from .safe_change_live import ControlPlaneCommandError

        try:
            payload = await self._runner.json(
                method, path, body=body, timeout_seconds=timeout_seconds
            )
        except ControlPlaneCommandError as exc:
            raise TargetConfigurationError("A Lakebase control-plane request failed") from exc
        return dict(payload)


class LakebaseCredentialProvider:
    def __init__(
        self,
        runner: CommandRunner | None = None,
        *,
        database: str | None = None,
        endpoint: str | None = None,
        profile: str | None = None,
        user: str | None = None,
        expected_region: str | None = None,
    ) -> None:
        self.endpoint = endpoint or os.environ.get("LAKEBASE_ENDPOINT_NAME", "")
        self.profile = (
            profile if profile is not None else os.environ.get("DATABRICKS_PROFILE", "")
        )
        self.database = database or os.environ.get("LAKEBASE_DATABASE", "anti_demo")
        self.user = user if user is not None else os.environ.get("LAKEBASE_USER", "")
        self.expected_region = expected_region or os.environ.get(
            "LAKEBASE_EXPECTED_REGION", ""
        )
        self.port = int(os.environ.get("LAKEBASE_PORT", "5432"))
        self.runner = runner or CommandRunner(profile=self.profile)

    def _require_endpoint(self) -> None:
        # A profile selects one of several credential sets on a laptop, and there
        # is nothing for it to select in the Apps runtime: `_bind_deployed_runtime`
        # pops it so a deployment can only authenticate as the ambient OAuth
        # identity it was granted, and `DatabricksRestRunner` already reads an
        # empty profile as "use the ambient client". Demanding it in both places
        # refused Round 1 on the deployed app for a binding that deployment is not
        # allowed to hold. The waiver asks `selfheal.deployed`, the same predicate
        # the runtime binder answers, so it cannot come to disagree with the pop
        # that makes it necessary. Imported here rather than at module scope
        # because `selfheal` reaches this module back through `lifecycle`.
        from .selfheal import deployed

        required: dict[str, str] = {}
        if not deployed():
            required["DATABRICKS_PROFILE"] = self.profile
        required["LAKEBASE_ENDPOINT_NAME"] = self.endpoint
        required["LAKEBASE_DATABASE"] = self.database
        required["LAKEBASE_EXPECTED_REGION"] = self.expected_region
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise TargetConfigurationError(
                f"Missing explicit Lakebase target binding: {', '.join(missing)}"
            )

    async def _endpoint(self) -> dict[str, Any]:
        from .safe_change_live import lakebase_resource_path

        self._require_endpoint()
        endpoint = await self.runner.json("GET", lakebase_resource_path(self.endpoint))
        returned_name = str(endpoint.get("name") or "")
        if returned_name and returned_name != self.endpoint:
            raise TargetConfigurationError("Lakebase control plane returned a different endpoint")
        host = str(((endpoint.get("status") or {}).get("hosts") or {}).get("host") or "")
        actual_region = lakebase_region_from_host(host)
        if actual_region != self.expected_region:
            raise TargetConfigurationError(
                f"Lakebase endpoint is in {actual_region}, expected {self.expected_region}"
            )
        return endpoint

    async def assert_armed(
        self,
        *,
        not_before: datetime | None = None,
    ) -> dict[str, object]:
        endpoint = await self._endpoint()
        checked_at = datetime.now(UTC)
        status = endpoint.get("status") or {}
        state = str(status.get("current_state") or "").upper()
        disabled = bool(status.get("disabled", False))
        if disabled:
            raise TargetNotArmedError("Lakebase is disabled, not naturally suspended at scale zero")
        if state != "IDLE":
            raise TargetNotArmedError(f"Lakebase endpoint is {state or 'UNKNOWN'}, not IDLE")

        # `update_time` is the endpoint resource's last-updated timestamp. The
        # provider does not document it as a state-transition timestamp, and an
        # IDLE endpoint can retain an old value indefinitely. Preserve it as
        # advisory metadata; the manager decides whether it corroborates a
        # post-connection observation or whether repeated independent IDLE reads
        # are needed. It must never become the stopwatch's transition time.
        provider_update_time: datetime | None = None
        raw_update_time = endpoint.get("update_time")
        if isinstance(raw_update_time, str):
            try:
                provider_update_time = datetime.fromisoformat(
                    raw_update_time.replace("Z", "+00:00")
                )
            except ValueError:
                provider_update_time = None
            if provider_update_time is not None and provider_update_time.tzinfo is not None:
                provider_update_time = provider_update_time.astimezone(UTC)
            else:
                provider_update_time = None
        post_close_update = bool(
            provider_update_time is not None
            and not_before is not None
            and not_before.astimezone(UTC) <= provider_update_time <= checked_at
        )
        return {
            "state": state,
            "disabled": disabled,
            "region": self.expected_region,
            "checked_at": checked_at.isoformat(),
            "provider_updated_at": (
                None if provider_update_time is None else provider_update_time.isoformat()
            ),
            "provider_update_after_not_before": post_close_update,
            # Carried so the on-screen capacity disclosure reports the range the
            # control plane actually holds rather than the range we asked for.
            "autoscaling_limit_min_cu": status.get("autoscaling_limit_min_cu"),
            "autoscaling_limit_max_cu": status.get("autoscaling_limit_max_cu"),
        }

    async def connection_material(self) -> ConnectionMaterial:
        from .safe_change_live import CURRENT_USER_PATH, LAKEBASE_API_ROOT

        endpoint = await self._endpoint()
        host = ((endpoint.get("status") or {}).get("hosts") or {}).get("host")
        credential = await self.runner.json(
            "POST",
            f"{LAKEBASE_API_ROOT}/credentials",
            body={"endpoint": self.endpoint},
        )
        token = credential.get("token")
        user = self.user
        if not user:
            current_user = await self.runner.json("GET", CURRENT_USER_PATH)
            user = current_user.get("userName")
        if not host or not token or not user:
            raise TargetConfigurationError("Lakebase host, OAuth credential, or user is missing")
        return ConnectionMaterial(
            host=str(host),
            port=self.port,
            database=self.database,
            user=str(user),
            password=str(token),
        )


def _aurora_pause_wait_reason(
    writer_id: str,
    occurred_at: datetime,
    message: str,
    auto_pause: int,
) -> str:
    """
    Why the successful-pause event has not landed yet, in the words of the gate
    that is actually blocking.

    WHY THIS EXISTS. Aurora's arming evidence has two sources and only the first
    one is the gate. When the newest RDS serverless transition is anything but a
    fresh `Successfully paused`, control used to fall straight through to the
    CloudWatch sampler, and *that* branch's failure text reached the screen --
    "Aurora writer has not produced two consecutive zero-capacity samples". For
    the whole of a measured 479-second Round 1 wait the operator was told the
    hold-up was a sampling condition. It was not: the two-zero-sample rule
    contributed zero seconds and never fired. The wait was 172s of Aurora
    holding plateau capacity after the app disconnected, then the 300s
    auto-pause floor, then 7s for AWS to publish the event.

    WHAT THE MESSAGES MAY NOT SAY. `occurred_at + auto_pause` is a floor and
    nothing more. The floor's clock starts when Aurora's own post-resume work
    quiesces rather than when the bout disconnects, and across six measured
    cycles the real pause landed 35-182 seconds past that floor. So the wording
    below states a time AWS cannot beat, never a time to expect.

    The four message forms are the live ones, read off `describe-events` on the
    sealed r1 cluster rather than assumed: `Initiated pause`, `Successfully
    paused`, `Initiated resume for the DB instance: <writer> due to user
    activity`, `Successfully resumed`. The last clause catches anything AWS adds
    later -- an unrecognised transition is still evidence that this writer moved,
    and naming it is more useful than describing a sampler that is not the gate.
    """
    resumed = (
        f"Successfully resumed the DB instance: {writer_id}",
        f"Initiated resume for the DB instance: {writer_id}",
    )
    if message.startswith(resumed):
        earliest = occurred_at + timedelta(seconds=auto_pause)
        return (
            f"Aurora resumed at {occurred_at:%H:%M:%SZ}, so AWS cannot pause it "
            f"before {earliest:%H:%M:%SZ} ({auto_pause}s idle floor). That floor's "
            "clock starts when Aurora's own post-resume work quiesces, not when "
            "the bout disconnects, so it is a floor and not a forecast"
        )
    if message.startswith(f"Initiated pause for the DB instance: {writer_id}"):
        return (
            f"Aurora began pausing at {occurred_at:%H:%M:%SZ}; waiting for AWS to "
            "publish the successful-pause event that proves scale zero"
        )
    return (
        f"Aurora's newest serverless event at {occurred_at:%H:%M:%SZ} is not a "
        f"successful pause of {writer_id}: {message}"
    )


class AuroraCredentialProvider:
    def __init__(self) -> None:
        self.profile = os.environ.get("AWS_PROFILE", "")
        self.auth_mode: AwsAuthMode = "profile"
        self.region = os.environ.get("AWS_REGION", "")
        self.expected_account_id = os.environ.get("AWS_EXPECTED_ACCOUNT_ID", "")
        self.cluster_id = os.environ.get("AURORA_CLUSTER_ID", "")
        self.secret_arn = os.environ.get("AURORA_SECRET_ARN", "")
        self.database = os.environ.get("AURORA_DATABASE", "anti_demo")
        self.expected_postgres_major = os.environ.get("EXPECTED_POSTGRES_MAJOR", "17")

    def _session(self):
        return boto3.Session(
            **session_arguments(self.auth_mode, self.profile, self.region)
        )

    def _require(self) -> None:
        auth, self.region, self.expected_account_id = _required_aws_environment()
        self.auth_mode = auth.mode
        self.profile = auth.profile
        missing = [
            name
            for name, value in {
                "AURORA_CLUSTER_ID": self.cluster_id,
                "AURORA_SECRET_ARN": self.secret_arn,
                "AURORA_DATABASE": self.database,
            }.items()
            if not value
        ]
        if missing:
            raise TargetConfigurationError(
                f"Missing live Aurora configuration: {', '.join(missing)}"
            )

    async def assert_armed(
        self,
        *,
        not_before: datetime | None = None,
    ) -> dict[str, object]:
        self._require()
        try:
            return await asyncio.to_thread(self._assert_armed_sync, not_before)
        except (TargetConfigurationError, TargetNotArmedError):
            raise
        except (BotoCoreError, ClientError, KeyError, IndexError, TypeError, ValueError) as exc:
            raise TargetConfigurationError(
                "Aurora control-plane validation failed"
            ) from exc

    def _topology_sync(
        self, session: boto3.Session
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        _assert_aws_identity(session, self.expected_account_id)
        clusters = session.client("rds").describe_db_clusters(
            DBClusterIdentifier=self.cluster_id
        ).get("DBClusters", [])
        if len(clusters) != 1:
            raise TargetConfigurationError("AURORA_CLUSTER_ID did not resolve exactly once")
        cluster = clusters[0]
        if str(cluster.get("DBClusterIdentifier") or "") != self.cluster_id:
            raise TargetConfigurationError("Aurora control plane returned a different cluster")
        cluster_arn = str(cluster.get("DBClusterArn") or "")
        _assert_arn_binding(
            cluster_arn,
            self.region,
            self.expected_account_id,
            "Aurora cluster ARN",
        )
        if str(cluster.get("Engine") or "").lower() != "aurora-postgresql":
            raise TargetConfigurationError("AURORA_CLUSTER_ID is not Aurora PostgreSQL")
        engine_version = str(cluster.get("EngineVersion") or "")
        if not engine_version.startswith(f"{self.expected_postgres_major}."):
            raise TargetConfigurationError(
                "Aurora PostgreSQL major version does not match EXPECTED_POSTGRES_MAJOR"
            )

        members = cluster.get("DBClusterMembers") or []
        writers = [member for member in members if member.get("IsClusterWriter") is True]
        if len(members) != 1 or len(writers) != 1:
            raise TargetConfigurationError(
                "Aurora must have exactly one writer and no reader instances for this proof"
            )
        writer_id = str(writers[0].get("DBInstanceIdentifier") or "")
        instances = session.client("rds").describe_db_instances(
            DBInstanceIdentifier=writer_id
        ).get("DBInstances", [])
        if len(instances) != 1:
            raise TargetConfigurationError("Aurora writer did not resolve exactly once")
        writer = instances[0]
        if str(writer.get("DBInstanceIdentifier") or "") != writer_id:
            raise TargetConfigurationError("Aurora control plane returned a different writer")
        if str(writer.get("DBClusterIdentifier") or "") != self.cluster_id:
            raise TargetConfigurationError("Aurora writer belongs to a different cluster")
        _assert_arn_binding(
            str(writer.get("DBInstanceArn") or ""),
            self.region,
            self.expected_account_id,
            "Aurora writer ARN",
        )
        if str(writer.get("DBInstanceClass") or "") != "db.serverless":
            raise TargetConfigurationError("Aurora writer is not db.serverless")
        if str(writer.get("Engine") or "").lower() != "aurora-postgresql":
            raise TargetConfigurationError("Aurora writer is not PostgreSQL")
        if str(writer.get("DBInstanceStatus") or "").lower() != "available":
            raise TargetNotArmedError("Aurora writer control plane is not available")
        return cluster, writer

    def _assert_armed_sync(
        self,
        not_before: datetime | None = None,
    ) -> dict[str, object]:
        session = self._session()
        cluster, writer = self._topology_sync(session)
        scaling = cluster.get("ServerlessV2ScalingConfiguration") or {}
        minimum = float(scaling.get("MinCapacity", -1))
        if minimum != 0:
            raise TargetNotArmedError("Aurora Serverless v2 minimum capacity is not zero")
        raw_maximum = scaling.get("MaxCapacity")
        maximum = None if raw_maximum is None else float(raw_maximum)
        auto_pause = int(scaling.get("SecondsUntilAutoPause", 0))
        if auto_pause <= 0:
            raise TargetConfigurationError("Aurora automatic pause is not enabled")
        if str(cluster.get("Status", "")).lower() != "available":
            raise TargetNotArmedError("Aurora cluster control plane is not available")

        end = datetime.now(UTC)
        start = end - timedelta(minutes=10)
        writer_id = str(writer["DBInstanceIdentifier"])
        rds = session.client("rds")
        event_response = rds.describe_events(
            SourceIdentifier=self.cluster_id,
            SourceType="db-cluster",
            Duration=60,
            EventCategories=["serverless"],
        )
        transitions = []
        for event in event_response.get("Events", []):
            message = str(event.get("Message") or "")
            occurred_at = event.get("Date")
            if writer_id not in message or not isinstance(occurred_at, datetime):
                continue
            if "pause" not in message.lower() and "resume" not in message.lower():
                continue
            transitions.append((occurred_at.astimezone(UTC), message))
        # Why the wait is still running, when the events can say. The sampler
        # below keeps its verdict -- every return and every refusal stays where
        # it was -- but it does not get to narrate a wait it is not gating.
        wait_reason: str | None = None
        if transitions:
            occurred_at, message = max(transitions, key=lambda item: item[0])
            expected = f"Successfully paused the DB instance: {writer_id}"
            fresh_for_window = (
                not_before is None or occurred_at >= not_before.astimezone(UTC)
            )
            if message != expected:
                wait_reason = _aurora_pause_wait_reason(
                    writer_id, occurred_at, message, auto_pause
                )
            if message == expected and fresh_for_window:
                return {
                    "state": "SCALE_ZERO",
                    "capacity_acu": 0.0,
                    "samples": 1,
                    "observed_at": occurred_at.isoformat(),
                    "evidence": "RDS_EVENT_SUCCESSFULLY_PAUSED",
                    "writer_instance_id": writer_id,
                    "engine_version": str(cluster.get("EngineVersion") or ""),
                    "auto_pause_seconds": auto_pause,
                    "min_capacity_acu": minimum,
                    "max_capacity_acu": maximum,
                }

        response = session.client("cloudwatch").get_metric_statistics(
            Namespace="AWS/RDS",
            MetricName="ServerlessDatabaseCapacity",
            Dimensions=[{"Name": "DBInstanceIdentifier", "Value": writer_id}],
            StartTime=start,
            EndTime=end,
            Period=60,
            Statistics=["Maximum"],
        )
        datapoints = sorted(response.get("Datapoints", []), key=lambda item: item["Timestamp"])
        if len(datapoints) < 2:
            raise TargetNotArmedError(
                wait_reason
                or "Aurora needs two recent writer-level capacity samples to prove scale zero"
            )
        samples = datapoints[-2:]
        observed_at = samples[-1]["Timestamp"]
        if not_before is not None and any(
            item["Timestamp"].astimezone(UTC) < not_before.astimezone(UTC)
            for item in samples
        ):
            raise TargetNotArmedError(
                wait_reason
                or "Aurora is waiting for two zero-capacity samples after the re-do clock began"
            )
        capacities = [float(item.get("Maximum", -1)) for item in samples]
        if any(capacity != 0 for capacity in capacities):
            raise TargetNotArmedError(
                wait_reason
                or "Aurora writer has not produced two consecutive zero-capacity samples"
            )
        if observed_at < end - timedelta(minutes=3):
            raise TargetNotArmedError(
                wait_reason or "Aurora zero-capacity evidence is stale"
            )
        # These datapoints are deliberately NOT integrated into a cost quantity.
        # The window above is `now - 10 min -> now`, sampled *before* the bout,
        # and the arming contract is that every sample in it reads zero -- the
        # two lines above raise unless they do. Feeding it to
        # `BoutTelemetry.observed_acu_seconds_above_floor` would therefore set
        # that field to 0 and stamp it `MEASURED`, which is a fabricated zero
        # wearing the highest evidence grade. Bout-window sampling lives in
        # `sample_acu_seconds` below, which reaches past the bell.
        return {
            "state": "SCALE_ZERO",
            "capacity_acu": 0.0,
            "samples": len(samples),
            "observed_at": observed_at.astimezone(UTC).isoformat(),
            "writer_instance_id": writer_id,
            "engine_version": str(cluster.get("EngineVersion") or ""),
            "auto_pause_seconds": auto_pause,
            "min_capacity_acu": minimum,
            "max_capacity_acu": maximum,
        }

    async def sample_acu_seconds(
        self,
        started_at: datetime,
        ended_at: datetime,
        *,
        writer_instance_id: str | None = None,
        cluster_level: bool = False,
    ) -> Decimal | None:
        """Integrate the capacity a bout actually consumed, in ACU-seconds.

        This is the *only* supported source for
        ``BoutTelemetry.observed_acu_seconds_above_floor``, and without it
        ``server/cost_model.py:_aurora_acu_quantity`` has nothing to price from
        and returns ``unavailable``. A lane clock is not a substitute: measured
        against CloudWatch on 2026-08-21, Round 1's 15.31-second bout provisioned
        420 seconds of billed capacity, 97.2% of it after the bell.

        ``started_at``/``ended_at`` must bracket the **bout**. They are widened
        by :func:`acu_sampling_window` to reach past the auto-pause descent,
        which is billed and outlives every clock the harness keeps. Do not pass
        the arming gate's pre-bout window: it is guaranteed to read zero, and a
        zero from it would be a fabricated measurement.

        **This can never fail a bout.** It is an opportunistic improvement to an
        estimate, not a gate, so an unreachable CloudWatch, an expired
        credential, a slow response or an empty result all return ``None`` and
        leave the telemetry field unset. ``None`` means unmeasured, and the
        Aurora line stays ``unavailable`` rather than becoming a zero. It is
        deliberately *not* wired into :meth:`assert_armed`, so no arming
        decision can ever depend on it.
        """

        try:
            return await asyncio.to_thread(
                self._sample_acu_seconds_sync,
                started_at,
                ended_at,
                writer_instance_id,
                cluster_level,
            )
        except Exception:
            return None

    def _sample_acu_seconds_sync(
        self,
        started_at: datetime,
        ended_at: datetime,
        writer_instance_id: str | None,
        cluster_level: bool,
    ) -> Decimal | None:
        window_start, window_end = acu_sampling_window(started_at, ended_at)
        session = self._session()
        if cluster_level:
            dimensions = [{"Name": "DBClusterIdentifier", "Value": self.cluster_id}]
        else:
            writer = writer_instance_id
            if not writer:
                _, instance = self._topology_sync(session)
                writer = str(instance.get("DBInstanceIdentifier") or "")
            if not writer:
                return None
            dimensions = [{"Name": "DBInstanceIdentifier", "Value": writer}]
        response = session.client("cloudwatch").get_metric_statistics(
            Namespace="AWS/RDS",
            MetricName="ServerlessDatabaseCapacity",
            Dimensions=dimensions,
            StartTime=window_start,
            EndTime=window_end,
            Period=int(ACU_SAMPLE_PERIOD_SECONDS),
            # `SampleCount` is asked for because Aurora publishes this metric
            # once per second, so it is the number of seconds a bucket actually
            # observed and it is what makes the partial first and last buckets
            # integrable rather than rounded up to a full minute.
            Statistics=["Average", "Maximum", "SampleCount"],
        )
        return integrate_acu_seconds(
            response.get("Datapoints", []),
            statistic="Average",
            period_seconds=ACU_SAMPLE_PERIOD_SECONDS,
        )

    async def connection_material(self) -> ConnectionMaterial:
        self._require()
        try:
            return await asyncio.to_thread(self._connection_material_sync)
        except TargetConfigurationError:
            raise
        except (BotoCoreError, ClientError, KeyError, TypeError, ValueError) as exc:
            raise TargetConfigurationError("Aurora credential validation failed") from exc

    def _connection_material_sync(self) -> ConnectionMaterial:
        session = self._session()
        cluster, _ = self._topology_sync(session)
        cluster_secret_arn = str(
            (cluster.get("MasterUserSecret") or {}).get("SecretArn") or ""
        )
        if not cluster_secret_arn or cluster_secret_arn != self.secret_arn:
            raise TargetConfigurationError(
                "AURORA_SECRET_ARN is not the master secret attached to AURORA_CLUSTER_ID"
            )
        _assert_arn_binding(
            self.secret_arn,
            self.region,
            self.expected_account_id,
            "Aurora secret ARN",
        )
        response = session.client("secretsmanager").get_secret_value(SecretId=self.secret_arn)
        response_arn = str(response.get("ARN") or self.secret_arn)
        if response_arn != self.secret_arn:
            raise TargetConfigurationError("Secrets Manager returned a different secret")
        payload = json.loads(response["SecretString"])
        required = ["username", "password"]
        if any(not payload.get(key) for key in required):
            raise TargetConfigurationError("Aurora secret is missing username or password")
        cluster_host = str(cluster.get("Endpoint") or "").rstrip(".").lower()
        if not cluster_host:
            raise TargetConfigurationError("Validated Aurora cluster has no writer endpoint")
        secret_host = str(payload.get("host") or "").rstrip(".").lower()
        if secret_host and secret_host != cluster_host:
            raise TargetConfigurationError(
                "Aurora secret host does not match the validated cluster writer endpoint"
            )
        secret_engine = str(payload.get("engine") or "postgres").lower()
        if secret_engine not in {"postgres", "aurora-postgresql"}:
            raise TargetConfigurationError("Aurora secret is not for PostgreSQL")
        secret_cluster_id = str(payload.get("dbClusterIdentifier") or self.cluster_id)
        if secret_cluster_id != self.cluster_id:
            raise TargetConfigurationError(
                "Aurora secret identifies a different database cluster"
            )
        expected_port = int(cluster.get("Port") or 5432)
        secret_port = int(payload.get("port") or expected_port)
        if secret_port != expected_port:
            raise TargetConfigurationError("Aurora secret port does not match the cluster")
        return ConnectionMaterial(
            # Target identity always comes from the validated RDS control plane.
            # The managed secret supplies credentials only and can never redirect a lane.
            host=str(cluster["Endpoint"]),
            port=secret_port,
            database=self.database,
            user=str(payload["username"]),
            password=str(payload["password"]),
        )


_RDS_NO_SCALE_TO_ZERO_REASON = (
    "RDS PostgreSQL has no automatic connection-triggered scale-to-zero and "
    "wake state; manual stop/start is a different operator action."
)


class RdsCredentialProvider:
    """Credentials and arming verdict for an Amazon RDS for PostgreSQL lane.

    ``round_id`` decides whether this lane is raced or merely disclosed, and it
    defaults to Round 1 because Round 1 is the only round that resolves a lane
    through :class:`TargetResolver`. Every other round builds its provider
    through ``server/lifecycle.py:_round_rds_provider`` and must pass its own
    round.
    """

    def __init__(self, *, round_id: RoundId = RoundId.WAKE_IDLE_APP) -> None:
        self.round_id = round_id
        self.profile = os.environ.get("AWS_PROFILE", "")
        self.auth_mode: AwsAuthMode = "profile"
        self.region = os.environ.get("AWS_REGION", "")
        self.expected_account_id = os.environ.get("AWS_EXPECTED_ACCOUNT_ID", "")
        self.instance_id = os.environ.get("RDS_INSTANCE_ID", "")
        self.secret_arn = os.environ.get("RDS_SECRET_ARN", "")
        self.database = os.environ.get("RDS_DATABASE", "anti_demo")
        self.expected_postgres_major = os.environ.get("EXPECTED_POSTGRES_MAJOR", "17")

    def _require(self, *, credentials: bool = False) -> None:
        auth, self.region, self.expected_account_id = _required_aws_environment()
        self.auth_mode = auth.mode
        self.profile = auth.profile
        values = {
            "RDS_INSTANCE_ID": self.instance_id,
        }
        if credentials:
            values.update(
                {
                    "RDS_SECRET_ARN": self.secret_arn,
                    "RDS_DATABASE": self.database,
                }
            )
        missing = [
            name
            for name, value in values.items()
            if not value
        ]
        if missing:
            raise TargetConfigurationError(
                f"Missing live RDS configuration: {', '.join(missing)}"
            )

    async def assert_armed(
        self,
        *,
        not_before: datetime | None = None,
    ) -> dict[str, object]:
        """Report whether this lane can start at scale zero. For RDS: never.

        In a round that does not race the RDS lane there is no instance to ask,
        and asking would be the wrong shape of question anyway. "RDS PostgreSQL
        cannot scale to zero" is a property of the engine; it does not become
        more or less true because a `db.t4g.medium` happens to be running. So
        the refusal is returned from the round and the engine alone, before any
        session is built, any credential is resolved, or any `describe-*` is
        issued. That is what lets Round 1 keep showing the refusal on screen
        with no r1 RDS instance provisioned, instead of failing to arm on a
        DBInstanceNotFound.

        A *scored* round still validates against the live control plane, because
        there its lane really does get prepared, connected to and timed.
        """

        del not_before
        if not rds_lane_is_scored(self.round_id):
            return self._structural_refusal()
        self._require()
        try:
            return await asyncio.to_thread(self._qualify_sync)
        except (BotoCoreError, ClientError) as exc:
            raise TargetConfigurationError(
                "The RDS PostgreSQL control-plane capability check failed"
            ) from exc

    def _structural_refusal(self) -> dict[str, object]:
        """The verdict for a round that never races this lane.

        Deliberately carries no ``instance_class`` or ``engine_version``. Those
        are observations, and there is nothing here to have observed them from;
        emitting the configured constants would let the capacity disclosure
        stamp `basis="observed"` on a box that does not exist.
        """

        return {
            "eligible": False,
            "state": "NO_SCALE_TO_ZERO",
            "reason": _RDS_NO_SCALE_TO_ZERO_REASON,
            "engine": "postgres",
            "basis": "engine_capability",
            "round_id": str(self.round_id),
            "aws_calls": 0,
        }

    def _instance_sync(self, session: boto3.Session) -> dict[str, Any]:
        _assert_aws_identity(session, self.expected_account_id)
        response = session.client("rds").describe_db_instances(
            DBInstanceIdentifier=self.instance_id
        )
        instances = response.get("DBInstances") or []
        if len(instances) != 1:
            raise TargetConfigurationError("RDS_INSTANCE_ID did not resolve exactly once")
        instance = instances[0]
        if str(instance.get("DBInstanceIdentifier") or "") != self.instance_id:
            raise TargetConfigurationError("RDS control plane returned a different instance")
        engine = str(instance.get("Engine") or "").lower()
        if engine != "postgres":
            raise TargetConfigurationError(
                "RDS_INSTANCE_ID is not an Amazon RDS for PostgreSQL instance"
            )
        engine_version = str(instance.get("EngineVersion") or "")
        if not engine_version.startswith(f"{self.expected_postgres_major}."):
            raise TargetConfigurationError(
                "RDS PostgreSQL major version does not match EXPECTED_POSTGRES_MAJOR"
            )
        instance_arn = str(instance.get("DBInstanceArn") or "")
        _assert_arn_binding(
            instance_arn,
            self.region,
            self.expected_account_id,
            "RDS instance ARN",
        )
        status = str(instance.get("DBInstanceStatus") or "").lower()
        if status != "available":
            raise TargetNotArmedError(
                f"RDS PostgreSQL control plane is {status.upper() or 'UNKNOWN'}, not AVAILABLE"
            )
        return instance

    def _qualify_sync(self) -> dict[str, object]:
        session = boto3.Session(
            **session_arguments(self.auth_mode, self.profile, self.region)
        )
        instance = self._instance_sync(session)
        engine = str(instance.get("Engine") or "").lower()
        return {
            "eligible": False,
            "state": "NO_SCALE_TO_ZERO",
            # The same constant the structural refusal returns, so the sentence a
            # reader sees cannot depend on whether an instance happened to exist.
            "reason": _RDS_NO_SCALE_TO_ZERO_REASON,
            "db_instance_identifier": str(instance.get("DBInstanceIdentifier") or ""),
            "db_instance_status": str(instance.get("DBInstanceStatus") or "UNKNOWN").upper(),
            "engine": engine,
            "engine_version": str(instance.get("EngineVersion") or ""),
            "instance_class": str(instance.get("DBInstanceClass") or ""),
            "publicly_accessible": bool(instance.get("PubliclyAccessible", False)),
        }

    async def connection_material(self) -> ConnectionMaterial:
        self._require(credentials=True)
        try:
            return await asyncio.to_thread(self._connection_material_sync)
        except (TargetConfigurationError, TargetNotArmedError):
            raise
        except (BotoCoreError, ClientError, KeyError, TypeError, ValueError) as exc:
            raise TargetConfigurationError("RDS credential validation failed") from exc

    def _connection_material_sync(self) -> ConnectionMaterial:
        session = boto3.Session(
            **session_arguments(self.auth_mode, self.profile, self.region)
        )
        instance = self._instance_sync(session)
        instance_secret_arn = str(
            (instance.get("MasterUserSecret") or {}).get("SecretArn") or ""
        )
        if not instance_secret_arn or instance_secret_arn != self.secret_arn:
            raise TargetConfigurationError(
                "RDS_SECRET_ARN is not the master secret attached to RDS_INSTANCE_ID"
            )
        _assert_arn_binding(
            self.secret_arn,
            self.region,
            self.expected_account_id,
            "RDS secret ARN",
        )
        response = session.client("secretsmanager").get_secret_value(SecretId=self.secret_arn)
        response_arn = str(response.get("ARN") or self.secret_arn)
        if response_arn != self.secret_arn:
            raise TargetConfigurationError("Secrets Manager returned a different secret")
        payload = json.loads(response["SecretString"])
        if not payload.get("username") or not payload.get("password"):
            raise TargetConfigurationError("RDS secret is missing username or password")
        endpoint = instance.get("Endpoint") or {}
        instance_host = str(endpoint.get("Address") or "").rstrip(".").lower()
        if not instance_host:
            raise TargetConfigurationError("Validated RDS instance has no endpoint")
        secret_host = str(payload.get("host") or "").rstrip(".").lower()
        if secret_host and secret_host != instance_host:
            raise TargetConfigurationError(
                "RDS secret host does not match the validated instance endpoint"
            )
        secret_engine = str(payload.get("engine") or "postgres").lower()
        if secret_engine != "postgres":
            raise TargetConfigurationError("RDS secret is not for PostgreSQL")
        secret_instance_id = str(payload.get("dbInstanceIdentifier") or self.instance_id)
        if secret_instance_id != self.instance_id:
            raise TargetConfigurationError("RDS secret identifies a different database instance")
        expected_port = int(endpoint.get("Port") or 5432)
        secret_port = int(payload.get("port") or expected_port)
        if secret_port != expected_port:
            raise TargetConfigurationError("RDS secret port does not match the instance")
        return ConnectionMaterial(
            host=str(endpoint["Address"]),
            port=secret_port,
            database=self.database,
            user=str(payload["username"]),
            password=str(payload["password"]),
        )


@dataclass(frozen=True)
class PsycopgPreparedTarget(PreparedTarget):
    id: str
    name: str
    material: ConnectionMaterial = field(repr=False)

    async def attempt(
        self,
        nonce: str,
        expected_value: str,
        timeout_seconds: float,
    ) -> datetime | None:
        try:
            connection = await psycopg.AsyncConnection.connect(
                host=self.material.host,
                port=self.material.port,
                dbname=self.material.database,
                user=self.material.user,
                password=self.material.password,
                sslmode="require",
                application_name="lakebase-anti-demo",
                connect_timeout=max(1, int(timeout_seconds)),
            )
            async with connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(
                        # `PROBE_TABLE` is a module constant, never a caller's
                        # value, so interpolating it carries no injection surface.
                        f"""
                        INSERT INTO {PROBE_TABLE} (probe_id, expected_value, created_at)
                        VALUES (%s, %s, clock_timestamp())
                        ON CONFLICT (probe_id) DO UPDATE
                        SET expected_value = EXCLUDED.expected_value,
                            created_at = EXCLUDED.created_at
                        """,  # noqa: S608
                        (nonce, expected_value),
                    )
                await connection.commit()
                async with connection.cursor() as cursor:
                    await cursor.execute(
                        f"SELECT expected_value FROM {PROBE_TABLE} WHERE probe_id = %s",  # noqa: S608
                        (nonce,),
                    )
                    row = await cursor.fetchone()
        except psycopg.Error as exc:
            sqlstate = exc.sqlstate
            fatal_prefixes = ("0A", "28", "2F", "3D", "3F", "42", "44")
            fatal_states = {"25006", "42501"}
            if sqlstate in fatal_states or (
                sqlstate is not None and sqlstate.startswith(fatal_prefixes)
            ):
                raise FatalProbeError(
                    f"PostgreSQL configuration failed (SQLSTATE {sqlstate})."
                ) from exc
            raise RuntimeError("PostgreSQL attempt failed") from exc
        if row is None or row[0] != expected_value:
            raise FatalProbeError("The transaction completed but nonce verification failed.")
        # The `async with connection` scope has exited before this line, so this
        # is the first wall-clock instant known to be after the lane's final
        # data-plane connection close. Round 1's idle clock starts here, not at
        # later all-lane settlement or cooldown lease bookkeeping.
        return datetime.now(UTC)


@dataclass(frozen=True)
class LiveTarget:
    id: str
    name: str
    provider: CredentialProvider

    async def assert_armed(
        self,
        *,
        not_before: datetime | None = None,
    ) -> dict[str, object]:
        return await self.provider.assert_armed(not_before=not_before)

    async def prepare(self) -> PsycopgPreparedTarget:
        material = await self.provider.connection_material()
        return PsycopgPreparedTarget(id=self.id, name=self.name, material=material)


class TargetResolver:
    def resolve(self, competitor: CompetitorId) -> tuple[LiveTarget, LiveTarget]:
        lakebase = LiveTarget(
            id="lakebase",
            name="Lakebase",
            provider=LakebaseCredentialProvider(),
        )
        if competitor == CompetitorId.AURORA_SERVERLESS_V2:
            challenger = LiveTarget(
                id="competitor",
                name="Aurora Serverless v2",
                provider=AuroraCredentialProvider(),
            )
        else:
            challenger = LiveTarget(
                id="competitor",
                name="RDS PostgreSQL",
                provider=RdsCredentialProvider(),
            )
        return lakebase, challenger
