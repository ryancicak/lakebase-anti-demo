from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import re
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol, cast
from urllib.parse import quote, urlencode

import boto3
import psycopg
from botocore.exceptions import ClientError
from databricks.sdk.errors import DatabricksError, NotFound

from .aws_auth import (
    AwsAuthMode,
    session_arguments,
    validate_runtime_auth,
)
from .manifest import DemoManifest, load_manifest
from .models import CompetitorId
from .safe_change import (
    DEFAULT_CANCEL_TEARDOWN_SECONDS,
    ArtifactInspection,
    SafeChangeAdapter,
    SafeChangeEngine,
    SafeChangeError,
    SafeChangeOwnershipScope,
    SafeChangePlan,
    SafeChangeProvider,
    SafeChangeSqlConnection,
    UnsafeCleanupError,
    deterministic_artifact_id,
)

LOGGER = logging.getLogger(__name__)


class SafeChangeLiveConfigurationError(SafeChangeError):
    """A live Round 2 target is missing or does not match its manifest binding."""


class SafeChangeControlPlaneError(SafeChangeError):
    """A bounded live control-plane operation failed."""


class ControlPlaneCommandError(SafeChangeControlPlaneError):
    def __init__(self, message: str, *, not_found: bool = False) -> None:
        self.not_found = not_found
        super().__init__(message)


class DataPlaneConnectionRefusedError(SafeChangeControlPlaneError):
    """A data-plane connection was refused, named by SQLSTATE and not by DSN.

    psycopg spells a connect refusal as ``connection to server at "<host>"
    (<ip>), port <port> failed: FATAL: ... user "<role>"``. That sentence is the
    only place in Round 2 where a third-party message reaches
    ``SafeChangeLaneResult.error``, which is quoted verbatim onto the SSE lane
    update, the bout record and the receipt -- none of which pass through
    ``manager._message_is_ours_to_quote``. So the endpoint hostname, a routable
    address and the login role would reach a screen an audience may be watching.

    The SQLSTATE is kept because it is the actionable half and the DSN is not:
    ``28P01`` is a bad credential, ``3D000`` a missing database, ``08*`` a
    transport fault worth retrying. Same trade ``lifecycle`` makes for the setup
    connection and ``targets`` for the probe, both of which already answer a
    non-retryable ``OperationalError`` with ``(SQLSTATE ...)`` and nothing else.
    """

    def __init__(self, label: str, sqlstate: str | None) -> None:
        self.sqlstate = sqlstate
        super().__init__(
            f"{label} data-plane connection was refused "
            f"(SQLSTATE {sqlstate or 'unreported'})"
        )


@dataclass(frozen=True)
class DatabaseCredentials:
    user: str
    password: str = field(repr=False)


@dataclass(frozen=True)
class DatabaseTarget:
    host: str
    port: int
    database: str
    credentials: DatabaseCredentials = field(repr=False)


class DatabricksRunner(Protocol):
    """One bounded control-plane request, addressed by REST method and path.

    Shaped as REST rather than as an argv list on purpose. The argv version of
    this protocol is what put a ``databricks`` binary on Round 2's and Round 3's
    critical path, and inside the Databricks Apps container that binary is
    whatever the base image happens to ship: the app declares only
    ``databricks-sdk``, nothing installs a CLI, and an image CLI predating the
    ``postgres`` command group rejects the command outright. The readiness gate
    runs Round 2's cleanup before it will arm anything, so a container detail
    nobody chose refused all six rounds.
    """

    async def json(
        self,
        method: str,
        path: str,
        *,
        body: Mapping[str, Any] | None = None,
        timeout_seconds: float,
    ) -> Mapping[str, Any]: ...

    async def run(
        self,
        method: str,
        path: str,
        *,
        body: Mapping[str, Any] | None = None,
        timeout_seconds: float,
    ) -> None: ...


def _control_plane_failure(error: DatabricksError) -> ControlPlaneCommandError:
    """Classify a control-plane refusal without repeating what it said.

    The message stays generic here and the refusal is chained, which is the
    division of labour this codebase settled for arm refusals: the classifier
    decides what happened, and `manager.operator_diagnosis` decides how much of
    the sentence may reach a screen. A `DatabricksError` message is the workspace
    answering a question about this app's own authorization, so it survives that
    filter -- which is the whole point, because the previous runner discarded it
    and left "Databricks control-plane command failed" as the only evidence.
    """

    if isinstance(error, NotFound):
        return ControlPlaneCommandError(
            "Databricks control-plane resource was not found", not_found=True
        )
    return ControlPlaneCommandError("Databricks control-plane request was refused")


class DatabricksRestRunner:
    """The Lakebase control plane over the SDK that is actually installed.

    ``pipeline_power.workspace_api`` is reused rather than re-implemented, and
    that reuse is load-bearing: it is already the app's REST accessor for
    ``/api/2.0/...``, already documented as existing because the Apps runtime has
    no ``databricks`` binary and no profile to name, and a rename or a signature
    change on it now breaks this import instead of leaving two accessors to drift
    apart. That is the same bargain `_coordination_runtime_grants` takes with the
    table names it imports.

    A ``WorkspaceClient`` is built lazily and once. Eagerly would move credential
    resolution into ``build_safe_change_engine``, which cleanup paths call on a
    manifest that may already be failing for unrelated reasons.
    """

    def __init__(
        self,
        *,
        profile: str = "",
        workspace_client: Any | None = None,
    ) -> None:
        self._profile = profile
        self._workspace = workspace_client
        self._api: Callable[..., dict[str, Any]] | None = None
        self._lock = asyncio.Lock()

    def _build_workspace(self) -> Any:
        from databricks.sdk import WorkspaceClient

        return WorkspaceClient(profile=self._profile) if self._profile else WorkspaceClient()

    async def _accessor(self) -> Callable[..., dict[str, Any]]:
        if self._api is not None:
            return self._api
        async with self._lock:
            if self._api is None:
                from .pipeline_power import workspace_api

                if self._workspace is None:
                    self._workspace = await asyncio.to_thread(self._build_workspace)
                self._api = workspace_api(self._workspace)
        return self._api

    async def _request(
        self,
        method: str,
        path: str,
        *,
        body: Mapping[str, Any] | None,
        timeout_seconds: float,
    ) -> Mapping[str, Any]:
        api = await self._accessor()
        payload = dict(body) if body is not None else None
        try:
            async with asyncio.timeout(timeout_seconds):
                # The in-flight HTTP request is not interrupted by this deadline:
                # `to_thread` cannot cancel a thread. That is a smaller exposure
                # than the killed subprocess it replaces, because every mutation
                # below is an asynchronous control-plane operation whose outcome
                # the adapter proves by polling the resource rather than by the
                # request returning.
                result = await asyncio.to_thread(api, self._profile, method, path, body=payload)
        except TimeoutError as exc:
            raise ControlPlaneCommandError(
                "Databricks control-plane request timed out"
            ) from exc
        except DatabricksError as exc:
            raise _control_plane_failure(exc) from exc
        if not isinstance(result, Mapping):
            raise ControlPlaneCommandError(
                "Databricks control-plane request returned an unexpected JSON shape"
            )
        return result

    async def json(
        self,
        method: str,
        path: str,
        *,
        body: Mapping[str, Any] | None = None,
        timeout_seconds: float,
    ) -> Mapping[str, Any]:
        return await self._request(method, path, body=body, timeout_seconds=timeout_seconds)

    async def run(
        self,
        method: str,
        path: str,
        *,
        body: Mapping[str, Any] | None = None,
        timeout_seconds: float,
    ) -> None:
        await self._request(method, path, body=body, timeout_seconds=timeout_seconds)


class PsycopgSafeChangeConnection(SafeChangeSqlConnection):
    """The minimal coordinator contract over one fresh psycopg connection."""

    def __init__(self, connection: Any) -> None:
        self._connection = connection

    async def execute(
        self,
        statement: str,
        parameters: Sequence[object] = (),
    ) -> None:
        async with self._connection.cursor() as cursor:
            await cursor.execute(statement, parameters)

    async def fetch_one(
        self,
        statement: str,
        parameters: Sequence[object] = (),
    ) -> Sequence[object] | None:
        async with self._connection.cursor() as cursor:
            await cursor.execute(statement, parameters)
            row = await cursor.fetchone()
        return row

    async def commit(self) -> None:
        await self._connection.commit()

    async def rollback(self) -> None:
        await self._connection.rollback()

    async def close(self) -> None:
        await self._connection.close()


PsycopgConnector = Callable[..., Awaitable[Any]]
Sleeper = Callable[[float], Awaitable[None]]


async def _fresh_tls_connection(
    target: DatabaseTarget,
    *,
    connector: PsycopgConnector,
    connect_timeout_seconds: float,
    label: str,
) -> PsycopgSafeChangeConnection:
    """One fresh TLS connection, or a refusal that names a SQLSTATE and no DSN.

    Both adapters connect through here -- Lakebase with no retry wrapper of its
    own -- so this is the one place that has to hold the line described on
    `DataPlaneConnectionRefusedError`. Callers that need to tell a transport
    fault from a credential fault read ``.sqlstate`` off the wrapper rather than
    re-inspecting a psycopg exception.
    """

    try:
        connection = await connector(
            host=target.host,
            port=target.port,
            dbname=target.database,
            user=target.credentials.user,
            password=target.credentials.password,
            sslmode="require",
            application_name="lakebase-anti-demo-round-2",
            connect_timeout=max(1, math.ceil(connect_timeout_seconds)),
        )
    except psycopg.OperationalError as exc:
        raise DataPlaneConnectionRefusedError(label, exc.sqlstate) from exc
    return PsycopgSafeChangeConnection(connection)


_LAKEBASE_ENDPOINT = re.compile(
    r"^projects/(?P<project>[a-z][a-z0-9-]{0,62})/branches/"
    r"(?P<branch>[a-z][a-z0-9-]{0,62})/endpoints/"
    r"(?P<endpoint>[a-z][a-z0-9-]{0,62})$"
)
_LAKEBASE_HOST = re.compile(
    r"^[^.]+\.database\.(?P<region>[a-z0-9-]+)\.cloud\.databricks\.com$",
    re.IGNORECASE,
)


def _lakebase_endpoint_parts(endpoint: str) -> tuple[str, str, str]:
    match = _LAKEBASE_ENDPOINT.fullmatch(endpoint)
    if match is None:
        raise SafeChangeLiveConfigurationError(
            "Lakebase source must be one exact Autoscaling endpoint resource name"
        )
    project = match.group("project")
    branch = match.group("branch")
    return f"projects/{project}", f"projects/{project}/branches/{branch}", match.group("endpoint")


def _lakebase_region(host: str) -> str:
    match = _LAKEBASE_HOST.fullmatch(host.strip().rstrip("."))
    if match is None:
        raise SafeChangeLiveConfigurationError(
            "Lakebase control plane returned an unrecognized endpoint host"
        )
    return match.group("region").lower()


def _lakebase_endpoint_type(endpoint: Mapping[str, Any]) -> str:
    spec = endpoint.get("spec") or {}
    status = endpoint.get("status") or {}
    spec_value = str(spec.get("endpoint_type") or "") if isinstance(spec, Mapping) else ""
    status_value = (
        str(status.get("endpoint_type") or "") if isinstance(status, Mapping) else ""
    )
    if spec_value and status_value and spec_value != status_value:
        raise SafeChangeLiveConfigurationError(
            "Lakebase endpoint type disagrees between spec and status"
        )
    return spec_value or status_value


#: Every Lakebase resource is addressed by its own resource name under this
#: prefix, which is why one accessor covers projects, branches and endpoints.
LAKEBASE_API_ROOT = "/api/2.0/postgres"
#: SCIM, not `/api/2.0/postgres`, and the only non-Lakebase read the lanes make.
CURRENT_USER_PATH = "/api/2.0/preview/scim/v2/Me"


def lakebase_resource_path(name: str) -> str:
    """The REST path for one Lakebase resource name.

    ``safe='/'`` because the slashes in ``projects/p/branches/b/endpoints/e``
    are path structure rather than data: the control plane addresses the nested
    resource, not a single opaque segment.
    """

    return f"{LAKEBASE_API_ROOT}/{quote(name, safe='/')}"


def _lakebase_create_path(parent: str, collection: str, id_parameter: str, value: str) -> str:
    """The create path for a child of ``parent``, with its ID as a query value.

    The ID is a query parameter rather than a path segment because that is what
    the control plane accepts: the resource name it will occupy does not exist
    yet, so the request addresses the collection and names the child beside it.
    """

    return f"{lakebase_resource_path(parent)}/{collection}?{urlencode({id_parameter: value})}"


def _lakebase_owner_endpoint_id(plan: SafeChangePlan) -> str:
    del plan
    # Autoscaling branches currently create their canonical read-write
    # `primary` endpoint with the branch. The deterministic branch name and
    # immutable source lineage are the ownership boundary; using that native
    # endpoint avoids trying to create a second read-write endpoint.
    return "primary"


@dataclass(frozen=True)
class LakebaseSafeChangeConfig:
    profile: str
    source_endpoint: str
    database: str
    user: str
    expected_region: str
    control_timeout_seconds: float = 900.0
    poll_timeout_seconds: float = 900.0
    poll_interval_seconds: float = 5.0
    connect_timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        missing = [
            name
            for name, value in {
                "Lakebase source endpoint": self.source_endpoint,
                "Lakebase database": self.database,
                "Lakebase expected region": self.expected_region,
            }.items()
            if not str(value).strip()
        ]
        if missing:
            raise SafeChangeLiveConfigurationError(
                "Missing live Lakebase binding: " + ", ".join(missing)
            )
        _lakebase_endpoint_parts(self.source_endpoint)
        if (
            min(
                self.control_timeout_seconds,
                self.poll_timeout_seconds,
                self.poll_interval_seconds,
                self.connect_timeout_seconds,
            )
            <= 0
        ):
            raise SafeChangeLiveConfigurationError("Lakebase live timeouts must be positive")


class LakebaseSafeChangeAdapter(SafeChangeAdapter):
    provider = SafeChangeProvider.LAKEBASE
    name = "Lakebase"

    def __init__(
        self,
        config: LakebaseSafeChangeConfig,
        *,
        runner: DatabricksRunner | None = None,
        connector: PsycopgConnector = psycopg.AsyncConnection.connect,
        sleep: Sleeper = asyncio.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.config = config
        self.source_id = config.source_endpoint
        self._runner = runner or DatabricksRestRunner(profile=config.profile)
        self._connector = connector
        self._sleep = sleep
        self._clock = clock
        self._project, self._source_branch, _ = _lakebase_endpoint_parts(self.source_id)

    def _assert_plan(self, plan: SafeChangePlan) -> None:
        if plan.provider != self.provider or plan.source_id != self.source_id:
            raise SafeChangeLiveConfigurationError("Lakebase safe-change plan binding changed")
        expected = deterministic_artifact_id(plan.scope.run_id, self.provider)
        if plan.artifact_id != expected:
            raise SafeChangeLiveConfigurationError(
                "Lakebase safe-change artifact ID is not deterministic"
            )

    async def _get(self, name: str) -> Mapping[str, Any]:
        """One Lakebase resource by its own resource name."""

        return await self._runner.json(
            "GET",
            lakebase_resource_path(name),
            timeout_seconds=self.config.control_timeout_seconds,
        )

    async def _get_or_none(self, name: str) -> Mapping[str, Any] | None:
        try:
            return await self._get(name)
        except ControlPlaneCommandError as exc:
            if exc.not_found:
                return None
            raise

    @staticmethod
    def _resource_name(resource: Mapping[str, Any], expected: str, label: str) -> None:
        actual = str(resource.get("name") or "")
        if actual != expected:
            raise SafeChangeLiveConfigurationError(
                f"Lakebase control plane returned a different {label}"
            )

    async def _source_endpoint(self) -> Mapping[str, Any]:
        endpoint = await self._get(self.source_id)
        self._resource_name(endpoint, self.source_id, "source endpoint")
        status = endpoint.get("status") or {}
        if not isinstance(status, Mapping):
            raise SafeChangeLiveConfigurationError("Lakebase endpoint status is malformed")
        host = str((status.get("hosts") or {}).get("host") or "")
        actual_region = _lakebase_region(host)
        if actual_region != self.config.expected_region:
            raise SafeChangeLiveConfigurationError(
                f"Lakebase endpoint is in {actual_region}, expected {self.config.expected_region}"
            )
        endpoint_type = _lakebase_endpoint_type(endpoint)
        if endpoint_type != "ENDPOINT_TYPE_READ_WRITE":
            raise SafeChangeLiveConfigurationError("Lakebase source endpoint is not read-write")
        state = str(status.get("current_state") or "").upper()
        if state not in {"ACTIVE", "IDLE"}:
            raise SafeChangeLiveConfigurationError(
                f"Lakebase source endpoint is {state or 'UNKNOWN'}, not ready"
            )
        if bool(status.get("disabled", False)):
            raise SafeChangeLiveConfigurationError("Lakebase source endpoint is disabled")
        return endpoint

    async def preflight(self, plan: SafeChangePlan) -> Mapping[str, object]:
        self._assert_plan(plan)
        project = await self._get(self._project)
        self._resource_name(project, self._project, "project")
        branch = await self._get(self._source_branch)
        self._resource_name(branch, self._source_branch, "source branch")
        branch_state = str((branch.get("status") or {}).get("current_state") or "").upper()
        if branch_state != "READY":
            raise SafeChangeLiveConfigurationError(
                f"Lakebase source branch is {branch_state or 'UNKNOWN'}, not READY"
            )
        endpoint = await self._source_endpoint()
        status = endpoint.get("status") or {}
        return {
            "capability": "native_branch",
            "source_branch": self._source_branch,
            "endpoint_state": str(status.get("current_state") or "").upper(),
            "region": self.config.expected_region,
        }

    async def inspect_artifact(
        self,
        plan: SafeChangePlan,
    ) -> ArtifactInspection | None:
        self._assert_plan(plan)
        branch_name = f"{self._project}/branches/{plan.artifact_id}"
        branch = await self._get_or_none(branch_name)
        if branch is None:
            return None
        self._resource_name(branch, branch_name, "isolated branch")
        status = branch.get("status") or {}
        spec = branch.get("spec") or {}
        source_branch = str(
            (status.get("source_branch") if isinstance(status, Mapping) else "")
            or (spec.get("source_branch") if isinstance(spec, Mapping) else "")
            or ""
        )
        endpoint_id = _lakebase_owner_endpoint_id(plan)
        endpoint_name = f"{branch_name}/endpoints/{endpoint_id}"
        endpoint = await self._get_or_none(endpoint_name)
        marker_valid = False
        endpoint_state = "ABSENT"
        if endpoint is not None:
            self._resource_name(endpoint, endpoint_name, "ownership endpoint")
            endpoint_status = endpoint.get("status") or {}
            endpoint_state = str(
                endpoint_status.get("current_state") if isinstance(endpoint_status, Mapping) else ""
            ).upper()
            marker_valid = (
                _lakebase_endpoint_type(endpoint) == "ENDPOINT_TYPE_READ_WRITE"
            )
        # The parent project is itself manifest-owned. Exact branch name plus
        # immutable source lineage is therefore sufficient for safe recovery
        # if the process dies between branch and endpoint creation. The hashed
        # endpoint remains additional ownership evidence for normal runs.
        owned = source_branch == self._source_branch
        branch_state = str(
            status.get("current_state") if isinstance(status, Mapping) else ""
        ).upper()
        return ArtifactInspection(
            artifact_id=plan.artifact_id,
            provider=self.provider,
            source_id=plan.source_id if owned else source_branch,
            run_id=(
                plan.scope.run_id if plan.artifact_id == f"safe-change-{plan.scope.run_id}" else ""
            ),
            owner=plan.scope.owner if owned else "",
            state=f"{branch_state or 'UNKNOWN'}/{endpoint_state or 'UNKNOWN'}",
            metadata={
                "branch_name": branch_name,
                "source_branch": source_branch,
                "ownership_endpoint": endpoint_name,
                "ownership_marker_valid": marker_valid,
            },
        )

    async def _wait_ready(self, plan: SafeChangePlan) -> ArtifactInspection:
        deadline = self._clock() + self.config.poll_timeout_seconds
        last_state = "UNKNOWN"
        while True:
            artifact = await self.inspect_artifact(plan)
            if artifact is None:
                raise SafeChangeControlPlaneError(
                    "Lakebase isolated branch disappeared while it was being created"
                )
            last_state = artifact.state
            if artifact.state in {"READY/ACTIVE", "READY/IDLE"}:
                return artifact
            if any(state in artifact.state for state in ("DEGRADED", "DELETED", "ARCHIVED")):
                raise SafeChangeControlPlaneError(
                    f"Lakebase isolated environment entered {artifact.state}"
                )
            if self._clock() >= deadline:
                raise SafeChangeControlPlaneError(
                    f"Lakebase isolated environment did not become ready ({last_state})"
                )
            await self._sleep(self.config.poll_interval_seconds)

    async def _wait_branch_present(self, branch_name: str) -> Mapping[str, Any]:
        """Wait for a requested branch to exist, before anything is created under it.

        The CLI's `--timeout` used to do this: `create-branch` did not return
        until the branch was READY, so the `get-endpoint` probe below could tell
        "the native primary endpoint is not there" from "the branch is not there
        yet". A REST create returns an operation immediately, so without this the
        probe would 404 on a branch mid-creation and the lane would try to create
        a second read-write endpoint on it.

        Presence, not readiness. `_wait_ready` is still the one waiter that
        decides a lane may be used, and duplicating its state machine here would
        give two places to disagree about what READY means.
        """

        deadline = self._clock() + self.config.poll_timeout_seconds
        while True:
            branch = await self._get_or_none(branch_name)
            if branch is not None:
                return branch
            if self._clock() >= deadline:
                raise SafeChangeControlPlaneError(
                    "Lakebase isolated branch never appeared after it was requested"
                )
            await self._sleep(self.config.poll_interval_seconds)

    async def create_isolated(self, plan: SafeChangePlan, report) -> ArtifactInspection:
        self._assert_plan(plan)
        branch_name = f"{self._project}/branches/{plan.artifact_id}"
        await self._runner.json(
            "POST",
            _lakebase_create_path(self._project, "branches", "branch_id", plan.artifact_id),
            body={"spec": {"source_branch": self._source_branch, "no_expiry": True}},
            timeout_seconds=self.config.control_timeout_seconds,
        )
        await report("Lakebase branch created from production")
        await self._wait_branch_present(branch_name)
        endpoint_id = _lakebase_owner_endpoint_id(plan)
        endpoint_name = f"{branch_name}/endpoints/{endpoint_id}"
        endpoint = await self._get_or_none(endpoint_name)
        if endpoint is None:
            await self._runner.json(
                "POST",
                _lakebase_create_path(branch_name, "endpoints", "endpoint_id", endpoint_id),
                body={
                    "spec": {
                        "endpoint_type": "ENDPOINT_TYPE_READ_WRITE",
                        "autoscaling_limit_min_cu": 0.5,
                        "autoscaling_limit_max_cu": 2.0,
                    }
                },
                timeout_seconds=self.config.control_timeout_seconds,
            )
        else:
            self._resource_name(endpoint, endpoint_name, "isolated endpoint")
        await report(
            "Waiting for the Lakebase branch and endpoint to become ready",
            f"GET {LAKEBASE_API_ROOT}/<branch> + <endpoint>",
        )
        return await self._wait_ready(plan)

    async def _credentials(self, endpoint_name: str) -> DatabaseCredentials:
        credential = await self._runner.json(
            "POST",
            f"{LAKEBASE_API_ROOT}/credentials",
            body={"endpoint": endpoint_name},
            timeout_seconds=self.config.control_timeout_seconds,
        )
        token = str(credential.get("token") or "")
        user = self.config.user
        if not user:
            current_user = await self._runner.json(
                "GET",
                CURRENT_USER_PATH,
                timeout_seconds=self.config.control_timeout_seconds,
            )
            user = str(current_user.get("userName") or "")
        if not token or not user:
            raise SafeChangeLiveConfigurationError(
                "Lakebase OAuth credential or database user is missing"
            )
        return DatabaseCredentials(user=user, password=token)

    async def _connect_endpoint(self, endpoint_name: str) -> SafeChangeSqlConnection:
        endpoint = await self._get(endpoint_name)
        self._resource_name(endpoint, endpoint_name, "connection endpoint")
        status = endpoint.get("status") or {}
        host = str((status.get("hosts") or {}).get("host") or "")
        actual_region = _lakebase_region(host)
        if actual_region != self.config.expected_region:
            raise SafeChangeLiveConfigurationError(
                f"Lakebase endpoint is in {actual_region}, expected {self.config.expected_region}"
            )
        credentials = await self._credentials(endpoint_name)
        return await _fresh_tls_connection(
            DatabaseTarget(
                host=host,
                port=5432,
                database=self.config.database,
                credentials=credentials,
            ),
            connector=self._connector,
            connect_timeout_seconds=self.config.connect_timeout_seconds,
            label=self.name,
        )

    async def connect_source(self, plan: SafeChangePlan) -> SafeChangeSqlConnection:
        self._assert_plan(plan)
        await self._source_endpoint()
        return await self._connect_endpoint(self.source_id)

    async def connect_isolated(
        self,
        plan: SafeChangePlan,
        artifact: ArtifactInspection,
    ) -> SafeChangeSqlConnection:
        self._assert_owned_artifact(plan, artifact)
        if artifact.metadata.get("ownership_marker_valid") is not True:
            raise SafeChangeLiveConfigurationError(
                "Lakebase isolated branch has no verified read-write endpoint"
            )
        endpoint_name = str(artifact.metadata.get("ownership_endpoint") or "")
        return await self._connect_endpoint(endpoint_name)

    def _assert_owned_artifact(
        self,
        plan: SafeChangePlan,
        artifact: ArtifactInspection,
    ) -> None:
        self._assert_plan(plan)
        expected_endpoint = (
            f"{self._project}/branches/{plan.artifact_id}/endpoints/"
            f"{_lakebase_owner_endpoint_id(plan)}"
        )
        if (
            artifact.artifact_id != plan.artifact_id
            or artifact.provider != self.provider
            or artifact.source_id != plan.source_id
            or artifact.run_id != plan.scope.run_id
            or artifact.owner != plan.scope.owner
            or artifact.metadata.get("source_branch") != self._source_branch
            or artifact.metadata.get("ownership_endpoint") != expected_endpoint
        ):
            raise UnsafeCleanupError(f"Lakebase artifact ownership mismatch for {plan.artifact_id}")

    async def delete_isolated(
        self,
        plan: SafeChangePlan,
        artifact: ArtifactInspection,
        report,
    ) -> None:
        self._assert_owned_artifact(plan, artifact)
        current = await self.inspect_artifact(plan)
        if current is None:
            return
        self._assert_owned_artifact(plan, current)
        branch_name = f"{self._project}/branches/{plan.artifact_id}"
        await self._runner.run(
            "DELETE",
            lakebase_resource_path(branch_name),
            timeout_seconds=self.config.control_timeout_seconds,
        )
        await report(
            "Waiting for the owned Lakebase branch deletion",
            f"GET {LAKEBASE_API_ROOT}/<branch>",
        )
        deadline = self._clock() + self.config.poll_timeout_seconds
        while await self.inspect_artifact(plan) is not None:
            if self._clock() >= deadline:
                raise SafeChangeControlPlaneError(
                    "Lakebase isolated branch still exists after deletion"
                )
            await self._sleep(self.config.poll_interval_seconds)

    async def abandon_isolated(self, plan: SafeChangePlan) -> None:
        """Issue branch deletion for a cancelled lane without waiting for it.

        The same DELETE ``delete_isolated`` issues, without the polling that
        follows it there: the request is what matters and absence is somebody
        else's job. There is no longer a CLI waiter to talk out of waiting -- a
        REST delete returns its operation immediately -- so the only difference
        between the two paths is the deadline and who proves the outcome.

        A branch that is already gone is the goal state. Anything else is left
        to the caller, which logs it against the branch name.
        """

        self._assert_plan(plan)
        branch_name = f"{self._project}/branches/{plan.artifact_id}"
        try:
            await self._runner.run(
                "DELETE",
                lakebase_resource_path(branch_name),
                timeout_seconds=DEFAULT_CANCEL_TEARDOWN_SECONDS,
            )
        except ControlPlaneCommandError as exc:
            if not exc.not_found:
                raise
            LOGGER.info(
                "Cancelled lane teardown: %s is already absent",
                branch_name,
            )
            return
        LOGGER.warning(
            "Cancelled lane teardown: issued delete-branch for %s",
            branch_name,
        )


_AWS_ACCOUNT = re.compile(r"^[0-9]{12}$")
_AWS_REGION = re.compile(r"^[a-z]{2}(?:-gov)?-[a-z]+-[0-9]+$")
_AWS_ID = re.compile(r"^[a-z](?:[a-z0-9-]{0,61}[a-z0-9])?$")
_TAG_RUN_ID = "anti-demo-run-id"
_TAG_OWNER = "owner"
_TAG_SOURCE = "safe-change-source-id"
_TAG_PROVIDER = "safe-change-provider"
_TAG_MANAGED_BY = "managed-by"
_SAFE_CHANGE_MANAGER = "lakebase-anti-demo-round-2"

# RDS serves client connections throughout `backing-up`. A point-in-time
# restore always runs a mandatory first automated snapshot immediately after
# the engine starts, and that snapshot was measured at over nine minutes when
# this fleet ran db.t4g.micro. The fleet now runs db.t4g.medium, which has not
# been re-measured; the nine minutes is kept as the conservative figure because
# a larger instance is not expected to snapshot more slowly. Either way, waiting
# for `available` puts it on the critical path even though the database is
# already reachable, so both readiness states count as ready. `_connect_target`
# still retries transient connection errors, which is what makes accepting the
# earlier state safe.
_RDS_SERVING_STATES = frozenset({"available", "backing-up"})
_RDS_TERMINAL_STATES = frozenset(
    {"failed", "incompatible-restore", "inaccessible-encryption-credentials"}
)

# `DEFAULT_POLL_TIMEOUT_SECONDS` bounds a single control-plane wait;
# `DEFAULT_RUN_TIMEOUT_SECONDS` bounds an entire lane from the bell. The run
# budget must stay comfortably above the poll budget, otherwise raising the
# poll budget alone just relocates the failure to the lane deadline. Rounds 2
# and 3 share both values.
DEFAULT_POLL_TIMEOUT_SECONDS = 900.0
DEFAULT_RUN_TIMEOUT_SECONDS = 1080.0


def _aws_child_id(artifact_id: str, suffix: str) -> str:
    candidate = f"{artifact_id}-{suffix}"
    if len(candidate) <= 63:
        return candidate
    digest = hashlib.sha256(candidate.encode("utf-8")).hexdigest()[:10]
    return f"{artifact_id[: 63 - len(digest) - 1]}-{digest}".rstrip("-")


def _arn_parts(arn: str) -> tuple[str, str, str, str, str, str]:
    parts = arn.split(":", 5)
    if len(parts) != 6 or parts[0] != "arn":
        raise SafeChangeLiveConfigurationError("AWS control plane returned a malformed ARN")
    return tuple(parts)  # type: ignore[return-value]


def _assert_arn(
    arn: str,
    *,
    service: str,
    region: str,
    account_id: str,
    resource_prefix: str,
    resource_id: str | None = None,
) -> None:
    _, _, actual_service, actual_region, actual_account, resource = _arn_parts(arn)
    expected_resource = f"{resource_prefix}{resource_id}" if resource_id else None
    if (
        actual_service != service
        or actual_region != region
        or actual_account != account_id
        or not resource.startswith(resource_prefix)
        or (expected_resource is not None and resource != expected_resource)
    ):
        raise SafeChangeLiveConfigurationError(
            f"AWS resource is not bound to account {account_id} and region {region}"
        )


def _client_error_code(exc: BaseException) -> str:
    if isinstance(exc, ClientError):
        return str(exc.response.get("Error", {}).get("Code") or "")
    return str(getattr(exc, "code", "") or "")


def _is_missing(exc: BaseException, *codes: str) -> bool:
    code = _client_error_code(exc).lower()
    return code in {value.lower() for value in codes}


#: Absent already. Deleting something that was never created, or that a
#: concurrent teardown already removed, is the goal state, not a failure.
_ABSENT_INSTANCE_CODES = ("DBInstanceNotFound", "DBInstanceNotFoundFault")
_ABSENT_CLUSTER_CODES = ("DBClusterNotFoundFault", "DBClusterNotFound")

#: Already on its way out, or not in a state RDS will accept a delete for. The
#: first case needs nothing further; the second cannot be resolved without
#: waiting, which the cancellation path is forbidden from doing. Both are
#: tolerated so one stuck resource cannot stop the next delete from being
#: issued, and both are logged with the identifier so the orphan is findable.
_UNDELETABLE_INSTANCE_CODES = ("InvalidDBInstanceState", "InvalidDBInstanceStateFault")
_UNDELETABLE_CLUSTER_CODES = ("InvalidDBClusterStateFault", "InvalidDBClusterState")


@dataclass(frozen=True)
class AwsSafeChangeConfig:
    profile: str
    region: str
    account_id: str
    database: str
    secret_arn: str
    db_subnet_group_name: str
    security_group_id: str
    auth_mode: AwsAuthMode = "profile"
    expected_postgres_major: str = "17"
    source_seeded_at: datetime | None = None
    control_timeout_seconds: float = 120.0
    poll_timeout_seconds: float = DEFAULT_POLL_TIMEOUT_SECONDS
    poll_interval_seconds: float = 5.0
    connect_timeout_seconds: float = 30.0
    connection_ready_timeout_seconds: float = 120.0

    def __post_init__(self) -> None:
        values = {
            "AWS region": self.region,
            "AWS account": self.account_id,
            "database": self.database,
            "managed secret ARN": self.secret_arn,
            "DB subnet group": self.db_subnet_group_name,
            "security group": self.security_group_id,
            "PostgreSQL major": self.expected_postgres_major,
        }
        missing = [name for name, value in values.items() if not str(value).strip()]
        if missing:
            raise SafeChangeLiveConfigurationError(
                "Missing live AWS binding: " + ", ".join(missing)
            )
        if self.auth_mode == "profile" and not self.profile.strip():
            raise SafeChangeLiveConfigurationError("Missing live AWS binding: AWS profile")
        if _AWS_ACCOUNT.fullmatch(self.account_id) is None:
            raise SafeChangeLiveConfigurationError("AWS account ID must be exactly 12 digits")
        if _AWS_REGION.fullmatch(self.region) is None:
            raise SafeChangeLiveConfigurationError("AWS region is invalid")
        _assert_arn(
            self.secret_arn,
            service="secretsmanager",
            region=self.region,
            account_id=self.account_id,
            resource_prefix="secret:",
        )
        if (
            min(
                self.poll_timeout_seconds,
                self.poll_interval_seconds,
                self.connect_timeout_seconds,
                self.connection_ready_timeout_seconds,
                self.control_timeout_seconds,
            )
            <= 0
        ):
            raise SafeChangeLiveConfigurationError("AWS live timeouts must be positive")


@dataclass(frozen=True)
class AuroraSafeChangeConfig(AwsSafeChangeConfig):
    source_cluster_id: str = ""
    source_writer_instance_id: str = ""

    def __post_init__(self) -> None:
        super().__post_init__()
        for label, value in (
            ("Aurora source cluster", self.source_cluster_id),
            ("Aurora source writer", self.source_writer_instance_id),
        ):
            if _AWS_ID.fullmatch(value) is None:
                raise SafeChangeLiveConfigurationError(f"{label} identifier is invalid")


@dataclass(frozen=True)
class RdsSafeChangeConfig(AwsSafeChangeConfig):
    source_instance_id: str = ""

    def __post_init__(self) -> None:
        super().__post_init__()
        if _AWS_ID.fullmatch(self.source_instance_id) is None:
            raise SafeChangeLiveConfigurationError("RDS source instance identifier is invalid")


AwsSessionFactory = Callable[..., Any]


class _AwsSafeChangeAdapter:
    provider: SafeChangeProvider
    name: str
    source_id: str

    def __init__(
        self,
        config: AwsSafeChangeConfig,
        *,
        session: Any | None,
        session_factory: AwsSessionFactory,
        connector: PsycopgConnector,
        sleep: Sleeper,
        clock: Callable[[], float],
    ) -> None:
        self.config = config
        self._session = session or session_factory(
            **session_arguments(config.auth_mode, config.profile, config.region)
        )
        actual_session_region = str(getattr(self._session, "region_name", "") or "")
        if actual_session_region and actual_session_region != config.region:
            raise SafeChangeLiveConfigurationError(
                f"AWS session is in {actual_session_region}, expected {config.region}"
            )
        self._connector = connector
        self._sleep = sleep
        self._clock = clock
        self._clients: dict[str, Any] = {}
        self._credential_cache: DatabaseCredentials | None = None
        self._pending_mutations: set[asyncio.Task[Any]] = set()

    def _client(self, service: str) -> Any:
        if service not in self._clients:
            self._clients[service] = self._session.client(service)
        return self._clients[service]

    async def _call(
        self,
        service: str,
        method: str,
        *,
        mutation: bool = False,
        **arguments: Any,
    ) -> Mapping[str, Any]:
        function = getattr(self._client(service), method)
        if mutation:
            task = asyncio.create_task(asyncio.to_thread(function, **arguments))
            self._pending_mutations.add(task)
            try:
                response = await asyncio.shield(task)
            finally:
                if task.done():
                    self._pending_mutations.discard(task)
        else:
            response = await asyncio.to_thread(function, **arguments)
        if not isinstance(response, Mapping):
            raise SafeChangeControlPlaneError(
                f"AWS {service} control plane returned an unexpected response"
            )
        return response

    async def _reconcile_pending_mutations(self) -> None:
        pending = [task for task in self._pending_mutations if not task.done()]
        if not pending:
            self._pending_mutations.difference_update(
                [task for task in self._pending_mutations if task.done()]
            )
            return
        try:
            async with asyncio.timeout(self.config.control_timeout_seconds):
                await asyncio.gather(*(asyncio.shield(task) for task in pending))
        except TimeoutError as exc:
            raise SafeChangeControlPlaneError(
                "AWS mutation is still unresolved; cleanup cannot report success"
            ) from exc
        finally:
            self._pending_mutations.difference_update(
                [task for task in self._pending_mutations if task.done()]
            )

    async def settle_pending_mutations(self) -> None:
        await self._reconcile_pending_mutations()

    async def _issue_delete(
        self,
        method: str,
        *,
        identifier: str,
        absent_codes: Sequence[str],
        undeletable_codes: Sequence[str],
        **arguments: Any,
    ) -> bool:
        """Ask AWS to delete one resource and return whether it accepted.

        Deliberately does not describe first and does not wait for the resource
        to disappear. A describe would double the number of round trips inside a
        budget measured in seconds, and would turn "already gone" from a cheap
        tolerated error code into an extra call that can itself hang. Waiting for
        absence takes minutes, which is the thing a cancellation path must never
        do.

        Returns True when the delete was accepted, False when the resource was
        already absent or was in a state RDS will not delete from. Never raises
        for either of those; anything else is the caller's problem to log.
        """

        try:
            await self._call("rds", method, mutation=True, **arguments)
        except Exception as exc:
            if _is_missing(exc, *absent_codes):
                LOGGER.info(
                    "Cancelled lane teardown: %s is already absent (%s)",
                    identifier,
                    method,
                )
                return False
            if _is_missing(exc, *undeletable_codes):
                # Either already on its way out, or in a state RDS will not
                # delete from -- most often an Aurora cluster whose writer is
                # still deleting. Only a later sweep can finish that, so the
                # identifier goes in the log at a level nobody filters out.
                LOGGER.error(
                    "ORPHAN RISK: RDS refused %s for %s (%s). If it was not "
                    "already deleting it is still billing; sweep it.",
                    method,
                    identifier,
                    _client_error_code(exc) or type(exc).__name__,
                )
                return False
            raise
        LOGGER.warning(
            "Cancelled lane teardown: issued %s for %s",
            method,
            identifier,
        )
        return True

    async def _delete_when_deletable(
        self,
        method: str,
        *,
        identifier: str,
        observe: Callable[[], Awaitable[str | None]],
        absent_codes: Sequence[str],
        undeletable_codes: Sequence[str],
        description: str,
        report: Callable[..., Awaitable[None]] | None = None,
        wire_call: str | None = None,
        **arguments: Any,
    ) -> None:
        """Delete one resource, waiting out the states RDS will not delete from.

        The cancellation path (:meth:`_issue_delete`) tolerates
        ``InvalidDB*State`` because it is forbidden from waiting. This is the
        other path -- the cooldown -- and it is the one whose success decides
        whether the ring lease is released, so tolerating a refusal here would
        release the ring over a resource nobody deleted, and ``reap.py``
        deliberately will not sweep a resource whose round still holds a lease.

        RDS genuinely cannot delete a clone mid-creation, so the only correct
        move is to wait for it to become deletable and then delete it. RDS
        itself is the authority on when that is: rather than guessing at a list
        of deletable statuses, the delete is re-issued on the state fault until
        it is accepted. ``observe`` supplies the status only for the two cheap
        early exits and for the log line.

        Bounded by ``poll_timeout_seconds``, the same budget a single
        control-plane wait gets elsewhere -- and deliberately under the lane's
        own reset budget, so exhaustion surfaces here, with the identifier, and
        not as a bare lane timeout. Exhaustion raises: the towel stays
        ``failed`` and retryable, and the retry gets a fresh budget by which
        time the clone has usually left ``creating``.
        """

        deadline = self._clock() + self.config.poll_timeout_seconds
        waited = False
        status = await observe()
        while True:
            if status is None:
                return
            if status == "deleting":
                # Already on its way out. The caller waits for absence.
                return
            try:
                await self._call("rds", method, mutation=True, **arguments)
                return
            except Exception as exc:
                if _is_missing(exc, *absent_codes):
                    return
                if not _is_missing(exc, *undeletable_codes):
                    raise
                refusal: BaseException = exc
                code = _client_error_code(exc) or type(exc).__name__
            if self._clock() >= deadline:
                # The resource exists, is not deleting, and RDS still will not
                # delete it. That is an orphan until something else finishes the
                # job, and the identifier is the only way to find it.
                LOGGER.error(
                    "ORPHAN RISK: RDS still refused %s for %s after %.0fs "
                    "(%s, last observed status %s). It is still billing; sweep it.",
                    method,
                    identifier,
                    self.config.poll_timeout_seconds,
                    code,
                    status,
                )
                raise SafeChangeControlPlaneError(
                    f"Timed out waiting for {description} to become deletable: "
                    f"{identifier} is still {status} and RDS refused {method} "
                    f"({code})"
                ) from refusal
            if not waited and report is not None:
                waited = True
                await report(
                    f"Waiting for {description} to reach a state AWS will delete "
                    f"(currently {status})",
                    wire_call,
                )
            LOGGER.warning(
                # No round in the prefix: this helper is now the cooldown
                # delete for Round 2's clones as well as Round 3's, and an
                # operator greps this line during either towel.
                "Cooldown cleanup: RDS refused %s for %s (%s, status %s); "
                "retrying until it is deletable",
                method,
                identifier,
                code,
                status,
            )
            await self._sleep(self.config.poll_interval_seconds)
            status = await observe()

    async def _assert_identity(self) -> None:
        identity = await self._call("sts", "get_caller_identity")
        account = str(identity.get("Account") or "")
        if account != self.config.account_id:
            raise SafeChangeLiveConfigurationError(
                f"AWS credentials resolved to account {account or 'UNKNOWN'}, expected "
                f"{self.config.account_id}"
            )

    def _assert_plan(self, plan: SafeChangePlan) -> None:
        if plan.provider != self.provider or plan.source_id != self.source_id:
            raise SafeChangeLiveConfigurationError(f"{self.name} safe-change plan binding changed")
        expected = deterministic_artifact_id(plan.scope.run_id, self.provider)
        if plan.artifact_id != expected or _AWS_ID.fullmatch(plan.artifact_id) is None:
            raise SafeChangeLiveConfigurationError(
                f"{self.name} safe-change artifact ID is not deterministic"
            )
        if (
            plan.scope.aws_account_id != self.config.account_id
            or plan.scope.aws_region != self.config.region
        ):
            raise SafeChangeLiveConfigurationError(
                f"{self.name} plan does not match the exact AWS target"
            )

    def _tags(self, plan: SafeChangePlan) -> list[dict[str, str]]:
        return [
            {"Key": _TAG_RUN_ID, "Value": plan.scope.run_id},
            {"Key": _TAG_OWNER, "Value": plan.scope.owner},
            {"Key": _TAG_SOURCE, "Value": plan.source_id},
            {"Key": _TAG_PROVIDER, "Value": plan.provider.value},
            {"Key": _TAG_MANAGED_BY, "Value": _SAFE_CHANGE_MANAGER},
        ]

    @staticmethod
    def _tag_mapping(tags: object) -> dict[str, str]:
        if not isinstance(tags, Sequence):
            return {}
        result: dict[str, str] = {}
        for item in tags:
            if isinstance(item, Mapping):
                key = str(item.get("Key") or "")
                if key:
                    result[key] = str(item.get("Value") or "")
        return result

    async def _resource_tags(self, arn: str) -> dict[str, str]:
        response = await self._call("rds", "list_tags_for_resource", ResourceName=arn)
        return self._tag_mapping(response.get("TagList"))

    def _ownership_from_tags(
        self,
        plan: SafeChangePlan,
        tags: Mapping[str, str],
        *,
        children_owned: bool,
    ) -> tuple[str, str, str]:
        marker_valid = (
            tags.get(_TAG_PROVIDER) == plan.provider.value
            and tags.get(_TAG_MANAGED_BY) == _SAFE_CHANGE_MANAGER
            and children_owned
        )
        return (
            tags.get(_TAG_SOURCE, ""),
            tags.get(_TAG_RUN_ID, ""),
            tags.get(_TAG_OWNER, "") if marker_valid else "",
        )

    async def _secret_credentials(
        self,
        *,
        attached_secret_arn: str,
        source_host: str,
        source_port: int,
        identifier_field: str,
        accepted_engines: set[str],
    ) -> DatabaseCredentials:
        if self._credential_cache is not None:
            return self._credential_cache
        if attached_secret_arn != self.config.secret_arn:
            raise SafeChangeLiveConfigurationError(
                f"{self.name} source is not attached to the manifest-managed secret"
            )
        _assert_arn(
            attached_secret_arn,
            service="secretsmanager",
            region=self.config.region,
            account_id=self.config.account_id,
            resource_prefix="secret:",
        )
        response = await self._call(
            "secretsmanager", "get_secret_value", SecretId=self.config.secret_arn
        )
        if str(response.get("ARN") or "") != self.config.secret_arn:
            raise SafeChangeLiveConfigurationError(
                "Secrets Manager returned a different managed secret"
            )
        secret_string = response.get("SecretString")
        if not isinstance(secret_string, str):
            raise SafeChangeLiveConfigurationError("Managed database secret is not JSON text")
        try:
            payload = json.loads(secret_string)
        except json.JSONDecodeError as exc:
            raise SafeChangeLiveConfigurationError(
                "Managed database secret contains invalid JSON"
            ) from exc
        if not isinstance(payload, Mapping):
            raise SafeChangeLiveConfigurationError(
                "Managed database secret has an unexpected JSON shape"
            )
        user = str(payload.get("username") or "")
        password = str(payload.get("password") or "")
        if not user or not password:
            raise SafeChangeLiveConfigurationError(
                "Managed database secret is missing username or password"
            )
        engine = str(payload.get("engine") or "").lower()
        if engine and engine not in accepted_engines:
            raise SafeChangeLiveConfigurationError(
                "Managed database secret is not for the expected PostgreSQL engine"
            )
        secret_identifier = str(payload.get(identifier_field) or "")
        if secret_identifier and secret_identifier != self.source_id:
            raise SafeChangeLiveConfigurationError(
                "Managed database secret identifies a different source database"
            )
        secret_host = str(payload.get("host") or "").rstrip(".").lower()
        if secret_host and secret_host != source_host.rstrip(".").lower():
            raise SafeChangeLiveConfigurationError(
                "Managed database secret host does not match the source control plane"
            )
        try:
            secret_port = int(payload.get("port") or source_port)
        except (TypeError, ValueError) as exc:
            raise SafeChangeLiveConfigurationError(
                "Managed database secret port is invalid"
            ) from exc
        if secret_port != source_port:
            raise SafeChangeLiveConfigurationError(
                "Managed database secret port does not match the source control plane"
            )
        credentials = DatabaseCredentials(user=user, password=password)
        # Restores inherit the source password at creation time. Reusing the
        # credential resolved during preflight avoids a later source-secret
        # rotation silently redirecting the short-lived proof.
        self._credential_cache = credentials
        return credentials

    def _assert_latest_restorable_covers_seed(self, value: object) -> None:
        """Check the restore target is at or after the seed. Only that.

        Round 2 restores with ``UseLatestRestorableTime``, so the upper bound is
        the only one that can invalidate the proof: if ``LatestRestorableTime``
        predates the seed, the copy comes up without the schema the proof asserts
        on.  That is reachable in practice -- PITR's latest edge lags several
        minutes, so it is the normal state for a few minutes after ``antidemo reset``.

        It deliberately does not compare the seed against
        ``EarliestRestorableTime``.  With ``backup_retention_period = 1`` a demo
        seeded over a day ago has its seed moment outside the window entirely,
        and that is correct for both callers: the schema was committed to the
        source and is still present at the latest restorable point, which is
        where Round 2 lands.  Round 3 restores to a specific time rather than to
        latest, and checks that time against both bounds -- the recovery point,
        not the seed -- in ``recovery_live.wait_recovery_point``.  So an
        earliest-bound check here would guard nothing either round needs while
        failing both for the ordinary steady state of a long-lived installation.
        """

        seeded_at = self.config.source_seeded_at
        if seeded_at is None:
            return
        if not isinstance(value, datetime):
            raise SafeChangeLiveConfigurationError(
                f"{self.name} latest restorable time is unavailable"
            )
        if value < seeded_at:
            raise SafeChangeLiveConfigurationError(
                f"{self.name} latest restorable time predates the seeded proof schema"
            )

    async def _connect_target(
        self,
        *,
        host: str,
        port: int,
        credentials: DatabaseCredentials,
    ) -> SafeChangeSqlConnection:
        if not host:
            raise SafeChangeLiveConfigurationError(
                f"{self.name} control plane did not return a database endpoint"
            )
        target = DatabaseTarget(
            host=host,
            port=port,
            database=self.config.database,
            credentials=credentials,
        )
        deadline = self._clock() + self.config.connection_ready_timeout_seconds
        while True:
            try:
                return await _fresh_tls_connection(
                    target,
                    connector=self._connector,
                    connect_timeout_seconds=self.config.connect_timeout_seconds,
                    label=self.name,
                )
            except DataPlaneConnectionRefusedError as exc:
                sqlstate = exc.sqlstate
                transient = sqlstate is None or sqlstate.startswith("08") or sqlstate == "57P03"
                if not transient:
                    raise
                last_error: BaseException = exc
            except (OSError, TimeoutError) as exc:
                last_error = exc
            if self._clock() >= deadline:
                raise SafeChangeControlPlaneError(
                    f"{self.name} data-plane endpoint did not become reachable"
                ) from last_error
            await self._sleep(self.config.poll_interval_seconds)

    async def _wait_for(
        self,
        check: Callable[[], Awaitable[Any]],
        *,
        description: str,
    ) -> Any:
        deadline = self._clock() + self.config.poll_timeout_seconds
        while True:
            result = await check()
            if result is not None:
                return result
            if self._clock() >= deadline:
                raise SafeChangeControlPlaneError(f"Timed out waiting for {description}")
            await self._sleep(self.config.poll_interval_seconds)

    async def _instance_status(self, instance_id: str) -> str | None:
        """The `observe` callable `_delete_when_deletable` wants for an instance.

        ``None`` means absent, which that helper reads as "nothing left to
        delete". Both AWS subclasses supply ``_describe_instance``.
        """

        instance = await self._describe_instance(instance_id, missing_ok=True)  # type: ignore[attr-defined]
        if instance is None:
            return None
        return str(instance.get("DBInstanceStatus") or "").lower()

    def _assert_owned_artifact(
        self,
        plan: SafeChangePlan,
        artifact: ArtifactInspection,
    ) -> None:
        self._assert_plan(plan)
        if (
            artifact.artifact_id != plan.artifact_id
            or artifact.provider != plan.provider
            or artifact.source_id != plan.source_id
            or artifact.run_id != plan.scope.run_id
            or artifact.owner != plan.scope.owner
            or artifact.aws_account_id != plan.scope.aws_account_id
            or artifact.aws_region != plan.scope.aws_region
            or artifact.metadata.get("children_owned") is not True
        ):
            raise UnsafeCleanupError(
                f"{self.name} artifact ownership mismatch for {plan.artifact_id}"
            )


class AuroraSafeChangeAdapter(_AwsSafeChangeAdapter, SafeChangeAdapter):
    provider = SafeChangeProvider.AURORA
    name = "Aurora Serverless v2"

    def __init__(
        self,
        config: AuroraSafeChangeConfig,
        *,
        session: Any | None = None,
        session_factory: AwsSessionFactory = boto3.Session,
        connector: PsycopgConnector = psycopg.AsyncConnection.connect,
        sleep: Sleeper = asyncio.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.aurora_config = config
        self.source_id = config.source_cluster_id
        super().__init__(
            config,
            session=session,
            session_factory=session_factory,
            connector=connector,
            sleep=sleep,
            clock=clock,
        )

    async def _describe_cluster(
        self,
        cluster_id: str,
        *,
        missing_ok: bool = False,
    ) -> Mapping[str, Any] | None:
        try:
            response = await self._call(
                "rds", "describe_db_clusters", DBClusterIdentifier=cluster_id
            )
        except Exception as exc:
            if missing_ok and _is_missing(
                exc,
                "DBClusterNotFoundFault",
                "DBClusterNotFound",
            ):
                return None
            raise
        clusters = response.get("DBClusters") or []
        if len(clusters) != 1 or not isinstance(clusters[0], Mapping):
            raise SafeChangeLiveConfigurationError(
                "Aurora cluster identifier did not resolve exactly once"
            )
        cluster = clusters[0]
        if str(cluster.get("DBClusterIdentifier") or "") != cluster_id:
            raise SafeChangeLiveConfigurationError(
                "Aurora control plane returned a different cluster"
            )
        _assert_arn(
            str(cluster.get("DBClusterArn") or ""),
            service="rds",
            region=self.config.region,
            account_id=self.config.account_id,
            resource_prefix="cluster:",
            resource_id=cluster_id,
        )
        return cluster

    async def _cluster_status(self, cluster_id: str) -> str | None:
        cluster = await self._describe_cluster(cluster_id, missing_ok=True)
        if cluster is None:
            return None
        return str(cluster.get("Status") or "").lower()

    async def _describe_instance(
        self,
        instance_id: str,
        *,
        missing_ok: bool = False,
    ) -> Mapping[str, Any] | None:
        try:
            response = await self._call(
                "rds", "describe_db_instances", DBInstanceIdentifier=instance_id
            )
        except Exception as exc:
            if missing_ok and _is_missing(
                exc,
                "DBInstanceNotFound",
                "DBInstanceNotFoundFault",
            ):
                return None
            raise
        instances = response.get("DBInstances") or []
        if len(instances) != 1 or not isinstance(instances[0], Mapping):
            raise SafeChangeLiveConfigurationError(
                "Aurora writer identifier did not resolve exactly once"
            )
        instance = instances[0]
        if str(instance.get("DBInstanceIdentifier") or "") != instance_id:
            raise SafeChangeLiveConfigurationError(
                "Aurora control plane returned a different writer"
            )
        _assert_arn(
            str(instance.get("DBInstanceArn") or ""),
            service="rds",
            region=self.config.region,
            account_id=self.config.account_id,
            resource_prefix="db:",
            resource_id=instance_id,
        )
        return instance

    async def _source(self) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
        await self._assert_identity()
        cluster = await self._describe_cluster(self.source_id)
        assert cluster is not None
        if str(cluster.get("Engine") or "").lower() != "aurora-postgresql":
            raise SafeChangeLiveConfigurationError("Aurora source is not Aurora PostgreSQL")
        version = str(cluster.get("EngineVersion") or "")
        if not version.startswith(f"{self.config.expected_postgres_major}."):
            raise SafeChangeLiveConfigurationError("Aurora source PostgreSQL major version changed")
        if str(cluster.get("Status") or "").lower() != "available":
            raise SafeChangeLiveConfigurationError("Aurora source cluster is not available")
        if str(cluster.get("DBSubnetGroup") or "") != self.config.db_subnet_group_name:
            raise SafeChangeLiveConfigurationError(
                "Aurora source DB subnet group does not match the manifest"
            )
        groups = {
            str(group.get("VpcSecurityGroupId") or "")
            for group in cluster.get("VpcSecurityGroups") or []
            if isinstance(group, Mapping)
        }
        if groups != {self.config.security_group_id}:
            raise SafeChangeLiveConfigurationError(
                "Aurora source security group does not match the manifest"
            )
        if int(cluster.get("BackupRetentionPeriod") or 0) < 1 or not cluster.get(
            "LatestRestorableTime"
        ):
            raise SafeChangeLiveConfigurationError(
                "Aurora source does not have a usable point-in-time restore window"
            )
        self._assert_latest_restorable_covers_seed(cluster.get("LatestRestorableTime"))
        members = cluster.get("DBClusterMembers") or []
        if len(members) != 1 or not isinstance(members[0], Mapping):
            raise SafeChangeLiveConfigurationError(
                "Aurora source must have exactly one writer for this proof"
            )
        member = members[0]
        if (
            str(member.get("DBInstanceIdentifier") or "")
            != self.aurora_config.source_writer_instance_id
            or member.get("IsClusterWriter") is not True
        ):
            raise SafeChangeLiveConfigurationError(
                "Aurora source writer does not match the manifest"
            )
        writer = await self._describe_instance(self.aurora_config.source_writer_instance_id)
        assert writer is not None
        if (
            str(writer.get("DBClusterIdentifier") or "") != self.source_id
            or str(writer.get("DBInstanceClass") or "") != "db.serverless"
            or str(writer.get("Engine") or "").lower() != "aurora-postgresql"
            or str(writer.get("DBInstanceStatus") or "").lower() != "available"
        ):
            raise SafeChangeLiveConfigurationError(
                "Aurora source writer is not the available db.serverless writer"
            )
        secret_arn = str((cluster.get("MasterUserSecret") or {}).get("SecretArn") or "")
        if secret_arn != self.config.secret_arn:
            raise SafeChangeLiveConfigurationError(
                "Aurora source managed secret does not match the manifest"
            )
        return cluster, writer

    async def preflight(self, plan: SafeChangePlan) -> Mapping[str, object]:
        self._assert_plan(plan)
        cluster, writer = await self._source()
        return {
            "capability": "copy_on_write_pitr_clone",
            "source_cluster": self.source_id,
            "source_writer": self.aurora_config.source_writer_instance_id,
            "writer_class": str(writer.get("DBInstanceClass") or ""),
            "engine_version": str(cluster.get("EngineVersion") or ""),
            "latest_restorable_time": _safe_datetime(cluster.get("LatestRestorableTime")),
            "region": self.config.region,
        }

    async def inspect_artifact(
        self,
        plan: SafeChangePlan,
    ) -> ArtifactInspection | None:
        self._assert_plan(plan)
        await self._reconcile_pending_mutations()
        await self._assert_identity()
        cluster = await self._describe_cluster(plan.artifact_id, missing_ok=True)
        if cluster is None:
            return None
        cluster_arn = str(cluster.get("DBClusterArn") or "")
        tags = await self._resource_tags(cluster_arn)
        writer_id = _aws_child_id(plan.artifact_id, "writer")
        members = cluster.get("DBClusterMembers") or []
        member_ids = {
            str(member.get("DBInstanceIdentifier") or "")
            for member in members
            if isinstance(member, Mapping)
        }
        member_shape_owned = not members or (
            len(members) == 1
            and isinstance(members[0], Mapping)
            and str(members[0].get("DBInstanceIdentifier") or "") == writer_id
            and members[0].get("IsClusterWriter") is True
        )
        target_groups = {
            str(group.get("VpcSecurityGroupId") or "")
            for group in cluster.get("VpcSecurityGroups") or []
            if isinstance(group, Mapping)
        }
        children_owned = (
            member_ids.issubset({writer_id})
            and member_shape_owned
            and str(cluster.get("Engine") or "").lower() == "aurora-postgresql"
            and str(cluster.get("DBSubnetGroup") or "") == self.config.db_subnet_group_name
            and target_groups == {self.config.security_group_id}
        )
        writer = await self._describe_instance(writer_id, missing_ok=True)
        writer_state = "ABSENT"
        if writer is not None:
            writer_state = str(writer.get("DBInstanceStatus") or "").upper()
            writer_arn = str(writer.get("DBInstanceArn") or "")
            writer_tags = await self._resource_tags(writer_arn)
            expected_tags = self._tag_mapping(self._tags(plan))
            children_owned = children_owned and all(
                writer_tags.get(key) == value for key, value in expected_tags.items()
            )
            children_owned = children_owned and (
                str(writer.get("DBClusterIdentifier") or "") == plan.artifact_id
                and str(writer.get("DBInstanceClass") or "") == "db.serverless"
                and str(writer.get("Engine") or "").lower() == "aurora-postgresql"
            )
        source_id, run_id, owner = self._ownership_from_tags(
            plan,
            tags,
            children_owned=children_owned,
        )
        state = str(cluster.get("Status") or "").upper()
        return ArtifactInspection(
            artifact_id=str(cluster.get("DBClusterIdentifier") or ""),
            provider=self.provider,
            source_id=source_id,
            run_id=run_id,
            owner=owner,
            state=f"{state or 'UNKNOWN'}/{writer_state or 'UNKNOWN'}",
            aws_account_id=self.config.account_id,
            aws_region=self.config.region,
            metadata={
                "cluster_arn": cluster_arn,
                "writer_id": writer_id,
                "member_ids": sorted(member_ids),
                "children_owned": children_owned,
                "endpoint": str(cluster.get("Endpoint") or ""),
                "port": int(cluster.get("Port") or 5432),
                "restore_type": "copy-on-write",
            },
        )

    async def create_isolated(self, plan: SafeChangePlan, report) -> ArtifactInspection:
        self._assert_plan(plan)
        cluster, source_writer = await self._source()
        scaling = cluster.get("ServerlessV2ScalingConfiguration") or {}
        restore_arguments: dict[str, Any] = {
            "SourceDBClusterIdentifier": self.source_id,
            "DBClusterIdentifier": plan.artifact_id,
            "RestoreType": "copy-on-write",
            "UseLatestRestorableTime": True,
            "DBSubnetGroupName": self.config.db_subnet_group_name,
            "VpcSecurityGroupIds": [self.config.security_group_id],
            "Port": int(cluster.get("Port") or 5432),
            "DeletionProtection": False,
            "CopyTagsToSnapshot": False,
            "Tags": self._tags(plan),
        }
        if isinstance(scaling, Mapping) and scaling:
            restore_arguments["ServerlessV2ScalingConfiguration"] = {
                key: scaling[key]
                for key in ("MinCapacity", "MaxCapacity", "SecondsUntilAutoPause")
                if key in scaling
            }
        network_type = str(cluster.get("NetworkType") or "")
        if network_type:
            restore_arguments["NetworkType"] = network_type
        await self._call(
            "rds",
            "restore_db_cluster_to_point_in_time",
            mutation=True,
            **restore_arguments,
        )
        await report(
            "Aurora copy-on-write PITR clone request accepted",
            "RDS.DescribeDBClusters",
        )

        async def cluster_ready() -> Mapping[str, Any] | None:
            target = await self._describe_cluster(plan.artifact_id)
            assert target is not None
            status = str(target.get("Status") or "").lower()
            if status == "available":
                return target
            if status in {"failed", "inaccessible-encryption-credentials"}:
                raise SafeChangeControlPlaneError(f"Aurora clone entered {status.upper()}")
            return None

        await self._wait_for(cluster_ready, description="Aurora clone availability")
        writer_id = _aws_child_id(plan.artifact_id, "writer")
        await self._call(
            "rds",
            "create_db_instance",
            mutation=True,
            DBInstanceIdentifier=writer_id,
            DBClusterIdentifier=plan.artifact_id,
            DBInstanceClass="db.serverless",
            Engine="aurora-postgresql",
            PubliclyAccessible=bool(source_writer.get("PubliclyAccessible", True)),
            AutoMinorVersionUpgrade=False,
            PromotionTier=0,
            Tags=self._tags(plan),
        )
        await report(
            "Aurora clone writer request accepted",
            "RDS.DescribeDBInstances",
        )

        async def writer_ready() -> Mapping[str, Any] | None:
            target = await self._describe_instance(writer_id)
            assert target is not None
            status = str(target.get("DBInstanceStatus") or "").lower()
            if status == "available":
                return target
            if status in {"failed", "incompatible-restore", "inaccessible-encryption-credentials"}:
                raise SafeChangeControlPlaneError(f"Aurora clone writer entered {status.upper()}")
            return None

        await self._wait_for(writer_ready, description="Aurora clone writer availability")
        artifact = await self.inspect_artifact(plan)
        if artifact is None:
            raise SafeChangeControlPlaneError("Aurora clone disappeared after creation")
        self._assert_owned_artifact(plan, artifact)
        if artifact.state != "AVAILABLE/AVAILABLE":
            raise SafeChangeControlPlaneError(f"Aurora clone is not ready ({artifact.state})")
        return artifact

    async def _source_credentials(self) -> DatabaseCredentials:
        cluster, _ = await self._source()
        return await self._secret_credentials(
            attached_secret_arn=str((cluster.get("MasterUserSecret") or {}).get("SecretArn") or ""),
            source_host=str(cluster.get("Endpoint") or ""),
            source_port=int(cluster.get("Port") or 5432),
            identifier_field="dbClusterIdentifier",
            accepted_engines={"postgres", "aurora-postgresql"},
        )

    async def connect_source(self, plan: SafeChangePlan) -> SafeChangeSqlConnection:
        self._assert_plan(plan)
        cluster, _ = await self._source()
        credentials = await self._source_credentials()
        return await self._connect_target(
            host=str(cluster.get("Endpoint") or ""),
            port=int(cluster.get("Port") or 5432),
            credentials=credentials,
        )

    async def connect_isolated(
        self,
        plan: SafeChangePlan,
        artifact: ArtifactInspection,
    ) -> SafeChangeSqlConnection:
        self._assert_owned_artifact(plan, artifact)
        current = await self.inspect_artifact(plan)
        if current is None:
            raise SafeChangeLiveConfigurationError("Aurora clone no longer exists")
        self._assert_owned_artifact(plan, current)
        cluster = await self._describe_cluster(plan.artifact_id)
        assert cluster is not None
        credentials = await self._source_credentials()
        return await self._connect_target(
            # Credentials come from the source managed secret. The target can
            # only come from the freshly validated target control-plane object.
            host=str(cluster.get("Endpoint") or ""),
            port=int(cluster.get("Port") or 5432),
            credentials=credentials,
        )

    async def delete_isolated(
        self,
        plan: SafeChangePlan,
        artifact: ArtifactInspection,
        report,
    ) -> None:
        self._assert_owned_artifact(plan, artifact)
        current = await self.inspect_artifact(plan)
        if current is None:
            return
        self._assert_owned_artifact(plan, current)
        writer_id = str(current.metadata.get("writer_id") or "")
        writer = await self._describe_instance(writer_id, missing_ok=True)
        if writer is not None:
            # The writer is the Serverless v2 compute, so it goes first: at or
            # before the cluster, never after.
            await self._delete_when_deletable(
                "delete_db_instance",
                identifier=writer_id,
                observe=lambda: self._instance_status(writer_id),
                absent_codes=_ABSENT_INSTANCE_CODES,
                undeletable_codes=_UNDELETABLE_INSTANCE_CODES,
                description="the owned Aurora clone writer",
                report=report,
                wire_call="RDS.DescribeDBInstances",
                DBInstanceIdentifier=writer_id,
                SkipFinalSnapshot=True,
                DeleteAutomatedBackups=True,
            )
            await report(
                "Waiting for the owned Aurora clone writer deletion",
                "RDS.DescribeDBInstances",
            )

            async def writer_absent() -> bool | None:
                return (
                    True
                    if await self._describe_instance(writer_id, missing_ok=True) is None
                    else None
                )

            await self._wait_for(writer_absent, description="Aurora writer deletion")
        cluster = await self._describe_cluster(plan.artifact_id, missing_ok=True)
        if cluster is None:
            return
        # Deleting the writer above is itself a modification of this cluster, so
        # `modifying` is an ordinary state to arrive in here, on top of the
        # `creating` a mid-restore towel leaves behind. Both are RDS refusals,
        # and both are waited out rather than tolerated.
        await self._delete_when_deletable(
            "delete_db_cluster",
            identifier=plan.artifact_id,
            observe=lambda: self._cluster_status(plan.artifact_id),
            absent_codes=_ABSENT_CLUSTER_CODES,
            undeletable_codes=_UNDELETABLE_CLUSTER_CODES,
            description="the owned Aurora copy-on-write PITR clone",
            report=report,
            wire_call="RDS.DescribeDBClusters",
            DBClusterIdentifier=plan.artifact_id,
            SkipFinalSnapshot=True,
            DeleteAutomatedBackups=True,
        )
        await report(
            "Waiting for the owned Aurora copy-on-write PITR clone deletion",
            "RDS.DescribeDBClusters",
        )

        async def cluster_absent() -> bool | None:
            return (
                True
                if await self._describe_cluster(plan.artifact_id, missing_ok=True) is None
                else None
            )

        await self._wait_for(cluster_absent, description="Aurora clone deletion")

    async def abandon_isolated(self, plan: SafeChangePlan) -> None:
        """Issue teardown for a cancelled lane without waiting for it.

        Both identifiers are derived from the plan rather than read from a
        control-plane response, so this works when cancellation landed before
        the clone existed, after the writer was created but before anything was
        recorded, or part way through an ordinary teardown.

        The writer goes first and its outcome does not gate the cluster: the
        writer is the Aurora Serverless v2 compute that made the original
        fifty-seven-minute leak expensive, so its delete must be issued before
        anything else can go wrong.
        """

        self._assert_plan(plan)
        writer_id = _aws_child_id(plan.artifact_id, "writer")
        try:
            await self._issue_delete(
                "delete_db_instance",
                identifier=writer_id,
                absent_codes=_ABSENT_INSTANCE_CODES,
                undeletable_codes=_UNDELETABLE_INSTANCE_CODES,
                DBInstanceIdentifier=writer_id,
                SkipFinalSnapshot=True,
                DeleteAutomatedBackups=True,
            )
        finally:
            # An empty Aurora cluster still carries storage, and leaving it
            # behind because the writer delete raised would leak the cheaper
            # half of the pair for the same reason as the expensive half.
            await self._issue_delete(
                "delete_db_cluster",
                identifier=plan.artifact_id,
                absent_codes=_ABSENT_CLUSTER_CODES,
                undeletable_codes=_UNDELETABLE_CLUSTER_CODES,
                DBClusterIdentifier=plan.artifact_id,
                SkipFinalSnapshot=True,
                DeleteAutomatedBackups=True,
            )


class RdsSafeChangeAdapter(_AwsSafeChangeAdapter, SafeChangeAdapter):
    provider = SafeChangeProvider.RDS
    name = "RDS PostgreSQL"

    def __init__(
        self,
        config: RdsSafeChangeConfig,
        *,
        session: Any | None = None,
        session_factory: AwsSessionFactory = boto3.Session,
        connector: PsycopgConnector = psycopg.AsyncConnection.connect,
        sleep: Sleeper = asyncio.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.rds_config = config
        self.source_id = config.source_instance_id
        super().__init__(
            config,
            session=session,
            session_factory=session_factory,
            connector=connector,
            sleep=sleep,
            clock=clock,
        )

    async def _describe_instance(
        self,
        instance_id: str,
        *,
        missing_ok: bool = False,
    ) -> Mapping[str, Any] | None:
        try:
            response = await self._call(
                "rds", "describe_db_instances", DBInstanceIdentifier=instance_id
            )
        except Exception as exc:
            if missing_ok and _is_missing(
                exc,
                "DBInstanceNotFound",
                "DBInstanceNotFoundFault",
            ):
                return None
            raise
        instances = response.get("DBInstances") or []
        if len(instances) != 1 or not isinstance(instances[0], Mapping):
            raise SafeChangeLiveConfigurationError(
                "RDS instance identifier did not resolve exactly once"
            )
        instance = instances[0]
        if str(instance.get("DBInstanceIdentifier") or "") != instance_id:
            raise SafeChangeLiveConfigurationError(
                "RDS control plane returned a different instance"
            )
        _assert_arn(
            str(instance.get("DBInstanceArn") or ""),
            service="rds",
            region=self.config.region,
            account_id=self.config.account_id,
            resource_prefix="db:",
            resource_id=instance_id,
        )
        return instance

    async def _source(self) -> Mapping[str, Any]:
        await self._assert_identity()
        instance = await self._describe_instance(self.source_id)
        assert instance is not None
        if str(instance.get("Engine") or "").lower() != "postgres":
            raise SafeChangeLiveConfigurationError("RDS source is not PostgreSQL")
        version = str(instance.get("EngineVersion") or "")
        if not version.startswith(f"{self.config.expected_postgres_major}."):
            raise SafeChangeLiveConfigurationError("RDS source PostgreSQL major version changed")
        if str(instance.get("DBInstanceStatus") or "").lower() != "available":
            raise SafeChangeLiveConfigurationError("RDS source instance is not available")
        subnet = instance.get("DBSubnetGroup") or {}
        if str(subnet.get("DBSubnetGroupName") or "") != self.config.db_subnet_group_name:
            raise SafeChangeLiveConfigurationError(
                "RDS source DB subnet group does not match the manifest"
            )
        groups = {
            str(group.get("VpcSecurityGroupId") or "")
            for group in instance.get("VpcSecurityGroups") or []
            if isinstance(group, Mapping)
        }
        if groups != {self.config.security_group_id}:
            raise SafeChangeLiveConfigurationError(
                "RDS source security group does not match the manifest"
            )
        if int(instance.get("BackupRetentionPeriod") or 0) < 1 or not instance.get(
            "LatestRestorableTime"
        ):
            raise SafeChangeLiveConfigurationError(
                "RDS source does not have a usable point-in-time restore window"
            )
        self._assert_latest_restorable_covers_seed(instance.get("LatestRestorableTime"))
        secret_arn = str((instance.get("MasterUserSecret") or {}).get("SecretArn") or "")
        if secret_arn != self.config.secret_arn:
            raise SafeChangeLiveConfigurationError(
                "RDS source managed secret does not match the manifest"
            )
        return instance

    async def preflight(self, plan: SafeChangePlan) -> Mapping[str, object]:
        self._assert_plan(plan)
        instance = await self._source()
        return {
            "capability": "native_pitr_restore",
            "source_instance": self.source_id,
            "instance_class": str(instance.get("DBInstanceClass") or ""),
            "engine_version": str(instance.get("EngineVersion") or ""),
            "latest_restorable_time": _safe_datetime(instance.get("LatestRestorableTime")),
            "region": self.config.region,
        }

    async def inspect_artifact(
        self,
        plan: SafeChangePlan,
    ) -> ArtifactInspection | None:
        self._assert_plan(plan)
        await self._reconcile_pending_mutations()
        await self._assert_identity()
        instance = await self._describe_instance(plan.artifact_id, missing_ok=True)
        if instance is None:
            return None
        arn = str(instance.get("DBInstanceArn") or "")
        tags = await self._resource_tags(arn)
        subnet = instance.get("DBSubnetGroup") or {}
        target_groups = {
            str(group.get("VpcSecurityGroupId") or "")
            for group in instance.get("VpcSecurityGroups") or []
            if isinstance(group, Mapping)
        }
        children_owned = (
            str(instance.get("Engine") or "").lower() == "postgres"
            and not str(instance.get("DBClusterIdentifier") or "")
            and str(subnet.get("DBSubnetGroupName") or "") == self.config.db_subnet_group_name
            and target_groups == {self.config.security_group_id}
        )
        source_id, run_id, owner = self._ownership_from_tags(
            plan,
            tags,
            children_owned=children_owned,
        )
        endpoint = instance.get("Endpoint") or {}
        return ArtifactInspection(
            artifact_id=str(instance.get("DBInstanceIdentifier") or ""),
            provider=self.provider,
            source_id=source_id,
            run_id=run_id,
            owner=owner,
            state=str(instance.get("DBInstanceStatus") or "").upper(),
            aws_account_id=self.config.account_id,
            aws_region=self.config.region,
            metadata={
                "instance_arn": arn,
                "children_owned": children_owned,
                "endpoint": str(endpoint.get("Address") or ""),
                "port": int(endpoint.get("Port") or 5432),
                "restore_type": "pitr",
            },
        )

    async def create_isolated(self, plan: SafeChangePlan, report) -> ArtifactInspection:
        self._assert_plan(plan)
        source = await self._source()
        restore_arguments: dict[str, Any] = {
            "SourceDBInstanceIdentifier": self.source_id,
            "TargetDBInstanceIdentifier": plan.artifact_id,
            "UseLatestRestorableTime": True,
            "DBInstanceClass": str(source.get("DBInstanceClass") or ""),
            "DBSubnetGroupName": self.config.db_subnet_group_name,
            "VpcSecurityGroupIds": [self.config.security_group_id],
            "PubliclyAccessible": bool(source.get("PubliclyAccessible", True)),
            "MultiAZ": False,
            "AutoMinorVersionUpgrade": False,
            "DeletionProtection": False,
            "CopyTagsToSnapshot": False,
            "Tags": self._tags(plan),
        }
        network_type = str(source.get("NetworkType") or "")
        if network_type:
            restore_arguments["NetworkType"] = network_type
        await self._call(
            "rds",
            "restore_db_instance_to_point_in_time",
            mutation=True,
            **restore_arguments,
        )
        await report(
            "RDS PostgreSQL PITR restore request accepted",
            "RDS.DescribeDBInstances",
        )

        async def instance_ready() -> Mapping[str, Any] | None:
            target = await self._describe_instance(plan.artifact_id)
            assert target is not None
            status = str(target.get("DBInstanceStatus") or "").lower()
            if status in _RDS_SERVING_STATES:
                return target
            if status in _RDS_TERMINAL_STATES:
                raise SafeChangeControlPlaneError(f"RDS PITR restore entered {status.upper()}")
            return None

        await self._wait_for(instance_ready, description="RDS PITR restore availability")
        artifact = await self.inspect_artifact(plan)
        if artifact is None:
            raise SafeChangeControlPlaneError("RDS PITR restore disappeared after creation")
        self._assert_owned_artifact(plan, artifact)
        if artifact.state.lower() not in _RDS_SERVING_STATES:
            raise SafeChangeControlPlaneError(f"RDS PITR restore is not ready ({artifact.state})")
        return artifact

    async def _source_credentials(self) -> DatabaseCredentials:
        source = await self._source()
        endpoint = source.get("Endpoint") or {}
        return await self._secret_credentials(
            attached_secret_arn=str((source.get("MasterUserSecret") or {}).get("SecretArn") or ""),
            source_host=str(endpoint.get("Address") or ""),
            source_port=int(endpoint.get("Port") or 5432),
            identifier_field="dbInstanceIdentifier",
            accepted_engines={"postgres"},
        )

    async def connect_source(self, plan: SafeChangePlan) -> SafeChangeSqlConnection:
        self._assert_plan(plan)
        source = await self._source()
        endpoint = source.get("Endpoint") or {}
        credentials = await self._source_credentials()
        return await self._connect_target(
            host=str(endpoint.get("Address") or ""),
            port=int(endpoint.get("Port") or 5432),
            credentials=credentials,
        )

    async def connect_isolated(
        self,
        plan: SafeChangePlan,
        artifact: ArtifactInspection,
    ) -> SafeChangeSqlConnection:
        self._assert_owned_artifact(plan, artifact)
        current = await self.inspect_artifact(plan)
        if current is None:
            raise SafeChangeLiveConfigurationError("RDS PITR restore no longer exists")
        self._assert_owned_artifact(plan, current)
        target = await self._describe_instance(plan.artifact_id)
        assert target is not None
        endpoint = target.get("Endpoint") or {}
        credentials = await self._source_credentials()
        return await self._connect_target(
            # The source secret contributes credentials only. The isolated
            # endpoint is always taken from this exact target instance.
            host=str(endpoint.get("Address") or ""),
            port=int(endpoint.get("Port") or 5432),
            credentials=credentials,
        )

    async def delete_isolated(
        self,
        plan: SafeChangePlan,
        artifact: ArtifactInspection,
        report,
    ) -> None:
        self._assert_owned_artifact(plan, artifact)
        current = await self.inspect_artifact(plan)
        if current is None:
            return
        self._assert_owned_artifact(plan, current)
        target = await self._describe_instance(plan.artifact_id)
        assert target is not None
        await self._delete_when_deletable(
            "delete_db_instance",
            identifier=plan.artifact_id,
            observe=lambda: self._instance_status(plan.artifact_id),
            absent_codes=_ABSENT_INSTANCE_CODES,
            undeletable_codes=_UNDELETABLE_INSTANCE_CODES,
            description="the owned RDS PostgreSQL PITR restore",
            report=report,
            wire_call="RDS.DescribeDBInstances",
            DBInstanceIdentifier=plan.artifact_id,
            SkipFinalSnapshot=True,
            DeleteAutomatedBackups=True,
        )
        await report(
            "Waiting for the owned RDS PostgreSQL PITR restore deletion",
            "RDS.DescribeDBInstances",
        )

        async def instance_absent() -> bool | None:
            return (
                True
                if await self._describe_instance(plan.artifact_id, missing_ok=True) is None
                else None
            )

        await self._wait_for(instance_absent, description="RDS PITR restore deletion")

    async def abandon_isolated(self, plan: SafeChangePlan) -> None:
        """Issue teardown for a cancelled lane without waiting for it.

        One instance, one call, derived from the plan so it holds whether or not
        the restore had been created when cancellation arrived.
        """

        self._assert_plan(plan)
        await self._issue_delete(
            "delete_db_instance",
            identifier=plan.artifact_id,
            absent_codes=_ABSENT_INSTANCE_CODES,
            undeletable_codes=_UNDELETABLE_INSTANCE_CODES,
            DBInstanceIdentifier=plan.artifact_id,
            SkipFinalSnapshot=True,
            DeleteAutomatedBackups=True,
        )


def _safe_datetime(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value or "")


def _positive_float(
    environment: Mapping[str, str],
    name: str,
    default: float,
) -> float:
    raw = environment.get(name, "")
    try:
        value = float(raw) if raw else default
    except ValueError as exc:
        raise SafeChangeLiveConfigurationError(f"{name} must be numeric") from exc
    if value <= 0:
        raise SafeChangeLiveConfigurationError(f"{name} must be positive")
    return value


def _manifest_binding(
    environment: Mapping[str, str],
    name: str,
    manifest_value: str,
) -> str:
    configured = environment.get(name, "").strip()
    if configured and configured != manifest_value:
        raise SafeChangeLiveConfigurationError(f"{name} does not match the owned demo manifest")
    if not manifest_value:
        raise SafeChangeLiveConfigurationError(f"Owned demo manifest is missing {name}")
    return manifest_value


def build_safe_change_engine(
    manifest: DemoManifest | None = None,
    *,
    cleanup_only: bool = False,
    round_number: int = 2,
    environment: Mapping[str, str] | None = None,
    databricks_runner: DatabricksRunner | None = None,
    session_factory: AwsSessionFactory = boto3.Session,
    connector: PsycopgConnector = psycopg.AsyncConnection.connect,
    sleep: Sleeper = asyncio.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> SafeChangeEngine:
    """Build the production Round 2 engine from one owned manifest boundary.

    Environment values may repeat manifest bindings, but can never redirect a
    source, account, region, secret, or workspace profile away from the manifest.
    Only bounded timeout tuning is environment-owned.
    """

    owned = manifest or load_manifest()
    if round_number not in (2, 3):
        raise SafeChangeLiveConfigurationError("Safe-change source round must be 2 or 3")
    if not cleanup_only:
        # A passed TTL is reported, not enforced: it is a provision-time wall-clock
        # value that says nothing about whether the Round 2/3 sources are healthy.
        # Refusing here left Rounds 2 and 3 dead while the rest of the app served
        # traffic. `status` below is a real readiness signal and still refuses.
        # The cleanup_only path never consulted expiry and still must not.
        expiry_warning = owned.expiry_warning()
        if expiry_warning is not None:
            print(f"WARN  {expiry_warning}", flush=True)
        if owned.status != "ready":
            raise SafeChangeLiveConfigurationError(
                f"Owned demo manifest is {owned.status.upper()}, not READY"
            )
    env = environment if environment is not None else os.environ
    app_mode = env.get("ANTI_DEMO_ENV", "").strip() == "databricks-app" or bool(
        env.get("DATABRICKS_APP_NAME", "").strip()
    )
    configured_auth_mode = env.get("AWS_AUTH_MODE", "").strip()
    auth_mode = configured_auth_mode or ("environment" if app_mode else owned.aws.auth_mode)
    if auth_mode not in {"profile", "environment"}:
        raise SafeChangeLiveConfigurationError(
            "AWS_AUTH_MODE must be profile or environment"
        )
    if app_mode and auth_mode != "environment":
        raise SafeChangeLiveConfigurationError(
            "Databricks App runtime requires AWS_AUTH_MODE=environment"
        )
    if not app_mode and configured_auth_mode and auth_mode != owned.aws.auth_mode:
        raise SafeChangeLiveConfigurationError(
            "AWS_AUTH_MODE does not match the owned demo manifest"
        )
    try:
        auth = validate_runtime_auth(cast(AwsAuthMode, auth_mode), owned.aws.profile, env)
    except RuntimeError as exc:
        raise SafeChangeLiveConfigurationError(str(exc)) from exc
    profile = auth.profile
    region = _manifest_binding(env, "AWS_REGION", owned.aws.region)
    account_id = _manifest_binding(env, "AWS_EXPECTED_ACCOUNT_ID", owned.aws.account_id)
    if app_mode:
        if owned.manifest_version < 2 or owned.round4 is None:
            raise SafeChangeLiveConfigurationError(
                "Databricks App runtime requires a sealed manifest v2 identity"
            )
        if env.get("DATABRICKS_PROFILE", "").strip() or env.get(
            "DATABRICKS_CONFIG_PROFILE", ""
        ).strip():
            raise SafeChangeLiveConfigurationError(
                "Databricks App runtime cannot use a Databricks profile"
            )
        dbx_profile = ""
        runtime_lakebase_user = owned.round4.app_service_principal_client_id
    else:
        dbx_profile = _manifest_binding(
            env, "DATABRICKS_PROFILE", owned.databricks.profile
        )
        runtime_lakebase_user = owned.databricks.user
    per_round = owned.round_environments is not None
    round_environment = owned.round_environment(round_number) if per_round else None
    owned_lakebase_endpoint = (
        round_environment.lakebase.endpoint_name
        if round_environment is not None
        else owned.databricks.endpoint_name
    )
    endpoint_environment_name = (
        f"ANTI_DEMO_ROUND{round_number}_LAKEBASE_ENDPOINT_NAME"
        if per_round
        else "LAKEBASE_ENDPOINT_NAME"
    )
    source_endpoint = _manifest_binding(
        env, endpoint_environment_name, owned_lakebase_endpoint
    )
    database = _manifest_binding(env, "LAKEBASE_DATABASE", owned.databricks.database)
    expected_source = owned_lakebase_endpoint
    if source_endpoint != expected_source:
        raise SafeChangeLiveConfigurationError(
            "Manifest Lakebase endpoint is not the owned production/primary endpoint"
        )
    configured_user = env.get("LAKEBASE_USER", "").strip()
    if configured_user and configured_user != runtime_lakebase_user:
        raise SafeChangeLiveConfigurationError(
            "LAKEBASE_USER does not match the owned demo manifest"
        )
    expected_region = env.get("LAKEBASE_EXPECTED_REGION", "").strip()
    if expected_region and expected_region != region:
        raise SafeChangeLiveConfigurationError(
            "LAKEBASE_EXPECTED_REGION does not match the owned AWS region"
        )
    if round_environment is not None:
        if round_environment.aurora is None or round_environment.rds is None:
            raise SafeChangeLiveConfigurationError(
                f"Round {round_number} competitor environments are not sealed"
            )
        aurora_id = round_environment.aurora.cluster_id
        aurora_secret = round_environment.aurora.secret_arn
        rds_id = round_environment.rds.instance_id
        rds_secret = round_environment.rds.secret_arn
        aurora_security_group_id = round_environment.aurora.security_group_id
        rds_security_group_id = round_environment.rds.security_group_id
        db_subnet_group_name = round_environment.aurora.db_subnet_group_name
    else:
        aurora_id = _manifest_binding(
            env,
            "AURORA_CLUSTER_ID",
            owned.aws.resources.aurora_cluster_id,
        )
        aurora_secret = _manifest_binding(
            env,
            "AURORA_SECRET_ARN",
            owned.aws.resources.aurora_secret_arn,
        )
        rds_id = _manifest_binding(
            env,
            "RDS_INSTANCE_ID",
            owned.aws.resources.rds_instance_id,
        )
        rds_secret = _manifest_binding(
            env,
            "RDS_SECRET_ARN",
            owned.aws.resources.rds_secret_arn,
        )
        aurora_security_group_id = owned.aws.resources.security_group_id
        rds_security_group_id = owned.aws.resources.rds_security_group_id
        db_subnet_group_name = owned.aws.resources.db_subnet_group_name
    for name in ("AURORA_DATABASE", "RDS_DATABASE"):
        configured_database = env.get(name, "").strip()
        if configured_database and configured_database != database:
            raise SafeChangeLiveConfigurationError(f"{name} does not match the owned demo database")
    expected_major = env.get("EXPECTED_POSTGRES_MAJOR", "17").strip()
    control_timeout = _positive_float(
        env,
        "ANTI_DEMO_SAFE_CHANGE_CONTROL_TIMEOUT_SECONDS",
        120.0,
    )
    poll_timeout = _positive_float(
        env,
        "ANTI_DEMO_SAFE_CHANGE_POLL_TIMEOUT_SECONDS",
        DEFAULT_POLL_TIMEOUT_SECONDS,
    )
    poll_interval = _positive_float(
        env,
        "ANTI_DEMO_SAFE_CHANGE_POLL_SECONDS",
        5.0,
    )
    connect_timeout = _positive_float(
        env,
        "ANTI_DEMO_SAFE_CHANGE_CONNECT_TIMEOUT_SECONDS",
        30.0,
    )
    connection_ready_timeout = _positive_float(
        env,
        "ANTI_DEMO_SAFE_CHANGE_CONNECTION_READY_TIMEOUT_SECONDS",
        120.0,
    )
    scope = SafeChangeOwnershipScope(
        run_id=owned.run_id,
        owner=owned.owner,
        aws_account_id=account_id,
        aws_region=region,
    )
    lakebase = LakebaseSafeChangeAdapter(
        LakebaseSafeChangeConfig(
            profile=dbx_profile,
            source_endpoint=source_endpoint,
            database=database,
            user=runtime_lakebase_user,
            expected_region=region,
            control_timeout_seconds=control_timeout,
            poll_timeout_seconds=poll_timeout,
            poll_interval_seconds=poll_interval,
            connect_timeout_seconds=connect_timeout,
        ),
        runner=databricks_runner,
        connector=connector,
        sleep=sleep,
        clock=clock,
    )
    common = {
        "auth_mode": auth.mode,
        "profile": profile,
        "region": region,
        "account_id": account_id,
        "database": database,
        "db_subnet_group_name": db_subnet_group_name,
        "expected_postgres_major": expected_major,
        "source_seeded_at": owned.last_reset_at or owned.created_at,
        "control_timeout_seconds": control_timeout,
        "poll_timeout_seconds": poll_timeout,
        "poll_interval_seconds": poll_interval,
        "connect_timeout_seconds": connect_timeout,
        "connection_ready_timeout_seconds": connection_ready_timeout,
    }
    aurora = AuroraSafeChangeAdapter(
        AuroraSafeChangeConfig(
            **common,
            secret_arn=aurora_secret,
            security_group_id=aurora_security_group_id,
            source_cluster_id=aurora_id,
            source_writer_instance_id=(
                round_environment.aurora.writer_instance_id
                if round_environment is not None and round_environment.aurora is not None
                else owned.aws.resources.aurora_writer_instance_id
            ),
        ),
        session_factory=session_factory,
        connector=connector,
        sleep=sleep,
        clock=clock,
    )
    rds = RdsSafeChangeAdapter(
        RdsSafeChangeConfig(
            **common,
            secret_arn=rds_secret,
            security_group_id=rds_security_group_id,
            source_instance_id=rds_id,
        ),
        session_factory=session_factory,
        connector=connector,
        sleep=sleep,
        clock=clock,
    )
    return SafeChangeEngine(
        scope=scope,
        lakebase=lakebase,
        competitors={
            CompetitorId.AURORA_SERVERLESS_V2: aurora,
            CompetitorId.RDS_POSTGRES: rds,
        },
        # Rounds 2 and 3 police their own arm age at the bell, independently of
        # the manager's expiry task, and both read this same variable -- Round 3
        # via build_recovery_engine, which copies the value out of here. The
        # default must therefore track RunManager._armed_ttl. Leaving it behind
        # would make a late bell fail inside the engine ("Round 2 arm expired")
        # while the manager still believed the card was armed, replacing a clean
        # automatic release with a refusal mid-bell.
        arm_ttl_seconds=_positive_float(env, "ANTI_DEMO_ARM_TTL_SECONDS", 180.0),
        preflight_timeout_seconds=_positive_float(
            env,
            "ANTI_DEMO_SAFE_CHANGE_PREFLIGHT_TIMEOUT_SECONDS",
            180.0,
        ),
        run_timeout_seconds=_positive_float(
            env,
            "ANTI_DEMO_SAFE_CHANGE_RUN_TIMEOUT_SECONDS",
            DEFAULT_RUN_TIMEOUT_SECONDS,
        ),
        reset_timeout_seconds=_positive_float(
            env,
            "ANTI_DEMO_SAFE_CHANGE_RESET_TIMEOUT_SECONDS",
            DEFAULT_RUN_TIMEOUT_SECONDS,
        ),
    )


__all__ = [
    "AuroraSafeChangeAdapter",
    "AuroraSafeChangeConfig",
    "ControlPlaneCommandError",
    "DatabaseCredentials",
    "DatabaseTarget",
    "DatabricksRestRunner",
    "DatabricksRunner",
    "LakebaseSafeChangeAdapter",
    "LakebaseSafeChangeConfig",
    "PsycopgSafeChangeConnection",
    "RdsSafeChangeAdapter",
    "RdsSafeChangeConfig",
    "SafeChangeControlPlaneError",
    "SafeChangeLiveConfigurationError",
    "build_safe_change_engine",
]
