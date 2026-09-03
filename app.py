from __future__ import annotations

import asyncio
import logging
import os
import time
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError
from databricks.sdk import WorkspaceClient
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles

from server.api import router
from server.aws_auth import AwsAuthConfigurationError, validate_app_aws_environment
from server.aws_credential_probe import (
    CredentialSentry,
    CredentialVerdict,
    effective_credential_verdict,
    expectations_from_manifest,
    has_any_credential_source,
)
from server.coordination import (
    INMEMORY_COORDINATION_LOSSES,
    ROUND5_RING_KEY,
    build_lease_store,
    is_transient_coordination_error,
    round_ring_key,
)
from server.cost_ledger import build_cost_ledger_store
from server.generation_lock import (
    TRANSITIONAL_STATUSES,
    generation_lock_path,
    lock_is_held,
    transitional_status_recovery,
)
from server.lifecycle import (
    OperatorIngressDrift,
    cached_installation_report,
    installation_presence_async,
    operator_ingress_drift_async,
)
from server.live_orders import LiveOrdersEngine
from server.manager import InvalidStateError, RunManager
from server.manifest import (
    MANIFEST_JSON_ENV,
    DemoManifest,
    load_manifest,
    manifest_path,
)
from server.model_score import ModelScoreEngine
from server.models import CompetitorId, RoundId
from server.pipeline_power import (
    DurablePipelinePowerStore,
    install_pipeline_power_store,
    load_owed_stop_snapshot,
    owed_stop_notice,
)
from server.posted_usage import PostedUsageCache
from server.process_registry import (
    register_serving_process,
    unregister_serving_process,
)
from server.readiness import (
    GATE_ESCALATE_AFTER_SECONDS,
    SETTLED,
    ReadinessStatus,
    RecoveryState,
    ShowtimeReadinessGate,
)
from server.reap import (
    AwsOrphanDeleter,
    ReapHealth,
    ReapReport,
    reap_health,
    reap_mode,
    reap_startup_orphans,
    write_audit,
)
from server.receipts import (
    DurableReceiptStore,
    drain_receipt_writes,
    install_receipt_store,
)
from server.reconcile import (
    PRESENCE_MISSING,
    PRESENCE_UNVERIFIED,
    InstallationPresence,
    presence_from_report,
    reconcile_live,
)
from server.round4_stop_recovery import build_inherited_round4_stop_recovery
from server.round5_cleanup_owed import round5_cleanup_owed_notice
from server.round_construction import (
    build_round,
    exception_diagnostic,
    round5_configs_build,
    round_unavailable,
)
from server.server_launch import (
    configure_operator_logging,
    read_restart_history,
    restart_record_path,
)

# Here, at import, because this is the one file every way of starting the server
# goes through: `antidemo serve`, `antidemo serve --background`, a bare `uvicorn
# app:app`, and the `python -m uvicorn app:app` in `app.yaml`. It runs after
# uvicorn's own `dictConfig` -- uvicorn configures logging in `Config.__init__`
# and imports the ASGI app later -- and uvicorn configures only its own three
# loggers, so until this call the root logger has no handlers and every
# `LOGGER.warning` in `server/*` falls through to `logging.lastResort`. That
# does reach stderr, and the daemon's stderr is the server log, so the lines
# were never lost; they arrived with no level, no timestamp and no logger name,
# which is why grepping the log for `ERROR` found nothing and the `ORPHAN RISK`
# line was reported as missing when it was merely unlabelled.
configure_operator_logging()

ROOT = Path(__file__).resolve().parent
FRONTEND_DIST = ROOT / "frontend" / "dist"
LOGGER = logging.getLogger(__name__)
STARTUP_ATTEMPTS = 3
SHELL_CACHE_HEADERS = {
    "Cache-Control": "no-store, max-age=0",
    "Pragma": "no-cache",
}

#: How often the serving process re-reads a manifest that another process is
#: mid-mutation on. Long enough that waiting costs nothing, short enough that a
#: reseal finishing is picked up before anybody reaches for the browser.
MUTATION_WAIT_POLL_SECONDS = 5.0

#: When this process started. Captured at import, so it names the *process* and
#: not the most recent lifespan: the mutation-wait path re-enters `_open_runtime`
#: every few seconds and the test suite opens the lifespan hundreds of times in
#: one interpreter, so a value taken there would report restarts that never
#: happened.
_PROCESS_STARTED_AT = datetime.now(UTC)


class ManifestMutationInProgressError(InvalidStateError):
    """The manifest is part-way through somebody else's mutation.

    Distinguished from every other non-ready manifest because it is the one case
    that is *not* this process's problem to fix: `provisioning`, `seeding` and
    `waiting_for_zero` are what a mutation in flight looks like from the outside,
    and the only correct response from a serving process is to wait and keep
    looking. It used to be fatal, so a reseal that died inside
    `wait_for_scale_zero` left a manifest nothing would start against.

    `lock_held` separates the two shapes of it. Held means a mutator is alive and
    the status will clear itself. Free means an interrupted run abandoned it, so
    it will never clear without an operator -- worth saying out loud, because
    those two need very different responses even though both are survivable.
    """

    def __init__(self, message: str, *, status: str, lock_held: bool) -> None:
        super().__init__(message)
        self.status = status
        self.lock_held = lock_held


def _mutation_lock_is_held() -> bool:
    """Whether somebody is mid-mutation, without ever holding the lock ourselves.

    `lock_is_held` answers by trying the lock and dropping it in the same breath,
    which is the only trustworthy answer -- the payload can be residue from a
    process that has exited. The serving process is never a holder: nothing here
    calls `hold_generation`, so no lock outlives this call and `antidemo setup` is
    never blocked by the server.
    """

    try:
        return lock_is_held(generation_lock_path(manifest_path()))
    except (RuntimeError, OSError, ValueError):
        return False


def _transitional_recovery(status: str) -> str | None:
    """Explain a non-ready status, without ever letting the lookup be the failure.

    Read-only, and takes no lock it keeps: the server is never a lock holder, so
    an operator can reseal a generation while this process keeps answering.
    """
    try:
        return transitional_status_recovery(status, manifest_path=manifest_path())
    except (RuntimeError, OSError, ValueError):
        return None


def _load_ready_manifest(*, require_v2: bool = False) -> DemoManifest:
    try:
        manifest = load_manifest()
    except Exception as exc:
        raise InvalidStateError("Demo setup is not ready: owned manifest is unavailable") from exc
    # A passed TTL does not make an installation unserviceable. The timestamp is
    # written once at provision time and never consulted by the resources
    # themselves, so an expired value says nothing about whether they are healthy
    # -- and an app that is answering requests is itself evidence the install is
    # in use. Refusing here bricked long-lived installs two ways: a deployed app
    # that restarted past expiry never came back, and every control action was
    # gated because this function backs `require_ready_manifest`.
    #
    # The cost discipline that replaces it is unchanged and was always the real
    # one: `antidemo cleanup --yes`. What is genuinely given up is that an abandoned
    # install no longer announces itself by failing here; see the trade-off note
    # in README.md. `antidemo renew --ttl-hours N` moves the timestamp forward.
    expiry_warning = manifest.expiry_warning()
    if expiry_warning is not None:
        LOGGER.warning("%s", expiry_warning)
    if manifest.status != "ready":
        # The status alone used to be the whole message, and a `seeding` manifest
        # with no process behind it was then a dead end: nothing said whether to
        # wait or to repair, and the cure (`antidemo setup`, which resumes) was
        # knowledge rather than instruction. The generation lock is what makes
        # those two cases distinguishable -- if nobody holds it, no mutation is in
        # flight -- so say which one this is. The leading sentence is unchanged
        # because operators, the deploy gate in bootstrap.sh, and the control API
        # all match on it.
        detail = f"Demo setup is currently {manifest.status.upper()}, not READY"
        recovery = _transitional_recovery(manifest.status)
        message = f"{detail}. {recovery}" if recovery else detail
        if manifest.status in TRANSITIONAL_STATUSES:
            # Survivable, and the only non-ready status that is: something else
            # is part-way through a mutation, or died part-way through one.
            # Startup waits it out; every other status still fails loudly.
            raise ManifestMutationInProgressError(
                message,
                status=manifest.status,
                lock_held=_mutation_lock_is_held(),
            )
        raise InvalidStateError(message)
    if require_v2 and (manifest.manifest_version < 2 or manifest.round4 is None):
        raise InvalidStateError("Demo setup is not ready: sealed manifest v2 or newer is required")
    return manifest


def require_ready_manifest() -> None:
    _load_ready_manifest()


def _ambient_workspace_user(workspace: Any) -> str:
    current_user = workspace.current_user.me()
    return str(
        getattr(current_user, "user_name", None)
        or (current_user.get("userName") if isinstance(current_user, dict) else "")
        or ""
    )


def _round_lakebase_endpoint(manifest: DemoManifest, round_number: int) -> str:
    if getattr(manifest, "round_environments", None) is not None:
        return manifest.round_lakebase(round_number).endpoint_name
    return manifest.databricks.endpoint_name


def _coordination_lakebase_endpoint(manifest: DemoManifest) -> str:
    sealed = getattr(manifest, "coordination_lakebase", None)
    if sealed is not None:
        return sealed.endpoint_name
    return (
        f"projects/{manifest.databricks.project_id}/branches/coordination/endpoints/primary"
    )


def _round_isolation_config(manifest: DemoManifest | None) -> tuple[bool, str]:
    """Enable independent ring fences only for a fully identified v7 install."""
    if manifest is None or manifest.manifest_version != 7:
        return False, ""
    if manifest.installation_id is None:
        raise InvalidStateError("Manifest v7 is missing its installation identity")
    return True, manifest.installation_id


def _round5_lease_ring_key(manifest: DemoManifest | None) -> str:
    round_isolation, installation_id = _round_isolation_config(manifest)
    if not round_isolation:
        return ROUND5_RING_KEY
    return round_ring_key(
        installation_id,
        RoundId.SURVIVE_CONNECTION_SPIKE.value,
        cleanup=True,
    )


def _bind_deployed_runtime(manifest: DemoManifest) -> None:
    round4 = manifest.round4
    if round4 is None:
        raise InvalidStateError("Demo setup is not ready: sealed Round 4 identity is missing")
    sealed_client_id = round4.app_service_principal_client_id
    injected_client_id = os.environ.get("DATABRICKS_CLIENT_ID", "").strip()
    if injected_client_id != sealed_client_id:
        raise InvalidStateError(
            "Databricks App client ID does not match the sealed manifest identity"
        )
    for name in (
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "DATABRICKS_PROFILE",
        "DATABRICKS_CONFIG_PROFILE",
    ):
        os.environ.pop(name, None)
    ambient_user = _ambient_workspace_user(WorkspaceClient())
    if ambient_user != sealed_client_id:
        raise InvalidStateError(
            "Ambient Databricks identity does not match the sealed app client ID"
        )

    expected_production = _round_lakebase_endpoint(manifest, 1)
    expected_round4 = _round_lakebase_endpoint(manifest, 4)
    expected_coordination = _coordination_lakebase_endpoint(manifest)
    if (
        manifest.databricks.endpoint_name != expected_production
        or round4.endpoint_name != expected_round4
        or manifest.databricks.coordination_endpoint_name != expected_coordination
        or round4.physical_database != manifest.databricks.database
    ):
        raise InvalidStateError("Sealed Databricks runtime targets are not exact")

    resources = manifest.aws.resources
    bindings = {
        "AWS_AUTH_MODE": "environment",
        "AWS_REGION": manifest.aws.region,
        "AWS_DEFAULT_REGION": manifest.aws.region,
        "AWS_EXPECTED_ACCOUNT_ID": manifest.aws.account_id,
        "AURORA_CLUSTER_ID": resources.aurora_cluster_id,
        "AURORA_SECRET_ARN": resources.aurora_secret_arn,
        "AURORA_DATABASE": manifest.databricks.database,
        "RDS_DATABASE": manifest.databricks.database,
        "LAKEBASE_ENDPOINT_NAME": expected_production,
        "ANTI_DEMO_ROUND2_LAKEBASE_ENDPOINT_NAME": _round_lakebase_endpoint(
            manifest, 2
        ),
        "ANTI_DEMO_ROUND3_LAKEBASE_ENDPOINT_NAME": _round_lakebase_endpoint(
            manifest, 3
        ),
        "LAKEBASE_DATABASE": manifest.databricks.database,
        "LAKEBASE_EXPECTED_REGION": manifest.aws.region,
        "LAKEBASE_USER": sealed_client_id,
        "ANTI_DEMO_COORDINATION_ENDPOINT_NAME": expected_coordination,
        "ANTI_DEMO_COORDINATION_DATABASE": manifest.databricks.database,
        "ANTI_DEMO_COORDINATION_USER": sealed_client_id,
        "EXPECTED_POSTGRES_MAJOR": "17",
    }
    missing = [name for name, value in bindings.items() if not value]
    if missing:
        raise InvalidStateError(
            "Sealed runtime manifest is missing bindings: " + ", ".join(missing)
        )
    # `aws.resources` is Round 1's flat legacy mirror, so these two name Round 1's
    # RDS instance and nothing else. Round 1 stands none up: its RDS lane refuses
    # to enter on engine semantics, so Terraform provisions no instance for it and
    # `manifest._require_round1_legacy_mirror` *forbids* these fields from being
    # filled from another round. Requiring them therefore demanded something no
    # v7 seal is permitted to supply, which stopped the deployed app from
    # starting. Bind them when a pre-v7 seal carries them and never demand them;
    # every round that actually races RDS resolves its own instance from
    # `round_environments[...].rds` through `lifecycle._round_rds_provider`.
    for name, value in (
        ("RDS_INSTANCE_ID", resources.rds_instance_id),
        ("RDS_SECRET_ARN", resources.rds_secret_arn),
    ):
        if value:
            bindings[name] = value
    os.environ.update(bindings)


def _manifest_source_description() -> str:
    """Name the source `load_manifest` just failed on, for the log line below.

    Precedence rather than preference: `load_manifest` consults
    `ANTI_DEMO_MANIFEST_JSON` before it looks at any path, so whenever that
    variable is set it is the thing that failed -- and it is the one a reader is
    least likely to guess, because in deployed mode it arrives from a Databricks
    secret and there is no file anywhere to inspect. Cannot itself raise: it is
    only ever called from inside an `except`, where a second exception would
    replace the diagnosis with a worse one.
    """

    if MANIFEST_JSON_ENV in os.environ:
        return f"the {MANIFEST_JSON_ENV} environment variable"
    try:
        return f"the manifest file at {manifest_path()}"
    except (RuntimeError, OSError, ValueError):
        return "an unidentifiable manifest source"


def _owned_manifest_or_none(manifest: DemoManifest | None) -> DemoManifest | None:
    """Resolve the owned manifest, treating "no installation" as quiet and normal.

    `RuntimeError` -- no manifest selected, or none at the selected path -- is the
    one swallow that must stay silent. A developer checkout has nothing
    configured, and saying so once per round on every start would bury the
    failures that matter under three lines of "as expected". Anything past this
    point is a *sealed* round that could not be built, which is what has to be
    loud.

    A manifest that exists and does not *parse* is the second case, and it used to
    escape. `load_manifest` ends in `DemoManifest.model_validate_json`, so a
    truncated file or a malformed `ANTI_DEMO_MANIFEST_JSON` secret raises
    `pydantic.ValidationError` -- a `ValueError`, and caught by neither the clause
    above nor `build_round`, which sits one frame *inside* each factory and so
    never sees this call at all. It therefore left the lifespan, and a repository
    whose premise is that any one round may be absent without the other five going
    with it had a server that would not start.

    Returning None instead is what this function's name already promises, and no
    caller wants the distinction: all three factories do `if owned is None: return
    None`. So the distinction is carried by the log rather than by the return type
    -- and it has to be carried, because an installation silently serving zero
    live rounds is precisely how the original Round 5 disappearance looked from
    outside. Once per factory, so three lines on a startup that hits this, which
    is the same shape `build_round` already produces per round; deduplicating
    would mean process-global state on the one path that must not grow any.
    """

    if manifest is not None:
        return manifest
    try:
        return load_manifest()
    except RuntimeError:
        return None
    except (OSError, ValueError) as error:
        LOGGER.error(
            "The owned manifest could not be read and is being ignored; every "
            "live round will be absent from this process. Source: %s. Cause: %s",
            _manifest_source_description(),
            exception_diagnostic(error),
            exc_info=True,
        )
        return None


def model_score_factory_from_manifest(
    manifest: DemoManifest | None = None,
) -> Callable[[], ModelScoreEngine] | None:
    """Return a lazy Round 4 factory for any manifest carrying the v2 seal."""
    owned = _owned_manifest_or_none(manifest)
    if owned is None:
        return None

    def build() -> Callable[[], ModelScoreEngine] | None:
        if owned.manifest_version < 2 or owned.round4 is None:
            return None

        def factory() -> ModelScoreEngine:
            from server.model_score_live import build_model_score_engine

            return build_model_score_engine(owned)

        return factory

    return build_round(4, RoundId.PUT_MODEL_SCORE_IN_APP, build)


def _round5_coordination_refusal(
    manifest: DemoManifest,
    lease_store: Any | None,
) -> str | None:
    """Name the precondition a sealed Round 5 is missing, or None if it has them all.

    These used to be four terms of one boolean that returned `None`, so a ring
    key that had drifted -- a bug, and one that would have Round 5 fencing
    another installation's artifacts -- and a developer's in-memory coordinator,
    which is normal, produced the identical symptom: Round 5 gone, nothing said.

    The order is load bearing. `mode` is checked before the ring key because
    resolving the expected ring key reads the manifest's installation identity,
    which a caller holding a partial seal may not have.
    """

    if lease_store is None:
        return "no Round 5 lease store was supplied to the factory"
    mode = getattr(lease_store, "mode", None)
    if mode != "lakebase":
        return (
            f"Round 5 needs durable Lakebase coordination to fence its per-bout "
            f"artifacts, but the lease store mode is {mode!r}"
        )
    expected_ring_key = _round5_lease_ring_key(manifest)
    actual_ring_key = getattr(lease_store, "ring_key", None)
    if actual_ring_key != expected_ring_key:
        return (
            f"the lease store fences ring {actual_ring_key!r}, not the expected "
            f"Round 5 ring {expected_ring_key!r}"
        )
    if not callable(getattr(lease_store, "_run", None)) or not callable(
        getattr(lease_store, "current", None)
    ):
        return "the lease store cannot run the durable Round 5 creation journal"
    return None


def connection_spike_factory_from_manifest(
    manifest: DemoManifest | None = None,
    *,
    lease_store: Any | None = None,
) -> Callable[[CompetitorId], object] | None:
    """Expose Round 5 only for the complete clean-baseline v5-or-later seal."""
    owned = _owned_manifest_or_none(manifest)
    if owned is None:
        return None
    manifest = owned

    def build() -> Callable[[CompetitorId], object] | None:
        if not manifest.round5_ready:
            return None
        refusal = _round5_coordination_refusal(manifest, lease_store)
        if refusal is not None:
            round_unavailable(5, RoundId.SURVIVE_CONNECTION_SPIKE, refusal)
            return None
        # Round 5 is the only round that validates its config eagerly, so this is
        # the only construction that can fail for a reason the seal alone knows
        # about. `antidemo doctor` asks the identical question through
        # `probe_round_construction`.
        round5_configs_build(manifest)
        return factory

    def factory(competitor: CompetitorId) -> object:
        from server.connection_spike_live import (
            LakebaseCreationJournalStore,
            build_connection_spike_live_engine,
        )

        class ActiveLeaseFence:
            async def assert_current(self, scope: Any) -> None:
                active = await lease_store.current()
                if (
                    active is None
                    or active.session_id != scope.bout_id
                    or active.fencing_token != scope.fencing_token
                ):
                    raise InvalidStateError("Round 5 ring fence is no longer current")

        deployed_runtime = os.environ.get("ANTI_DEMO_ENV") == "databricks-app" or bool(
            os.environ.get("DATABRICKS_APP_NAME")
        )
        workspace = (
            WorkspaceClient()
            if deployed_runtime
            else WorkspaceClient(profile=manifest.databricks.profile)
        )

        async def fresh_lakebase_host() -> str:
            endpoint = await asyncio.to_thread(
                workspace.postgres.get_endpoint,
                _round_lakebase_endpoint(manifest, 5),
            )
            status = getattr(endpoint, "status", None)
            hosts = getattr(status, "hosts", None)
            host = str(
                getattr(hosts, "read_write_pooled_host", None)
                or (
                    ((endpoint.get("status") or {}).get("hosts") or {}).get(
                        "read_write_pooled_host"
                    )
                    if isinstance(endpoint, dict)
                    else ""
                )
                or ""
            )
            if not host:
                raise InvalidStateError("Fresh Lakebase pooled host is unavailable")
            return host

        return build_connection_spike_live_engine(
            manifest,
            competitor_id=competitor.value,
            journal=LakebaseCreationJournalStore(
                lease_store._run,
                authority_ring_key=lease_store.ring_key,
            ),
            fence=ActiveLeaseFence(),
            fresh_lakebase_host=fresh_lakebase_host,
        )

    return build_round(5, RoundId.SURVIVE_CONNECTION_SPIKE, build)


def live_orders_factory_from_manifest(
    manifest: DemoManifest | None = None,
) -> Callable[[], LiveOrdersEngine] | None:
    """Expose Round 6 only when its native-CDF contract has been sealed."""
    owned = _owned_manifest_or_none(manifest)
    if owned is None:
        return None

    def build() -> Callable[[], LiveOrdersEngine] | None:
        if not getattr(owned, "round6_ready", False):
            return None

        def factory() -> LiveOrdersEngine:
            from server.live_orders import build_live_orders_engine

            return build_live_orders_engine(owned)

        return factory

    return build_round(6, RoundId.ANALYZE_LIVE_ORDERS, build)


async def _refresh_posted_usage(cache: PostedUsageCache) -> None:
    """Keep the posted read warm without ever putting it on a request.

    The read runs in a worker thread because it is a blocking warehouse round
    trip, and every failure is swallowed for the same reason the reaper's are:
    losing the posted comparison narrows one panel, and taking the event loop
    down over a billing query would lose the demo.
    """

    while True:
        try:
            await asyncio.to_thread(cache.refresh)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - a billing read may never take the app down
            LOGGER.warning("Could not refresh posted Databricks usage", exc_info=True)
        await asyncio.sleep(cache.interval_seconds)


#: Failures that mean the sweep could not *look*, rather than that the sweep is
#: broken. Each is an answer about the environment -- no usable credentials, an
#: absent profile, ambient keys and a profile set together -- and each already
#: has a first-class home in the credential verdict on `/readyz`. They are
#: absorbed and named, not treated as defects, because on this install an
#: expired SSO session is the ordinary reason a sweep cannot run and an error
#: for it every startup would train the reader to ignore the loud one.
_REAP_ENVIRONMENT_FAULTS = (AwsAuthConfigurationError, BotoCoreError, ClientError)


async def _reap_startup_orphans(
    manifest: DemoManifest | None,
    lease_store: Any,
) -> ReapReport:
    """Clean up after the previous process's death, and never block this one.

    Every failure mode here -- an unloadable manifest, an unreachable account, a
    lease store that raises -- has to end in the server serving anyway. Turning
    a cost problem into an outage would be strictly worse than the leak.

    Fail-soft is not the same as silent, and this used to be both. One
    ``except BaseException`` logged "Startup orphan reap could not run" for
    every cause it could have, which conflated two answers that need opposite
    responses: *no usable AWS session*, which is an environment fact the
    credential verdict already reports and which the sweep itself would have
    reached a moment later, and *anything else*, which means the guard against
    leaked billable resources is broken. Both are absorbed -- the sweep may
    never cost an outage -- but only the second is a defect, so only the second
    is logged at error level, and every outcome now names its own cause and
    reaches ``/readyz`` through :func:`reap_health`.
    """

    try:
        from server.lifecycle import _aws_session

        session = _aws_session(manifest) if manifest is not None else None
    except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
        # Shutdown, not a reap failure. Absorbing these turned an interrupt
        # during startup into a server that carried on serving.
        raise
    except _REAP_ENVIRONMENT_FAULTS as exc:
        # Expected, and already reported elsewhere: an expired SSO session, an
        # absent profile, a manifest whose auth block cannot be satisfied. The
        # sweep could not *look*, which `ran: false` says exactly.
        LOGGER.warning("REAP could not open an AWS session: %s", _named(exc))
        return _reap_failure(manifest, f"no AWS session for the sweep ({_named(exc)})")
    except BaseException as exc:  # noqa: BLE001 - startup must survive a broken sweep
        LOGGER.error(
            "REAP IS BROKEN: the startup orphan sweep could not be built, so "
            "nothing swept this run's leaked per-bout clones. Startup "
            "continues; the leak does not clean itself up.",
            exc_info=True,
        )
        return _reap_failure(
            manifest, f"the sweep could not be constructed ({_named(exc)})", broken=True
        )

    try:
        return await reap_startup_orphans(
            manifest,
            reconcile=lambda: reconcile_live(manifest, _aws_session),
            lease_store=lease_store,
            deleter=AwsOrphanDeleter(session) if session is not None else None,
        )
    except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
        raise
    except BaseException as exc:  # noqa: BLE001 - startup must survive a broken sweep
        # `reap_startup_orphans` documents that it never raises and absorbs
        # everything below it itself, so reaching here means that contract is
        # broken rather than that the account was unreachable.
        LOGGER.error(
            "REAP IS BROKEN: the sweep raised past its own never-raises "
            "contract, so nothing swept this run's leaked per-bout clones.",
            exc_info=True,
        )
        return _reap_failure(
            manifest, f"the sweep raised past its own guard ({_named(exc)})", broken=True
        )


def _named(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"


def _reap_failure(
    manifest: DemoManifest | None,
    unavailable: str,
    *,
    broken: bool = False,
) -> ReapReport:
    """A sweep that did not happen, saying which kind of not-happening it was.

    Journaled here rather than only returned. `reap_startup_orphans` writes its
    own audit line, but these two failures happen *before* it is reached, so
    without this the only outcomes missing from `startup-reap.jsonl` were the
    two that most need to outlive the process -- and `/readyz` cannot answer for
    a process that has already died. Best effort, exactly like the sweep's own
    audit: a log file must never be the reason startup fails.
    """

    stamp = datetime.now(UTC).isoformat()
    report = ReapReport(
        mode=reap_mode(),
        run_id=getattr(manifest, "run_id", "") or "",
        unavailable=unavailable,
        failed=True,
        broken=broken,
        started_at=stamp,
        finished_at=stamp,
    )
    write_audit(report)
    return report


@dataclass
class _Runtime:
    """Everything one started runtime owns, so shutdown can give all of it back."""

    lease_store: Any
    round5_lease_store: Any
    cost_ledger_store: Any
    readiness_task: asyncio.Task[None] | None
    posted_usage_task: asyncio.Task[None]
    process_record: Any
    credential_task: asyncio.Task[None] | None = None
    receipt_store: Any = None
    round4_stop_recovery_task: asyncio.Task[None] | None = None


async def _open_receipt_store(lease_store: Any) -> DurableReceiptStore | None:
    """Give this process somewhere durable to keep its bout receipts, or nothing.

    A deployed container has no manifest state directory, so `artifact_root()`
    correctly resolves to nothing and every sealed bout logged "there is nowhere
    to keep a bout receipt" -- which left the app an audience looks at as the one
    surface with no history of its own bouts. The store shares the coordination
    runner with the ring lease for the same reason the readiness row and the
    Round 5 journal do: one endpoint, one credential, one connection shape.

    Every failure here ends in a server that serves, and returning None is how
    that is expressed -- there is no failure of a souvenir worth an outage. It is
    not silent either: an absent table or a refused grant is logged at error
    level, because a deployed app that quietly keeps no history is exactly
    tonight's defect a second time.
    """

    run = getattr(lease_store, "_run", None)
    if getattr(lease_store, "mode", None) != "lakebase" or not callable(run):
        # Process-local coordination keeps process-local receipts: the files
        # beside the manifest are the whole history, which is correct for a
        # developer checkout and already works.
        return None
    store = DurableReceiptStore(run)
    try:
        await store.initialize()
    except Exception:
        LOGGER.error(
            "This process cannot keep durable bout receipts, so its history will "
            "be limited to whatever the local filesystem holds -- on a deployed "
            "replica that is nothing. The recap and the share cards read that "
            "history.",
            exc_info=True,
        )
        return None
    return store


async def _open_pipeline_power_store(lease_store: Any) -> DurablePipelinePowerStore | None:
    """Give this process somewhere durable to record deliberate pipeline stops.

    The app now starts the Round 4 pipeline at arm and stops it once a bout has
    settled, which is what takes that line from ~$14.57/day resident to about
    $0.32 for the bout that needed it. Those two verbs already work without this
    store. What does not work
    without it is *telling anyone afterwards*: `pipeline_power`'s file marker
    resolves through `manifest_path()`, which raises in a Databricks App because
    the manifest arrives as an environment variable and there is no directory to
    sit beside. With no record, a stop the app deliberately made is
    indistinguishable from a pipeline that fell over -- the synced table reports
    `SYNCED_TABLE_ONLINE_PIPELINE_FAILED` either way -- so `doctor` reads red at
    an installation that is doing exactly what it was told.

    Exactly the shape of `_open_receipt_store` above, deliberately, including
    that every failure ends in a server that serves. That matters more here than
    it did there: the deployed branch now boots *degraded* on a bad AWS
    credential rather than refusing, so this line is reached under conditions it
    previously was not, and a store install that raised on a degraded boot would
    turn a serving app into a dead one over a bookkeeping table.
    """

    run = getattr(lease_store, "_run", None)
    if getattr(lease_store, "mode", None) != "lakebase" or not callable(run):
        # Process-local coordination keeps the local marker file, which is the
        # whole record on a developer checkout and already works.
        return None
    store = DurablePipelinePowerStore(run)
    try:
        await store.initialize()
    except Exception:
        LOGGER.error(
            "This process cannot record deliberate Round 4 pipeline stops durably. "
            "It will still stop and start the pipeline, but a later `antidemo "
            "doctor` will report a deliberate, money-saving stop as a Round 4 "
            "failure rather than as the choice it was.",
            exc_info=True,
        )
        return None
    return store


def _deployed_credential_preflight() -> CredentialVerdict | None:
    """Ask AWS whether this deployed app's credentials work, and report the answer.

    Asked exactly as before -- same function, same STS call, same place in the
    startup order, before the lease store or the `RunManager` exists. What has
    changed is that a refusal is no longer fatal.

    Why it stopped being fatal. `AwsAuthConfigurationError` is not a transient
    coordination error, so the retry loop re-raised it on the first attempt, it
    escaped the lifespan, and the container did not start at all -- including
    for Rounds 4 and 6, which reach Lakebase and no AWS whatsoever. That is a
    dated problem rather than a theoretical one: the sweep that deletes this
    account's databases deletes its IAM users with them, so the credentials a
    running process validated once at boot are gone by the next restart, and the
    next restart was the one that would have brought up nothing.

    The state it degrades into is not invented here. It is the state this
    process already reaches when credentials die *under* a serving replica: the
    verdict goes into `credentials_state`, `degraded_capabilities` names every
    lane lost, `ring_ready` is untouched so the app keeps its 200 and stays in
    rotation, and the credential probe keeps re-asking AWS -- so a key published
    after the container started clears this without a restart. That path is
    already built, already honest and already self-healing; the only change is
    that startup now enters it instead of dying in front of it.

    Only `AwsAuthConfigurationError` is absorbed, which is the one class
    `validate_app_aws_environment` raises for a credential it cannot use.
    Anything else out of that function is a fault in the check rather than a
    verdict about the credentials, and reporting one as the other would put a
    bug of ours into a field an operator reads as a fact about their account.

    Loud on the way through, because the reason fail-closed was chosen is that a
    misconfigured app is worse than no app, and that trade only holds if the
    operator is told: error level, the provider's own words, and the traceback.
    """

    try:
        validate_app_aws_environment(os.environ)
    except AwsAuthConfigurationError as exc:
        # `absent` and `rejected` are separated for the same reason the probe
        # separates them: nothing exported and something exported that AWS will
        # not accept need different fixes, and `absent` is terminal -- no amount
        # of re-asking invents a key pair. The predicate is the probe's own, so
        # this verdict and the one the probe reports moments later cannot come to
        # disagree about the same environment.
        #
        # The probe's third state, `misconfigured` -- two credential sources
        # exported at once -- is not reachable here: `_bind_deployed_runtime`
        # pops `AWS_PROFILE` and `AWS_DEFAULT_PROFILE` two lines earlier. If it
        # ever were, this would say `rejected`, which is a total fault either
        # way, and the probe would correct the word within one interval.
        exported = has_any_credential_source(os.environ)
        state = "rejected" if exported else "absent"
        detail = (
            "THIS DEPLOYED APP STARTED WITHOUT USABLE AWS CREDENTIALS and is "
            f"serving degraded rather than refusing to boot: {exc}. It is in "
            "exactly the state it would be in if these credentials had died "
            "under it while it was serving -- the rounds that race a live "
            "Aurora or RDS opponent cannot arm, and Rounds 4 and 6 reach "
            "Lakebase and no AWS at all, so they are unaffected. The credential "
            "probe re-asks AWS on its own interval, so a working key published "
            "into this app clears this without a restart, and a restart clears "
            "nothing that a working key does not."
        )
        LOGGER.error(
            "THE DEPLOYED APP'S AWS CREDENTIAL CHECK WAS REFUSED AT STARTUP and "
            "this process is coming up degraded instead of not coming up at all: "
            "%s. /readyz reports it as credentials_state=%s with the full "
            "diagnosis in credentials_detail. Every round that races a live "
            "Aurora or RDS opponent will refuse to arm until this clears; "
            "Rounds 4 and 6 are unaffected.",
            exc,
            state,
            exc_info=True,
        )
        return CredentialVerdict(
            state=state,
            detail=detail,
            recovery=RecoveryState(
                # `absent` is where the probe itself gives up, so saying
                # anything else here would promise a self-recovery that is not
                # coming. Everything else is still being re-asked.
                "given_up" if state == "absent" else "retrying",
                attempts=1,
                detail=detail,
                error=type(exc).__name__,
            ),
            attempts=1,
            # AWS really was asked, so this is not the unchecked state -- which
            # matters, because `credentials_checked: false` is how a process with
            # no probe at all reports itself.
            checked_at_monotonic=time.monotonic(),
        )
    return None


async def _open_runtime(app: FastAPI, *, deployed: bool) -> _Runtime:
    manifest: DemoManifest | None = None
    lease_store: Any | None = None
    round5_lease_store: Any | None = None
    cost_ledger_store: Any | None = None
    startup_credential_verdict: CredentialVerdict | None = None
    for attempt in range(1, STARTUP_ATTEMPTS + 1):
        candidate_store: Any | None = None
        candidate_round5_store: Any | None = None
        candidate_cost_store: Any | None = None
        try:
            if deployed:
                manifest = _load_ready_manifest(require_v2=True)
                _bind_deployed_runtime(manifest)
                # Ask about the app's service credentials and exact account before
                # the lease store or RunManager can accept an end-user request.
                # Reported rather than raised: see `_deployed_credential_preflight`.
                startup_credential_verdict = _deployed_credential_preflight()
            candidate_store = build_lease_store()
            # A profile-bound localhost process can use the same durable Lakebase
            # fence as the deployed app. It must also reconcile crash residue before
            # reopening the ring; the in-memory developer fallback remains lightweight.
            if not deployed and candidate_store.mode == "lakebase":
                manifest = _load_ready_manifest(require_v2=True)
            candidate_round5_store = build_lease_store(
                ring_key=_round5_lease_ring_key(manifest)
            )
            if manifest is not None and manifest.manifest_version == 7:
                candidate_cost_store = build_cost_ledger_store()
                if candidate_cost_store.mode != "lakebase":
                    raise InvalidStateError(
                        "Manifest v7 requires its durable coordination cost ledger"
                    )
            await candidate_store.initialize()
            await candidate_round5_store.initialize()
            if candidate_cost_store is not None:
                await candidate_cost_store.initialize()
            lease_store = candidate_store
            round5_lease_store = candidate_round5_store
            cost_ledger_store = candidate_cost_store
            break
        except Exception as exc:
            if candidate_cost_store is not None:
                try:
                    await candidate_cost_store.close()
                except Exception:
                    LOGGER.warning("Failed cost ledger startup cleanup", exc_info=True)
            if candidate_round5_store is not None:
                try:
                    await candidate_round5_store.close()
                except Exception:
                    LOGGER.warning("Failed Round 5 startup store cleanup", exc_info=True)
            if candidate_store is not None:
                try:
                    await candidate_store.close()
                except Exception:
                    LOGGER.warning("Failed startup store cleanup", exc_info=True)
            if attempt >= STARTUP_ATTEMPTS or not is_transient_coordination_error(exc):
                raise
            delay = 0.5 * (2 ** (attempt - 1))
            LOGGER.warning(
                "Transient %s during runtime startup; retrying in %.1fs (%d/%d)",
                type(exc).__name__,
                delay,
                attempt,
                STARTUP_ATTEMPTS,
            )
            await asyncio.sleep(delay)

    if lease_store is None or round5_lease_store is None:
        raise RuntimeError("Runtime coordination could not be initialized")
    # Everything from here to the returned `_Runtime` is a partial runtime: the
    # readiness gate below is started as a task that claims a *durable* fenced
    # lease, and the three coordination stores are already open. A failure past
    # this point used to leak all four -- `_round_isolation_config` raises on a v7
    # manifest with no installation identity, and the `RunManager` constructor
    # validates too -- and the readiness gate would be left holding its fence with
    # nothing able to cancel it. That matters most on the transitional-wait path,
    # which re-enters this function every few seconds: each failed attempt would
    # add another gate contending for the same ring, indefinitely.
    readiness_task: asyncio.Task[None] | None = None
    posted_usage_task: asyncio.Task[None] | None = None
    credential_task: asyncio.Task[None] | None = None
    round4_stop_recovery_task: asyncio.Task[None] | None = None
    receipt_store: DurableReceiptStore | None = None
    pipeline_power_store: DurablePipelinePowerStore | None = None
    inherited_round4_stop: dict[str, Any] | None = None
    try:
        receipt_store = await _open_receipt_store(lease_store)
        install_receipt_store(receipt_store)
        pipeline_power_store = await _open_pipeline_power_store(lease_store)
        install_pipeline_power_store(pipeline_power_store)
        if manifest is not None:
            # One read, here, because this is the only moment the answer can be
            # read cheaply and the only question worth asking: did the process
            # before this one leave a stop owed and not come back to perform it?
            # `/readyz` re-evaluates the snapshot against its own due time on
            # every poll, so the redo window is honoured without a second read.
            inherited_round4_stop = await load_owed_stop_snapshot(manifest)
        if deployed or lease_store.mode == "lakebase":
            assert manifest is not None

            def safe_change_cleanup(owned: DemoManifest) -> object:
                from server.safe_change_live import build_safe_change_engine

                return build_safe_change_engine(owned, cleanup_only=True)

            def recovery_cleanup(owned: DemoManifest) -> object:
                from server.recovery_live import build_recovery_engine

                return build_recovery_engine(owned, cleanup_only=True)

            def round5_cleanup(
                competitor_id: str, journal: object, fence: object
            ) -> object:
                from server.connection_spike_live import build_connection_spike_live_engine

                workspace = (
                    WorkspaceClient()
                    if deployed
                    else WorkspaceClient(profile=manifest.databricks.profile)
                )

                async def fresh_lakebase_host() -> str:
                    endpoint = await asyncio.to_thread(
                        workspace.postgres.get_endpoint,
                        _round_lakebase_endpoint(manifest, 5),
                    )
                    status = getattr(endpoint, "status", None)
                    hosts = getattr(status, "hosts", None)
                    host = str(
                        getattr(hosts, "read_write_pooled_host", None)
                        or (
                            ((endpoint.get("status") or {}).get("hosts") or {}).get(
                                "read_write_pooled_host"
                            )
                            if isinstance(endpoint, dict)
                            else ""
                        )
                        or ""
                    )
                    if not host:
                        raise InvalidStateError("Fresh Lakebase pooled host is unavailable")
                    return host

                return build_connection_spike_live_engine(
                    manifest,
                    competitor_id=competitor_id,
                    journal=journal,
                    fence=fence,
                    fresh_lakebase_host=fresh_lakebase_host,
                )

            readiness_gate = ShowtimeReadinessGate(
                manifest,
                lease_store,
                safe_change_factory=safe_change_cleanup,
                recovery_factory=recovery_cleanup,
                round5_factory=round5_cleanup if manifest.round5_ready else None,
                round5_lease_store=round5_lease_store,
                manifest_check=require_ready_manifest,
            )
            readiness_task = asyncio.create_task(
                readiness_gate.run(), name="showtime-startup-readiness"
            )
            readiness_task.add_done_callback(
                lambda task: task.exception() if not task.cancelled() else None
            )
            readiness_verified = True
        else:
            class LocalReadinessGate:
                status = ReadinessStatus(True, "ready", None)
                round5_status = ReadinessStatus(True, "ready", None)

                @staticmethod
                def require_ready() -> None:
                    require_ready_manifest()

                @staticmethod
                def require_round5_ready() -> None:
                    require_ready_manifest()

                @staticmethod
                async def round5_prearm_guard(
                    _session_id: str,
                    _fencing_token: int,
                ) -> None:
                    require_ready_manifest()

            readiness_gate = LocalReadinessGate()
            # Its `status` is a constant, not an observation. The health surface has to
            # say so, or "ready" from this gate reads exactly like "ready" from the
            # durable one that actually reconciled crash residue.
            readiness_verified = False

        app.state.readiness_gate = readiness_gate
        app.state.readiness_verified = readiness_verified
        app.state.round5_lease_store = round5_lease_store
        round_isolation, installation_id = _round_isolation_config(manifest)
        # The standing-cost panel is rebuilt on every read of a session, and the
        # posted half of it is a warehouse round trip. It is refreshed on a task of
        # its own so that no request ever waits on one: until the first refresh
        # lands, `current()` is None and the disclosure renders its unavailable
        # posted state, which is the same honest degradation a billing outage gets.
        posted_usage_cache = PostedUsageCache(manifest)
        app.state.posted_usage_cache = posted_usage_cache
        app.state.run_manager = RunManager(
            lease_store=lease_store,
            round5_lease_store=round5_lease_store,
            readiness_check=readiness_gate.require_ready,
            readiness_status=lambda: readiness_gate.status,
            round5_readiness_check=readiness_gate.require_round5_ready,
            round5_readiness_status=lambda: readiness_gate.round5_status,
            round5_prearm_guard=readiness_gate.round5_prearm_guard,
            model_score_factory=model_score_factory_from_manifest(manifest),
            connection_spike_factory=connection_spike_factory_from_manifest(
                manifest, lease_store=round5_lease_store
            ),
            live_orders_factory=live_orders_factory_from_manifest(manifest),
            round_isolation=round_isolation,
            installation_id=installation_id,
            cost_ledger_store=cost_ledger_store,
            cost_manifest=manifest if cost_ledger_store is not None else None,
            posted_usage=posted_usage_cache.current,
            # The cache, never a live sweep. This hook is called synchronously
            # while a disclosure is rendered, so pointing it at the account would
            # put three paginated describes behind every poll of a session. It
            # reads whatever the last `/readyz` sweep left behind, and None until
            # there has been one -- which the disclosure renders as not-read.
            drift_report=cached_installation_report,
        )
        app.state.round4_stop_recovery = None
        if inherited_round4_stop is not None:
            try:
                if pipeline_power_store is None:
                    raise InvalidStateError(
                        "The inherited stop debt has no durable power store to settle into"
                    )
                workspace = (
                    WorkspaceClient()
                    if deployed
                    else WorkspaceClient(profile=manifest.databricks.profile)
                )
                round4_stop_recovery = build_inherited_round4_stop_recovery(
                    manifest,
                    lease_store,
                    pipeline_power_store,
                    inherited_round4_stop,
                    workspace=workspace,
                )
            except Exception as exc:
                LOGGER.error(
                    "Inherited Round 4 pipeline stop debt remains visible because its "
                    "restart-safe recovery task could not be built: %s",
                    exc,
                    exc_info=True,
                )
                app.state.round4_stop_recovery = RecoveryState(
                    "given_up",
                    attempts=1,
                    detail=(
                        "ROUND 4 PIPELINE STOP RECOVERY COULD NOT START · "
                        f"{type(exc).__name__} · OPERATOR ACTION REQUIRED"
                    ),
                    error=type(exc).__name__,
                )
            else:
                app.state.round4_stop_recovery = round4_stop_recovery
                round4_stop_recovery_task = asyncio.create_task(
                    round4_stop_recovery.run(),
                    name="round4-inherited-pipeline-stop-recovery",
                )
                round4_stop_recovery_task.add_done_callback(
                    lambda task: task.exception() if not task.cancelled() else None
                )
        app.state.coordination_mode = lease_store.mode
        # Before this process claims the pidfile, so the record it reads still
        # describes the *previous* generation. A crashed predecessor leaves an
        # `exited` record and its leaked per-bout clones; a live one leaves a `live`
        # record and the sweep declines. Deliberately not a shutdown hook: the
        # dying process is the worst place to run destructive AWS calls.
        app.state.startup_reap = await _reap_startup_orphans(manifest, lease_store)
        posted_usage_task = asyncio.create_task(_refresh_posted_usage(posted_usage_cache))
        # What the startup check found, kept for the window before the probe's
        # first answer -- and for the case where there is no probe at all,
        # which is the one that would otherwise report a refused credential as
        # an unremarkable `unprobed` forever.
        app.state.startup_credential_verdict = startup_credential_verdict
        credential_task = _start_credential_sentry(app, manifest)
        app.state.restart_history = _restart_history()
        # Claimed only now, so the pidfile means "a server that finished startup is
        # serving this port" and a process that dies during startup never claims it.
        try:
            process_record = register_serving_process()
        except OSError:
            LOGGER.warning("Could not claim the server launch record", exc_info=True)
            process_record = None
        return _Runtime(
            lease_store=lease_store,
            round5_lease_store=round5_lease_store,
            cost_ledger_store=cost_ledger_store,
            readiness_task=readiness_task,
            posted_usage_task=posted_usage_task,
            process_record=process_record,
            credential_task=credential_task,
            receipt_store=receipt_store,
            round4_stop_recovery_task=round4_stop_recovery_task,
        )
    except BaseException:
        # Uninstalled before the stores below are closed: the write hooks are
        # process-global, so a half-started runtime that left either pointing at
        # a closed runner would hand every subsequent bout a failing write.
        install_receipt_store(None)
        install_pipeline_power_store(None)
        # Awaited, not merely requested: `cancel()` only schedules the
        # CancelledError, and a gate that has not yet processed it can still be
        # mid-claim against a store this is about to close.
        for task in (
            round4_stop_recovery_task,
            readiness_task,
            posted_usage_task,
            credential_task,
        ):
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        for store in (round5_lease_store, cost_ledger_store, lease_store):
            if store is None:
                continue
            try:
                await store.close()
            except Exception:
                LOGGER.warning(
                    "Failed to close a coordinator after a startup failure",
                    exc_info=True,
                )
        raise


def _start_credential_sentry(
    app: FastAPI,
    manifest: DemoManifest | None,
) -> asyncio.Task[None] | None:
    """Watch the AWS credentials this process is holding, and only watch them.

    Started last, after everything a bout needs is already built, because it is
    an observer: a server whose credentials are fine must not be delayed by the
    check that says so, and a server whose credentials are broken still has to
    come up and say so through `/readyz` rather than refuse to start. Refusing
    would put the operator in front of a process that will not run instead of a
    page that explains why -- and it would make a transient STS blip during
    startup fatal.

    A local in-memory start has no manifest, so there are no expectations to
    probe against. That is not a failure and must not be logged as one: without
    the early return, `expectations_from_manifest(None)` raised `AttributeError`,
    the handler below turned it into a warning traceback on every dev start, and
    the reported credential state was indistinguishable from a real probe
    failure.

    The sentry is also the readiness gate's only source of "the AWS fault you
    are backing off from has cleared". Handed over as a callback rather than the
    gate polling for it: the sentry already asks AWS on an interval and caches
    the answer, so a second poller would be a second set of calls to learn the
    same thing. Read off `app.state` rather than taken as a parameter so a start
    with no durable gate -- the local in-memory path, the mutation-wait path --
    simply has nothing to notify.
    """

    app.state.credential_sentry = None
    if manifest is None:
        return None
    gate = getattr(app.state, "readiness_gate", None)
    notify = getattr(gate, "notify_credentials_recovered", None)
    try:
        sentry = CredentialSentry(
            expectations_from_manifest(manifest),
            on_recovered=notify if callable(notify) else None,
        )
    except Exception:  # noqa: BLE001 - an observer may never break startup
        LOGGER.warning("Could not start the AWS credential probe", exc_info=True)
        return None
    app.state.credential_sentry = sentry
    task = asyncio.create_task(sentry.run(), name="aws-credential-probe")
    # Its exceptions are consumed here for the same reason the readiness task's
    # are: an unretrieved task exception is a warning on the way out, and this
    # task failing must never look like the application failing.
    task.add_done_callback(lambda done: done.exception() if not done.cancelled() else None)
    return task


async def _close_runtime(app: FastAPI, runtime: _Runtime) -> None:
    try:
        unregister_serving_process(runtime.process_record)
    except OSError:
        LOGGER.warning("Could not release the server launch record", exc_info=True)
    # Before the coordination stores close, and before anything else here can
    # raise: the last bout of a session is the one somebody just watched, and its
    # receipt is in flight on a task that shutdown would otherwise cancel.
    if runtime.receipt_store is not None:
        try:
            await drain_receipt_writes()
        except Exception:
            LOGGER.warning("Could not drain in-flight bout receipts", exc_info=True)
    install_receipt_store(None)
    # This task can hold the exact Round 4 ring and can be inside the guarded
    # pipeline stop. Cancel and await it before the manager or either
    # coordination store closes; requesting cancellation without awaiting would
    # leave the recovery itself as the next zombie.
    if runtime.round4_stop_recovery_task is not None:
        runtime.round4_stop_recovery_task.cancel()
        await asyncio.gather(
            runtime.round4_stop_recovery_task,
            return_exceptions=True,
        )
    app.state.round4_stop_recovery = None
    runtime.posted_usage_task.cancel()
    await asyncio.gather(runtime.posted_usage_task, return_exceptions=True)
    if runtime.credential_task is not None:
        runtime.credential_task.cancel()
        await asyncio.gather(runtime.credential_task, return_exceptions=True)
    app.state.credential_sentry = None
    app.state.restart_history = None
    if runtime.readiness_task is not None:
        runtime.readiness_task.cancel()
        await asyncio.gather(runtime.readiness_task, return_exceptions=True)
    close_manager = getattr(app.state.run_manager, "close", None)
    try:
        if callable(close_manager):
            await close_manager()
    finally:
        # After `close()`, not before it. `RunManager.close()` performs the
        # Round 4 pipeline stop this process owes and records that stop
        # durably, and uninstalling the store first -- which is what used to
        # happen here -- meant that record went nowhere. The whole point of the
        # record is that a lost stop stops being invisible, so the store has to
        # outlive the shutdown path that writes to it.
        install_pipeline_power_store(None)
        try:
            await runtime.round5_lease_store.close()
        finally:
            try:
                if runtime.cost_ledger_store is not None:
                    await runtime.cost_ledger_store.close()
            finally:
                await runtime.lease_store.close()


class _MutationWaitGate:
    """The readiness gate while somebody else's mutation is in flight.

    Observation only, and deliberately so. Everything this class could
    "helpfully" do -- finish the seeding, reset the manifest, resume the reseal --
    is a mutation, and a mutation from the serving process is how a live
    measurement gets silently corrupted. So it reads, refuses, and says why.

    Its shape is a readiness gate because that is the seam the control API and
    `/readyz` already speak through: every control action goes through
    `require_ready`, so a single truthful refusal here covers all of them without
    a second refusal path to keep in step.
    """

    def __init__(self, error: ManifestMutationInProgressError) -> None:
        self.observe(error)

    def observe(self, error: ManifestMutationInProgressError) -> None:
        self.detail = (
            f"A MUTATION IS IN PROGRESS · DEMO SETUP IS {error.status.upper()} · "
            "SHOWTIME WILL UNLOCK WHEN IT FINISHES"
            if error.lock_held
            else (
                f"DEMO SETUP WAS INTERRUPTED WHILE {error.status.upper()} · "
                "NOTHING IS MUTATING IT NOW · AN OPERATOR MUST FINISH IT"
            )
        )
        self.reason = str(error)
        # A held lock means a live mutator, so this clears itself: that is a
        # retrying wait. A free lock means an interrupted run abandoned the
        # status, and nothing but an operator will move it -- the process keeps
        # looking anyway, so the fix needs no restart, but a monitor must not be
        # told to expect self-recovery.
        self.recovery = RecoveryState(
            "retrying" if error.lock_held else "given_up",
            detail=self.detail,
            next_attempt_seconds=MUTATION_WAIT_POLL_SECONDS,
            error=type(error).__name__,
        )
        self.status = ReadinessStatus(
            False,
            "maintenance" if error.lock_held else "blocked",
            self.detail,
        )
        self.round5_status = self.status

    def require_ready(self) -> None:
        raise InvalidStateError(self.reason)

    def require_round5_ready(self) -> None:
        raise InvalidStateError(self.reason)

    async def round5_prearm_guard(
        self,
        _session_id: str,
        _fencing_token: int,
    ) -> None:
        raise InvalidStateError(self.reason)


def _install_mutation_wait(
    app: FastAPI,
    error: ManifestMutationInProgressError,
) -> _MutationWaitGate:
    """Serve a truthful "not yet" instead of refusing to start at all."""

    gate = _MutationWaitGate(error)
    app.state.readiness_gate = gate
    app.state.readiness_verified = False
    app.state.coordination_mode = "pending"
    app.state.round5_lease_store = None
    app.state.startup_reap = None
    app.state.posted_usage_cache = None
    # No credential probe while waiting: the manifest is mid-rewrite, so the
    # account and the Round 5 trusted principal this would hold AWS against are
    # exactly the values somebody else is in the middle of changing. It starts
    # with the rest of the runtime once the status is servable. The startup
    # verdict is cleared for the same reason: `_load_ready_manifest` refused
    # before the credential check was reached, so there is no answer to report.
    app.state.credential_sentry = None
    app.state.startup_credential_verdict = None
    # Read here too: a restart landing in the middle of somebody else's mutation
    # is exactly when it matters most that this process says it is a replacement.
    app.state.restart_history = _restart_history()
    # A real manager, with no coordination and no factories: every control path
    # calls `readiness_check` first, so all of them refuse with the reason above,
    # and the read-only surfaces (`/api/catalog`, `/api/bout`) keep answering
    # honestly instead of raising an AttributeError at a missing manager.
    app.state.run_manager = RunManager(
        readiness_check=gate.require_ready,
        readiness_status=lambda: gate.status,
        round5_readiness_check=gate.require_round5_ready,
        round5_readiness_status=lambda: gate.round5_status,
        round5_prearm_guard=gate.round5_prearm_guard,
    )
    LOGGER.warning(
        "Serving in a waiting state: %s. This process will not write the "
        "manifest; it re-reads it every %.0fs and starts normally once the "
        "status becomes servable.",
        gate.reason,
        MUTATION_WAIT_POLL_SECONDS,
    )
    return gate


async def _open_runtime_when_manifest_settles(
    app: FastAPI,
    *,
    deployed: bool,
    gate: _MutationWaitGate,
    opened: list[_Runtime],
) -> None:
    """Poll a mid-mutation manifest until it is servable, then start normally.

    Reads only. The transition out of this state is somebody else's write
    landing, never one of ours.
    """

    attempts = 0
    first_transient_at: float | None = None
    while True:
        await asyncio.sleep(MUTATION_WAIT_POLL_SECONDS)
        attempts += 1
        try:
            runtime = await _open_runtime(app, deployed=deployed)
        except asyncio.CancelledError:
            raise
        except ManifestMutationInProgressError as exc:
            # Still mid-mutation, and possibly a different shape of it than a
            # moment ago: a live mutator can die while we wait.
            first_transient_at = None
            gate.observe(exc)
            continue
        except Exception as exc:  # noqa: BLE001 - classified immediately below
            if is_transient_coordination_error(exc):
                # The mutation cleared and startup is now failing for its own
                # reasons. Keep saying "waiting for a mutation" and the health
                # surface would be describing a problem that has moved on.
                if first_transient_at is None:
                    first_transient_at = time.monotonic()
                escalated = (
                    time.monotonic() - first_transient_at
                ) >= GATE_ESCALATE_AFTER_SECONDS
                gate.detail = (
                    f"STARTUP IS RETRYING AFTER THE MUTATION FINISHED · "
                    f"{type(exc).__name__.upper()} · ATTEMPT {attempts}"
                )
                gate.reason = f"Runtime startup is retrying a transient failure: {exc}"
                gate.status = ReadinessStatus(
                    False,
                    "blocked" if escalated else "maintenance",
                    gate.detail,
                )
                gate.round5_status = gate.status
                gate.recovery = RecoveryState(
                    "escalated" if escalated else "retrying",
                    attempts=attempts,
                    detail=gate.detail,
                    next_attempt_seconds=MUTATION_WAIT_POLL_SECONDS,
                    error=type(exc).__name__,
                )
                LOGGER.warning(
                    "Runtime startup is still failing after the mutation cleared; "
                    "retrying (attempt %d)",
                    attempts,
                    exc_info=True,
                )
                continue
            # The manifest became servable and startup then failed for a reason
            # no retry clears. The lifespan has already yielded, so this cannot
            # be raised; it has to be reported instead, and loudly.
            gate.detail = (
                "STARTUP FAILED AFTER THE MUTATION FINISHED · "
                f"{type(exc).__name__} · AN OPERATOR MUST LOOK"
            )
            gate.reason = f"Runtime startup failed after the mutation finished: {exc}"
            gate.status = ReadinessStatus(False, "blocked", gate.detail)
            gate.round5_status = gate.status
            gate.recovery = RecoveryState(
                "given_up",
                attempts=attempts,
                detail=gate.detail,
                error=type(exc).__name__,
            )
            LOGGER.error(
                "Runtime startup failed after the manifest became servable; "
                "this process will keep serving a refused state",
                exc_info=True,
            )
            return
        opened.append(runtime)
        LOGGER.warning(
            "The mutation finished after %d checks; the runtime is now serving normally",
            attempts,
        )
        return


@asynccontextmanager
async def lifespan(app: FastAPI):
    deployed = os.environ.get("ANTI_DEMO_ENV") == "databricks-app" or bool(os.environ.get(
        "DATABRICKS_APP_NAME"
    ))
    try:
        runtime = await _open_runtime(app, deployed=deployed)
    except ManifestMutationInProgressError as exc:
        # The one startup failure that is not this process's to fix. Refusing to
        # start turned somebody else's in-flight (or half-dead) mutation into an
        # outage that outlived it; waiting does not.
        gate = _install_mutation_wait(app, exc)
        waiting_manager = app.state.run_manager
        opened: list[_Runtime] = []
        waiter = asyncio.create_task(
            _open_runtime_when_manifest_settles(
                app, deployed=deployed, gate=gate, opened=opened
            ),
            name="manifest-mutation-wait",
        )
        try:
            yield
        finally:
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
            try:
                for started in opened:
                    await _close_runtime(app, started)
            finally:
                await waiting_manager.close()
        return
    try:
        yield
    finally:
        await _close_runtime(app, runtime)


app = FastAPI(
    title="Lakebase: The Anti-Demo",
    version="0.1.0",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://127.0.0.1:5173", "http://localhost:5173"],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type", "Last-Event-ID"],
)


@app.middleware("http")
async def response_safety_headers(request: Any, call_next: Callable[..., Any]) -> Response:
    response = await call_next(request)
    path = request.url.path
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    if path.startswith("/api/") or path in {"/healthz", "/readyz"}:
        response.headers["Cache-Control"] = "no-store"
    elif path.startswith("/assets/") and response.status_code == 200:
        # Vite content-hashes every production asset, so these files are immutable.
        response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
    return response


app.include_router(router)


_UNVERIFIED_READINESS_DETAIL = (
    "COORDINATION IS PROCESS-LOCAL: ring_ready is a constant here, not a checked "
    "state. Nothing reconciled crash residue and no other process is fenced out."
)
_UNKNOWN_COORDINATION_DETAIL = (
    "COORDINATION MODE IS NOT ESTABLISHED: startup has not finished choosing a "
    "lease store, so readiness cannot be believed either way."
)
_UNCHECKED_GATE_DETAIL = (
    "READINESS IS NOT BEING CHECKED: coordination is durable but no readiness gate "
    "is running in this process."
)
_PENDING_COORDINATION_DETAIL = (
    "COORDINATION IS NOT ESTABLISHED YET: the manifest is part-way through a "
    "mutation, so no ring has been opened. Nothing is lost and nothing is fenced; "
    "this process is waiting and will start normally when the status is servable."
)
#: What `/readyz` says when the manifest has reached the one status that refuses
#: every control action and will never clear on its own. `cleanup_failed` is set
#: by the destroy path's `except Exception:` and by nothing else, so it means a
#: teardown stopped part-way and nobody has decided what survived.
_MANIFEST_REFUSED_DETAIL = (
    "THE MANIFEST STATUS IS 'cleanup_failed': a teardown stopped part-way and "
    "this installation refuses every control action until a person settles it. "
    "Arming any round raises 'Demo setup is currently CLEANUP_FAILED, not READY'. "
    "This status is not resumable -- 'antidemo setup' and 'antidemo renew' both "
    "refuse it by name. Run 'antidemo cleanup --dry-run' and read what it finds. "
    "Sealed resources may still exist and bill."
)
#: A transitional status is the ordinary shape of somebody else's mutation, and
#: it clears itself. It is published so it is never invisible, but it does not
#: degrade -- `never_checked` does not either, and for the same reason: every
#: reseal would otherwise flap the field an operator checks before a demo.
_MANIFEST_MUTATING_DETAIL = (
    "THE MANIFEST IS MID-MUTATION: control actions refuse until it settles, which "
    "is expected to happen without an operator. Nothing here needs a restart."
)
#: No readable manifest. Reported, deliberately not degraded: this is the
#: ordinary state of a process serving the in-memory developer fallback, which
#: needs no owned manifest at all, and degrading on it would fire on every local
#: run rather than on a fault. The deployed path cannot reach it quietly -- it
#: loads the manifest at startup through `_load_ready_manifest` and refuses to
#: come up at all if that fails.
_MANIFEST_UNREADABLE_DETAIL = (
    "THE MANIFEST COULD NOT BE READ: this process has no owned manifest selected, "
    "so its lifecycle status is unknown rather than healthy. This is expected on "
    "the process-local developer fallback and a fault anywhere else."
)
_RETRYING_DETAIL = (
    "RECOVERY IS IN PROGRESS: a transient failure is being retried in this process "
    "with bounded backoff, and no restart is required for it to clear."
)
_ESCALATED_DETAIL = (
    "RECOVERY HAS OUTLASTED ITS BUDGET: retries continue at the ceiling interval, so "
    "this can still clear itself, but it has been failing long enough that somebody "
    "should look at it."
)
_GIVEN_UP_DETAIL = (
    "RECOVERY HAS STOPPED: the last failure is one that retrying cannot clear, so "
    "nothing further will be attempted without an operator."
)
_RECOVERY_DETAILS = {
    "retrying": _RETRYING_DETAIL,
    "escalated": _ESCALATED_DETAIL,
    "given_up": _GIVEN_UP_DETAIL,
}


def _recovery_detail(recovery_state: str, waiting_on: object) -> str:
    """The recovery sentence, naming what is being waited for when that is known.

    A gate retrying an expired SSO session is not broken, and the difference
    matters most to whoever is deciding at 2am whether to restart the process --
    which would lose the retry and fix nothing. So when the fault is
    environmental, the sentence says what has to come back and that nothing needs
    restarting to pick it up.
    """

    base = _RECOVERY_DETAILS[recovery_state]
    if not isinstance(waiting_on, str) or not waiting_on:
        return base
    if recovery_state == "given_up":
        return base
    return (
        f"THIS SERVER IS WAITING ON {waiting_on.upper()}, NOT BROKEN: startup "
        "cleanup cannot finish until they are usable again, and it is still "
        "retrying indefinitely, so it will recover on its own once they are -- "
        f"no restart is needed and a restart would not help. {base}"
    )

#: Credential verdicts that stop every lane, as opposed to narrowing one round.
#: These take the single `degraded_detail` slot from anything else that wants it:
#: a ring being retried does not matter to anybody if no bout can reach AWS at
#: all, so the more fundamental fault is the one worth reading first.
_TOTAL_CREDENTIAL_FAULTS = frozenset(
    {"absent", "misconfigured", "rejected", "wrong_account", "unpermitted", "stale"}
)
_ROUND5_RECOVERY_DETAILS = {
    "retrying": (
        "THE ROUND 5 RECONCILER IS RETRYING a transient failure. The ring is ready; "
        "round 5 settlement is behind until this clears, which needs no restart."
    ),
    "escalated": (
        "THE ROUND 5 RECONCILER HAS BEEN FAILING for longer than its budget. It is "
        "still retrying, but round 5 should not be armed until it settles."
    ),
    "given_up": (
        "THE ROUND 5 RECONCILER HAS STOPPED on a failure retrying cannot clear. The "
        "ring is ready, so the other rounds are unaffected, but round 5 will not "
        "settle without an operator."
    ),
}


#: There is one `degraded_detail` sentence and seven signals that want it, so the
#: order is written down in one place rather than left to be reconstructed from
#: the call order of the blocks below. The rule is breadth of outage: the fault
#: that stops the most wins the sentence, because that is the one an operator
#: needs to read first. Every losing signal is still readable in a field of its
#: own, so nothing is hidden by losing -- only unsaid in that one line.
#:
#:   1. a credential fault that stops every lane (`_TOTAL_CREDENTIAL_FAULTS`)
#:   2. the ring gate's own recovery
#:   3. the round 5 reconciler's recovery
#:   4. the coordination mode / unverified readiness
#:   5. the sealed AWS infrastructure being gone, or unverifiable
#:   6. operator ingress drift (the direct Aurora and RDS rounds)
#:   7. a narrower credential fault, then a recent restart
#:
#: Five sits above six because a missing installation subsumes a stale allowance:
#: there is no security group left for an address to be wrong about. It sits below
#: one deliberately, and that ordering is the whole point on a real reap -- the
#: sweep that deletes the databases deletes the IAM users too, so the account
#: cannot be read, the presence signal is `unverified` rather than `missing`, and
#: the credential fault is both the wider outage and the true root cause. Letting
#: presence take the sentence there would report a guess over a fact.
#:
#: The startup orphan sweep is deliberately *not* on this list, even though it
#: is reported in the same payload by `_apply_startup_reap`. Every rank above is
#: a functional fault -- something that stops a lane, a round, or the ring --
#: and this list is ordered by breadth of outage. A sweep that did not run stops
#: nothing; its cost is spend. Giving it a rank would mean pretending it has a
#: breadth of outage, and lowering `status` for it would make the one field an
#: operator checks before a demo less trustworthy rather than more -- the same
#: reason `never_checked` and a credential verdict of `unknown` are reported
#: without degrading. It is not silent: `startup_reap_state` distinguishes a
#: broken sweep from one that could not see the account from one that swept
#: cleanly, and a broken one is logged at error level as well. Promote it here
#: only if a sweep failure is ever shown to stop a round from arming.
#:
#: An owed Round 4 pipeline stop and an undeleted Round 5 RDS Proxy are off the
#: list for the identical reason, and each says so at its own applier. All three
#: are spend, none of them is availability, and the three of them together are
#: why the rule is written here rather than re-argued each time.
#:
#: A `cleanup_failed` manifest is the one signal that borrowed their *mechanism*
#: -- dedicated fields of its own -- while failing their *test*. It is not spend.
#: `ShowtimeReadinessGate.require_ready` calls the manifest check before it looks
#: at `ring_ready` at all, so the status stops every round on every lane,
#: including Rounds 4 and 6, which survive even rank one. It is therefore wider
#: than anything on the list above and it degrades. It is still ranked *last* for
#: the one detail sentence, deliberately: ranking it first by breadth would let
#: it displace a live fault, and it is the one signal whose whole diagnosis is
#: already guaranteed a field of its own in `manifest_lifecycle_detail`. See
#: `_apply_manifest_lifecycle`.
#:
#: Separately from the sentence: every one of these sets `degraded` true, and
#: every one lowers `status` from "ready" to "degraded" and no further. None of
#: them changes the status code, which answers a different question -- whether
#: the ring can serve at all -- so a degraded process keeps its 200 and stays in
#: rotation. Only a not-ready ring is a 503.
def _readiness_response(
    ingress_drift: OperatorIngressDrift | None = None,
    presence: InstallationPresence | None = None,
) -> JSONResponse:
    gate = getattr(app.state, "readiness_gate", None)
    current = getattr(gate, "status", None)
    ring_ready = bool(getattr(current, "ring_ready", False))
    maintenance_state = str(getattr(current, "maintenance_state", "starting"))
    detail = getattr(current, "maintenance_detail", None)
    # Whether anybody is still working on a not-ready ring, which
    # `maintenance_state` cannot say. Without it, a blip being retried and a
    # permanent fault nobody will touch again both read as "not_ready" forever,
    # so a monitor cannot tell the difference and neither can an operator.
    recovery = getattr(gate, "recovery", None) or SETTLED
    recovery_state = str(getattr(recovery, "state", "settled"))
    # Round 5's reconciler runs for the life of the process, long after the ring
    # is ready. Reported separately because it does not make the ring unready --
    # but a reconciler that has quietly stopped is exactly the drift this box is
    # meant not to have, so it cannot be invisible either.
    round5_recovery = getattr(gate, "round5_recovery", None) or SETTLED
    round5_recovery_state = str(getattr(round5_recovery, "state", "settled"))
    # Absent a sweep, the honest answer is that nobody has looked -- never
    # `verified_present`, which is the whole reason this signal exists.
    installation = presence if presence is not None else presence_from_report(None)
    # `ring_ready` alone cannot separate a healthy process from a degraded one:
    # the local fallback's gate hardcodes ready, so this endpoint reported the
    # same green answer whether or not readiness had ever been checked. The
    # coordination mode and whether the gate is a real one are reported beside
    # it, and a ring that claims ready without being checked is named degraded.
    coordination_mode = str(getattr(app.state, "coordination_mode", "") or "unknown")
    durable = coordination_mode == "lakebase"
    readiness_verified = durable and bool(getattr(app.state, "readiness_verified", False))
    if not ring_ready:
        status = "not_ready"
    elif readiness_verified:
        status = "ready"
    else:
        status = "degraded"
    payload = {
        "status": status,
        "ring_ready": ring_ready,
        # When this process started. Nothing else in this payload separates a
        # container that has been up for days from one that came back a minute
        # ago, so how often the platform recycles the deployed app was pure
        # guesswork -- and on the deployed path `restarts` cannot answer it
        # either, for the reason `_apply_restart_history` gives. Two polls of
        # this endpoint now do: the value changing *is* the restart.
        #
        # An instant rather than an elapsed count, deliberately. A reader needs
        # no clock of its own to compare this against itself, and a payload
        # carrying a number that moves between two calls would break the one
        # assertion that proves `/api/ready` is the same endpoint as `/readyz`.
        "process_started_at": _PROCESS_STARTED_AT.isoformat(),
        "maintenance_state": maintenance_state,
        "maintenance_detail": detail,
        "coordination_mode": coordination_mode,
        "coordination_durable": durable,
        "readiness_verified": readiness_verified,
        "degraded": not readiness_verified,
        "degraded_detail": None,
        "degraded_capabilities": [],
        # "settled" | "retrying" | "escalated" | "given_up". Only the first two
        # are expected to clear without a human: "escalated" still retries but
        # has been failing long enough to alert on, and "given_up" will not
        # change until an operator changes something.
        "recovery_state": recovery_state,
        "recovery_detail": getattr(recovery, "detail", None),
        "recovery_attempts": int(getattr(recovery, "attempts", 0) or 0),
        "recovery_next_attempt_seconds": getattr(recovery, "next_attempt_seconds", None),
        "recovery_error": getattr(recovery, "error", None),
        # What outside this process has to come back before a retry can work.
        # Without it, an expired SSO session and a server thrashing on a real
        # bug both read as `recovery_state: "retrying"`, and the one with a
        # one-command fix is indistinguishable from the one that needs a person.
        "recovery_waiting_on": getattr(recovery, "waiting_on", None),
        "recovering": recovery_state in {"retrying", "escalated"},
        "round5_recovery_state": round5_recovery_state,
        "round5_recovery_detail": getattr(round5_recovery, "detail", None),
        # Four states, deliberately not a boolean: "the account was read and they
        # are gone", "the account could not be read", "read and they are there",
        # and "not asked yet in this process". Three of those are not a green
        # light, and only one of them means anything needs re-creating.
        "installation_state": installation.state,
        "installation_detail": installation.detail,
        "installation_sealed_resources": installation.sealed,
        "installation_absent_resources": installation.absent,
        "installation_checked": installation.checked,
    }
    if coordination_mode == "unknown":
        payload["degraded_detail"] = _UNKNOWN_COORDINATION_DETAIL
    elif coordination_mode == "pending":
        # Not the in-memory ring's losses: no ring has been opened at all, so
        # naming cross-process fencing as lost here would be a false alarm.
        payload["degraded_detail"] = _PENDING_COORDINATION_DETAIL
    elif not durable:
        payload["degraded_detail"] = _UNVERIFIED_READINESS_DETAIL
        payload["degraded_capabilities"] = list(INMEMORY_COORDINATION_LOSSES)
    elif not readiness_verified:
        payload["degraded_detail"] = _UNCHECKED_GATE_DETAIL
    if round5_recovery_state != "settled":
        payload["degraded"] = True
        payload["degraded_detail"] = _ROUND5_RECOVERY_DETAILS[round5_recovery_state]
        if payload["status"] == "ready":
            # The point of the three-value status is that a monitor comparing
            # against "ready" fails rather than being reassured. A reconciler
            # that has stopped is precisely that case.
            payload["status"] = "degraded"
    if recovery_state != "settled":
        # The ring gate outranks the round 5 reconciler for the one detail slot:
        # if the ring is not settled, round 5 is downstream of that anyway.
        # A recovery state is the more specific answer, so it wins the one
        # `degraded_detail` slot. `degraded_capabilities` is left as whatever the
        # coordination mode put there: what a degraded ring cannot do does not
        # change because something is being retried.
        payload["degraded"] = True
        payload["degraded_detail"] = _recovery_detail(
            recovery_state,
            payload["recovery_waiting_on"],
        )
        if payload["status"] == "ready":
            # Same reasoning as the round 5 block above, and it matters more
            # here: a ring being retried, escalated, or given up on is the
            # server's central fault, and until now it was the one degraded
            # signal that left `status` saying "ready". Guarded so it can only
            # ever lower a ready ring, never lift a not-ready one to "degraded".
            payload["status"] = "degraded"
    if installation.state in {PRESENCE_MISSING, PRESENCE_UNVERIFIED}:
        # Degraded rather than not_ready, on the same reasoning as ingress drift
        # below: the ring is still able to serve, Rounds 4 and 6 need no AWS at
        # all, and turning an observation into a 503 would take the app out of
        # rotation over something a monitor can already read here.
        #
        # `unverified` degrades as well as `missing`, because a surface that
        # cannot see the account and says nothing is the defect this whole signal
        # exists to close. It claims no lost capabilities though -- nothing is
        # known to be lost -- and it never says anything is missing.
        #
        # `never_checked` deliberately does not degrade. It is the first seconds
        # of a process's life before the first sweep answers, exactly like the
        # credential verdict's "unknown" below, and degrading on it would make
        # every startup flap. It is still reported in its own field.
        payload["degraded"] = True
        payload["degraded_capabilities"] = [
            *payload["degraded_capabilities"],
            *installation.capabilities,
        ]
        if payload["degraded_detail"] is None:
            payload["degraded_detail"] = installation.detail
        if payload["status"] == "ready":
            payload["status"] = "degraded"
    if ingress_drift is not None:
        # Degraded, never not_ready. A 503 takes the app out of rotation, and
        # this condition breaks only the rounds that connect straight to Aurora
        # or RDS -- the Lakebase lanes are unaffected -- so refusing to serve
        # would turn a diagnosable fault into an outage. The ring is still ready.
        payload["degraded"] = True
        if payload["status"] == "ready":
            payload["status"] = "degraded"
        payload["degraded_capabilities"] = [
            *payload["degraded_capabilities"],
            *ingress_drift.capabilities,
        ]
        if payload["degraded_detail"] is None:
            # Yields to the ring, round 5 and the coordination mode, and is
            # claimed before `_apply_credential_verdict` runs so that it also
            # outranks a narrow credential fault: losing a whole class of rounds
            # is the wider outage. A total credential fault still takes the slot
            # back off it, which is the same ordering by breadth.
            payload["degraded_detail"] = ingress_drift.detail
    _apply_credential_verdict(payload)
    _apply_restart_history(payload)
    _apply_startup_reap(payload)
    _apply_round4_stop_recovery(payload)
    _apply_owed_pipeline_stop(payload)
    _apply_owed_round5_cleanup(payload)
    # Last, so it yields the one `degraded_detail` sentence to all seven ranked
    # claimants above. It still degrades and still lowers `status`: it is the
    # only signal down here that costs availability rather than spend.
    _apply_manifest_lifecycle(payload)
    return JSONResponse(payload, status_code=200 if ring_ready else 503)


def _apply_manifest_lifecycle(payload: dict[str, Any]) -> None:
    """Report the manifest lifecycle status the control gate actually enforces.

    **This endpoint reported `status: "ready", degraded: false` for twenty-six
    minutes against a manifest that was refusing every arm.** The process bound a
    `ready` manifest at startup, `cleanup_failed` was written underneath it half
    an hour later, and nothing here noticed, because everything above reads
    `app.state` and the manifest is on disk. `ring_ready` and `maintenance_state`
    come off ``ShowtimeReadinessGate._status``, which only ever describes the ring
    lease reconciliation -- the manifest check is a *second* conjunct that lives
    outside it::

        def require_ready(self) -> None:
            self._manifest_check()          # <- refuses cleanup_failed
            if not self._status.ring_ready: # <- the only half /readyz published

    So the endpoint was publishing one of the two things `require_ready` checks
    and calling the answer "ready". Read fresh on every poll rather than cached,
    because staleness is the entire defect: the check the gate runs reads the file
    on every control action, and a health surface that answers from a snapshot
    taken at startup is how this got missed. It costs a local file read (~0.3ms,
    measured) and no socket, so unlike the AWS probes above there is nothing to
    amortise.

    **This degrades, where the three signals below deliberately do not.** They are
    spend -- a leaked Proxy, an owed pipeline stop, a sweep that did not run --
    and none of them stops a round. This one stops *every* round, on every lane,
    including Rounds 4 and 6, which survive even a total credential fault. By the
    breadth-of-outage rule written above the precedence list that is the widest
    fault this payload can carry, so calling it `ready` is not a narrower true
    proposition, it is the wrong answer to the question `status` asks.

    **It takes the `degraded_detail` slot only when nothing else wants it.** There
    is one sentence and seven claimants already, and a pre-existing degradation
    masking a new one is this repository's recurring injury. Ranking last cannot
    displace anything, and nothing is lost by losing: the full sentence is always
    in ``manifest_lifecycle_detail``, which no other signal can take.

    All three keys are present on every response, so a reader cannot mistake a
    missing key for a healthy manifest -- the same rule the owed-stop fields keep.
    """

    try:
        status = str(load_manifest().status)
    except Exception:
        # Never the reason a health endpoint stops answering. An unreadable
        # manifest is reported in its own field and does not degrade; see
        # `_MANIFEST_UNREADABLE_DETAIL` for why that is not a swallowed fault.
        payload["manifest_status"] = None
        payload["manifest_lifecycle_state"] = "unreadable"
        payload["manifest_lifecycle_detail"] = _MANIFEST_UNREADABLE_DETAIL
        return

    payload["manifest_status"] = status
    if status == "ready":
        payload["manifest_lifecycle_state"] = "ready"
        payload["manifest_lifecycle_detail"] = None
        return
    if status in TRANSITIONAL_STATUSES:
        payload["manifest_lifecycle_state"] = "mutating"
        payload["manifest_lifecycle_detail"] = _MANIFEST_MUTATING_DETAIL
        return

    payload["manifest_lifecycle_state"] = "refused"
    payload["manifest_lifecycle_detail"] = _MANIFEST_REFUSED_DETAIL
    payload["degraded"] = True
    if payload["status"] == "ready":
        # Guarded exactly like every block above it, so it can only ever lower a
        # ready answer and never lift a not-ready ring. Degraded rather than
        # not_ready on the settled rule: a 503 takes the app out of rotation, and
        # the process can still serve the page that explains why nothing arms.
        payload["status"] = "degraded"
    if payload["degraded_detail"] is None:
        payload["degraded_detail"] = _MANIFEST_REFUSED_DETAIL


def _apply_owed_round5_cleanup(payload: dict[str, Any]) -> None:
    """Report an RDS Proxy this app created and could not prove it deleted.

    **The most expensive thing any round creates, and until now the one nobody
    was told about.** Round 5 stands up an RDS Proxy per bout and is the only
    thing that deletes it. A towel thrown during setup left one ``available``
    for twenty minutes while this endpoint answered ``status: ready, degraded:
    false``: the towel sat at ``cleaning``, ``cleanup_failure`` was null because
    the automatic retry had not exhausted its budget yet, and no receipt had
    sealed. Every one of those was individually defensible and the sum of them
    was an operator with no signal at all. A surface that reports health has to
    name what it actually checked.

    ``round5_setup.cleanup_failure`` and the sealed receipt do carry this, and
    both still do. Neither reaches somebody who is not already looking at that
    bout, which is the same gap ``_apply_owed_pipeline_stop`` was added to close
    for Round 4 -- an operator under the standing posture inputs nothing, and
    ``/readyz`` is the surface reachable without input.

    **Nothing here touches ``status``, ``degraded`` or ``degraded_detail``**, on
    the rule ``_apply_startup_reap`` and ``_apply_owed_pipeline_stop`` both
    record: spend is not availability. A leaked Proxy stops no round from arming
    -- the next bout builds its own under a different deterministic name -- and
    lowering the one field an operator checks before a demo, for a money
    problem, makes that field less trustworthy rather than more. Promote it only
    if a leaked Proxy is ever shown to stop a round.

    ``round5_cleanup_owed_since`` is the actionable datum, exactly as it is for
    the Round 4 stop: how long it has been billing. All three keys are present
    and ``None``/``False`` when there is nothing owed, so a reader cannot
    mistake a missing key for a cleanup that succeeded.
    """

    notice = round5_cleanup_owed_notice()
    payload["round5_cleanup_owed"] = notice is not None
    payload["round5_cleanup_owed_since"] = notice.since if notice is not None else None
    payload["round5_cleanup_owed_detail"] = notice.detail if notice is not None else None


def _apply_owed_pipeline_stop(payload: dict[str, Any]) -> None:
    """Report a Round 4 pipeline stop a previous process owed and never made.

    **This is the only place the deployed app says this at all.** The mitigation
    for a forgotten stop is `pipeline_power.session_notice`, printed by
    `cli._serve`; `app.yaml` runs `python -m uvicorn app:app`, so `cli.py` never
    executes here and that notice has never once run on the installation that
    actually bills. `doctor` is a CLI command and reaches a deployed operator no
    better. `/readyz` is what is left: an operator under the standing posture
    inputs nothing, and this is the surface reachable without input.

    Three other channels were considered and rejected. `maintenance_detail`
    reaches a rendered banner, but only while the ring is locked -- borrowing it
    would announce maintenance that is not happening. `/api/catalog` would have
    to call the round unavailable, which is the defect that shipped tonight when
    it advertised rounds as ready after two failed arms; an owed stop breaks no
    round, since the next arm finds the pipeline already up. A startup log line
    can only ask the question once, and a process that restarts promptly asks it
    inside the redo window, where the correct answer is silence -- so it would be
    quiet in exactly the case it exists for.

    **Nothing here touches ``status``, ``degraded`` or ``degraded_detail``**, on
    the reasoning `_apply_startup_reap` records above and for the identical
    reason: this costs spend, not availability. It stops no round from arming,
    and lowering the one field an operator checks before a demo for a money
    problem would make that field less trustworthy rather than more. Promote it
    only if an owed stop is ever shown to stop a round.

    ``round4_stop_owed_since`` is the actionable datum -- how long it has been
    leaking -- and is ``None`` rather than absent when there is nothing owed, so
    a reader cannot mistake a missing key for a settled pipeline.
    """

    notice = owed_stop_notice()
    payload["round4_stop_owed"] = notice is not None
    payload["round4_stop_owed_since"] = notice.since if notice is not None else None
    payload["round4_stop_owed_detail"] = notice.detail if notice is not None else None


def _apply_round4_stop_recovery(payload: dict[str, Any]) -> None:
    """Report what this replica is doing about inherited Round 4 stop debt.

    Kept separate from the debt fields below: the debt is durable authority,
    while this is process-local work against it. A permanent refusal must remain
    visible even though the serving process and the other five rounds stay
    healthy, and a transient retry must say that nobody has silently given up.
    """

    recovery = getattr(app.state, "round4_stop_recovery", None)
    status = getattr(recovery, "status", recovery) or SETTLED
    payload["round4_stop_recovery_state"] = str(
        getattr(status, "state", "settled")
    )
    payload["round4_stop_recovery_attempts"] = int(
        getattr(status, "attempts", 0) or 0
    )
    payload["round4_stop_recovery_detail"] = getattr(status, "detail", None)
    payload["round4_stop_recovery_next_attempt_seconds"] = getattr(
        status,
        "next_attempt_seconds",
        None,
    )
    payload["round4_stop_recovery_error"] = getattr(status, "error", None)
    payload["round4_stop_recovery_lease_held"] = bool(
        getattr(recovery, "lease_held", False)
    )


def _apply_startup_reap(payload: dict[str, Any]) -> None:
    """Report what the startup orphan sweep actually did, or could not do.

    The sweep's report was already being kept on ``app.state.startup_reap`` and
    nothing read it, so a sweep that failed was visible only in a log line --
    which is the shape of failure that let a leaked Aurora writer run for
    fifty-seven minutes. Six states, deliberately not a boolean, and five of
    them are not a green light:

    * ``swept`` -- it looked, and ``observed_orphans`` means something
    * ``unavailable`` -- it tried and could not see the account
    * ``broken`` -- it tried and the sweep itself failed
    * ``skipped`` -- there was nothing for it to do, which is the ordinary state
      of a process serving an installation it does not own
    * ``disabled`` -- switched off by an operator
    * ``not_started`` -- the first moments of a process, before startup finishes

    ``startup_reap_observed_orphans`` is ``None`` unless the sweep genuinely
    looked. A zero from a sweep that could not reach the account is the same lie
    as a green health check that never ran, and is precisely what the 03:39Z
    record in ``startup-reap.jsonl`` said.

    Nothing here touches ``status``, ``degraded`` or ``degraded_detail``. See the
    note above the precedence list for why, and for what would justify changing
    that.
    """

    health: ReapHealth = reap_health(getattr(app.state, "startup_reap", None))
    payload["startup_reap_state"] = health.state
    payload["startup_reap_detail"] = health.detail
    payload["startup_reap_observed_orphans"] = health.observed_orphans


#: What `/readyz` says when this runtime has no record to read *from*, as
#: opposed to a record that says nothing has happened. Modelled on the
#: `unverified` installation detail, and for the identical reason: reporting "I
#: could not look" as "there is nothing there" is this project's worst recurring
#: defect. `antidemo status` has always drawn this distinction -- see
#: `cli._restart_history_check`, which answers "NO STATE DIRECTORY · NO RESTART
#: HISTORY TO READ" rather than "NO RESTARTS RECORDED" -- and `/readyz` did not.
_RESTART_HISTORY_UNAVAILABLE_DETAIL = (
    "THE RESTART HISTORY COULD NOT BE READ: this runtime has no manifest state "
    "directory, so there is nowhere a supervisor could have written one and no "
    "supervisor watching this process. This is not a report that it has never "
    "been restarted -- nothing was read, so a restart is neither confirmed nor "
    "ruled out. Measure it from process_started_at instead, which this process "
    "does know first-hand."
)


def _restart_history() -> Any:
    """What the supervisor wrote down about the deaths before this process.

    Read once at startup rather than per request: the record cannot change while
    this process is the one running, because the only writer is the supervisor
    that is waiting on it, and it only writes when this process has died.

    ``None`` when there is no record *location*, which is a different answer
    from an empty record and must not collapse into it. It is the deployed
    app's permanent condition: `app.yaml` binds `ANTI_DEMO_MANIFEST_JSON` and
    not `ANTI_DEMO_MANIFEST`, so `state_dir_from_environ` resolves to nothing,
    `restart_record_path()` is `None`, and `read_restart_history(None)` returns
    the same zeroed history a first run has. Handing that to the payload
    published `restarts: 0` and `supervisor_gave_up: false` as literals that no
    container event could ever move.
    """
    record = restart_record_path()
    if record is None:
        return None
    return read_restart_history(record)


def _apply_restart_history(payload: dict[str, Any]) -> None:
    """Make a resurrection loud in the same fields as everything else.

    The whole objection to a restarting supervisor was that it hides why the
    server went down. So the process that came back publishes the count and the
    reason it came back for, and while the deaths are recent it reports itself
    degraded -- a flapping server that answers "ready" between crashes is the
    same lie as a credentialless one that answers "ready" between failed bouts.

    ``restart_history_state`` is what keeps that promise honest where the record
    cannot be reached. Two values, and the fields it governs are ``None`` rather
    than zero in the second:

    * ``read`` -- a record location existed and was read. An empty record here
      genuinely means no restarts, because the supervisor writing it is the
      thing that would have recorded one.
    * ``unavailable`` -- there is nowhere for the record to live, so nothing was
      read. The deployed app is always this.

    Unavailability deliberately does not set ``degraded`` or take the detail
    slot, on the same reasoning as `_apply_startup_reap` and as the credential
    verdict's ``unknown``: it is a permanent property of the deployed runtime
    rather than a fault, and a box that reported itself degraded every second of
    its life would make the one field an operator checks worth less, not more.
    The state and its sentence are in fields of their own, so nothing is silent.
    """

    history = getattr(app.state, "restart_history", None)
    if history is None:
        payload["restart_history_state"] = "unavailable"
        payload["restart_history_detail"] = _RESTART_HISTORY_UNAVAILABLE_DETAIL
        # Written out rather than left absent. A monitor that reads a missing
        # key as a falsy zero lands back on the claim this is here to withdraw,
        # and an explicit null cannot be mistaken for a count.
        payload["restarts"] = None
        payload["restarts_recent"] = None
        payload["last_restart_at"] = None
        payload["last_restart_reason"] = None
        payload["supervisor_gave_up"] = None
        return
    payload["restart_history_state"] = "read"
    payload["restart_history_detail"] = None
    payload["restarts"] = history.restarts
    payload["restarts_recent"] = history.recent
    payload["last_restart_at"] = history.last_at
    payload["last_restart_reason"] = history.last_reason
    payload["supervisor_gave_up"] = history.gave_up
    if not history.flapping:
        # An old restart is history, not a fault. Reporting the count is enough:
        # degrading forever over a crash three weeks ago would make the field
        # useless for spotting the crash three minutes ago.
        return
    detail = (
        f"THIS SERVER HAS BEEN RESTARTED {history.recent} TIME(S) RECENTLY "
        f"({history.restarts} in total) · LAST BECAUSE OF {history.last_reason} "
        f"AT {history.last_at}"
    )
    if history.gave_up:
        detail += (
            " · THE SUPERVISOR HAS STOPPED RESTARTING IT, so the next crash "
            "ends the demo until an operator intervenes"
        )
    payload["degraded"] = True
    if payload["degraded_detail"] is None:
        # Lower precedence than a credential fault or an unsettled ring on
        # purpose: those say why the server cannot do its job right now, and
        # this says the process saying it is a replacement. Both are in the
        # payload either way; only the one sentence has to be chosen.
        payload["degraded_detail"] = detail
    if payload["status"] == "ready":
        payload["status"] = "degraded"


def _apply_credential_verdict(payload: dict[str, Any]) -> None:
    """Fold the AWS credential verdict into an otherwise finished payload.

    Reads a cached verdict; issues nothing. A health check must not be able to
    turn into an AWS call, or a monitor polling every second becomes a load
    generator against STS.

    The status can only ever be *lowered* here, and never below `degraded` --
    a credential fault does not make this process `not_ready`, because
    `not_ready` is the ring's word and returning 503 for an observation would
    let a probe fault look like the server having failed. The point is only that
    a monitor comparing status to "ready" stops being reassured.

    Two sources -- the running probe and the deployed startup check's refusal --
    reconciled by `effective_credential_verdict` rather than here, because the
    catalog has to reach the same answer from the same two objects and a rule
    written twice is a rule that comes apart.
    """

    verdict = effective_credential_verdict(app.state)
    if verdict is None:
        # No probe in this process and no startup refusal to report: say so
        # rather than imply a green answer, and do not degrade.
        payload["credentials_state"] = "unprobed"
        payload["credentials_detail"] = None
        payload["credentials_recovery_state"] = "settled"
        payload["credentials_checked"] = False
        return

    payload["credentials_state"] = verdict.state
    payload["credentials_detail"] = verdict.detail
    payload["credentials_recovery_state"] = verdict.recovery.state
    payload["credentials_recovery_attempts"] = int(verdict.recovery.attempts or 0)
    payload["credentials_principal"] = verdict.arn
    payload["credentials_checked"] = verdict.checked_at_monotonic is not None
    if verdict.state in {"ok", "unknown"}:
        # "unknown" is the first few seconds of a process's life, before the
        # first probe has answered. Degrading on it would make every startup
        # flap; a probe that stops answering for longer than that ages into
        # "stale", which does degrade.
        return

    payload["degraded"] = True
    losses = list(payload["degraded_capabilities"]) + list(verdict.capabilities_lost)
    payload["degraded_capabilities"] = losses
    if verdict.state in _TOTAL_CREDENTIAL_FAULTS or payload["degraded_detail"] is None:
        payload["degraded_detail"] = verdict.detail
    if payload["status"] == "ready":
        payload["status"] = "degraded"


@app.get("/healthz", include_in_schema=False)
async def liveness() -> JSONResponse:
    return JSONResponse({"status": "ok"})


@app.get("/readyz", include_in_schema=False)
@app.get("/api/ready", include_in_schema=False)
async def readiness() -> JSONResponse:
    # Awaited rather than called inline: both probes are blocking socket reads,
    # and both are cached, so this is a network call at most once every few
    # minutes and never on the event loop. Neither can raise, so a detection
    # failure cannot break the endpoint a monitor is watching. Gathered because
    # they are independent and a monitor should not pay for them in series.
    # `/healthz` is deliberately left alone: liveness must not depend on reaching
    # an external service.
    ingress_drift, presence = await asyncio.gather(
        operator_ingress_drift_async(),
        installation_presence_async(),
    )
    return _readiness_response(ingress_drift, presence)

if FRONTEND_DIST.exists():
    assets = FRONTEND_DIST / "assets"
    if assets.exists():
        app.mount("/assets", StaticFiles(directory=assets), name="assets")



_POST_ONLY_SESSION_CONTROLS = frozenset(
    {"arm", "cancel-arm", "run", "redo", "retry-cleanup", "towel", "cooldown"}
)


@app.get("/api/sessions/{session_id}/{control}", include_in_schema=False)
async def reject_get_session_control(session_id: str, control: str) -> JSONResponse:
    del session_id
    if control not in _POST_ONLY_SESSION_CONTROLS:
        return JSONResponse({"detail": "API endpoint not found"}, status_code=404)
    return JSONResponse(
        {"detail": "Method Not Allowed"},
        status_code=405,
        headers={"Allow": "POST"},
    )


@app.get("/api/installation/recover", include_in_schema=False)
async def reject_get_installation_recovery() -> JSONResponse:
    return JSONResponse(
        {"detail": "Method Not Allowed"},
        status_code=405,
        headers={"Allow": "POST"},
    )


@app.get("/{full_path:path}", include_in_schema=False)
async def serve_frontend(full_path: str) -> Response:
    # Unknown API routes must stay JSON 404s; returning index.html here makes
    # fetch callers fail JSON decoding and disguises a route/deploy mismatch.
    if full_path == "api" or full_path.startswith("api/"):
        return JSONResponse({"detail": "API endpoint not found"}, status_code=404)

    dist = FRONTEND_DIST.resolve()
    candidate = (FRONTEND_DIST / full_path).resolve()
    if full_path and candidate.is_relative_to(dist) and candidate.is_file():
        headers = SHELL_CACHE_HEADERS if candidate.suffix == ".html" else {
            "Cache-Control": "no-cache"
        }
        return FileResponse(candidate, headers=headers)

    # A missing fingerprinted asset is a real deploy mismatch, never an SPA route.
    if full_path.startswith("assets/"):
        return JSONResponse({"detail": "Static asset not found"}, status_code=404)

    index = FRONTEND_DIST / "index.html"
    if not index.is_file():
        return HTMLResponse(
            "<!doctype html><title>Anti-Demo unavailable</title>"
            "<h1>The application build is unavailable.</h1>"
            "<p>Redeploy the complete frontend/dist bundle.</p>",
            status_code=503,
            headers=SHELL_CACHE_HEADERS,
        )
    return FileResponse(index, headers=SHELL_CACHE_HEADERS)
