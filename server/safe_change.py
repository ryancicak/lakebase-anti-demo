from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Protocol
from uuid import uuid4

from .models import CompetitorId

LOGGER = logging.getLogger(__name__)

#: How long a cancelled lane may spend issuing its teardown calls.
#:
#: This is a *shutdown* budget, not a control-plane budget. Deleting a per-bout
#: clone is a single control-plane request per resource and returns in well
#: under a second; five seconds absorbs a slow-but-alive endpoint plus one
#: client retry, and both lanes spend it concurrently rather than serially.
#:
#: The ceiling is human rather than technical. Someone who has just pressed
#: Ctrl-C reads a few seconds as "shutting down" and anything longer as "hung",
#: and a hung shutdown is worse than the leak it is trying to prevent: the leak
#: is recoverable and costs money, whereas a stop that appears to hang gets
#: killed, which loses the teardown *and* the log line naming the orphan. The
#: adapters' own `control_timeout_seconds` (120s) is right for a healthy request
#: and completely wrong here.
DEFAULT_CANCEL_TEARDOWN_SECONDS = 5.0


class SafeChangeError(RuntimeError):
    """Base class for Round 2 orchestration failures."""


class SafeChangeNotArmedError(SafeChangeError):
    """The two source systems do not satisfy the agreed start contract."""


class SafeChangeProofError(SafeChangeError):
    """The isolated-change acceptance contract was not proved."""


class UnsafeCleanupError(SafeChangeError):
    """Cleanup was refused because ownership or identity did not match."""


class SafeChangeResetError(SafeChangeError):
    """One or more deterministic Round 2 artifacts could not be reset.

    A fan-in wrapper: several lanes are reset concurrently and this summarises
    whichever of them failed, so it has no single ``__cause__`` to inherit. That
    made the exception type the only thing a classifier could see, and the type
    is the same whether a lane died of an expired SSO session or of an ownership
    mismatch -- one of which must be retried forever and the other of which must
    stop. :meth:`underlying_causes` is what makes the difference readable.
    """

    def __init__(self, result: SafeChangeResetResult) -> None:
        self.result = result
        failed = [lane.name for lane in result.lanes.values() if not lane.ok]
        super().__init__(f"Round 2 reset failed for: {', '.join(failed)}")

    def underlying_causes(self) -> tuple[BaseException, ...]:
        return reset_lane_causes(self.result)


class SafeChangeProvider(StrEnum):
    LAKEBASE = "lakebase"
    AURORA = "aurora"
    RDS = "rds"


class SafeChangePhase(StrEnum):
    PREFLIGHT = "preflight"
    ARMED = "armed"
    CREATING = "creating"
    MIGRATING = "migrating"
    VERIFYING_APPLICATION = "verifying_application"
    VERIFYING_SOURCE = "verifying_source"
    VERIFIED = "verified"
    FAILED = "failed"
    RESETTING = "resetting"
    RESET = "reset"


class SafeChangeLaneState(StrEnum):
    VERIFIED = "verified"
    FAILED = "failed"


_RUN_ID = re.compile(r"^[a-z](?:[a-z0-9-]{1,46}[a-z0-9])$")
_AWS_ACCOUNT_ID = re.compile(r"^[0-9]{12}$")
_AWS_REGION = re.compile(r"^[a-z]{2}(?:-gov)?-[a-z]+-[0-9]+$")
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SAFE_CHANGE_MIGRATION_PATH = PROJECT_ROOT / "sql" / "003_add_delivery_instructions.sql"

#: The application table both Round 2 and Round 3 drive, schema-qualified and
#: exported for the same reason ``server.targets.PROBE_TABLE`` is: the privilege
#: the deployed app is granted has to be derived from the module that issues the
#: statements, not written out by hand somewhere else. Round 3 refused at arm with
#: `permission denied for table orders` because the grant plan had no entry for
#: this relation at all -- the fourth time this project has shipped a plan that
#: did not cover what the code reads.
#:
#: Deliberately not interpolated into the contract SQL below. Those strings are
#: hashed into ``SafeChangeContract.sha256``, which is sealed in receipts, so the
#: text is left byte-for-byte as it was and
#: ``tests/test_lifecycle.py`` asserts this constant against what it parses out of
#: them instead. A rename that misses a statement fails that test.
ORDERS_TABLE = "public.orders"
CANONICAL_SAFE_CHANGE_MIGRATION = SAFE_CHANGE_MIGRATION_PATH.read_text(encoding="utf-8")


@dataclass(frozen=True)
class SafeChangeOwnershipScope:
    """Exact ownership boundary inherited from the secret-free run manifest."""

    run_id: str
    owner: str
    aws_account_id: str
    aws_region: str

    def __post_init__(self) -> None:
        if _RUN_ID.fullmatch(self.run_id) is None or "--" in self.run_id:
            raise ValueError("run_id must be a lowercase deterministic resource identifier")
        if not self.owner.strip():
            raise ValueError("owner is required")
        if _AWS_ACCOUNT_ID.fullmatch(self.aws_account_id) is None:
            raise ValueError("aws_account_id must be exactly 12 digits")
        if _AWS_REGION.fullmatch(self.aws_region) is None:
            raise ValueError("aws_region is invalid")


@dataclass(frozen=True)
class SafeChangePlan:
    lane_id: str
    name: str
    provider: SafeChangeProvider
    source_id: str
    artifact_id: str
    scope: SafeChangeOwnershipScope


@dataclass(frozen=True)
class ArtifactInspection:
    """Control-plane identity evidence required before use or deletion.

    Adapters populate these fields from authoritative control-plane responses.
    Secret values and database passwords must never appear in metadata.
    """

    artifact_id: str
    provider: SafeChangeProvider
    source_id: str
    run_id: str
    owner: str
    state: str
    aws_account_id: str | None = None
    aws_region: str | None = None
    metadata: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class SafeChangeProgress:
    lane_id: str
    lane_name: str
    phase: SafeChangePhase
    status: str
    occurred_at: datetime
    elapsed_ms: float | None = None
    error: str | None = None
    wire_call: str | None = None


@dataclass(frozen=True)
class SafeChangeLaneArm:
    plan: SafeChangePlan
    evidence: Mapping[str, object]


@dataclass(frozen=True)
class SafeChangeArm:
    competitor: CompetitorId
    armed_at: datetime
    armed_at_monotonic_ns: int
    contract_sha256: str
    scope: SafeChangeOwnershipScope
    lanes: Mapping[str, SafeChangeLaneArm]


@dataclass(frozen=True)
class SafeChangeLaneResult:
    lane_id: str
    name: str
    provider: SafeChangeProvider
    state: SafeChangeLaneState
    elapsed_ms: float
    first_action_ns: int
    completed_ns: int
    artifact_id: str
    error: str | None = None


@dataclass(frozen=True)
class SafeChangeRunResult:
    competitor: CompetitorId
    nonce: str
    started_ns: int
    completed_ns: int
    launch_skew_ms: float
    contract_sha256: str
    lanes: Mapping[str, SafeChangeLaneResult]

    @property
    def all_verified(self) -> bool:
        return bool(self.lanes) and all(
            lane.state == SafeChangeLaneState.VERIFIED for lane in self.lanes.values()
        )


@dataclass(frozen=True)
class SafeChangeResetLaneResult:
    lane_id: str
    name: str
    provider: SafeChangeProvider
    artifact_id: str
    ok: bool
    already_absent: bool = False
    error: str | None = None
    #: The exception that actually failed this lane, kept alongside the message
    #: rather than instead of it. `error` is what an operator reads; this is what
    #: a classifier needs, because "expired SSO session" and "ownership
    #: mismatch" are the same string type and opposite decisions. Excluded from
    #: equality and repr so a preserved cause cannot change what a lane result
    #: *is* -- every existing comparison of these results still holds.
    cause: BaseException | None = field(default=None, compare=False, repr=False)


@dataclass(frozen=True)
class SafeChangeResetResult:
    competitor: CompetitorId | None
    lanes: Mapping[str, SafeChangeResetLaneResult]

    @property
    def ok(self) -> bool:
        return bool(self.lanes) and all(lane.ok for lane in self.lanes.values())


def reset_lane_causes(result: SafeChangeResetResult) -> tuple[BaseException, ...]:
    """Every preserved lane cause, in lane order. Shared by both reset wrappers."""

    return tuple(
        lane.cause for lane in result.lanes.values() if lane.cause is not None
    )


@dataclass(frozen=True)
class SafeChangeContract:
    """The exact PostgreSQL acceptance contract used in both lanes."""

    baseline_order_id: str = "00000000-0000-4000-8000-000000000001"
    baseline_customer_email: str = "ringside@example.com"
    baseline_total_cents: int = 4299
    baseline_status: str = "ready"
    application_customer_email: str = "round2-verifier@example.com"
    application_total_cents: int = 4200
    application_status: str = "validated"
    application_delivery_instructions: str = "Leave at the front desk"

    baseline_sql: str = """
        SELECT customer_email, total_cents, status
        FROM public.orders
        WHERE order_id = %s
    """
    column_sql: str = """
        SELECT data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'orders'
          AND column_name = 'delivery_instructions'
    """
    migration_sql: str = CANONICAL_SAFE_CHANGE_MIGRATION
    migrated_baseline_sql: str = """
        SELECT delivery_instructions
        FROM public.orders
        WHERE order_id = %s
    """
    application_insert_sql: str = """
        INSERT INTO public.orders (
            order_id,
            customer_email,
            total_cents,
            status,
            created_at,
            delivery_instructions
        ) VALUES (%s, %s, %s, %s, clock_timestamp(), %s)
    """
    application_readback_sql: str = """
        SELECT customer_email, total_cents, status, delivery_instructions
        FROM public.orders
        WHERE order_id = %s
    """
    nonce_count_sql: str = """
        SELECT count(*)
        FROM public.orders
        WHERE order_id = %s
    """

    @property
    def sha256(self) -> str:
        values = (
            self.baseline_order_id,
            self.baseline_customer_email,
            str(self.baseline_total_cents),
            self.baseline_status,
            self.application_customer_email,
            str(self.application_total_cents),
            self.application_status,
            self.application_delivery_instructions,
            self.baseline_sql,
            self.column_sql,
            self.migration_sql,
            self.migrated_baseline_sql,
            self.application_insert_sql,
            self.application_readback_sql,
            self.nonce_count_sql,
        )
        return hashlib.sha256("\0".join(values).encode("utf-8")).hexdigest()


DEFAULT_SAFE_CHANGE_CONTRACT = SafeChangeContract()


class SafeChangeSqlConnection(Protocol):
    async def execute(
        self,
        statement: str,
        parameters: Sequence[object] = (),
    ) -> None: ...

    async def fetch_one(
        self,
        statement: str,
        parameters: Sequence[object] = (),
    ) -> Sequence[object] | None: ...

    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...

    async def close(self) -> None: ...


class AdapterReporter(Protocol):
    async def __call__(
        self,
        status: str,
        wire_call: str | None = None,
    ) -> None: ...


ProgressCallback = Callable[[SafeChangeProgress], Awaitable[None]]


class SafeChangeAdapter(Protocol):
    """A thin control-plane and connection adapter for one database product."""

    provider: SafeChangeProvider
    name: str
    source_id: str

    async def preflight(self, plan: SafeChangePlan) -> Mapping[str, object]:
        """Return secret-free source readiness and capability evidence."""

    async def inspect_artifact(
        self,
        plan: SafeChangePlan,
    ) -> ArtifactInspection | None:
        """Describe the deterministic isolated environment without changing it."""

    async def create_isolated(
        self,
        plan: SafeChangePlan,
        report: AdapterReporter,
    ) -> ArtifactInspection:
        """Create exactly plan.artifact_id and return authoritative identity.

        The adapter must make its first control-plane request before awaiting
        report, so presentation I/O cannot bias launch timing.
        """

    async def connect_source(self, plan: SafeChangePlan) -> SafeChangeSqlConnection:
        """Open a fresh TLS connection to the source database."""

    async def connect_isolated(
        self,
        plan: SafeChangePlan,
        artifact: ArtifactInspection,
    ) -> SafeChangeSqlConnection:
        """Open a fresh TLS connection to the isolated environment."""

    async def delete_isolated(
        self,
        plan: SafeChangePlan,
        artifact: ArtifactInspection,
        report: AdapterReporter,
    ) -> None:
        """Delete only the already-inspected deterministic isolated environment."""

    async def abandon_isolated(self, plan: SafeChangePlan) -> None:
        """Issue teardown for a cancelled lane and return without waiting.

        Optional. The engine calls this only on cancellation, and only ever
        inside a short bounded shield, so an implementation must issue its
        delete requests and return -- never wait for a resource to disappear.

        Identifiers must come from ``plan`` rather than from a remembered
        creation result: cancellation can arrive before anything was created,
        after creation but before the artifact was recorded, or part way through
        an ordinary teardown, and all three must be safe. Deleting something
        absent, never created, or already deleting must not raise.

        The default is a no-op, for adapters whose isolated environment costs
        nothing to leave behind.
        """


def _render_identifier(identifier: str | Callable[[], str]) -> str:
    """Resolve a lazy identifier without letting it defeat the report.

    A caller that computes its identifier from live in-flight state can raise
    while doing so, and an ORPHAN RISK line that never gets logged because the
    naming machinery failed is the worst possible outcome here -- strictly worse
    than a vague one. So a broken namer degrades the sentence, never removes it.
    """

    if not callable(identifier):
        return identifier
    try:
        return identifier()
    except Exception:
        LOGGER.error("Could not name the resources at risk", exc_info=True)
        return "an unnameable resource (see the traceback logged beside this line)"


async def abandon_on_cancel(
    abandon: Callable[[], Awaitable[None]],
    *,
    identifier: str | Callable[[], str],
    timeout_seconds: float,
    remedy: str = "verify the resource is gone",
) -> bool:
    """Run a cancelled lane's teardown under a shield that cannot outstay its welcome.

    Three things have to be true at once here, and each one rules out the
    obvious way of writing the other two.

    The teardown must survive the cancellation that triggered it, so it runs as
    a shielded task rather than inline. The shutdown must not be able to hang,
    so the wait on that task is bounded and the bound is short. And the
    cancellation must still be delivered, so this function never raises: the
    caller re-raises the ``CancelledError`` it was already handling, whatever
    happened in here.

    Returns whether the teardown was confirmed. A False return has already been
    logged at error level with ``identifier``, which is what makes an orphan
    findable afterwards -- the original incident was expensive because it was
    invisible, not because it was slow to clean up.

    ``remedy`` closes that log line and is what the reader is expected to do
    about it. The default suits the three callers abandoning a provider
    resource. Round 4 abandons a ring lease and a Delta row instead, so it
    supplies its own; it is a whole clause rather than a bare noun because a
    plural subject cannot be dropped into "verify the ... is gone".

    ``identifier`` may be a callable, and a caller whose at-risk set is still
    being discovered should pass one. It is resolved only in the branches that
    actually log, which means *after* the bounded wait rather than at the moment
    of cancellation. That difference is the whole point: at the instant a
    cancellation arrives, a caller with requests in flight genuinely does not
    yet know what it created, so an identifier computed then describes the
    moment of maximum ignorance. Computing it when the line is written spends
    the bounded wait learning, and reports what is *still* unaccounted for.
    """

    task = asyncio.ensure_future(abandon())
    # The shield below can return before the task does. Retrieve the outcome
    # here so an abandoned task cannot surface as "exception was never
    # retrieved" noise during interpreter shutdown.
    task.add_done_callback(lambda done: done.cancelled() or done.exception())
    try:
        async with asyncio.timeout(timeout_seconds):
            await asyncio.shield(task)
    except TimeoutError:
        LOGGER.error(
            "ORPHAN RISK: teardown of %s was still unconfirmed after %.1fs and "
            "the shutdown will not wait longer. The delete request may yet "
            "land; %s.",
            _render_identifier(identifier),
            timeout_seconds,
            remedy,
        )
        return False
    except asyncio.CancelledError:
        # Cancelled a second time while tearing down. The request may already be
        # in flight, but nothing here can confirm it any more.
        LOGGER.error(
            "ORPHAN RISK: teardown of %s was interrupted by a second "
            "cancellation before it could be confirmed; %s.",
            _render_identifier(identifier),
            remedy,
        )
        return False
    except Exception:
        LOGGER.error(
            "ORPHAN RISK: teardown of %s failed; %s.",
            _render_identifier(identifier),
            remedy,
            exc_info=True,
        )
        return False
    return True


def deterministic_artifact_id(
    run_id: str,
    provider: SafeChangeProvider,
) -> str:
    """Return the one allowed Round 2 child identifier for a run and provider."""

    if _RUN_ID.fullmatch(run_id) is None or "--" in run_id:
        raise ValueError("run_id must be a lowercase deterministic resource identifier")
    if provider == SafeChangeProvider.LAKEBASE:
        return f"safe-change-{run_id}"
    return f"adsc-{run_id}-{provider.value}"


class SafeChangeEngine:
    """Neutral Round 2 coordinator.

    The engine owns fairness-sensitive behavior: start barrier, monotonic timing,
    one SQL contract, final source-isolation proof, and cleanup authorization.
    Product adapters own only control-plane mechanics and connection material.
    """

    def __init__(
        self,
        *,
        scope: SafeChangeOwnershipScope,
        lakebase: SafeChangeAdapter,
        competitors: Mapping[CompetitorId, SafeChangeAdapter],
        contract: SafeChangeContract = DEFAULT_SAFE_CHANGE_CONTRACT,
        arm_ttl_seconds: float = 60.0,
        preflight_timeout_seconds: float = 120.0,
        run_timeout_seconds: float = 1800.0,
        reset_timeout_seconds: float = 1800.0,
        progress_timeout_seconds: float = 2.0,
        cancel_teardown_timeout_seconds: float = DEFAULT_CANCEL_TEARDOWN_SECONDS,
        clock_ns: Callable[[], int] = time.monotonic_ns,
        nonce_factory: Callable[[], str] | None = None,
    ) -> None:
        if lakebase.provider != SafeChangeProvider.LAKEBASE:
            raise ValueError("lakebase adapter must identify as provider lakebase")
        if arm_ttl_seconds <= 0:
            raise ValueError("arm_ttl_seconds must be positive")
        if min(
            preflight_timeout_seconds,
            run_timeout_seconds,
            reset_timeout_seconds,
            progress_timeout_seconds,
            cancel_teardown_timeout_seconds,
        ) <= 0:
            raise ValueError("safe-change timeouts must be positive")
        expected = {
            CompetitorId.AURORA_SERVERLESS_V2: SafeChangeProvider.AURORA,
            CompetitorId.RDS_POSTGRES: SafeChangeProvider.RDS,
        }
        for competitor, adapter in competitors.items():
            if adapter.provider != expected[competitor]:
                raise ValueError(
                    f"{competitor.value} adapter must identify as provider "
                    f"{expected[competitor].value}"
                )
        self.scope = scope
        self.lakebase = lakebase
        self.competitors = dict(competitors)
        self.contract = contract
        self.arm_ttl_seconds = arm_ttl_seconds
        self.preflight_timeout_seconds = preflight_timeout_seconds
        self.run_timeout_seconds = run_timeout_seconds
        self.reset_timeout_seconds = reset_timeout_seconds
        self.progress_timeout_seconds = progress_timeout_seconds
        self.cancel_teardown_timeout_seconds = cancel_teardown_timeout_seconds
        self._clock_ns = clock_ns
        self._nonce_factory = nonce_factory or (lambda: str(uuid4()))

    def plans_for(self, competitor: CompetitorId) -> tuple[SafeChangePlan, SafeChangePlan]:
        challenger = self._competitor_adapter(competitor)
        return (
            self._plan("lakebase", self.lakebase),
            self._plan("competitor", challenger),
        )

    async def arm(
        self,
        competitor: CompetitorId,
        on_progress: ProgressCallback | None = None,
    ) -> SafeChangeArm:
        plans = self.plans_for(competitor)
        checks = await asyncio.gather(
            *(
                self._preflight_lane(plan, on_progress, cleanup_existing=True)
                for plan in plans
            ),
            return_exceptions=True,
        )
        errors = [
            f"{plan.name}: {check}"
            for plan, check in zip(plans, checks, strict=True)
            if isinstance(check, BaseException)
        ]
        if errors:
            raise SafeChangeNotArmedError(" · ".join(errors))

        armed_at_ns = self._clock_ns()
        lanes = {
            plan.lane_id: SafeChangeLaneArm(plan=plan, evidence=check)
            for plan, check in zip(plans, checks, strict=True)
            if isinstance(check, Mapping)
        }
        for plan in plans:
            await self._emit(
                on_progress,
                plan,
                SafeChangePhase.ARMED,
                "Source and isolated-change start state verified",
            )
        return SafeChangeArm(
            competitor=competitor,
            armed_at=datetime.now(UTC),
            armed_at_monotonic_ns=armed_at_ns,
            contract_sha256=self.contract.sha256,
            scope=self.scope,
            lanes=lanes,
        )

    async def run(
        self,
        arm: SafeChangeArm,
        on_progress: ProgressCallback | None = None,
    ) -> SafeChangeRunResult:
        plans = self._validate_arm(arm)

        # Revalidate immediately before the barrier. Credential preparation and
        # time between operator actions can otherwise invalidate an earlier arm.
        checks = await asyncio.gather(
            *(self._preflight_lane(plan, on_progress) for plan in plans),
            return_exceptions=True,
        )
        errors = [
            f"{plan.name}: {check}"
            for plan, check in zip(plans, checks, strict=True)
            if isinstance(check, BaseException)
        ]
        if errors:
            raise SafeChangeNotArmedError(
                "Start state changed before the bell: " + " · ".join(errors)
            )

        barrier = asyncio.Event()
        start_holder: dict[str, int] = {}
        nonce = self._nonce_factory()
        tasks = [
            asyncio.create_task(
                self._run_lane(plan, barrier, start_holder, nonce, on_progress),
                name=f"safe-change-{plan.lane_id}",
            )
            for plan in plans
        ]
        await asyncio.sleep(0)
        started_ns = self._clock_ns()
        start_holder["started_ns"] = started_ns
        barrier.set()
        lane_results = await asyncio.gather(*tasks)
        completed_ns = self._clock_ns()
        first_actions = [result.first_action_ns for result in lane_results]
        return SafeChangeRunResult(
            competitor=arm.competitor,
            nonce=nonce,
            started_ns=started_ns,
            completed_ns=completed_ns,
            launch_skew_ms=(max(first_actions) - min(first_actions)) / 1_000_000,
            contract_sha256=self.contract.sha256,
            lanes={result.lane_id: result for result in lane_results},
        )

    async def reset(
        self,
        competitor: CompetitorId,
        on_progress: ProgressCallback | None = None,
    ) -> SafeChangeResetResult:
        await self.settle_pending_mutations(competitor)
        return await self._reset_plans(self.plans_for(competitor), competitor, on_progress)

    async def settle_pending_mutations(self, competitor: CompetitorId) -> None:
        """Wait for shielded lane mutations before inspecting cleanup state."""

        await asyncio.gather(
            *(
                self._settle_adapter(self._adapter_for(plan))
                for plan in self.plans_for(competitor)
            )
        )

    @staticmethod
    async def _settle_adapter(adapter: SafeChangeAdapter) -> None:
        settle = getattr(adapter, "settle_pending_mutations", None)
        if settle is not None:
            await settle()

    async def reset_all(
        self,
        on_progress: ProgressCallback | None = None,
    ) -> SafeChangeResetResult:
        plans = [self._plan("lakebase", self.lakebase)]
        plans.extend(
            self._plan(f"competitor-{competitor.value}", adapter)
            for competitor, adapter in self.competitors.items()
        )
        await asyncio.gather(
            *(self._settle_adapter(self._adapter_for(plan)) for plan in plans)
        )
        return await self._reset_plans(tuple(plans), None, on_progress)

    def _plan(self, lane_id: str, adapter: SafeChangeAdapter) -> SafeChangePlan:
        if not adapter.source_id:
            raise ValueError(f"{adapter.name} adapter source_id is required")
        return SafeChangePlan(
            lane_id=lane_id,
            name=adapter.name,
            provider=adapter.provider,
            source_id=adapter.source_id,
            artifact_id=deterministic_artifact_id(self.scope.run_id, adapter.provider),
            scope=self.scope,
        )

    def _competitor_adapter(self, competitor: CompetitorId) -> SafeChangeAdapter:
        try:
            return self.competitors[competitor]
        except KeyError as exc:
            raise ValueError(f"No safe-change adapter exists for {competitor.value}") from exc

    def _adapter_for(self, plan: SafeChangePlan) -> SafeChangeAdapter:
        if plan.provider == SafeChangeProvider.LAKEBASE:
            return self.lakebase
        for adapter in self.competitors.values():
            if adapter.provider == plan.provider:
                return adapter
        raise ValueError(f"No adapter exists for provider {plan.provider.value}")

    async def _preflight_lane(
        self,
        plan: SafeChangePlan,
        on_progress: ProgressCallback | None,
        *,
        cleanup_existing: bool = False,
    ) -> Mapping[str, object]:
        try:
            async with asyncio.timeout(self.preflight_timeout_seconds):
                adapter = self._adapter_for(plan)
                await self._emit(
                    on_progress,
                    plan,
                    SafeChangePhase.PREFLIGHT,
                    "Verifying source and deterministic artifact boundary",
                )
                existing = await adapter.inspect_artifact(plan)
        except TimeoutError as exc:
            raise SafeChangeNotArmedError(
                f"Preflight exceeded {self.preflight_timeout_seconds:.0f} seconds"
            ) from exc

        if existing is not None:
            self._validate_artifact(plan, existing)
            if not cleanup_existing:
                raise SafeChangeNotArmedError(
                    f"Owned isolated environment {plan.artifact_id} appeared after arming"
                )
            await self._emit(
                on_progress,
                plan,
                SafeChangePhase.RESETTING,
                "Clearing the previous owned isolated environment",
            )
            reset = await self._reset_lane(plan, on_progress)
            if not reset.ok:
                raise SafeChangeNotArmedError(
                    f"Could not clear owned isolated environment {plan.artifact_id}: "
                    f"{reset.error or 'cleanup incomplete'}"
                )
            await self._emit(
                on_progress,
                plan,
                SafeChangePhase.PREFLIGHT,
                "Previous owned environment cleared · verifying clean source",
            )

        try:
            async with asyncio.timeout(self.preflight_timeout_seconds):
                evidence = dict(await adapter.preflight(plan))
                source = await adapter.connect_source(plan)
                try:
                    await self._assert_source_clean(source)
                finally:
                    await self._close(source)
                return evidence
        except TimeoutError as exc:
            raise SafeChangeNotArmedError(
                f"Preflight exceeded {self.preflight_timeout_seconds:.0f} seconds"
            ) from exc

    async def _run_lane(
        self,
        plan: SafeChangePlan,
        barrier: asyncio.Event,
        start_holder: Mapping[str, int],
        nonce: str,
        on_progress: ProgressCallback | None,
    ) -> SafeChangeLaneResult:
        await barrier.wait()
        started_ns = start_holder["started_ns"]
        first_action_ns = self._clock_ns()
        adapter = self._adapter_for(plan)

        async def report(status: str, wire_call: str | None = None) -> None:
            await self._emit(
                on_progress,
                plan,
                SafeChangePhase.CREATING,
                status,
                started_ns,
                wire_call=wire_call,
            )

        try:
            async with asyncio.timeout(self.run_timeout_seconds):
                await self._execute_lane(
                    plan,
                    adapter,
                    nonce,
                    on_progress,
                    started_ns,
                    report,
                )
        except asyncio.CancelledError:
            # `create_isolated` shields its mutations but not the readiness polls
            # between them, so cancellation here can leave a clone that AWS was
            # never told to delete. Ordinary teardown runs from the cooldown task
            # after `run` returns, and a cancelled lane never gets that far, so
            # this is the only place the delete can still be issued in-process.
            await self._abandon_lane(plan, adapter)
            raise
        except Exception as exc:
            completed_ns = self._clock_ns()
            error = (
                f"Safe-change lane exceeded {self.run_timeout_seconds:.0f} seconds"
                if isinstance(exc, TimeoutError)
                else str(exc) or type(exc).__name__
            )
            await self._emit(
                on_progress,
                plan,
                SafeChangePhase.FAILED,
                "The isolated schema change could not be verified",
                started_ns,
                error=error,
            )
            return SafeChangeLaneResult(
                lane_id=plan.lane_id,
                name=plan.name,
                provider=plan.provider,
                state=SafeChangeLaneState.FAILED,
                elapsed_ms=(completed_ns - started_ns) / 1_000_000,
                first_action_ns=first_action_ns,
                completed_ns=completed_ns,
                artifact_id=plan.artifact_id,
                error=error,
            )

        completed_ns = self._clock_ns()
        await self._emit(
            on_progress,
            plan,
            SafeChangePhase.VERIFIED,
            "Migration, application transaction, and source isolation verified",
            started_ns,
            elapsed_ns=completed_ns,
        )
        return SafeChangeLaneResult(
            lane_id=plan.lane_id,
            name=plan.name,
            provider=plan.provider,
            state=SafeChangeLaneState.VERIFIED,
            elapsed_ms=(completed_ns - started_ns) / 1_000_000,
            first_action_ns=first_action_ns,
            completed_ns=completed_ns,
            artifact_id=plan.artifact_id,
        )

    async def _abandon_lane(
        self,
        plan: SafeChangePlan,
        adapter: SafeChangeAdapter,
    ) -> None:
        abandon = getattr(adapter, "abandon_isolated", None)
        if abandon is None:
            return
        await abandon_on_cancel(
            lambda: abandon(plan),
            identifier=f"{plan.name} isolated environment {plan.artifact_id}",
            timeout_seconds=self.cancel_teardown_timeout_seconds,
        )

    async def _execute_lane(
        self,
        plan: SafeChangePlan,
        adapter: SafeChangeAdapter,
        nonce: str,
        on_progress: ProgressCallback | None,
        started_ns: int,
        report: AdapterReporter,
    ) -> None:
        artifact = await adapter.create_isolated(plan, report)
        self._validate_artifact(plan, artifact)

        isolated = await adapter.connect_isolated(plan, artifact)
        try:
            await self._assert_unmigrated_copy(isolated)
            await self._emit(
                on_progress,
                plan,
                SafeChangePhase.MIGRATING,
                "Applying the identical schema migration",
                started_ns,
            )
            await self._apply_migration(isolated)
            await self._emit(
                on_progress,
                plan,
                SafeChangePhase.VERIFYING_APPLICATION,
                "Committing and reading back the identical application transaction",
                started_ns,
            )
            await self._verify_application(isolated, nonce)
        finally:
            await self._close(isolated)

        await self._emit(
            on_progress,
            plan,
            SafeChangePhase.VERIFYING_SOURCE,
            "Proving the source remained unchanged",
            started_ns,
        )
        source = await adapter.connect_source(plan)
        try:
            await self._assert_source_clean(source, nonce=nonce)
        finally:
            await self._close(source)

    async def _assert_source_clean(
        self,
        connection: SafeChangeSqlConnection,
        *,
        nonce: str | None = None,
    ) -> None:
        baseline = await connection.fetch_one(
            self.contract.baseline_sql,
            (self.contract.baseline_order_id,),
        )
        expected_baseline = (
            self.contract.baseline_customer_email,
            self.contract.baseline_total_cents,
            self.contract.baseline_status,
        )
        if tuple(baseline or ()) != expected_baseline:
            raise SafeChangeProofError("The source baseline row does not match the contract")
        column = await connection.fetch_one(self.contract.column_sql)
        if column is not None:
            raise SafeChangeProofError("The source already contains the proposed schema change")
        if nonce is not None:
            count = await connection.fetch_one(self.contract.nonce_count_sql, (nonce,))
            if count is None or int(count[0]) != 0:
                raise SafeChangeProofError("The application nonce leaked into the source")

    async def _assert_unmigrated_copy(self, connection: SafeChangeSqlConnection) -> None:
        baseline = await connection.fetch_one(
            self.contract.baseline_sql,
            (self.contract.baseline_order_id,),
        )
        expected_baseline = (
            self.contract.baseline_customer_email,
            self.contract.baseline_total_cents,
            self.contract.baseline_status,
        )
        if tuple(baseline or ()) != expected_baseline:
            raise SafeChangeProofError("The isolated copy does not contain the source baseline")
        if await connection.fetch_one(self.contract.column_sql) is not None:
            raise SafeChangeProofError("The isolated copy was already migrated before the bell")

    async def _apply_migration(self, connection: SafeChangeSqlConnection) -> None:
        try:
            await connection.execute(self.contract.migration_sql)
            await connection.commit()
        except Exception:
            await connection.rollback()
            raise
        column = await connection.fetch_one(self.contract.column_sql)
        if tuple(column or ()) != ("text", "YES"):
            raise SafeChangeProofError("The migrated column definition does not match the contract")
        baseline_default = await connection.fetch_one(
            self.contract.migrated_baseline_sql,
            (self.contract.baseline_order_id,),
        )
        if tuple(baseline_default or ()) != (None,):
            raise SafeChangeProofError(
                "Existing data is not compatible with the nullable migration"
            )

    async def _verify_application(
        self,
        connection: SafeChangeSqlConnection,
        nonce: str,
    ) -> None:
        try:
            await connection.execute(
                self.contract.application_insert_sql,
                (
                    nonce,
                    self.contract.application_customer_email,
                    self.contract.application_total_cents,
                    self.contract.application_status,
                    self.contract.application_delivery_instructions,
                ),
            )
            await connection.commit()
        except Exception:
            await connection.rollback()
            raise
        row = await connection.fetch_one(self.contract.application_readback_sql, (nonce,))
        expected = (
            self.contract.application_customer_email,
            self.contract.application_total_cents,
            self.contract.application_status,
            self.contract.application_delivery_instructions,
        )
        if tuple(row or ()) != expected:
            raise SafeChangeProofError(
                "The application transaction committed but read-back verification failed"
            )

    async def _reset_plans(
        self,
        plans: tuple[SafeChangePlan, ...],
        competitor: CompetitorId | None,
        on_progress: ProgressCallback | None,
    ) -> SafeChangeResetResult:
        lane_results = await asyncio.gather(
            *(self._reset_lane(plan, on_progress) for plan in plans)
        )
        result = SafeChangeResetResult(
            competitor=competitor,
            lanes={lane.lane_id: lane for lane in lane_results},
        )
        if not result.ok:
            raise SafeChangeResetError(result)
        return result

    async def _reset_lane(
        self,
        plan: SafeChangePlan,
        on_progress: ProgressCallback | None,
    ) -> SafeChangeResetLaneResult:
        try:
            async with asyncio.timeout(self.reset_timeout_seconds):
                return await self._reset_lane_bounded(plan, on_progress)
        except Exception as exc:
            error = (
                f"Reset exceeded {self.reset_timeout_seconds:.0f} seconds"
                if isinstance(exc, TimeoutError)
                else str(exc) or type(exc).__name__
            )
            await self._emit(
                on_progress,
                plan,
                SafeChangePhase.FAILED,
                "Reset refused or incomplete",
            )
            return SafeChangeResetLaneResult(
                lane_id=plan.lane_id,
                name=plan.name,
                provider=plan.provider,
                artifact_id=plan.artifact_id,
                ok=False,
                error=error,
                cause=exc,
            )

    async def _reset_lane_bounded(
        self,
        plan: SafeChangePlan,
        on_progress: ProgressCallback | None,
    ) -> SafeChangeResetLaneResult:
        adapter = self._adapter_for(plan)
        artifact = await adapter.inspect_artifact(plan)
        if artifact is None:
            await self._emit(
                on_progress,
                plan,
                SafeChangePhase.RESET,
                "Isolated environment already absent",
            )
            return SafeChangeResetLaneResult(
                lane_id=plan.lane_id,
                name=plan.name,
                provider=plan.provider,
                artifact_id=plan.artifact_id,
                ok=True,
                already_absent=True,
            )
        self._validate_artifact(plan, artifact)

        async def report(status: str, wire_call: str | None = None) -> None:
            await self._emit(
                on_progress,
                plan,
                SafeChangePhase.RESETTING,
                status,
                wire_call=wire_call,
            )

        await report("Deleting the owned isolated environment")
        await adapter.delete_isolated(plan, artifact, report)
        remaining = await adapter.inspect_artifact(plan)
        if remaining is not None:
            self._validate_artifact(plan, remaining)
            raise SafeChangeError("The isolated environment still exists after deletion")
        await self._emit(
            on_progress,
            plan,
            SafeChangePhase.RESET,
            "Owned isolated environment deleted",
        )
        return SafeChangeResetLaneResult(
            lane_id=plan.lane_id,
            name=plan.name,
            provider=plan.provider,
            artifact_id=plan.artifact_id,
            ok=True,
        )

    def _validate_arm(self, arm: SafeChangeArm) -> tuple[SafeChangePlan, SafeChangePlan]:
        if arm.scope != self.scope:
            raise SafeChangeNotArmedError("Arm ownership scope does not match this engine")
        if arm.contract_sha256 != self.contract.sha256:
            raise SafeChangeNotArmedError("The schema-change contract changed after arming")
        age_ns = self._clock_ns() - arm.armed_at_monotonic_ns
        if age_ns < 0 or age_ns > int(self.arm_ttl_seconds * 1e9):
            raise SafeChangeNotArmedError("Round 2 arm expired; preflight again")
        plans = self.plans_for(arm.competitor)
        if set(arm.lanes) != {plan.lane_id for plan in plans}:
            raise SafeChangeNotArmedError("Arm does not contain both required lanes")
        if any(arm.lanes[plan.lane_id].plan != plan for plan in plans):
            raise SafeChangeNotArmedError("Arm plan changed after preflight")
        return plans

    def _validate_artifact(
        self,
        plan: SafeChangePlan,
        artifact: ArtifactInspection,
    ) -> None:
        failures: list[str] = []
        if artifact.artifact_id != plan.artifact_id:
            failures.append("artifact ID")
        if artifact.provider != plan.provider:
            failures.append("provider")
        if artifact.source_id != plan.source_id:
            failures.append("source")
        if artifact.run_id != plan.scope.run_id:
            failures.append("run ID")
        if artifact.owner != plan.scope.owner:
            failures.append("owner")
        if plan.provider != SafeChangeProvider.LAKEBASE:
            if artifact.aws_account_id != plan.scope.aws_account_id:
                failures.append("AWS account")
            if artifact.aws_region != plan.scope.aws_region:
                failures.append("AWS region")
        if failures:
            raise UnsafeCleanupError(
                f"Artifact ownership mismatch for {plan.artifact_id}: {', '.join(failures)}"
            )

    async def _emit(
        self,
        callback: ProgressCallback | None,
        plan: SafeChangePlan,
        phase: SafeChangePhase,
        status: str,
        started_ns: int | None = None,
        *,
        elapsed_ns: int | None = None,
        error: str | None = None,
        wire_call: str | None = None,
    ) -> None:
        if callback is None:
            return
        current_ns = elapsed_ns if elapsed_ns is not None else self._clock_ns()
        elapsed_ms = (
            (current_ns - started_ns) / 1_000_000 if started_ns is not None else None
        )
        try:
            async with asyncio.timeout(self.progress_timeout_seconds):
                await callback(
                    SafeChangeProgress(
                        lane_id=plan.lane_id,
                        lane_name=plan.name,
                        phase=phase,
                        status=status,
                        occurred_at=datetime.now(UTC),
                        elapsed_ms=elapsed_ms,
                        error=error,
                        wire_call=wire_call,
                    )
                )
        except Exception:
            # Progress is observational. A disconnected browser or failed event
            # sink must not change the database result or authoritative timing.
            return

    @staticmethod
    async def _close(connection: SafeChangeSqlConnection) -> None:
        try:
            await connection.close()
        except Exception:
            # Closing an already-completed proof connection is best-effort. The
            # adapter must still avoid pools and expose only fresh connections.
            return
