from __future__ import annotations

import json
import logging
import os
from collections.abc import Awaitable
from dataclasses import replace
from datetime import UTC, date, datetime

from fastapi import APIRouter, Header, HTTPException, Request, status
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

from . import round_availability, selfheal
from .aws_credential_probe import effective_credential_verdict
from .catalog import catalog
from .lifecycle import cached_installation_report, deployed_aws_posture
from .manager import (
    AmbiguousRingQueryError,
    InvalidStateError,
    RunManager,
    SessionNotFoundError,
)
from .manifest import (
    LOCAL_OPERATOR_EMAIL_ENV,
    LOCAL_OPERATOR_ENV,
    LOCAL_OPERATOR_ID_ENV,
)
from .models import (
    AllBoutStatus,
    Availability,
    BoutOperator,
    BoutStatus,
    CatalogResponse,
    FightCardRoundStatus,
    FightCardState,
    RoundId,
    SessionCreate,
    SessionSnapshot,
)
from .receipts import ReceiptsResponse, current_installation, load_receipts_async
from .reconcile import presence_from_report

router = APIRouter(prefix="/api")
logger = logging.getLogger(__name__)

_CONTROL_UNAVAILABLE = (
    "Ring state is temporarily unavailable. Refresh before retrying this action."
)
_CLEANUP_PHASES = frozenset(
    {
        "cooldown",
        "towel_cleanup",
        "cleanup_retry",
        "round5_cleanup",
        "round5_cleanup_recovery",
        "startup_cleanup",
    }
)
_CLEANUP_DETAIL = {
    RoundId.WAKE_IDLE_APP: (
        "This round will reopen when both corners return to the required idle state. "
        "Other rounds remain available."
    ),
    RoundId.MAKE_SCHEMA_CHANGE_SAFELY: (
        "This round will reopen when both isolated environments are confirmed deleted. "
        "Other rounds remain available."
    ),
    RoundId.RECOVER_DELETED_ORDER: (
        "This round will reopen when both recovery environments are confirmed deleted. "
        "Other rounds remain available."
    ),
    RoundId.PUT_MODEL_SCORE_IN_APP: (
        "This round will reopen when its current cleanup finishes. "
        "Other rounds remain available."
    ),
    RoundId.SURVIVE_CONNECTION_SPIKE: (
        "Round 5 will reopen automatically when its Proxy and security group are "
        "confirmed deleted. Other rounds remain available."
    ),
    RoundId.ANALYZE_LIVE_ORDERS: (
        "This round will reopen when its current cleanup finishes. "
        "Other rounds remain available."
    ),
}


def manager(request: Request) -> RunManager:
    return request.app.state.run_manager


def operator_from_request(request: Request) -> BoutOperator:
    if not os.environ.get("DATABRICKS_APP_NAME"):
        return BoutOperator(
            display_name=os.environ.get(LOCAL_OPERATOR_ENV, "Local operator"),
            email=os.environ.get(LOCAL_OPERATOR_EMAIL_ENV) or None,
            subject=os.environ.get(LOCAL_OPERATOR_ID_ENV, "local-operator"),
        )

    preferred = (request.headers.get("x-forwarded-preferred-username") or "").strip()
    email = (request.headers.get("x-forwarded-email") or "").strip()
    if not email and "@" in preferred:
        email = preferred
    subject = (
        request.headers.get("x-forwarded-user")
        or request.headers.get("x-databricks-user-id")
        or email
    ).strip()
    if not email or not subject:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Databricks SSO identity headers are required to control the ring",
        )
    display_name = preferred or email
    if "@" in display_name:
        local_part = display_name.split("@", 1)[0]
        words = local_part.replace(".", " ").replace("_", " ").replace("-", " ").split()
        if words:
            display_name = " ".join(word.capitalize() for word in words)
    return BoutOperator(display_name=display_name, email=email, subject=subject)


async def _control_operation(
    operation: Awaitable[SessionSnapshot],
    *,
    value_error_status: int | None = None,
    missing_session: tuple[Request, str] | None = None,
) -> SessionSnapshot:
    """Keep every control mutation JSON-safe and consistent for the browser.

    ``missing_session`` is passed by the routes that name a fight card, and it
    buys the 404 an explanation: a process that has lost the session can still
    read the durable ring and say whether a bout is held there. It stays a 404
    on purpose -- the browser clears a stale live snapshot on exactly that
    status, so promoting this to a 409 would strand the screen an operator is
    presenting from. Only the mutating routes ask for it. A poll that 404s is
    reconciling rather than acting on an operator's click, and it discards the
    detail anyway, so paying a ring read per poll would buy nothing.
    """

    try:
        return await operation
    except SessionNotFoundError as exc:
        detail = "Session not found"
        if missing_session is not None:
            request, session_id = missing_session
            try:
                detail = await manager(request).missing_session_detail(session_id)
            except Exception:  # noqa: BLE001 - never lose the 404 to its own explanation
                logger.warning("Could not describe a missing fight card", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=detail,
        ) from exc
    except InvalidStateError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from exc
    except ValueError as exc:
        if value_error_status is not None:
            raise HTTPException(status_code=value_error_status, detail=str(exc)) from exc
        logger.error("Unexpected control validation failure: %s", type(exc).__name__)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=_CONTROL_UNAVAILABLE,
        ) from exc
    except Exception as exc:
        # Do not serialize provider exception text, resource IDs, or credentials.
        logger.error("Unexpected control-path failure: %s", type(exc).__name__)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=_CONTROL_UNAVAILABLE,
        ) from exc


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "database_connections": "sealed"}


def _availability_signals(request: Request) -> round_availability.AvailabilitySignals:
    """Collect the cached live state the round-select screen has to respect.

    Every read here is free, and the two that are not free by construction are
    free by cache. The gate is in-process, `deployed()` reads two environment
    variables, `effective_credential_verdict()` reads two cached verdicts off
    `app.state` and is documented as issuing nothing, `deployed_aws_posture()` is
    at most one manifest read per five minutes and opens no socket, and
    `cached_installation_report()` is the deliberately non-probing accessor -- it
    returns the last sweep or None and never refreshes, which is what keeps three
    paginated describes off the demo's critical path. The catalog must stay as
    fast as it was before it started telling the truth.

    Read off `app.state` rather than through the manager on purpose: these are
    the same objects `/readyz` reads, so the two surfaces cannot drift apart by
    consulting different copies of the answer. The one exception is
    `grant_refusals`, which no other surface has: it is what the arm path
    observed Databricks refuse, and the run manager is the only thing that made
    the call. Reading it is a dict copy.
    """

    # Through the shared accessor rather than off the sentry directly, and the
    # difference is one whole class of refusal. The deployed startup credential
    # check reports rather than raises, so a container can come up for the two
    # rounds that need no AWS -- and in the window before the probe's first
    # answer that refusal is the only thing that knows AWS has already said no.
    # Reading the sentry alone was safe only while the deployed network refusal
    # covered every AWS-backed round unconditionally; an installation that seals
    # the published egress prefixes removes that cover, and the catalog would go
    # back to offering three rounds that cannot arm on the first render after a
    # restart. `/readyz` reconciles the same two sources through the same rule.
    verdict = effective_credential_verdict(request.app.state)
    run_manager = getattr(request.app.state, "run_manager", None)
    # What this installation sealed for the deployed app. Cached on the same TTL
    # as every other signal here and reading no socket at all, so the catalog
    # stays as cheap as the docstring above promises. It decides whether the two
    # deployed refusals still apply: before the egress prefixes are sealed the
    # app is refused at the network, and after they are sealed refusing it would
    # be the round-select screen telling the room a round cannot run while it
    # demonstrably does.
    posture = deployed_aws_posture()
    signals = round_availability.AvailabilitySignals(
        credentials=verdict,
        presence=presence_from_report(cached_installation_report()),
        deployed=selfheal.deployed(),
        deployed_aws_path_sealed=posture.egress_sealed,
        round5_runtime_role_sealed=posture.runtime_role_sealed,
        grant_refusals=getattr(run_manager, "grant_refusals", None) or {},
    )

    gate = getattr(request.app.state, "readiness_gate", None)
    if gate is None:
        # No gate to ask, which is the local in-memory path and the unit-test
        # path -- exactly the reading `RunManager.bout_status` takes when
        # `_readiness_status` is unset. There is no disagreement to have.
        return signals
    # A gate that exists but has not reported yet is a different thing entirely,
    # and it defaults the other way for a reason: `/readyz` reads a missing
    # `ring_ready` as False and turns it into a 503. Defaulting permissively here
    # would recreate the very disagreement this module exists to remove, in the
    # narrow window where it matters most -- the first moments of a process's
    # life, which is when somebody is most likely to be loading the screen.
    status_now = getattr(gate, "status", None)
    round5_status = getattr(gate, "round5_status", None)
    round5_reason_code = getattr(round5_status, "reason_code", None)
    if (
        round5_reason_code is None
        and getattr(round5_status, "maintenance_state", None) == "maintenance"
    ):
        # The readiness reconciler is replica-local and can observe the durable
        # cleanup phase one poll after the result publisher. Maintenance without
        # a durable blocked verdict is conservatively cleanup, never generic
        # UNAVAILABLE. A live lease still outranks this as BOUT IN PROGRESS in
        # the all-round endpoint.
        round5_reason_code = "cleanup_in_progress"
    return replace(
        signals,
        ring_ready=bool(getattr(status_now, "ring_ready", False)),
        ring_detail=getattr(status_now, "maintenance_detail", None),
        round5_ring_ready=bool(getattr(round5_status, "ring_ready", False)),
        round5_reason_code=round5_reason_code,
        round5_detail=getattr(round5_status, "maintenance_detail", None),
    )


@router.get("/catalog", response_model=CatalogResponse)
async def get_catalog(request: Request) -> CatalogResponse:
    run_manager = manager(request)
    sealed = catalog(
        model_score_available=run_manager.model_score_available,
        connection_spike_available=run_manager.connection_spike_available,
        live_orders_available=run_manager.live_orders_available,
    )
    # The seal says what was built; this says what can arm. Offering the first as
    # though it were the second is what put six green rounds on the round-select
    # screen while /readyz was reporting that none of them could start.
    return sealed.model_copy(
        update={
            "rounds": round_availability.apply(
                sealed.rounds, _availability_signals(request)
            )
        }
    )


def _fight_card_round_status(
    round_id: RoundId,
    availability: object,
    bout: BoutStatus,
) -> FightCardRoundStatus:
    active_phase = bout.phase if bout.active else None
    if bout.active and active_phase == "cooldown_failed":
        state = FightCardState.UNAVAILABLE
        detail = (
            "Return-to-idle confirmation timed out. The finite safety fence will "
            "expire without a heartbeat; inspect the failed cleanup before retrying."
        )
    elif bout.active and active_phase in _CLEANUP_PHASES:
        state = FightCardState.CLEANUP_IN_PROGRESS
        detail = _CLEANUP_DETAIL[round_id]
    elif bout.active:
        state = FightCardState.BOUT_IN_PROGRESS
        detail = "BOUT IN PROGRESS · This round is already in use. Other rounds remain available."
    elif getattr(availability, "availability_reason_code", None) == "cleanup_in_progress":
        state = FightCardState.CLEANUP_IN_PROGRESS
        detail = _CLEANUP_DETAIL[round_id]
    elif (
        getattr(availability, "availability", None) != Availability.READY
        or not bout.can_start
    ):
        state = FightCardState.UNAVAILABLE
        detail = (
            getattr(availability, "availability_headline", None)
            or "This round is unavailable right now."
        )
    else:
        state = FightCardState.READY
        detail = None
    return FightCardRoundStatus(
        round_id=round_id,
        state=state,
        can_start=state == FightCardState.READY,
        active_phase=active_phase,
        detail=detail,
        updated_at=bout.updated_at,
        expires_at=bout.expires_at,
    )


@router.get("/bout/all", response_model=AllBoutStatus)
async def get_all_bout_statuses(request: Request) -> AllBoutStatus:
    """Return the six fight-card states without exposing lease ownership."""

    try:
        run_manager = manager(request)
        bouts = await run_manager.all_bout_statuses()
        live_catalog = await get_catalog(request)
        availability = {item.id: item for item in live_catalog.rounds}
        return AllBoutStatus(
            rounds={
                round_id: _fight_card_round_status(
                    round_id,
                    availability[round_id],
                    bouts[round_id],
                )
                for round_id in RoundId
            },
            updated_at=datetime.now(UTC),
        )
    except Exception as exc:
        logger.error("All-round status lookup failed: %s", type(exc).__name__)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=_CONTROL_UNAVAILABLE,
        ) from exc


@router.get("/bout", response_model=BoutStatus)
async def get_bout_status(
    request: Request,
    round_id: RoundId | None = None,
) -> BoutStatus:
    try:
        return await manager(request).bout_status(round_id=round_id)
    except AmbiguousRingQueryError as exc:
        # 400 and not 503: the installation is fine, the question was not
        # answerable. The message names the parameter and the valid rounds, so a
        # curl that used to get a false green light now gets the fix.
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except Exception as exc:
        logger.error("Ring status lookup failed: %s", type(exc).__name__)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=_CONTROL_UNAVAILABLE,
        ) from exc


@router.post("/sessions", response_model=SessionSnapshot, status_code=status.HTTP_201_CREATED)
async def create_session(body: SessionCreate, request: Request) -> SessionSnapshot:
    return await _control_operation(
        manager(request).create(body),
        value_error_status=status.HTTP_422_UNPROCESSABLE_ENTITY,
    )


@router.get("/sessions/{session_id}", response_model=SessionSnapshot)
async def get_session(session_id: str, request: Request) -> SessionSnapshot:
    return await _control_operation(manager(request).get(session_id))


@router.get("/receipts", response_model=ReceiptsResponse)
async def get_receipts(since: str | None = None) -> ReceiptsResponse:
    """Every sealed bout of this installation on or after `since`, oldest first.

    Read-only and independent of the in-memory session store, so a bout stays
    inspectable long after its session has been released -- and after the process
    that ran it has exited.

    Both stores are read, because which one holds a given bout depends on where
    it was run: an operator's laptop writes files beside its manifest, and a
    deployed replica -- which has no manifest directory and no filesystem that
    outlives a restart -- writes rows on the coordination database. A bout held
    in both is reported once.

    Receipts belonging to a previous installation are withheld. Its cloud resources
    have been destroyed, so its numbers describe infrastructure that no longer
    exists, and showing them beside today's would present them as this demo's.
    """
    parsed: date | None = None
    if since:
        try:
            parsed = date.fromisoformat(since)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="since must be an ISO date, for example 2026-08-20",
            ) from exc
    return ReceiptsResponse(
        receipts=await load_receipts_async(parsed, installation=current_installation())
    )


# ---------------------------------------------------------------------------
# Installation health, and the one route that may spend money
#
# Kept apart from the session routes on purpose. Everything above answers "what
# is happening in this bout"; these three answer "does the infrastructure the
# bouts run on still exist", which is a question that has to remain answerable
# precisely when the ring is refusing to serve. None of them touches the
# readiness gate, and none of them is behind it.
# ---------------------------------------------------------------------------


class RecoveryAttemptView(BaseModel):
    """One recovery attempt, as read back off disk."""

    attempt_id: str
    phase: str
    detail: str
    started_at: str = ""
    finished_at: str = ""
    exit_code: int | None = None
    pid: int | None = None
    log_tail: list[str] = Field(default_factory=list)


class RecoveryOfferView(BaseModel):
    offered: bool
    code: str
    refusal: str
    confirmation_phrase: str
    usd_per_day: str
    usd_per_day_basis: str
    plan: str
    attempts_in_window: int
    attempts_allowed: int


class InstallationView(BaseModel):
    """The four presence states, the manifest, and whether a repair is offered.

    Four states and not a boolean, because "I looked and they are gone", "I
    could not look", "I looked and they are there" and "nobody has looked yet"
    are four different things and three of them are not a green light. Collapsing
    them is this project's most-repeated defect.
    """

    state: str
    detail: str
    sealed_resources: int
    absent_resources: int
    checked: bool
    checked_seconds_ago: int
    reason: str
    deployed: bool
    manifest_status: str
    manifest_run_id: str
    transitional_recovery: str
    mutation_in_progress: bool
    mutation_holder: str
    recovery: RecoveryOfferView
    attempt: RecoveryAttemptView | None = None
    #: "operator" or "viewer". Which of the two this is decides what the prose
    #: fields above contain, so a client cannot render operator advice to a
    #: viewer even by ignoring this: `_for_viewer` has already emptied them.
    audience: str = selfheal.AUDIENCE_OPERATOR


class RecoveryRequest(BaseModel):
    """The confirmation. No default, so an empty body is a 422 rather than a spend."""

    confirm: str


#: One place decides the code, one place turns it into a status. A 409 is the
#: honest answer for every refusal that a change of state would clear; the two
#: exceptions are the deployed refusal, which no change of state clears, and the
#: rate limit, which is what 429 is for.
_REFUSAL_STATUS = {
    selfheal.CODE_DEPLOYED: status.HTTP_403_FORBIDDEN,
    selfheal.CODE_RATE_LIMITED: status.HTTP_429_TOO_MANY_REQUESTS,
}


def _attempt_view(view: selfheal.AttemptView | None) -> RecoveryAttemptView | None:
    if view is None:
        return None
    return RecoveryAttemptView(
        attempt_id=view.attempt_id,
        phase=view.phase,
        detail=view.detail,
        started_at=view.started_at,
        finished_at=view.finished_at,
        exit_code=view.exit_code,
        pid=view.pid,
        log_tail=list(view.log_tail),
    )


def _offer_view(offer: selfheal.RecoveryOffer) -> RecoveryOfferView:
    return RecoveryOfferView(
        offered=offer.offered,
        code=offer.code,
        refusal=offer.refusal,
        confirmation_phrase=offer.confirmation_phrase,
        usd_per_day=offer.usd_per_day,
        usd_per_day_basis=selfheal.COST_BASIS,
        plan=offer.plan,
        attempts_in_window=offer.attempts_in_window,
        attempts_allowed=offer.attempts_allowed,
    )


def _latest_attempt() -> selfheal.AttemptView | None:
    try:
        return selfheal.latest_attempt()
    except (RuntimeError, OSError, ValueError):
        # No manifest path, which is the deployed case. Not an error: there is
        # simply no attempt history where there can be no attempts.
        return None


def caller_email(request: Request) -> str:
    """The authenticated end user's email, or empty when nothing forwarded one.

    Deliberately not `operator_from_request`, which raises 401 on a request that
    carries no identity headers. That is right for a control action -- an
    unidentified caller must not move the ring -- and wrong here: this route
    renders a screen, and turning a missing header into an error would replace
    the demo with an error page for the one class of caller least able to read
    it. Absent is answered as absent, and `audience_for` treats absent as a
    viewer.
    """
    header = request.headers.get("x-forwarded-email") or ""
    if not header.strip():
        preferred = (request.headers.get("x-forwarded-preferred-username") or "").strip()
        header = preferred if "@" in preferred else ""
    return header.strip()


def _for_viewer(view: InstallationView) -> InstallationView:
    """The same facts with every operator sentence removed before it is serialised.

    Emptied on the server rather than hidden in the browser, and the difference
    is the point. The text being withheld names a Terraform state file, a
    mutation lock, a secret environment variable and a shell command, and it
    carries whatever the provider said -- an `ExpiredToken` trace, a resource
    identifier, a log tail. None of that is a viewer's to read, and none of it
    needs to cross the wire to be rendered as nothing.

    What survives is the machine-readable skeleton: the presence state, the two
    counts, whether this is the deployed app, and the refusal *code*. Those are
    facts without advice, they carry no identifier, and `/readyz` publishes the
    same signals to anybody who asks. Withholding them would be hiding the fault
    rather than re-addressing it.

    `offered` is forced false. Today it cannot be anything else -- a viewer is
    only ever classified in the deployed app, where `build_offer` refuses before
    it considers anything -- so this asserts an invariant rather than changing an
    outcome, and it is the invariant worth asserting: no viewer is ever one
    client-side bug away from a control that spends money.
    """
    return view.model_copy(
        update={
            "audience": selfheal.AUDIENCE_VIEWER,
            "detail": "",
            "reason": "",
            "transitional_recovery": "",
            "mutation_holder": "",
            "recovery": view.recovery.model_copy(
                update={
                    "offered": False,
                    "refusal": "",
                    "confirmation_phrase": "",
                    "usd_per_day": "",
                    "usd_per_day_basis": "",
                    "plan": "",
                }
            ),
            "attempt": None,
        }
    )


@router.get("/installation", response_model=InstallationView)
async def get_installation(request: Request, recheck: bool = False) -> InstallationView:
    """Whether the sealed AWS infrastructure is still there, and what may be done.

    `recheck=true` drops the cached verdict and sweeps the account live. It is
    what the "Check the account now" control calls, and it is the only way an
    operator can turn `never_checked` into an answer without waiting.

    Answered in one of two registers. An operator gets the whole diagnosis,
    unchanged, and locally that is every caller. A viewer of the deployed app
    gets the states and none of the prose, because every remedy this surface
    names is a command on a machine they are not sitting at -- and the banner
    this feeds is fixed to the top of a screen that gets projected. Which rounds
    can actually run tonight is `/api/catalog`'s answer and is untouched by this.
    """
    presence, age = await selfheal.observe_presence(force=recheck)
    state = selfheal.manifest_state()
    held, holder = selfheal.mutation_in_progress()
    offer = selfheal.build_offer(presence, state)
    view = InstallationView(
        state=presence.state,
        detail=presence.detail,
        sealed_resources=presence.sealed,
        absent_resources=presence.absent,
        checked=presence.checked,
        checked_seconds_ago=int(age),
        reason=presence.reason,
        deployed=selfheal.deployed(),
        manifest_status=state.status,
        manifest_run_id=state.run_id,
        transitional_recovery=state.transitional_recovery,
        mutation_in_progress=held,
        mutation_holder=holder,
        recovery=_offer_view(offer),
        attempt=_attempt_view(_latest_attempt()),
    )
    if selfheal.audience_for(caller_email(request)) == selfheal.AUDIENCE_VIEWER:
        return _for_viewer(view)
    return view


@router.get("/installation/recovery/{attempt_id}", response_model=RecoveryAttemptView)
async def get_recovery_attempt(attempt_id: str) -> RecoveryAttemptView:
    """Progress for one recovery, read from the file its mutator writes.

    This is the progress channel, and it is a file rather than a stream on
    purpose -- see the note in `server/selfheal.py`. The short version: the SSE
    endpoint is session-scoped and there is no session during a recovery, and
    the thing being watched outlives the watcher, because the mutator is
    detached and this server may be restarted while it runs.
    """
    if "/" in attempt_id or "\\" in attempt_id or attempt_id.startswith("."):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No such attempt")
    try:
        view = selfheal.attempt_view(attempt_id)
    except (RuntimeError, OSError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="This installation records no recovery attempts",
        ) from exc
    if view is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No such attempt")
    result = _attempt_view(view)
    assert result is not None
    return result


@router.post("/installation/recover", status_code=status.HTTP_202_ACCEPTED)
async def recover_installation(
    body: RecoveryRequest,
    request: Request,
    recovery_token: str | None = Header(
        default=None, alias=selfheal.RECOVERY_TOKEN_HEADER
    ),
) -> dict[str, object]:
    """Spawn a detached installer. The only route in this app that spends money.

    Every guard is a refusal an operator can read, and the order is deliberate:
    the cheap and categorical ones first, the live account sweep last, so the
    "verified missing" finding that authorises the spend is taken *immediately*
    before the fork rather than at render time. A verdict that was true when the
    screen was drawn is not a verdict that is true when the button is pressed.
    """
    if selfheal.deployed():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail=selfheal.deployed_refusal()
        )
    denial = selfheal.authorisation_refusal(
        getattr(request.client, "host", "") or "",
        recovery_token,
    )
    if denial:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=denial)
    try:
        selfheal.recovery_paths()
    except (RuntimeError, OSError, ValueError) as exc:
        # No manifest path means no lock, no journal and no Terraform state.
        # Physically the deployed case, so it gets the deployed words.
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail=selfheal.deployed_refusal()
        ) from exc

    # The live read. Condition 2 of D9a: never a cached or inferred verdict.
    presence, _ = await selfheal.observe_presence(force=True)
    state = selfheal.manifest_state()
    offer = selfheal.build_offer(presence, state)
    if not offer.offered:
        raise HTTPException(status_code=_REFUSAL_STATUS.get(offer.code, 409), detail=offer.refusal)
    if body.confirm.strip() != offer.confirmation_phrase:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                "RECOVERY IS REFUSED: the confirmation does not match. Type the "
                f"phrase exactly: {offer.confirmation_phrase}\n\n"
                "It is issued by the server and it names both the generation "
                "that will be re-created and what it costs to keep alive, so "
                "confirming cannot be done without having read both. "
                f"{offer.plan}"
            ),
        )
    operator = operator_from_request(request)
    result = selfheal.spawn_recovery(
        run_id=state.run_id,
        plan=offer.plan,
        operator=operator.display_name,
        usd_per_day=offer.usd_per_day,
    )
    logger.warning(
        "Recovery attempt %s authorised by %s; this process provisions nothing itself",
        result.attempt_id,
        operator.display_name,
    )
    return {
        "attempt_id": result.attempt_id,
        "pid": result.pid,
        "log_path": result.log_path,
        "plan": offer.plan,
        "usd_per_day": offer.usd_per_day,
        "poll": f"/api/installation/recovery/{result.attempt_id}",
    }


@router.post("/sessions/{session_id}/arm", response_model=SessionSnapshot)
async def arm_session(session_id: str, request: Request) -> SessionSnapshot:
    return await _control_operation(
        manager(request).start_arm(session_id, operator_from_request(request)),
        missing_session=(request, session_id),
    )


@router.post("/sessions/{session_id}/cancel-arm", response_model=SessionSnapshot)
async def cancel_session_arm(session_id: str, request: Request) -> SessionSnapshot:
    return await _control_operation(
        manager(request).cancel_arm(
            session_id,
            operator_from_request(request),
        ),
        missing_session=(request, session_id),
    )


@router.post("/sessions/{session_id}/run", response_model=SessionSnapshot)
async def run_session(session_id: str, request: Request) -> SessionSnapshot:
    return await _control_operation(
        manager(request).start_run(session_id, operator_from_request(request)),
        missing_session=(request, session_id),
    )


@router.post("/sessions/{session_id}/redo", response_model=SessionSnapshot)
async def redo_session(session_id: str, request: Request) -> SessionSnapshot:
    return await _control_operation(
        manager(request).start_redo(
            session_id,
            operator_from_request(request),
        ),
        missing_session=(request, session_id),
    )


@router.post("/sessions/{session_id}/retry-cleanup", response_model=SessionSnapshot)
async def retry_session_cleanup(session_id: str, request: Request) -> SessionSnapshot:
    return await _control_operation(
        manager(request).retry_connection_spike_cleanup(
            session_id,
            operator_from_request(request),
        ),
        missing_session=(request, session_id),
    )


@router.post("/sessions/{session_id}/towel", response_model=SessionSnapshot)
async def throw_towel(session_id: str, request: Request) -> SessionSnapshot:
    return await _control_operation(
        manager(request).start_towel(
            session_id,
            operator_from_request(request),
        ),
        missing_session=(request, session_id),
    )


@router.post("/sessions/{session_id}/cooldown", response_model=SessionSnapshot)
async def start_cooldown(session_id: str, request: Request) -> SessionSnapshot:
    return await _control_operation(
        manager(request).start_cooldown(
            session_id,
            operator_from_request(request),
        ),
        missing_session=(request, session_id),
    )


@router.get("/sessions/{session_id}/events")
async def session_events(
    session_id: str,
    request: Request,
    after: int = 0,
    last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
) -> EventSourceResponse:
    resume_after = max(after, 0)
    if last_event_id is not None:
        try:
            header_after = int(last_event_id)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Last-Event-ID must be a nonnegative integer",
            ) from exc
        if header_after < 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Last-Event-ID must be a nonnegative integer",
            )
        resume_after = max(resume_after, header_after)

    await _control_operation(manager(request).get(session_id))

    async def stream():
        async for item in manager(request).events(session_id, resume_after):
            if await request.is_disconnected():
                break
            yield {
                "id": str(item.sequence),
                "event": item.event,
                "data": json.dumps(item.model_dump(mode="json")),
            }

    return EventSourceResponse(stream(), ping=15)
