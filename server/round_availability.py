"""Whether each round can actually arm right now, and why not when it cannot.

The defect this closes: ``/api/catalog`` reported all six rounds
``availability: "ready"`` with no reason attached -- including Round 5 -- at the
same moment ``/readyz`` was returning 503 ``not_ready``, no round could arm, and
every bout failed at the starting line. The catalog is the round-select screen an
audience looks at, so a round shown green that dies when the bell rings is the
worst failure mode this project has.

The cause was structural rather than a stale value. Nothing in the catalog
consulted live state at all: Rounds 1-3 were literal ``Availability.READY``
constants in the ``ROUNDS`` list, and Rounds 4-6 were recomputed from
adapter-factory presence, which is decided once at process construction from the
sealed manifest and cannot change afterwards. There was no seam for any of the
six to be reported unavailable through.

Four rules this module exists to keep.

**It consults the objects ``/readyz`` consults, and applies the same severity
rules.** Agreement between the two surfaces is the requirement, so a signal that
would make one of them call a round fine while the other says nothing can arm is
a bug here rather than a difference of opinion. Every refusal below corresponds
to a state ``/readyz`` already reports as ``not_ready`` or ``degraded``.

**It reads caches only, and never calls AWS.** Each signal is a verdict somebody
else refreshes on an interval -- the credential sentry every 300s, the account
sweep behind a 300s TTL, the readiness gate in-process and free. Nothing here
opens a socket. The catalog is on the demo's critical path and a health surface
that turns into a load generator against STS is a defect of its own.

**It never upgrades.** A round sealed ``planned`` or ``preview`` stays that way.
This can only take readiness away, so no live signal can talk a non-executable
round into being offered.

**It refuses to read "nobody has looked" as good news.** A signal that has never
answered is passed through as itself and is never counted as a positive
confirmation. It does not manufacture a refusal either: ``/readyz`` does not
degrade on an unread account sweep or a first-few-seconds credential verdict, so
neither does this, because inventing a refusal the health surface disagrees with
would re-create the same disagreement pointed the other way.

THE SECOND INCIDENT, 2026-08-23. The deployed app was driven for the first time
and Rounds 4 and 6 were both refused by Databricks on their very first Lakebase
call -- one missing ``SELECT`` on a synced table, one missing ``Can Use`` on a
database project -- while this module was reporting both of them ``ready``. The
four rules above were all being kept. The cause was that app-side Databricks
grants were not among the signals at all, so a refusal that had *already
happened* had nowhere to be reported through.

``grant_refusals`` is that seam, and the shape it takes is the one the four
rules force. A live permission probe per render is out under the second rule,
and a probe cached behind a TTL is the same socket moved somewhere less
predictable. What is left is the only evidence about Databricks grants this
process can have for free: what happened the last time a round actually tried.
An arm refused on authorization is a *stronger* answer than any probe could give
-- it is the real call, against the real principal, failing -- and letting the
catalog keep the answer is the whole fix. A round Databricks has already refused
by name is not offered again until an arm of that round succeeds, which is the
one piece of evidence more recent and more authoritative than the refusal.

The consequence, stated plainly because it is a real cost: the very first render
after a fresh start still shows such a round ``ready``, because nothing has
asked yet and rule four forbids manufacturing a refusal out of not having asked.
What is closed is the repeat -- a round that has died in front of the room once
cannot be advertised green to the room again.

THE THIRD INCIDENT, 2026-08-23. Every refusal above was correct, and every one
of them was being read out to the wrong person. The fight card for a round the
deployed app cannot run filled its WHY panel with the paragraph below about TCP
5432, database security groups, operator CIDRs and a /16 of general-purpose EC2
-- true, argued, and written for the one person in the building who provisioned
the install. A viewer got nine lines telling them to do something only the
operator can do, on a machine only the operator has.

That is this module's opening paragraph again, one screen further in. A round
shown green that dies at the bell and a round whose refusal is unreadable to
the room are the same failure: the round-select screen saying something to an
audience that the audience cannot act on. So a refusal is now two sentences
rather than one -- see ``RoundRefusal``. The detail is unchanged and still goes
out to the catalog; what is new is that a headline goes with it, decided in the
same branch so the two cannot come to disagree, and the fight card leads with
the headline and folds the detail behind a disclosure.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from .models import Availability, RoundDefinition, RoundId
from .reconcile import PRESENCE_MISSING, InstallationPresence

#: Rounds that reach a live Aurora or RDS opponent, and so cannot arm without
#: working AWS credentials and the sealed infrastructure still being in the
#: account. Rounds 4 and 6 are deliberately absent: both are capability-gap
#: rounds whose AWS lane is disclosed rather than raced, so they execute against
#: Lakebase alone and need no AWS at all. `/readyz` reasons about them the same
#: way -- it refuses to turn a missing installation into a 503 partly because
#: "Rounds 4 and 6 need no AWS at all".
#:
#: This set also decides the deployed-context network refusal, because "reaches a
#: live Aurora or RDS opponent" is exactly "must open TCP 5432 to a security group
#: that admits only what this installation sealed". Measured 2026-08-22 from
#: CloudTrail: the deployed app called AWS from two different egress addresses
#: inside one 70-minute window, across a restart, while the only IPv4 range any of
#: the seven database groups admitted was the sealed operator /32. The specific
#: addresses are deliberately not recorded here -- the finding is that the egress
#: *varies*, so writing either one down would state a fixed value that is already
#: known not to be fixed.
#:
#: What that measurement did not establish, and what turned out to be the whole
#: question, is that the varying addresses are not unbounded. Databricks publishes
#: the serverless egress prefixes its apps leave from, and an installation that
#: seals them admits the app without ever naming an individual address -- which is
#: why the refusal below is conditional on `deployed_aws_path_sealed` rather than
#: on `deployed` alone. (Round 5's pair also admits the in-VPC runner group, which
#: is not a path a Databricks App can take either, and never needed to be.)
AWS_BACKED_ROUNDS = frozenset(
    {
        RoundId.WAKE_IDLE_APP,
        RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
        RoundId.RECOVER_DELETED_ORDER,
        RoundId.SURVIVE_CONNECTION_SPIKE,
    }
)

#: Credential verdicts that stop every AWS lane. Taken from the probe's own
#: classification rather than re-derived: `CredentialVerdict.capabilities_lost`
#: returns `_EVERY_LANE_LOST` for exactly these, and the one thing this module
#: must not do is disagree with the surface it is being reconciled against.
#:
#: `unknown` and `ok` are absent because they are not faults. `stale` is absent
#: on purpose and it is the subtlest of the set: it means the probe stopped
#: reporting, which says nothing about whether the credentials work, and
#: `capabilities_lost` claims nothing for it either.
_TOTAL_CREDENTIAL_FAULTS = frozenset(
    {
        "absent",
        "misconfigured",
        "rejected",
        "wrong_account",
        "unpermitted",
        "unreachable",
    }
)

#: Round 5 alone. Its runner assumes a control role whose trust policy seals
#: exactly one principal, so a process authenticating as anything else is denied
#: when it tries -- and the other five rounds authenticate as this principal
#: directly and are unaffected. Same scoping the probe applies for `_ROUND5_LOST`.
_ROUND5_ONLY_CREDENTIAL_FAULT = "principal_mismatch"

#: The opening every audience-facing refusal shares. The screen it lands on is
#: called the fight card, so this is the demo's own vocabulary rather than a
#: euphemism invented to avoid saying something went wrong.
NOT_ON_THE_CARD = "THIS ROUND IS NOT ON TONIGHT'S CARD."

_RING_HEADLINE = (
    f"{NOT_ON_THE_CARD} Backstage has not finished getting the ring ready, so "
    "no round can start yet."
)

_CREDENTIALS_HEADLINE = (
    f"{NOT_ON_THE_CARD} It races a live AWS database, and this show cannot "
    "reach one right now."
)

_AWS_LANE_DEPLOYED_HEADLINE = (
    f"{NOT_ON_THE_CARD} It races a live AWS database that only the operator's "
    "own machine is allowed to reach, and this is the hosted app."
)

_ROUND5_OPERATOR_ONLY_HEADLINE = (
    f"{NOT_ON_THE_CARD} Round 5 is run by the operator, from the operator's own "
    "machine, as the one identity its controls were sealed to."
)

_GRANT_HEADLINE = (
    f"{NOT_ON_THE_CARD} This app is missing a Databricks permission the round "
    "reads, and only a workspace admin can grant it."
)

_ROUND5_RING_HEADLINE = (
    f"{NOT_ON_THE_CARD} Round 5's own backstage cleanup has not finished. The "
    "other rounds are unaffected."
)

_ABSENT_OPPONENT_HEADLINE = (
    f"{NOT_ON_THE_CARD} The opponent it races is not set up in the account "
    "right now."
)

AWS_LANE_DEPLOYED_REFUSAL = (
    "THIS ROUND CANNOT RUN IN THE DEPLOYED APP UNTIL THE INSTALLATION ADMITS IT, "
    "AND THIS IS NOT A FAULT TO WAIT OUT. It races a live Aurora or RDS opponent "
    "over TCP 5432, and every one of those database security groups admits a "
    "single operator CIDR: the laptop that provisioned the install. The deployed "
    "Databricks App egresses from addresses this account neither owns nor "
    "controls, and they vary from one app restart to the next, so the connection "
    "is refused at the network before any credential is read. What closes this is "
    "an ingress rule, and there is one to add: Databricks publishes the serverless "
    "egress prefixes its apps leave from, and for this region they are a handful "
    "of narrow ranges rather than the /16 of general-purpose EC2 that was once "
    "believed to be the only option and was refused outright as a boundary in "
    "front of a live database. Sealing them beside the operator CIDR is what "
    "admits this app, it needs no account admin and no new spend, and it is done "
    "by an operator running './antidemo setup' -- which re-polls the published "
    "list, reseals it and re-applies the security groups. This installation has "
    "not sealed that list, so until it does, the round runs from a local "
    "checkout, attended, from the sealed operator address. Rounds 4 and 6 reach "
    "no AWS database and are unaffected here."
)

ROUND5_DEPLOYED_REFUSAL = (
    "ROUND 5 CANNOT RUN IN THE DEPLOYED APP UNTIL THE INSTALLATION SEALS A ROLE "
    "BOTH CALLERS CAN ASSUME, AND THIS IS NOT A FAULT TO WAIT OUT. Its runner "
    "assumes a control role whose trust policy names exactly one principal, this "
    "installation sealed no shared runtime role for the app and the operator to "
    "reach it through, and nothing in this app can change a sealed trust policy. "
    "The mechanism that closes this exists and is switched off: a runtime role "
    "trusted by both the app's IAM principal and the operator's own, which the "
    "control role then trusts as its single principal. It can only be sealed at "
    "first provision, so it is a fresh install rather than a repair. Until then "
    "Round 5 runs from a local checkout, attended, as the sealed principal; the "
    "other five rounds are unaffected here."
)

_UNSETTLED_RING_REFUSAL = (
    "NO ROUND CAN ARM: the backstage readiness gate has not reported a usable "
    "ring, so every bout would be refused before the bell. This is the same "
    "state /readyz reports as not_ready."
)

_ROUND5_UNSETTLED_REFUSAL = (
    "ROUND 5 CANNOT ARM: its own backstage cleanup has not reported ready, so "
    "arming it would be refused before the bell. The other rounds are unaffected."
)

#: The one sentence every Databricks authorization refusal carries, on both
#: surfaces that report one. `server.manager` puts it on the bout's failure
#: banner and `grant_refusal` below puts it on the round-select screen, from the
#: same constant, so an operator who reads either is sent to the same place.
#: Naming the workspace admin is the point: this is the one class of refusal
#: nothing in this app or on the operator's laptop can clear.
GRANT_REFUSAL_HEADLINE = (
    "DATABRICKS REFUSED THIS ROUND ON AUTHORIZATION, AND THIS IS NOT A FAULT TO "
    "WAIT OUT. This app's service principal has not been granted something the "
    "round reads, and only a workspace admin can grant it."
)


def grant_refusal(diagnosis: str) -> str:
    """The round-select screen's sentence for a round Databricks has refused.

    ``diagnosis`` is Databricks' own words, carried through rather than
    paraphrased, because the paraphrase is what the incident was: the refusals
    named the exact table, the exact principal and the exact permission, and the
    app replaced all three with "could not be verified".
    """

    return (
        f"THIS ROUND CANNOT ARM: {GRANT_REFUSAL_HEADLINE} It was refused the last "
        "time it was armed in this process, and it will be offered again as soon "
        f"as an arm succeeds. Databricks said: {diagnosis}"
    )


@dataclass(frozen=True)
class AvailabilitySignals:
    """The cached live state the catalog is allowed to consult.

    Every field defaults to the permissive answer, and that default is load
    bearing rather than lazy. A process with no readiness gate is the local
    in-memory path and the unit-test path, where there is genuinely no gate to
    disagree with -- exactly the reading ``RunManager.bout_status`` already takes
    when ``_readiness_status`` is unset. Defaulting the other way would report
    six unavailable rounds on every installation that has no gate to ask.

    ``credentials`` and ``presence`` are the cached verdicts themselves rather
    than booleans, so the severity rules stay in one place and the reason text an
    operator reads is the one the probe wrote.
    """

    ring_ready: bool = True
    ring_detail: str | None = None
    round5_ring_ready: bool = True
    round5_detail: str | None = None
    #: A `server.aws_credential_probe.CredentialVerdict`, or None when this
    #: process runs no probe. Typed loosely to keep this module importable
    #: without pulling botocore in behind it.
    credentials: Any | None = None
    presence: InstallationPresence | None = None
    #: Whether this is the deployed Databricks App rather than a local checkout.
    #: `server.selfheal.deployed()` is the one predicate that decides this, so
    #: the catalog and the recovery refusals cannot disagree about which they are.
    deployed: bool = False
    #: Whether this installation has sealed the published Databricks serverless
    #: egress prefixes into its database security groups. Until it has, the
    #: deployed app is refused at the network on Rounds 1, 2, 3 and 5; once it
    #: has, those groups admit the app's egress and the refusal below would be a
    #: lie told to a room about a round that works.
    #:
    #: Defaults to the *refusing* answer rather than the permissive one, which is
    #: the opposite of every other field here and is deliberate. The permissive
    #: default elsewhere means "there is no gate to disagree with"; here there is
    #: a definite fact -- an installation that sealed no list admits nobody -- and
    #: guessing it sealed one would put a green round on the card that dies at the
    #: bell. It only ever matters when `deployed` is true, and that is false by
    #: default too, so no local or unit-test path is affected.
    deployed_aws_path_sealed: bool = False
    #: Whether this installation sealed a runtime role that both the deployed app
    #: and the operator can assume, which is what lets Round 5's control role keep
    #: trusting exactly one principal while both callers reach it. Same default
    #: and same reasoning as the field above.
    round5_runtime_role_sealed: bool = False
    #: Rounds Databricks has already refused on authorization in this process,
    #: keyed to the reason an operator should read. Written by the arm path in
    #: `server.manager`, which is the only thing here that has made the call, and
    #: cleared by that round's next successful arm. Empty is the honest default
    #: and the ordinary one: nothing has been refused yet.
    grant_refusals: Mapping[RoundId, str] = field(default_factory=dict)


@dataclass(frozen=True)
class RoundRefusal:
    """Why a round cannot arm, written once for the room and once for the operator.

    Both registers come out of the same branch because the alternative is two
    chains that drift, and a headline that disagrees with its own detail is the
    audience-visible failure this module's opening paragraph is about, wearing a
    politer face.

    ``detail`` is unchanged from what this module has always returned: the full
    technical account, naming the security group, the trust policy, the exact
    permission Databricks refused. It is the reason an operator can act, and
    nothing here shortens it.

    ``headline`` is the same fact with none of that vocabulary, because the
    person most likely to be reading the fight card is not the person who
    provisioned the install. Measured against one requirement: a viewer who
    reads it knows this round will not run tonight and is not left waiting for
    it. What a viewer must never be handed is an instruction only the operator
    can carry out, on a machine only the operator has -- that is the same defect
    as a round shown green that dies at the bell, arriving one screen earlier.
    """

    headline: str
    detail: str


def _credential_state(signals: AvailabilitySignals) -> str | None:
    return getattr(signals.credentials, "state", None) if signals.credentials else None


def _credential_detail(signals: AvailabilitySignals) -> str | None:
    return getattr(signals.credentials, "detail", None) if signals.credentials else None


def refusal(round_id: RoundId, signals: AvailabilitySignals) -> RoundRefusal | None:
    """Why this round cannot arm right now, or None if nothing says it cannot.

    The order is the breadth order ``/readyz`` uses for its one
    ``degraded_detail`` slot: whatever stops the most is reported first, because
    that is what an operator has to read first. A round only ever carries one
    sentence, so the losing reasons stay readable on ``/readyz`` rather than
    being crammed in here.

    ``grant_refusals`` is the one entry that is not placed by breadth, because it
    is the one entry that is an observation rather than an inference. Its comment
    below argues its position.

    Every branch names its own headline rather than deriving one from the detail
    it returns, because half of these details are written by somebody else --
    the credential probe, the account sweep, Databricks itself -- and a headline
    summarised out of arbitrary operator prose would be a guess about text this
    module does not control.
    """

    if not signals.ring_ready:
        # The widest refusal there is, and the one the original incident was:
        # every round goes through `require_ready` before it can arm, so a ring
        # that is not ready means nothing can start regardless of its own state.
        return RoundRefusal(
            _RING_HEADLINE, signals.ring_detail or _UNSETTLED_RING_REFUSAL
        )

    credential_state = _credential_state(signals)
    if round_id in AWS_BACKED_ROUNDS and credential_state in _TOTAL_CREDENTIAL_FAULTS:
        return RoundRefusal(
            _CREDENTIALS_HEADLINE,
            _credential_detail(signals)
            or (
                "THIS ROUND CANNOT ARM: the AWS credentials in this process are "
                f"{credential_state} and every lane arms through them."
            ),
        )

    if signals.deployed and round_id in AWS_BACKED_ROUNDS:
        # Deployed before the transient signals below: these refusals are
        # structural, and reporting a structural refusal as one of the transient
        # ones would invite an operator to wait for something that will not
        # change on its own. Round 5 keeps its own sentence because its blocker
        # is a trust policy rather than the network, and an operator who reads
        # the wrong one will look in the wrong place.
        #
        # Both are now conditional, and that is the correction rather than a
        # softening. They were unconditional on `deployed` because the network
        # path was believed to be unopenable -- "there is no stable address to
        # admit" -- and that premise was false: Databricks publishes the egress
        # prefixes its apps leave from, and an installation that seals them
        # admits the app. An unconditional refusal on an installation that has
        # sealed them would be this module's opening paragraph inverted: not a
        # round shown green that dies at the bell, but a round shown refused
        # that demonstrably runs. Both are the round-select screen lying to the
        # room, and the second is the one that wastes the argument the project
        # exists to make.
        #
        # The four rules survive intact. This still only ever takes readiness
        # away, it still consults a cached verdict rather than opening a socket,
        # and a sealed list is a *positive fact recorded by a mutator* rather
        # than an absence of bad news -- so rule four is not being bent: nothing
        # here reads "nobody has looked" as good news, because the signal
        # defaults to the refusing answer.
        # Round 5's trust-policy blocker is reported before the network one
        # because it is the more specific of the two and the harder to clear:
        # the network path is an ordinary reseal, and the runtime role can only
        # be sealed at first provision. An operator told about the reseal first
        # would do it, restart, and find Round 5 still refused.
        if (
            round_id == RoundId.SURVIVE_CONNECTION_SPIKE
            and not signals.round5_runtime_role_sealed
        ):
            return RoundRefusal(_ROUND5_OPERATOR_ONLY_HEADLINE, ROUND5_DEPLOYED_REFUSAL)
        # And the network refusal covers all four, Round 5 included: it races a
        # live Aurora and RDS opponent like the rest, and a trusted role does not
        # open a security group.
        if not signals.deployed_aws_path_sealed:
            return RoundRefusal(_AWS_LANE_DEPLOYED_HEADLINE, AWS_LANE_DEPLOYED_REFUSAL)

    grant_refused = signals.grant_refusals.get(round_id)
    if grant_refused:
        # Below the two structural refusals and above every inferred one, and the
        # boundary is what an operator would do next.
        #
        # Below `deployed`, because an AWS-backed round in the deployed app
        # cannot run there whatever is granted, and sending somebody to a
        # workspace admin for a grant that will not help is a wasted evening. Two
        # of those rounds could in principle record a Databricks refusal, and
        # "in principle it cannot happen" is the reasoning that produced this
        # defect, so the order settles it rather than the argument.
        #
        # Above everything after this, because every one of those is inferred
        # from a cached verdict about something that *might* also stop this
        # round, and this is the round itself having been refused by name. A
        # prediction does not outrank an observation.
        #
        # For Rounds 4 and 6 -- the only two this realistically fires for, since
        # they are the only two that reach Lakebase and no AWS -- none of the
        # branches above applies at all, so this is effectively the first check.
        return RoundRefusal(_GRANT_HEADLINE, grant_refused)

    if round_id == RoundId.SURVIVE_CONNECTION_SPIKE:
        if credential_state == _ROUND5_ONLY_CREDENTIAL_FAULT:
            return RoundRefusal(
                _ROUND5_OPERATOR_ONLY_HEADLINE,
                _credential_detail(signals) or ROUND5_DEPLOYED_REFUSAL,
            )
        if not signals.round5_ring_ready:
            return RoundRefusal(
                _ROUND5_RING_HEADLINE,
                signals.round5_detail or _ROUND5_UNSETTLED_REFUSAL,
            )

    presence = signals.presence
    if (
        round_id in AWS_BACKED_ROUNDS
        and presence is not None
        and presence.state == PRESENCE_MISSING
    ):
        # Only a *verified* absence refuses. `unverified` and `never_checked` are
        # not evidence that anything is gone -- that distinction is the whole
        # reason `InstallationPresence` has four states instead of a boolean --
        # and `/readyz` claims no lost capabilities for either of them.
        return RoundRefusal(_ABSENT_OPPONENT_HEADLINE, presence.detail)

    return None


def resolve(round_id: RoundId, sealed: Availability, signals: AvailabilitySignals) -> tuple[
    Availability, RoundRefusal | None
]:
    """One round's live availability and the reason behind it.

    A round that was not ``ready`` to begin with keeps its sealed answer and
    gains no reason: ``planned`` and ``preview`` already mean non-executable, and
    layering a live refusal on top would replace a durable fact about the build
    with a transient one about this minute.
    """

    if sealed != Availability.READY:
        return sealed, None
    reason = refusal(round_id, signals)
    if reason is None:
        return Availability.READY, None
    return Availability.UNAVAILABLE, reason


def apply(
    rounds: Iterable[RoundDefinition],
    signals: AvailabilitySignals,
) -> list[RoundDefinition]:
    """Overlay live availability onto sealed round definitions."""

    resolved: list[RoundDefinition] = []
    for item in rounds:
        availability, reason = resolve(item.id, item.availability, signals)
        if availability == item.availability and reason is None:
            resolved.append(item)
            continue
        resolved.append(
            item.model_copy(
                update={
                    "availability": availability,
                    "availability_reason": None if reason is None else reason.detail,
                    "availability_headline": (
                        None if reason is None else reason.headline
                    ),
                },
                deep=True,
            )
        )
    return resolved


def unavailable_round_ids(rounds: Sequence[RoundDefinition]) -> tuple[RoundId, ...]:
    """The rounds this catalog is refusing to offer. For tests and diagnostics."""

    return tuple(
        item.id for item in rounds if item.availability == Availability.UNAVAILABLE
    )


__all__ = [
    "AWS_BACKED_ROUNDS",
    "AWS_LANE_DEPLOYED_REFUSAL",
    "GRANT_REFUSAL_HEADLINE",
    "NOT_ON_THE_CARD",
    "ROUND5_DEPLOYED_REFUSAL",
    "AvailabilitySignals",
    "RoundRefusal",
    "apply",
    "grant_refusal",
    "refusal",
    "resolve",
    "unavailable_round_ids",
]
