"""Loud, non-fatal construction for rounds that must never crash the server.

`app.py` builds every round factory inside a clause that must not be fatal: an
installation where five rounds work has to keep serving five rounds, so a
construction failure can only ever end in a factory that is absent. What this
module adds is that the absence is *explained*. Round 5 once disappeared from a
live installation and the only evidence anywhere was that it had stopped being
armable, which is indistinguishable from never having been installed.

Three things live here rather than at the call sites, because a round vanishing
is a whole-installation property and a per-call-site helper cannot see it:

* `build_round`, the swallow itself, so there is exactly one place that decides
  what a construction failure looks like in a log.
* `CONSTRUCTED_ROUNDS`, the enumeration of every round whose factory is built on
  the startup path. A round that is not in here is a round whose disappearance
  nothing reports, so adding a construction site without adding it here is the
  bug this constant exists to make visible.
* `round5_configs_build`, the eager Round 5 config validation, shared between
  `app.py` and the `antidemo doctor` probe so that the two cannot drift into asking
  different questions about the same seal.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from server.models import CompetitorId, RoundId

LOGGER = logging.getLogger(__name__)

#: The substring every construction failure carries, so a log scraper and a test
#: can find them without matching the whole sentence.
CONSTRUCTION_FAILED = "could not be constructed"

#: The banner for a round that is sealed, built cleanly, and still withheld
#: because a precondition outside the seal is unmet. Distinguished from
#: `CONSTRUCTION_FAILED` because nothing raised: these are the cases that used to
#: be terms of one boolean returning `None`.
ROUND_UNAVAILABLE = "ROUND UNAVAILABLE"

#: Every round whose factory is constructed on the server startup path, keyed by
#: the round number an operator says out loud.
#:
#: Rounds 1 to 3 are deliberately absent: they have no factory and no seal gate,
#: so there is no construction for them to fail. Every other round is here, and
#: `tests/test_round_construction.py` fails if a round in `catalog.ROUNDS` gains a
#: factory without being added, or loses one without being removed -- because a
#: construction site nothing enumerates is a round that can vanish unreported,
#: which is the entire failure this module exists for.
CONSTRUCTED_ROUNDS: Mapping[int, RoundId] = {
    4: RoundId.PUT_MODEL_SCORE_IN_APP,
    5: RoundId.SURVIVE_CONNECTION_SPIKE,
    6: RoundId.ANALYZE_LIVE_ORDERS,
}

#: The AWS opponents Round 5 seals a config for. Round 5 is the only round that
#: builds its config eagerly, and it builds one per competitor, so both have to
#: succeed for the round to be offered at all. Shared with `app.py` so the doctor
#: probe cannot end up validating a narrower set than the app requires.
ROUND5_COMPETITORS: tuple[CompetitorId, ...] = (
    CompetitorId.RDS_POSTGRES,
    CompetitorId.AURORA_SERVERLESS_V2,
)


def exception_diagnostic(error: BaseException) -> str:
    """Render an exception and its whole cause chain on one line.

    The chain matters: the Round 5 failure surfaced as an `InvalidStateError`
    whose actual cause was three frames down, and the outermost type was the
    least informative link in it. Not redacted, because every message in this
    chain is one this codebase raised about its own manifest.

    A cause cycle is possible -- `__context__` in particular can point back at
    something already visited -- and the diagnostic must never become the
    failure, so identity of the exceptions already rendered is what terminates
    the walk rather than a depth limit.
    """

    parts: list[str] = []
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        parts.append(f"{type(current).__name__}: {current}")
        current = current.__cause__ or current.__context__
    return " <- ".join(parts)


def build_round[T](
    round_number: int,
    round_id: RoundId,
    build: Callable[[], T | None],
    *,
    logger: logging.Logger | None = None,
) -> T | None:
    """Run a round's construction so that failure is recorded, not just absent.

    Still returns `None` on failure, and still catches everything, because the
    startup path genuinely must not die over one round. The difference is that
    the absence now has an account of itself attached.
    """

    try:
        return build()
    except Exception as error:
        (logger or LOGGER).error(
            "round=%d (%s) %s and will be absent from this process: %s",
            round_number,
            round_id.value,
            CONSTRUCTION_FAILED,
            exception_diagnostic(error),
            exc_info=True,
        )
        return None


def round_unavailable(
    round_number: int,
    round_id: RoundId,
    reason: str,
    *,
    logger: logging.Logger | None = None,
) -> None:
    """Say why a sealed round is being withheld for an unmet precondition.

    A warning rather than an error: unlike a construction failure, every caller
    of this is a state an operator can be in legitimately -- a local checkout
    with an in-memory coordinator is the ordinary development case. What was
    wrong before was that it said nothing at all, so a genuine bug (a lease
    store fencing another installation's ring) and that ordinary case produced
    the identical silence.
    """

    (logger or LOGGER).warning(
        "%s round=%d (%s) is sealed but will be absent from this process: %s",
        ROUND_UNAVAILABLE,
        round_number,
        round_id.value,
        reason,
    )


def round5_configs_build(manifest: Any) -> None:
    """Build both Round 5 configs, raising whatever the seal is wrong about.

    The single question "does this seal still produce a Round 5 config?", asked
    the same way by the app that has to serve the round and by the doctor that
    claims the installation is healthy. `_round5_topology_check` validates the
    same seal against live AWS and Databricks and never asks this, which is how
    a manifest field only a config builder reads could pass every doctor line
    and still remove the round.

    Imported inside the function, both to keep `server.connection_spike_live`
    off the import graph of anything that merely wants to log a failure, and
    because that is what lets a test replace a builder and have both callers see
    the replacement.
    """

    from server.connection_spike_live import (
        connection_spike_live_config_from_manifest,
        connection_spike_setup_config_from_manifest,
    )

    for competitor in ROUND5_COMPETITORS:
        connection_spike_live_config_from_manifest(manifest, competitor.value)
        connection_spike_setup_config_from_manifest(manifest, competitor.value)


@dataclass(frozen=True)
class RoundConstructionProbe:
    """One round's answer to "can this seal still be built?", for `antidemo doctor`."""

    round_number: int
    round_id: RoundId
    ok: bool
    detail: str

    @property
    def check_name(self) -> str:
        return f"round{self.round_number}_construction"


def probe_round_construction(manifest: Any) -> tuple[RoundConstructionProbe, ...]:
    """Ask whether each sealed round can still build its config.

    Only Round 5 builds eagerly, so only Round 5 is genuinely answerable without
    standing up a live engine. Rounds 4 and 6 build inside `factory()`, so a
    probe for them would either be a no-op or would have to reach Databricks;
    the `build_round` log lines are what covers those. Extend as other rounds
    gain eager config.

    Nothing here calls a provider, so this probe cannot disturb a neighbouring
    doctor check and its position in the list is immaterial.
    """

    probes: list[RoundConstructionProbe] = []
    if getattr(manifest, "round5_ready", False):
        detail = "config builds for both competitors"
        ok = True
        try:
            round5_configs_build(manifest)
        except Exception as error:  # noqa: BLE001 - doctor reports, never raises
            ok = False
            detail = exception_diagnostic(error)
        probes.append(
            RoundConstructionProbe(
                5, RoundId.SURVIVE_CONNECTION_SPIKE, ok, detail
            )
        )
    return tuple(probes)


__all__ = [
    "CONSTRUCTED_ROUNDS",
    "CONSTRUCTION_FAILED",
    "ROUND5_COMPETITORS",
    "ROUND_UNAVAILABLE",
    "RoundConstructionProbe",
    "build_round",
    "exception_diagnostic",
    "probe_round_construction",
    "round5_configs_build",
    "round_unavailable",
]
