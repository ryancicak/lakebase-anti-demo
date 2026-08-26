"""One source of truth for configured compute capacity on both sides of the ring.

Every capacity figure the demo applies, discloses, or gates on is defined here.
The provisioning code reads these constants when it configures a lane, the
session snapshot reads them when it discloses a lane, and `capacity_parity`
reads them when it proves the live control planes still agree. Keeping the three
consumers on one definition is what stops a quiet edit on one side from
producing an unfair result on the other.

The matched band is memory. Databricks documents one Lakebase CU as roughly 2 GB
of memory plus proportional CPU; AWS documents one Aurora Serverless v2 ACU as
"approximately 2 gibibytes (GiB) of memory, corresponding CPU, and networking".
Both sides are therefore configured to the same 4 GB/GiB ceiling, and the RDS
instance class is chosen to land on that same ceiling as a fixed size.

The *floors* are matched by nothing and are not supposed to be. Aurora rests at
0 ACU and Lakebase at 0.5 CU, each its own vendor's lowest supported setting,
and the gap between them is one of the things this demo exists to show. So
`capacity_parity` discloses both floors and fails on neither.
"""

from __future__ import annotations

from dataclasses import dataclass

from .models import (
    CapacityDisclosure,
    CapacityLaneDisclosure,
    CompetitorId,
    RoundId,
)

# Lakebase autoscaling range applied by server/lifecycle.py:_configure_lakebase.
LAKEBASE_MIN_CU = 0.5
LAKEBASE_MAX_CU = 2.0

# Lakebase's shortest supported automatic scale-to-zero timeout.
LAKEBASE_SUSPEND_SECONDS = 60

# Aurora Serverless v2 range applied by infra/aws/aurora.tf. A zero minimum is
# the only configuration in which Aurora will auto-pause at all, and Round 1
# refuses to arm without it (server/targets.py:_assert_armed_sync).
AURORA_MIN_ACU = 0.0
AURORA_MAX_ACU = 2.0

# AWS's documented minimum *and* default auto-pause interval. AWS will not
# accept a lower value, so this is a product floor rather than a tuning choice.
AURORA_AUTO_PAUSE_SECONDS = 300

# RDS PostgreSQL instance class applied by infra/aws/rds.tf.
RDS_INSTANCE_CLASS = "db.t4g.medium"

# The RDS lane's on-screen product label. Named once because it is also the
# discriminator `observed_rds_instance_class` uses to tell an RDS competitor
# lane from an Aurora one, and two copies of it could drift apart.
RDS_PRODUCT_LABEL = "RDS PostgreSQL"

# Engine versions applied by infra/aws/aurora.tf and infra/aws/rds.tf. Lakebase
# reports a major version only, so the two sides can be proved equal on the
# major and no minor-version claim is made for Lakebase.
AWS_ENGINE_VERSION = "17.10"
LAKEBASE_POSTGRES_MAJOR = 17

# Published Databricks maximum connections per Lakebase compute size. Unlike the
# AWS side this is a documented table rather than a parameter-group formula.
LAKEBASE_MAX_CONNECTIONS_BY_CU: dict[float, int] = {0.5: 105, 1.0: 218, 2.0: 443}

# Documented memory per vendor capacity unit.
CU_MEMORY_GB = 2.0
ACU_MEMORY_GIB = 2.0

# Fixed-size RDS classes whose memory is known. Only classes on this table may
# be configured, so an unrecognised class fails parity loudly instead of being
# silently assumed to match.
RDS_CLASS_MEMORY_GIB: dict[str, float] = {
    "db.t4g.micro": 1.0,
    "db.t4g.small": 2.0,
    "db.t4g.medium": 4.0,
    "db.t4g.large": 8.0,
    "db.m6g.large": 8.0,
}

# Divisor from the live `default.postgres17` and `default.aurora-postgresql17`
# parameter groups: LEAST({DBInstanceClassMemory/9531392},5000).
MAX_CONNECTIONS_DIVISOR = 9531392
MAX_CONNECTIONS_CEILING = 5000
_BYTES_PER_GIB = 1073741824

# Rounds 4 and 6 provision no AWS database at all; infra/aws/locals.tf builds
# Aurora for r1, r2, r3 and r5 only.
LAKEBASE_ONLY_ROUNDS = frozenset(
    {RoundId.PUT_MODEL_SCORE_IN_APP, RoundId.ANALYZE_LIVE_ORDERS}
)

# Rounds in which the RDS PostgreSQL lane is actually raced: prepared, connected
# to, and timed against Lakebase. Round 1 is deliberately absent. Its bout is a
# wake-from-idle race, and RDS PostgreSQL has no automatic idle state to wake
# from, so its lane refuses to enter and is never timed. That refusal is a
# property of the engine, not an observation of a box, which is why Round 1
# provisions no RDS instance at all (infra/aws/locals.tf:v7_rds_round_keys) and
# why `RdsCredentialProvider.assert_armed` answers for an unscored round without
# reaching AWS.
RDS_SCORED_ROUNDS = frozenset(
    {
        RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
        RoundId.RECOVER_DELETED_ORDER,
        RoundId.SURVIVE_CONNECTION_SPIKE,
    }
)

# No round disables suspension. Round 6 alone once did, on the theory that its
# change feed needed a continuously live replication connection. A live
# experiment settled it the other way: an endpoint replicating change data and
# reporting CDF_STATE_STREAMING held IDLE for 24x its suspend window, and a
# no-CDF control behaved identically, so the feed survives scale-to-zero.
#
# Kept as a set rather than deleted because _lakebase_lane generates the
# on-screen idle policy from it, so a future exception would disclose itself
# instead of needing the copy edited to match.
NO_SUSPENSION_ROUNDS: frozenset[RoundId] = frozenset()

# Peak concurrent client connections one Round 5 lane must absorb: the frozen
# contract drives max_concurrent_attempts_per_lane plus witness_clients_per_lane
# (server/manifest.py:Round5FrozenConstants).
ROUND5_PEAK_CLIENTS_PER_LANE = 128


def max_connections_for_memory_gib(memory_gib: float) -> int:
    """Apply the parameter-group formula to a nominal memory size.

    The result is an upper bound, not an observed value: `DBInstanceClassMemory`
    sits somewhat below nominal and the engine reserves superuser slots.
    """

    if memory_gib <= 0:
        return 0
    derived = int(memory_gib * _BYTES_PER_GIB / MAX_CONNECTIONS_DIVISOR)
    return min(derived, MAX_CONNECTIONS_CEILING)


def rds_memory_gib(instance_class: str) -> float | None:
    """Memory for a known fixed-size class, or None when the class is unknown."""

    return RDS_CLASS_MEMORY_GIB.get(instance_class)


def lakebase_memory_gb(cu: float) -> float:
    return cu * CU_MEMORY_GB


def aurora_memory_gib(acu: float) -> float:
    return acu * ACU_MEMORY_GIB


@dataclass(frozen=True)
class CapacityParityResult:
    """The verdict, plus the individual mismatches behind a failing one.

    ``failures`` is carried separately from ``detail`` because the on-screen
    note has to name what did not match. Reading it out of ``detail`` would mean
    re-parsing a sentence that also carries the sizes and the floors.
    """

    ok: bool
    detail: str
    failures: tuple[str, ...] = ()


def capacity_parity(
    *,
    lakebase_max_cu: float | None,
    aurora_max_acu: float | None,
    rds_instance_class: str | None,
    lakebase_min_cu: float | None = None,
    aurora_min_acu: float | None = None,
    round_id: RoundId | None = None,
) -> CapacityParityResult:
    """Assert both sides are configured to the same memory ceiling.

    Each ceiling argument is the value observed on a live control plane, or None
    when that lane is not provisioned for the round under test. A lane that is
    absent cannot be unfair, so it is reported as not applicable rather than
    failing.

    The floors are *reported and never compared*. Aurora sits at 0 ACU and
    Lakebase at 0.5 CU on purpose: that gap is the demo's subject, not a defect
    in it, and it is disclosed alongside each vendor's shortest idle timeout
    (Aurora 300s auto-pause, Lakebase 60s suspend). Failing on it would turn a
    deliberate, published asymmetry into a red check. Passing floors in is
    optional; omitting them says so rather than inventing a number.

    ``round_id`` scopes the Round 5 client-budget rule. The 128-client contract
    belongs to :data:`RoundId.SURVIVE_CONNECTION_SPIKE` alone — no other round
    puts a burst anywhere near it — so applying it to every lane would refuse an
    instance class that no round in question ever has to seat. ``None`` means no
    round context was supplied and the burst rule does not apply.
    """

    if lakebase_max_cu is None:
        unreported = "Lakebase maximum CU was not reported"
        return CapacityParityResult(False, unreported, (unreported,))

    lakebase_gb = lakebase_memory_gb(lakebase_max_cu)
    parts = [f"Lakebase {_number(lakebase_max_cu)} CU (~{_number(lakebase_gb)} GB)"]
    failures: list[str] = []

    if aurora_max_acu is None:
        parts.append("Aurora not provisioned")
    else:
        aurora_gib = aurora_memory_gib(aurora_max_acu)
        parts.append(f"Aurora {_number(aurora_max_acu)} ACU (~{_number(aurora_gib)} GiB)")
        if aurora_gib != lakebase_gb:
            failures.append(
                f"Aurora ceiling ~{_number(aurora_gib)} GiB does not match "
                f"Lakebase ~{_number(lakebase_gb)} GB"
            )

    if rds_instance_class is None:
        parts.append("RDS not provisioned")
    else:
        rds_gib = rds_memory_gib(rds_instance_class)
        if rds_gib is None:
            parts.append(f"RDS {rds_instance_class} (memory unknown)")
            failures.append(
                f"RDS instance class {rds_instance_class} is not on the approved "
                "matched list in server/capacity.py"
            )
        else:
            parts.append(f"RDS {rds_instance_class} ({_number(rds_gib)} GiB)")
            if rds_gib != lakebase_gb:
                failures.append(
                    f"RDS {rds_instance_class} has {_number(rds_gib)} GiB against "
                    f"Lakebase ~{_number(lakebase_gb)} GB"
                )
            headroom = max_connections_for_memory_gib(rds_gib)
            if (
                round_id is RoundId.SURVIVE_CONNECTION_SPIKE
                and headroom < ROUND5_PEAK_CLIENTS_PER_LANE
            ):
                failures.append(
                    f"RDS {rds_instance_class} has a nominal connection budget of about "
                    f"{headroom}, under the {ROUND5_PEAK_CLIENTS_PER_LANE}-client Round 5 "
                    "contract (derived from instance memory, not an observed limit — RDS "
                    "Proxy multiplexing may still seat the burst)"
                )

    parts.append(
        _floor_disclosure(
            lakebase_min_cu=lakebase_min_cu,
            aurora_min_acu=aurora_min_acu,
            aurora_provisioned=aurora_max_acu is not None,
            rds_instance_class=rds_instance_class,
        )
    )

    detail = " · ".join(parts)
    if failures:
        return CapacityParityResult(
            False, f"{detail} · {'; '.join(failures)}", tuple(failures)
        )
    return CapacityParityResult(True, f"{detail} · matched memory ceiling")


def _floor_disclosure(
    *,
    lakebase_min_cu: float | None,
    aurora_min_acu: float | None,
    aurora_provisioned: bool,
    rds_instance_class: str | None,
) -> str:
    """Say where each lane bottoms out, and say the gap is on purpose.

    Comparing ceiling against ceiling made the floors invisible, which is the
    half of the configuration that decides what an idle lane costs. This states
    them without ranking them.
    """

    floors: list[str] = []
    if lakebase_min_cu is None:
        floors.append("Lakebase floor not reported")
    else:
        floors.append(
            f"Lakebase {_number(lakebase_min_cu)} CU "
            f"(~{_number(lakebase_memory_gb(lakebase_min_cu))} GB, "
            f"suspends after {LAKEBASE_SUSPEND_SECONDS}s)"
        )
    if aurora_provisioned:
        if aurora_min_acu is None:
            floors.append("Aurora floor not reported")
        else:
            floors.append(
                f"Aurora {_number(aurora_min_acu)} ACU "
                f"(auto-pauses after {AURORA_AUTO_PAUSE_SECONDS}s)"
            )
    if rds_instance_class is not None:
        floors.append(f"RDS {rds_instance_class} never idles, so its floor is its ceiling")
    return (
        f"floors {' / '.join(floors)} · disclosed, not compared: each side is at its "
        "own vendor minimum and the difference between them is the finding"
    )


def _number(value: float) -> str:
    """Render a capacity figure without a trailing .0 on whole numbers."""

    return f"{value:g}"


def round_has_aws_lane(round_id: RoundId) -> bool:
    return round_id not in LAKEBASE_ONLY_ROUNDS


def rds_lane_is_scored(round_id: RoundId) -> bool:
    """Whether this round races the RDS lane rather than merely disclosing it."""

    return round_id in RDS_SCORED_ROUNDS


def configured_rds_instance_class(round_id: RoundId) -> str | None:
    """The instance class Terraform applies for this round, if any.

    Only the rounds that race the RDS lane get an instance. Round 1 has an
    Aurora cluster but no RDS box, because its RDS lane refuses to enter on
    engine semantics and a provisioned instance would bill without measuring.
    """

    return RDS_INSTANCE_CLASS if rds_lane_is_scored(round_id) else None


def competitor_is_aurora(competitor_id: CompetitorId) -> bool:
    return competitor_id == CompetitorId.AURORA_SERVERLESS_V2


@dataclass(frozen=True)
class ObservedCapacity:
    """Capacity read back from a live control plane during arming.

    Every field is optional. A field left as None means that control plane did
    not report the value on this run, and the disclosure says so instead of
    falling back to the configured constant.
    """

    lakebase_min_cu: float | None = None
    lakebase_max_cu: float | None = None
    aurora_min_acu: float | None = None
    aurora_max_acu: float | None = None
    aurora_auto_pause_seconds: int | None = None
    aurora_engine_version: str | None = None
    rds_instance_class: str | None = None
    rds_engine_version: str | None = None


def _lakebase_lane(
    round_id: RoundId,
    observed: ObservedCapacity | None,
) -> tuple[CapacityLaneDisclosure, float, float]:
    min_cu = observed.lakebase_min_cu if observed else None
    max_cu = observed.lakebase_max_cu if observed else None
    basis: str = "observed"
    if min_cu is None or max_cu is None:
        min_cu, max_cu = LAKEBASE_MIN_CU, LAKEBASE_MAX_CU
        basis = "configured"
    idle_policy = (
        "Suspension disabled so the change feed stays live"
        if round_id in NO_SUSPENSION_ROUNDS
        else f"Scale to zero after {LAKEBASE_SUSPEND_SECONDS}s (vendor minimum)"
    )
    lane = CapacityLaneDisclosure(
        lane_id="lakebase",
        product="Lakebase",
        configured=f"{_number(min_cu)}–{_number(max_cu)} CU",
        memory=(
            f"~{_number(lakebase_memory_gb(min_cu))}–"
            f"{_number(lakebase_memory_gb(max_cu))} GB"
        ),
        engine_version=f"PostgreSQL {LAKEBASE_POSTGRES_MAJOR} (major only)",
        idle_policy=idle_policy,
        basis=basis,  # type: ignore[arg-type]
        max_connections=LAKEBASE_MAX_CONNECTIONS_BY_CU.get(max_cu),
    )
    return lane, min_cu, max_cu


def _aurora_lane(
    observed: ObservedCapacity | None,
) -> tuple[CapacityLaneDisclosure, float, float]:
    min_acu = observed.aurora_min_acu if observed else None
    max_acu = observed.aurora_max_acu if observed else None
    auto_pause = observed.aurora_auto_pause_seconds if observed else None
    engine = (observed.aurora_engine_version if observed else None) or None
    basis: str = "observed"
    if min_acu is None or max_acu is None:
        min_acu, max_acu = AURORA_MIN_ACU, AURORA_MAX_ACU
        basis = "configured"
    if auto_pause is None:
        auto_pause = AURORA_AUTO_PAUSE_SECONDS
    lane = CapacityLaneDisclosure(
        lane_id="competitor",
        product="Aurora Serverless v2",
        configured=f"{_number(min_acu)}–{_number(max_acu)} ACU",
        memory=(
            f"~{_number(aurora_memory_gib(min_acu))}–"
            f"{_number(aurora_memory_gib(max_acu))} GiB"
        ),
        engine_version=f"PostgreSQL {engine}" if engine else f"PostgreSQL {AWS_ENGINE_VERSION}",
        idle_policy=f"Auto-pause after {auto_pause}s (AWS documented minimum)",
        basis=basis,  # type: ignore[arg-type]
        max_connections=max_connections_for_memory_gib(aurora_memory_gib(max_acu)),
    )
    return lane, min_acu, max_acu


def _rds_lane(observed: ObservedCapacity | None) -> tuple[CapacityLaneDisclosure, str]:
    instance_class = observed.rds_instance_class if observed else None
    engine = (observed.rds_engine_version if observed else None) or None
    basis: str = "observed"
    if not instance_class:
        instance_class = RDS_INSTANCE_CLASS
        basis = "configured"
    memory = rds_memory_gib(instance_class)
    lane = CapacityLaneDisclosure(
        lane_id="competitor",
        product=RDS_PRODUCT_LABEL,
        configured=instance_class,
        memory=f"{_number(memory)} GiB" if memory is not None else "memory not published here",
        engine_version=f"PostgreSQL {engine}" if engine else f"PostgreSQL {AWS_ENGINE_VERSION}",
        idle_policy="No automatic idle pause exists for provisioned RDS",
        basis=basis,  # type: ignore[arg-type]
        max_connections=(
            max_connections_for_memory_gib(memory) if memory is not None else None
        ),
    )
    return lane, instance_class


def observed_rds_instance_class(disclosure: CapacityDisclosure | None) -> str | None:
    """The RDS class a live control plane actually reported, or None.

    The standing-cost RDS lane needs this to say whether the class it prices is
    the class AWS is running. It used to answer that from a hardcoded constant
    and was wrong the moment the fleet was resized, so the reading has to come
    from somewhere that looked.

    A ``configured`` basis is deliberately not an answer. Falling back to the
    constant is what the disclosure does when no plane reported, and returning
    it here would launder that fallback into an observation -- the exact
    conflation this function exists to prevent.
    """

    if disclosure is None:
        return None
    for lane in disclosure.lanes:
        if lane.lane_id != "competitor" or lane.product != RDS_PRODUCT_LABEL:
            continue
        if lane.basis != "observed":
            continue
        return lane.configured or None
    return None


def build_capacity_disclosure(
    round_id: RoundId,
    competitor_id: CompetitorId,
    *,
    observed: ObservedCapacity | None = None,
) -> CapacityDisclosure:
    """Assemble the on-screen compute disclosure for one round.

    Rounds 4 and 6 provision no AWS database, so they disclose the Lakebase lane
    alone and say plainly that there is no opposing box to compare against.
    """

    lakebase, lakebase_min_cu, lakebase_max_cu = _lakebase_lane(round_id, observed)
    if not round_has_aws_lane(round_id):
        return CapacityDisclosure(
            lanes=[lakebase],
            matched=True,
            summary=f"{lakebase.product} {lakebase.configured} ({lakebase.memory})",
            note=(
                "No Aurora or RDS database is provisioned for this round, so no "
                "compute comparison is made and no margin is claimed."
            ),
        )

    if competitor_is_aurora(competitor_id):
        competitor, aurora_min_acu, aurora_max_acu = _aurora_lane(observed)
        rds_class = None
    else:
        competitor, rds_class = _rds_lane(observed)
        aurora_min_acu = None
        aurora_max_acu = None
    parity = capacity_parity(
        lakebase_max_cu=lakebase_max_cu,
        aurora_max_acu=aurora_max_acu,
        rds_instance_class=rds_class,
        lakebase_min_cu=lakebase_min_cu,
        aurora_min_acu=aurora_min_acu,
        round_id=round_id,
    )
    return CapacityDisclosure(
        lanes=[lakebase, competitor],
        matched=parity.ok,
        summary=(
            f"{lakebase.product} {lakebase.configured} ({lakebase.memory}) · "
            f"{competitor.product} {competitor.configured} ({competitor.memory})"
        ),
        note=_disclosure_note(parity),
    )


def _disclosure_note(parity: CapacityParityResult) -> str:
    """The paragraph under the lanes, which may not out-claim the verdict.

    The matched sentence used to be unconditional, so a disclosure whose summary
    read "Ceilings do not match" carried a paragraph underneath asserting that
    both sides were hand-set to the same ceiling. Two claims, one screen,
    contradicting each other, and the paragraph was the false one. It is the
    same defect the frontend summary had: a parity claim made where no parity
    was established.

    The second half holds either way and is kept on both branches. Both sides
    being hand-set is a statement about configuration rather than about
    agreement, and it stays true -- indeed it is what makes a mismatch a defect
    in the constants rather than an accident of vendor defaults.
    """

    provenance = (
        "These are not vendor defaults, and each idle policy is that vendor's "
        "shortest supported setting."
    )
    if parity.ok:
        return (
            "Both sides are hand-set to the same memory ceiling, single node, "
            f"no HA on either side. {provenance}"
        )
    return (
        "The two sides are not on the same memory ceiling, so no capacity "
        f"parity is claimed for this round: {'; '.join(parity.failures)}. "
        f"Single node, no HA on either side. {provenance}"
    )
