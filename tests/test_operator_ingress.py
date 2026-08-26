"""Operator IP drift must be visible at runtime and repaired only on command.

`_refresh_operator_cidr` runs from exactly one place -- `reconcile_infrastructure`,
so only during `antidemo setup`. After that, nothing looked again. When the operator's
laptop changed address (a DHCP lease, a VPN toggle, a different network) the AWS
security groups kept allowing an address nobody held, every round that connects
directly to Aurora or RDS started failing to connect, and no screen said why. On
an install meant to sit up for days, that is the silent killer.

Two properties are load-bearing and pull against each other, so both are pinned
here:

*   **Detection is available to the serving process.** It is cached, short, and
    cannot raise, because a detector that costs a network round trip per request
    or that can break a round is worse than no detector.
*   **Repair is not.** Nothing in this path writes a manifest, touches AWS, or
    takes the generation lock. `antidemo serve` releases the mutation lock before
    serving on purpose; a server that helpfully rewrote a security group on a bad
    inference would corrupt a live measurement or spend real money.
"""

from __future__ import annotations

import asyncio
import json
import re

import pytest
from pydantic import ValidationError
from test_lifecycle import make_manifest

from server import cli as cli_module
from server import lifecycle
from server.manifest import SERVERLESS_EGRESS_MIN_PREFIXLEN, AwsManifest

SEALED = "203.0.113.10/32"
MOVED = "198.51.100.7/32"

# RFC5737 documentation space, in the shape the real feed publishes -- a /24, a
# /25, a /28 and a /32. It has to be documentation space: every prefix Databricks
# actually publishes is globally routable, and
# tests/test_no_live_identifiers_committed.py refuses a routable IPv4 literal in
# any file it can see, tracked or not. That is why nothing in this repository ever
# holds the real values and why they are fetched at reconcile time instead.
PUBLISHED = (
    "192.0.2.0/24",
    "198.51.100.0/25",
    "198.51.100.128/28",
    "203.0.113.200/32",
)

# A host that no sealed prefix covers, for the cases that must still report
# drift. `MOVED` cannot serve here: 198.51.100.7 falls inside the /25 above, so
# once the app's prefixes are sealed it is an *admitted* address -- which is the
# containment working, and exactly why the two are kept apart.
OUTSIDE_EVERY_PREFIX = "198.51.100.200/32"


class _FakeFeed:
    """Stands in for `urllib.request.urlopen`'s context manager over the feed."""

    def __init__(self, body: str) -> None:
        self._body = body.encode("utf-8")

    def __enter__(self) -> _FakeFeed:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def read(self, _size: int | None = None) -> bytes:
        return self._body


@pytest.fixture
def live_probe(monkeypatch):
    """Unseal the cache the suite-wide fixture seeds, and stub the probe.

    Returns a recorder: append a value to `answers` to script one observation,
    and read `calls` to count how many times the network was reached.
    """
    lifecycle.reset_operator_ingress_cache()

    class Probe:
        def __init__(self) -> None:
            self.answers: list[object] = []
            self.calls = 0
            self.timeouts: list[float] = []

        def __call__(self, *, timeout_seconds: float = 10.0) -> str:
            self.calls += 1
            self.timeouts.append(timeout_seconds)
            answer = self.answers.pop(0) if self.answers else MOVED
            if isinstance(answer, BaseException):
                raise answer
            return str(answer)

    probe = Probe()
    monkeypatch.setattr(lifecycle, "detect_operator_cidr", probe)
    return probe


# --------------------------------------------------------------------------
# Detection
# --------------------------------------------------------------------------


def test_a_moved_address_is_detected_and_names_the_command_that_fixes_it(live_probe) -> None:
    manifest = make_manifest()
    assert manifest.aws.operator_cidr == SEALED
    live_probe.answers = [MOVED]

    drift = lifecycle.operator_ingress_drift(manifest=manifest)

    assert drift is not None
    assert (drift.sealed_cidr, drift.observed_cidr) == (SEALED, MOVED)
    # Both halves of an actionable message: what breaks, and the exact command.
    assert SEALED in drift.detail and MOVED in drift.detail
    assert "Aurora or RDS" in drift.detail
    assert lifecycle.OPERATOR_INGRESS_REPAIR_COMMAND in drift.detail
    # And in the /readyz vocabulary, which lists losses rather than prose.
    assert len(drift.capabilities) == 1
    assert MOVED in drift.capabilities[0]


def test_a_matching_address_is_not_drift(live_probe) -> None:
    live_probe.answers = [SEALED]

    assert lifecycle.operator_ingress_drift(manifest=make_manifest()) is None


@pytest.mark.parametrize(
    "failure",
    [
        OSError("network is unreachable"),
        TimeoutError(),
        # What an IPv6-only network produces: `detect_operator_cidr` refuses a
        # non-IPv4 answer rather than sealing something a /32 cannot express.
        RuntimeError("Round 1 currently requires an operator public IPv4 address"),
    ],
    ids=["offline", "timeout", "ipv6_only"],
)
def test_a_probe_that_cannot_answer_never_claims_drift(live_probe, failure) -> None:
    """A false positive tells an operator to re-apply Terraform for no reason.

    Every one of these is "I do not know", and none of them is "your address
    moved". Reporting them as drift would make an offline laptop or an IPv6
    network indistinguishable from the real failure.
    """
    live_probe.answers = [failure]

    assert lifecycle.operator_ingress_drift(manifest=make_manifest()) is None


def test_an_unreadable_seal_never_claims_drift(live_probe, monkeypatch) -> None:
    monkeypatch.setattr(
        lifecycle,
        "load_manifest",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("no manifest")),
    )

    assert lifecycle.operator_ingress_drift() is None
    # The seal could not be read, so the probe was never worth making.
    assert live_probe.calls == 0


def test_a_nat_or_corporate_egress_is_not_a_false_positive(live_probe) -> None:
    """Both sides of the comparison come from the same probe.

    Behind NAT, `checkip.amazonaws.com` reports the egress address -- which is
    exactly the address the security group has to name, and exactly what was
    sealed at provision time. Comparing like with like is what makes the answer
    mean something instead of flagging every NAT'd network.
    """
    manifest = make_manifest()
    live_probe.answers = [SEALED]
    assert lifecycle.operator_ingress_drift(manifest=manifest) is None
    assert live_probe.calls == 1

    # And the mechanism really is the shared one, not a second implementation.
    lifecycle.reset_operator_ingress_cache()
    live_probe.answers = [SEALED]
    lifecycle.operator_ingress_drift(manifest=manifest)
    assert live_probe.calls == 2


# --------------------------------------------------------------------------
# The cache: this is what makes the detector safe to call per request
# --------------------------------------------------------------------------


def test_repeated_calls_make_one_network_call_per_ttl(live_probe, monkeypatch) -> None:
    clock = [1_000.0]
    monkeypatch.setattr(lifecycle.time, "monotonic", lambda: clock[0])
    manifest = make_manifest()

    for _ in range(50):
        assert lifecycle.operator_ingress_drift(manifest=manifest) is not None
    assert live_probe.calls == 1

    clock[0] += lifecycle.OPERATOR_INGRESS_TTL_SECONDS - 1
    lifecycle.operator_ingress_drift(manifest=manifest)
    assert live_probe.calls == 1

    clock[0] += 2
    lifecycle.operator_ingress_drift(manifest=manifest)
    assert live_probe.calls == 2


def test_a_failed_probe_is_retried_sooner_than_a_good_one(live_probe, monkeypatch) -> None:
    """Cached so an offline laptop is not probed per request; briefly, so the
    signal returns promptly once the network does."""
    assert (
        lifecycle.OPERATOR_INGRESS_FAILURE_TTL_SECONDS < lifecycle.OPERATOR_INGRESS_TTL_SECONDS
    )
    clock = [1_000.0]
    monkeypatch.setattr(lifecycle.time, "monotonic", lambda: clock[0])
    manifest = make_manifest()
    live_probe.answers = [OSError("offline")]

    assert lifecycle.operator_ingress_drift(manifest=manifest) is None
    clock[0] += lifecycle.OPERATOR_INGRESS_FAILURE_TTL_SECONDS + 1
    live_probe.answers = [MOVED]

    assert lifecycle.operator_ingress_drift(manifest=manifest) is not None
    assert live_probe.calls == 2


def test_the_signal_clears_itself_after_a_repair(live_probe, monkeypatch) -> None:
    """The reason the seal is re-read on every refresh rather than captured once.

    `antidemo setup` rebinds the allowance and writes the new address into the
    manifest. A server that had cached the old sealed value would report drift
    forever afterwards and would need a restart to stop -- which is the opposite
    of a signal that helps.
    """
    clock = [1_000.0]
    monkeypatch.setattr(lifecycle.time, "monotonic", lambda: clock[0])
    stale = make_manifest()
    monkeypatch.setattr(lifecycle, "load_manifest", lambda *args, **kwargs: stale)

    assert lifecycle.operator_ingress_drift() is not None

    # The repair: a mutator moved the sealed value to the address now observed.
    stale.aws.operator_cidr = MOVED
    clock[0] += lifecycle.OPERATOR_INGRESS_TTL_SECONDS + 1

    assert lifecycle.operator_ingress_drift() is None


def test_the_runtime_probe_does_not_sit_on_a_ten_second_socket(live_probe) -> None:
    """It runs beside a live demo, so it gets a short timeout; mutators keep the
    long one, because a provision or a repair is allowed to wait."""
    lifecycle.operator_ingress_drift(manifest=make_manifest())

    assert live_probe.timeouts == [lifecycle.OPERATOR_INGRESS_PROBE_TIMEOUT_SECONDS]
    assert lifecycle.OPERATOR_INGRESS_PROBE_TIMEOUT_SECONDS < 10.0


def test_the_async_entry_point_keeps_the_blocking_probe_off_the_event_loop(
    live_probe, monkeypatch
) -> None:
    """A blocking socket read in a coroutine stalls every other request on the
    worker -- including the SSE stream a bout is riding on."""
    threads: list[object] = []
    real_to_thread = asyncio.to_thread

    async def recording_to_thread(function, /, *args, **kwargs):
        threads.append(function)
        return await real_to_thread(function, *args, **kwargs)

    monkeypatch.setattr(lifecycle.asyncio, "to_thread", recording_to_thread)
    manifest = make_manifest()

    drift = asyncio.run(lifecycle.operator_ingress_drift_async(manifest=manifest))

    assert drift is not None
    assert threads == [lifecycle.operator_ingress_drift]


# --------------------------------------------------------------------------
# The sealed set: widened to admit the deployed app, and still exact
# --------------------------------------------------------------------------
#
# Rounds 1, 2, 3 and 5 all race a live Aurora or RDS opponent over TCP 5432, and
# the deployed Databricks App could not run any of them for one reason: the
# security groups admitted a single address, the laptop that provisioned the
# install. Those four rounds are the entire argument this project exists to make,
# so a regression that shuts the app back out is a regression that reaches an
# audience.
#
# The invariant being protected is not "one address" and never was. It is
# "exactly the set this installation sealed, and nothing else" -- so these pin
# both halves: the sealed set is admitted, and anything beyond it is still
# refused.


def permission(*cidrs: str, group: str | None = None) -> dict:
    """One IpPermission in the shape `describe_security_groups` returns.

    Terraform renders one inline `ingress` block carrying several `cidr_blocks`
    as a *single* permission with several `IpRanges`, because AWS groups
    permissions by protocol and port range. The whole `{1}` / `{1, 2}` arithmetic
    in `_postgres_ingress_is_exact` rests on that, which is why the helper builds
    it that way rather than one permission per CIDR.
    """

    built: dict = {"IpProtocol": "tcp", "FromPort": 5432, "ToPort": 5432}
    if cidrs:
        built["IpRanges"] = [{"CidrIp": cidr} for cidr in cidrs]
    if group is not None:
        built["UserIdGroupPairs"] = [{"GroupId": group}]
    return built


def test_the_sealed_operator_and_app_prefixes_are_admitted_together() -> None:
    """Both, not either. Admitting the app must not evict the laptop.

    This is the whole of section 1 of the design: `_validate_operator_cidr` still
    seals one laptop at one /32, and the app is added beside it. A change that
    swapped one for the other would deliver the deployed rounds by taking away
    every local one, which is not the trade anybody asked for.
    """

    assert lifecycle._postgres_ingress_is_exact(
        [permission(SEALED, *PUBLISHED)],
        operator_cidr=SEALED,
        runner_group=None,
        serverless_egress_cidrs=PUBLISHED,
    )


def test_terraform_renders_the_widened_set_as_one_permission() -> None:
    """The arithmetic `len(permissions)` does is unchanged, and this says why.

    If several `cidr_blocks` rendered as several permissions, widening the set
    would break the `{1}` / `{1, 2}` count and every ingress check would start
    reporting a correctly-configured group as wrong -- on a live install, in
    `antidemo doctor`, with no obvious cause.
    """

    permissions = [permission(SEALED, *PUBLISHED)]

    assert len(permissions) == 1
    assert len(permissions[0]["IpRanges"]) == 1 + len(PUBLISHED)
    assert lifecycle._postgres_ingress_is_exact(
        permissions,
        operator_cidr=SEALED,
        runner_group=None,
        serverless_egress_cidrs=PUBLISHED,
    )


def test_round5s_pair_still_admits_the_runner_group_beside_the_widened_set() -> None:
    """Round 5's runner was never an address problem: it is authorised by
    security-group reference, which is why it needed nothing here. It still has
    to survive the widening."""

    assert lifecycle._postgres_ingress_is_exact(
        [permission(SEALED, *PUBLISHED), permission(group="sg-0123abcd")],
        operator_cidr=SEALED,
        runner_group="sg-0123abcd",
        serverless_egress_cidrs=PUBLISHED,
    )


@pytest.mark.parametrize(
    ("admitted", "sealed", "why"),
    [
        (
            (SEALED, *PUBLISHED, "192.0.2.0/25"),
            PUBLISHED,
            "a rule added by hand is still refused, and Terraform still revokes it",
        ),
        (
            (SEALED, *PUBLISHED[:-1]),
            PUBLISHED,
            "a sealed prefix that has gone missing is refused, so a half-applied "
            "security group cannot read as correct",
        ),
        (
            (*PUBLISHED,),
            PUBLISHED,
            "the operator's own address disappearing is refused too -- admitting "
            "the app must never quietly cost the laptop its access",
        ),
        (
            (SEALED, "192.0.2.0/16"),
            ("192.0.2.0/24",),
            "a widened prefix is not the prefix that was sealed, even though it "
            "contains it",
        ),
    ],
    ids=["extra_rule", "missing_prefix", "operator_evicted", "widened_prefix"],
)
def test_anything_other_than_the_sealed_set_is_still_refused(admitted, sealed, why) -> None:
    """The seal stays exact; only its size changed.

    Every case here passed the old `cidrs == [operator_cidr]` check for the wrong
    reason -- by failing it. What must not happen is that widening the set turns
    the comparison into a subset test, which is the easy mistake and the one that
    would let a hand-added `0.0.0.0/0` sit in front of a live database unreported.
    """

    assert not lifecycle._postgres_ingress_is_exact(
        [permission(*admitted)],
        operator_cidr=SEALED,
        runner_group=None,
        serverless_egress_cidrs=sealed,
    ), why


def test_an_installation_that_sealed_nothing_behaves_exactly_as_before() -> None:
    """Every installation provisioned before this existed seals no list.

    Those installs must keep admitting one address and refusing everything else,
    with no new failure mode introduced by a feature they do not use.
    """

    assert lifecycle._postgres_ingress_is_exact(
        [permission(SEALED)], operator_cidr=SEALED, runner_group=None
    )
    assert not lifecycle._postgres_ingress_is_exact(
        [permission(SEALED, *PUBLISHED)], operator_cidr=SEALED, runner_group=None
    )


# --------------------------------------------------------------------------
# The seal itself
# --------------------------------------------------------------------------


def test_the_list_and_its_timestamp_are_one_seal() -> None:
    """A list nobody can age is a list nobody will re-poll.

    The drift observer below compares the sealed timestamp against now. Seal the
    prefixes without it and that observer silently has nothing to compare, so the
    re-poll obligation stops existing without anything looking wrong -- which is
    the exact failure the observer was added to prevent.
    """

    manifest = make_manifest()

    with pytest.raises(ValidationError, match="sealed together or not at all"):
        AwsManifest.model_validate(
            manifest.aws.model_dump() | {"serverless_egress_cidrs": PUBLISHED}
        )
    with pytest.raises(ValidationError, match="sealed together or not at all"):
        AwsManifest.model_validate(
            manifest.aws.model_dump() | {"serverless_egress_published_at": 1}
        )


@pytest.mark.parametrize(
    ("bad", "why"),
    [
        (("198.18.0.0/16",), "a /16 in front of a live database is the premise this removes"),
        (("0.0.0.0/0",), "the widest possible rule must never be sealable"),
        (("192.0.2.0/24", "192.0.2.0/24"), "a duplicate is a seal that disagrees with itself"),
        ((), "an empty tuple claims a seal that admits nobody"),
        (("192.0.2.1/24",), "a host address in a network slot is a typo, not a prefix"),
        (("127.0.0.0/24",), "nothing egresses to a database from loopback"),
    ],
    ids=["slash_16", "everything", "duplicate", "empty", "not_a_network", "loopback"],
)
def test_the_egress_seal_refuses_what_the_operator_seal_never_had_to(bad, why) -> None:
    """A separate rule from `_validate_operator_cidr`, because it is a separate claim.

    That function is untouched and still seals one laptop at one /32. This one
    says something weaker about a different thing, so it gets its own validator
    rather than widening the operator CIDR's contract to cover both.
    """

    with pytest.raises(ValueError):
        lifecycle._validate_serverless_egress_cidrs(bad)


def test_the_seal_admits_the_shape_the_feed_actually_publishes() -> None:
    """A /24, a /25, a /28 and a /32 -- four prefixes, 401 addresses in the real
    feed. The floor has to be permissive enough for the values that exist, or the
    validator refuses reality."""

    assert lifecycle._validate_serverless_egress_cidrs(PUBLISHED) == PUBLISHED


def test_a_manifest_sealed_before_this_existed_round_trips_unchanged() -> None:
    """`save_manifest` writes with `exclude_none=True`, so both fields default to
    None rather than to an empty tuple. An older manifest must not silently gain
    two keys and a different digest."""

    manifest = make_manifest()

    assert manifest.aws.serverless_egress_cidrs is None
    assert manifest.aws.serverless_egress_published_at is None
    assert "serverless_egress" not in manifest.aws.model_dump_json(exclude_none=True)


def test_no_published_prefix_is_written_into_this_repository() -> None:
    """The constraint that shaped the whole design, asserted rather than trusted.

    Every prefix Databricks publishes is globally routable, and
    tests/test_no_live_identifiers_committed.py refuses a routable IPv4 literal in
    any tracked *or untracked* file. So they can never live in a Terraform
    default, a Python constant or a fixture -- which forces fetch-at-reconcile,
    and which happens to be the right design anyway because a hardcoded list goes
    stale silently and a fetched one cannot.
    """

    from test_no_live_identifiers_committed import _ip_is_publishable

    for cidr in PUBLISHED:
        publishable, why = _ip_is_publishable(cidr.split("/")[0])
        assert publishable, f"{cidr} would be refused by the identifier guard: {why}"

    # And the three files that carry this feature hold no routable address at
    # all -- not the prefixes, and not anything else. `0.0.0.0/0` on the egress
    # rules is unspecified rather than allocated, which is exactly the
    # distinction the guard's own predicate draws, so it is reused here rather
    # than re-litigated with a second regex.
    for path in ("infra/aws/variables.tf", "infra/aws/network.tf", "server/lifecycle.py"):
        text = (lifecycle.PROJECT_ROOT / path).read_text(encoding="utf-8")
        assert "ip-ranges.json" in text or "serverless_egress" in text
        for literal in re.findall(r"(?<![0-9A-Za-z.])(?:\d{1,3}\.){3}\d{1,3}", text):
            publishable, why = _ip_is_publishable(literal)
            assert publishable, f"{path} names a routable address: {why}"


# --------------------------------------------------------------------------
# The re-poll obligation, which has to be visible or it is not an obligation
# --------------------------------------------------------------------------


@pytest.fixture
def sealed_egress_manifest(monkeypatch):
    """A manifest that admits the app, with the feed snapshot's age controllable."""

    lifecycle.reset_deployed_aws_posture_cache()

    def build(*, age_days: float) -> object:
        manifest = make_manifest()
        manifest.aws.serverless_egress_cidrs = PUBLISHED
        manifest.aws.serverless_egress_published_at = int(
            1_800_000_000 - age_days * 86400
        )
        monkeypatch.setattr(lifecycle.time, "time", lambda: 1_800_000_000.0)
        return manifest

    return build


def test_a_stale_published_list_is_reported_and_names_the_command(
    live_probe, sealed_egress_manifest
) -> None:
    """Databricks republishes as often as every 30 days, with new addresses live
    60 days after publication. The slack is real but it is only slack if somebody
    is watching -- an obligation nobody can see is an obligation nobody meets."""

    live_probe.answers = [SEALED]
    manifest = sealed_egress_manifest(age_days=45)

    drift = lifecycle.operator_ingress_drift(manifest=manifest)

    assert isinstance(drift, lifecycle.ServerlessEgressDrift)
    assert drift.sealed_age_days == 45
    assert drift.prefix_count == len(PUBLISHED)
    assert lifecycle.OPERATOR_INGRESS_REPAIR_COMMAND in drift.detail
    # It must not read as an outage. Nothing is broken yet, and an operator who
    # believes the app is down will go looking for a fault that is not there.
    assert "Nothing is broken right now" in drift.detail
    assert len(drift.capabilities) == 1


def test_a_freshly_polled_list_is_not_reported(live_probe, sealed_egress_manifest) -> None:
    live_probe.answers = [SEALED]

    assert lifecycle.operator_ingress_drift(manifest=sealed_egress_manifest(age_days=3)) is None


def test_an_admitted_app_address_is_not_drift(live_probe, sealed_egress_manifest) -> None:
    """Caught live, on the deployed app, after the security groups were widened.

    The question this observer answers is "is this host admitted", and it used to
    be able to conflate that with "is this host the operator" because the sealed
    set had exactly one member. Once the app's own prefixes are sealed, the app
    reports an address that is deliberately allowed and *not* the operator's --
    so the old comparison called a working deployment stale, on /readyz, which is
    the endpoint the round list consults. The result would have been the four
    AWS-backed rounds coming off the card for the crime of being reachable.

    Containment, not equality: the sealed entries are ranges precisely because
    the address inside them changes from one app restart to the next.
    """

    live_probe.answers = ["192.0.2.77/32"]

    assert lifecycle.operator_ingress_drift(manifest=sealed_egress_manifest(age_days=3)) is None


def test_an_address_outside_every_sealed_prefix_is_still_drift(
    live_probe, sealed_egress_manifest
) -> None:
    """The widening must not become "anything goes". A host in none of the
    sealed ranges genuinely cannot reach the databases, and saying so is the
    entire job."""

    live_probe.answers = [OUTSIDE_EVERY_PREFIX]

    drift = lifecycle.operator_ingress_drift(manifest=sealed_egress_manifest(age_days=3))

    assert isinstance(drift, lifecycle.OperatorIngressDrift)
    assert drift.observed_cidr == OUTSIDE_EVERY_PREFIX


def test_a_malformed_seal_cannot_take_down_the_endpoint_reporting_on_it() -> None:
    """This runs inside /readyz. It reports faults; it must not be able to be one."""

    assert lifecycle._sealed_ingress_admits("192.0.2.77/32", ("not-a-cidr",)) is False
    assert lifecycle._sealed_ingress_admits("nonsense", PUBLISHED) is False
    assert lifecycle._sealed_ingress_admits("192.0.2.77/32", ()) is False


def test_the_operator_s_own_address_outranks_the_re_poll_obligation(
    live_probe, sealed_egress_manifest
) -> None:
    """One detail slot on /readyz, and the acute finding takes it.

    A moved laptop means rounds are failing now, in front of whoever is watching.
    A stale snapshot means they might fail later. Reporting the second while the
    first is true would send an operator to the wrong repair.
    """

    live_probe.answers = [OUTSIDE_EVERY_PREFIX]

    drift = lifecycle.operator_ingress_drift(manifest=sealed_egress_manifest(age_days=90))

    assert isinstance(drift, lifecycle.OperatorIngressDrift)


def test_the_obligation_needs_no_network_call_of_its_own(
    live_probe, sealed_egress_manifest
) -> None:
    """It is an age, not a comparison against a freshly fetched feed.

    /readyz already pays for two blocking socket reads per TTL. A third, against a
    third party's CDN, on the endpoint a monitor hits, would also go quiet exactly
    when the network is unhealthy -- and the repair is the same command either
    way, so it would buy nothing an operator would act on differently.
    """

    live_probe.answers = [SEALED]
    fetched: list[str] = []
    lifecycle.operator_ingress_drift(manifest=sealed_egress_manifest(age_days=90))

    assert fetched == []
    # And the fetch really is confined to the mutator path.
    import inspect

    source = inspect.getsource(lifecycle)
    assert source.count("fetch_serverless_egress_cidrs(") == 2  # the def, and the one caller
    assert "fetch_serverless_egress_cidrs(" in inspect.getsource(
        lifecycle._refresh_serverless_egress_cidrs
    )


def test_an_installation_that_sealed_no_list_reports_no_obligation(live_probe) -> None:
    """It has no app access to lose, so telling its operator to re-poll a feed it
    does not use would be noise on every /readyz for the life of the install."""

    lifecycle.reset_deployed_aws_posture_cache()
    live_probe.answers = [SEALED]

    assert lifecycle.operator_ingress_drift(manifest=make_manifest()) is None


# --------------------------------------------------------------------------
# The boundary: detection here, repair only in a mutator
# --------------------------------------------------------------------------


def test_detection_mutates_nothing(live_probe, monkeypatch) -> None:
    """The hard requirement. A serving process that provisions, reseeds, deletes
    a resource, rewrites a security group or writes the manifest on a bad
    inference corrupts a live measurement or spends real money."""
    manifest = make_manifest()
    forbidden: list[str] = []
    for name in (
        "save_manifest",
        "_refresh_operator_cidr",
        "reconcile_infrastructure",
        "_terraform_init",
        "_terraform_plan",
        "_terraform_apply",
        "_aws_session",
    ):
        monkeypatch.setattr(
            lifecycle,
            name,
            lambda *args, _name=name, **kwargs: forbidden.append(_name),
        )

    lifecycle.operator_ingress_drift(manifest=manifest)
    lifecycle.reset_operator_ingress_cache()
    lifecycle.operator_ingress_check(manifest)

    assert forbidden == []
    assert manifest.aws.operator_cidr == SEALED


def test_the_repair_still_lives_only_in_the_locked_mutator(monkeypatch) -> None:
    """`reconcile_infrastructure` is reached by `antidemo setup`, which holds the
    generation lock at the CLI choke point. That is the one place the security
    group is rewritten, and this change does not add a second."""
    import inspect

    source = inspect.getsource(lifecycle)
    # One call site each, and both are inside reconcile_infrastructure. The
    # second repair rewrites the same security groups as the first, so it is held
    # to the same rule: a serving process never mutates.
    assert source.count("_refresh_operator_cidr(manifest)") == 1
    assert source.count("_refresh_serverless_egress_cidrs(manifest)") == 1
    reconcile = inspect.getsource(lifecycle.reconcile_infrastructure)
    assert "_refresh_operator_cidr(manifest)" in reconcile
    assert "_refresh_serverless_egress_cidrs(manifest)" in reconcile


def test_the_app_reseal_keeps_the_ownership_refusal(monkeypatch) -> None:
    """It rewrites database ingress, so it must not do so on somebody else's
    resources. Same refusal `_refresh_operator_cidr` carries, and it has to be
    the same one: this is the second thing that can widen a live security group.
    """

    manifest = make_manifest()
    monkeypatch.setattr(
        lifecycle,
        "fetch_serverless_egress_cidrs",
        lambda region, **_kwargs: (PUBLISHED, 1_700_000_000),
    )
    monkeypatch.setattr(
        lifecycle, "_aws_ownership", lambda _m: lifecycle.Check("aws_ownership", False, "no")
    )
    monkeypatch.setattr(lifecycle, "_sealed_databases_absent", lambda _m: False)
    saved: list[object] = []
    monkeypatch.setattr(lifecycle, "save_manifest", lambda m: saved.append(m))

    with pytest.raises(RuntimeError, match="could not be verified"):
        lifecycle._refresh_serverless_egress_cidrs(manifest)

    assert saved == []
    assert manifest.aws.serverless_egress_cidrs is None


def test_an_unreachable_feed_keeps_the_existing_seal_rather_than_failing_setup(
    monkeypatch, capsys
) -> None:
    """`antidemo setup` is the repair path. Letting a third party's CDN being
    briefly unreachable abort a reconcile would make repairing this installation
    conditional on somebody else's uptime -- and would do it at exactly the
    moment an operator is already trying to fix something.
    """

    manifest = make_manifest()
    manifest.aws.serverless_egress_cidrs = PUBLISHED
    manifest.aws.serverless_egress_published_at = 1_700_000_000
    monkeypatch.setattr(
        lifecycle,
        "fetch_serverless_egress_cidrs",
        lambda region, **_kwargs: (_ for _ in ()).throw(OSError("feed unreachable")),
    )
    saved: list[object] = []
    monkeypatch.setattr(lifecycle, "save_manifest", lambda m: saved.append(m))

    lifecycle._refresh_serverless_egress_cidrs(manifest)

    assert saved == []
    assert manifest.aws.serverless_egress_cidrs == PUBLISHED
    warning = capsys.readouterr().out
    assert "WARN" in warning and lifecycle.OPERATOR_INGRESS_REPAIR_COMMAND in warning


def test_a_region_the_feed_does_not_cover_is_refused_rather_than_sealed_empty(
    monkeypatch,
) -> None:
    """An empty allowlist and "this region publishes nothing" are different facts.

    Sealing the first as though it were the second would silently un-admit the
    deployed app on the next reconcile, and the only symptom would be four rounds
    that used to work quietly not working.
    """

    monkeypatch.setattr(
        lifecycle.urllib.request,
        "urlopen",
        lambda *_a, **_k: _FakeFeed('{"timestampSeconds": 1, "prefixes": []}'),
    )

    with pytest.raises(RuntimeError, match="Refusing to seal an empty allowlist"):
        lifecycle.fetch_serverless_egress_cidrs("us-west-2")


def test_the_feed_is_read_as_one_row_carrying_many_prefixes(monkeypatch) -> None:
    """The feed's actual shape, and the mistake it invites.

    Each row is a (platform, region, service, type) tuple carrying an
    `ipv4Prefixes` *list* -- not one row per prefix. Reading it the other way
    finds nothing and reports it as "the region publishes none", which is
    indistinguishable from the refusal above and would strand the deployed app.
    Only outbound rows count: inbound is where Databricks arrives from, which is
    not a path anything here takes.
    """

    body = json.dumps(
        {
            "timestampSeconds": 1_700_000_000,
            "prefixes": [
                {
                    "platform": "aws",
                    "region": "us-west-2",
                    "service": "Databricks",
                    "type": "outbound",
                    "ipv4Prefixes": list(PUBLISHED),
                },
                {
                    "platform": "aws",
                    "region": "us-west-2",
                    "service": "Databricks",
                    "type": "inbound",
                    "ipv4Prefixes": ["192.0.2.128/25"],
                },
                {
                    "platform": "aws",
                    "region": "eu-west-1",
                    "service": "Databricks",
                    "type": "outbound",
                    "ipv4Prefixes": ["198.18.0.0/24"],
                },
            ],
        }
    )
    monkeypatch.setattr(
        lifecycle.urllib.request, "urlopen", lambda *_a, **_k: _FakeFeed(body)
    )

    cidrs, published_at = lifecycle.fetch_serverless_egress_cidrs("us-west-2")

    assert sorted(cidrs) == sorted(PUBLISHED)
    assert published_at == 1_700_000_000


def test_a_feed_offering_something_too_wide_is_refused(monkeypatch) -> None:
    """The false premise this whole change removes was that the only option was a
    /16 of general-purpose EC2, refused outright as a boundary in front of a live
    database. That refusal is kept: nothing wider than a /24 is sealed, whoever
    publishes it.
    """

    body = json.dumps(
        {
            "timestampSeconds": 1,
            "prefixes": [
                {
                    "platform": "aws",
                    "region": "us-west-2",
                    "service": "Databricks",
                    "type": "outbound",
                    "ipv4Prefixes": ["192.0.2.0/24", "198.18.0.0/16"],
                }
            ],
        }
    )
    monkeypatch.setattr(
        lifecycle.urllib.request, "urlopen", lambda *_a, **_k: _FakeFeed(body)
    )

    with pytest.raises(ValueError, match="wider than a /24"):
        lifecycle.fetch_serverless_egress_cidrs("us-west-2")


def test_the_python_floor_and_the_terraform_floor_are_the_same_number() -> None:
    """Two enforcers, one rule. If they disagree, whichever runs first decides,
    and the other becomes decoration -- which is how a guard rots quietly."""

    hcl = (lifecycle.AWS_INFRA_DIR / "variables.tf").read_text(encoding="utf-8")

    assert f">= {SERVERLESS_EGRESS_MIN_PREFIXLEN}" in hcl
    assert f"/{SERVERLESS_EGRESS_MIN_PREFIXLEN} or narrower" in hcl


def test_terraform_admits_the_sealed_list_beside_the_operator_never_instead_of_it() -> None:
    """All four database security groups, and the concat order is the assertion.

    A group that took `var.serverless_egress_cidrs` alone would admit the app and
    lock the operator out of the very install they are running the demo from.
    """

    hcl = (lifecycle.AWS_INFRA_DIR / "network.tf").read_text(encoding="utf-8")

    assert hcl.count("concat([var.operator_cidr], var.serverless_egress_cidrs)") == 4
    # Inline `ingress` blocks are what make Terraform authoritative over the whole
    # rule set, so a hand-added rule is revoked on the next apply. Standalone rule
    # resources would give that up, and the seal would become advisory.
    assert 'resource "aws_vpc_security_group_ingress_rule"' not in hcl


# --------------------------------------------------------------------------
# Where an operator actually sees it
# --------------------------------------------------------------------------


def test_demo_status_reports_the_drift(live_probe, monkeypatch, tmp_path) -> None:
    """`antidemo status` is what an operator runs when something looks wrong, and
    nothing else on its output mentions ingress."""
    generation = tmp_path / "manifest.json"
    generation.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(generation))
    monkeypatch.setattr(cli_module, "load_manifest", lambda path=None: make_manifest())

    checks = {check.name: check for check in cli_module._generation_checks()}

    ingress = checks["operator_ingress"]
    assert ingress.ok is False
    # Advisory: a stale allowance is not evidence that `antidemo status` failed, and
    # it must not decide the exit code on its own.
    assert ingress.advisory is True
    assert cli_module.checks_passed([ingress]) is True
    assert lifecycle.OPERATOR_INGRESS_REPAIR_COMMAND in ingress.detail


def test_demo_status_stays_quiet_when_the_allowance_matches(
    live_probe, monkeypatch, tmp_path
) -> None:
    generation = tmp_path / "manifest.json"
    generation.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(generation))
    monkeypatch.setattr(cli_module, "load_manifest", lambda path=None: make_manifest())
    live_probe.answers = [SEALED]

    checks = {check.name: check for check in cli_module._generation_checks()}

    assert checks["operator_ingress"].ok is True


def test_demo_status_still_answers_when_the_probe_cannot(
    live_probe, monkeypatch, tmp_path
) -> None:
    """A detection failure must never be what breaks the diagnostic."""
    generation = tmp_path / "manifest.json"
    generation.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(generation))
    monkeypatch.setattr(cli_module, "load_manifest", lambda path=None: make_manifest())
    live_probe.answers = [OSError("no route to host")]

    checks = {check.name: check for check in cli_module._generation_checks()}

    assert checks["operator_ingress"].ok is True
    assert checks["manifest_status"].ok is True


def test_the_doctor_line_names_what_breaks_and_what_fixes_it(monkeypatch) -> None:
    """It used to print two addresses and leave the reader to infer the rest.

    This is the function `doctor` itself appends, not a reproduction of it.
    """
    manifest = make_manifest()
    monkeypatch.setattr(lifecycle, "detect_operator_cidr", lambda **_kwargs: MOVED)

    check = lifecycle._operator_cidr_check(manifest)

    assert check.name == "operator_cidr"
    assert check.ok is False
    assert SEALED in check.detail and MOVED in check.detail
    assert "Aurora or RDS" in check.detail
    assert lifecycle.OPERATOR_INGRESS_REPAIR_COMMAND in check.detail
    # Doctor is an operator asking now, so it probes directly rather than
    # reading a verdict that may be five minutes old.
    assert "doctor" in lifecycle._operator_cidr_check.__doc__


def test_the_doctor_line_is_the_one_doctor_uses(monkeypatch) -> None:
    """Guards against this test passing while doctor grows its own copy."""
    import inspect

    assert "_operator_cidr_check(manifest)" in inspect.getsource(lifecycle.doctor)


def test_the_doctor_line_passes_when_the_address_matches(monkeypatch) -> None:
    monkeypatch.setattr(lifecycle, "detect_operator_cidr", lambda **_kwargs: SEALED)

    check = lifecycle._operator_cidr_check(make_manifest())

    assert check.ok is True
    assert lifecycle.OPERATOR_INGRESS_REPAIR_COMMAND not in check.detail


def test_the_doctor_line_reports_a_failed_probe_rather_than_passing(monkeypatch) -> None:
    monkeypatch.setattr(
        lifecycle,
        "detect_operator_cidr",
        lambda **_kwargs: (_ for _ in ()).throw(OSError("no route to host")),
    )

    check = lifecycle._operator_cidr_check(make_manifest())

    assert check.ok is False
    assert "no route to host" in check.detail
