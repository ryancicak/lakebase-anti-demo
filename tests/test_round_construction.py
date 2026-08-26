"""A round that cannot be built must leave evidence, not just disappear.

`app.py` builds every round factory inside a clause that must not be fatal, so
a construction failure will always end in a round that is absent.  What these
tests pin is that the absence is *explained*: the log names the round and the
exception that caused it.  A test that only checked the factory was `None`
would have passed against the code that deleted Round 5 from a live
installation without saying anything.

Landed 2026-08-23.  This file was written against a fix that was reverted for a
demo freeze (19 August, Chicago), and it skipped at import for as long as
`server/round_construction.py` was absent.  The module is now in the tree, so
the import below is deliberately *hard*: an `importorskip` here would mean that
deleting the module turns thirteen assertions about a silent failure into a
silent skip, which is the same defect one level up.

Two claims the original file made about the tree are no longer true, and the
docstrings below say so where it matters.  `DemoManifest.assert_not_expired` --
named here as the `RuntimeError` that vanished Round 5 -- has since been deleted
outright, and a passed TTL is now a printed warning that refuses nothing.  The
observed incident therefore cannot recur by that route.  The swallow it went
through is still here, which is what these tests are about.
"""

from __future__ import annotations

import logging

import pytest

import app as app_module
import server.catalog as catalog
import server.connection_spike_live as connection_spike_live
import server.round_construction as round_construction
from server.connection_spike_live import ConnectionSpikeLiveConfigurationError
from server.manager import InvalidStateError
from server.models import Availability, RoundId

CONSTRUCTED_ROUNDS = round_construction.CONSTRUCTED_ROUNDS
CONSTRUCTION_FAILED = round_construction.CONSTRUCTION_FAILED
exception_diagnostic = round_construction.exception_diagnostic
probe_round_construction = round_construction.probe_round_construction

ROUND_CONSTRUCTION_LOGGER = "server.round_construction"


class LakebaseLeaseStore:
    """The shape `connection_spike_factory_from_manifest` demands of Round 5."""

    mode = "lakebase"

    def __init__(self, ring_key: str) -> None:
        self.ring_key = ring_key

    def _run(self, *_args, **_kwargs) -> None: ...

    async def current(self) -> None:
        return None


def _round5_manifest(tmp_path):
    from test_manifest import _v7_manifest

    manifest = _v7_manifest(tmp_path)
    assert manifest.round5_ready, "fixture must seal Round 5"
    return manifest


def _round5_lease_store(manifest) -> LakebaseLeaseStore:
    return LakebaseLeaseStore(app_module._round5_lease_ring_key(manifest))


def _failures(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if CONSTRUCTION_FAILED in record.getMessage()
    ]


def test_round5_construction_failure_names_the_round_and_the_exception(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The exact failure that deleted Round 5, now with something to read.

    `connection_spike_live_config_from_manifest` raising is what a missing
    manifest field, a malformed seal, or a typo in the builder looks like from
    here.  It was also what a passed TTL looked like, until `assert_not_expired`
    was deleted and expiry became a printed warning -- so that one specific
    cause is gone, and every other way the builder can raise is not.
    """

    manifest = _round5_manifest(tmp_path)

    def unbuildable(*_args, **_kwargs):
        raise ConnectionSpikeLiveConfigurationError(
            "the sealed proxy secret arn is missing"
        )

    monkeypatch.setattr(
        connection_spike_live,
        "connection_spike_live_config_from_manifest",
        unbuildable,
    )

    with caplog.at_level(logging.DEBUG, logger=ROUND_CONSTRUCTION_LOGGER):
        factory = app_module.connection_spike_factory_from_manifest(
            manifest, lease_store=_round5_lease_store(manifest)
        )

    assert factory is None, "a round that cannot be built must not be armable"

    failures = _failures(caplog)
    assert len(failures) == 1, "the failure must be reported exactly once"
    record = failures[0]
    message = record.getMessage()
    assert record.levelno == logging.ERROR
    assert "round=5" in message, "a reader must not have to infer which round vanished"
    assert "survive_connection_spike" in message
    assert "ConnectionSpikeLiveConfigurationError" in message
    assert "the sealed proxy secret arn is missing" in message
    assert record.exc_info is not None, "the traceback must survive the swallow"


def test_round5_construction_failure_reports_the_whole_cause_chain(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The top-level type alone can be the least informative link.

    A config builder that re-raises a `KeyError` as an `InvalidStateError` tells
    an operator far more when both names are present: one says the seal is
    wrong, the other says which field.
    """

    manifest = _round5_manifest(tmp_path)

    def unbuildable(*_args, **_kwargs):
        raise InvalidStateError("the Round 5 seal is incomplete") from KeyError(
            "proxy_secret_arn"
        )

    monkeypatch.setattr(
        connection_spike_live,
        "connection_spike_setup_config_from_manifest",
        unbuildable,
    )

    with caplog.at_level(logging.DEBUG, logger=ROUND_CONSTRUCTION_LOGGER):
        factory = app_module.connection_spike_factory_from_manifest(
            manifest, lease_store=_round5_lease_store(manifest)
        )

    assert factory is None
    message = _failures(caplog)[0].getMessage()
    assert "InvalidStateError: the Round 5 seal is incomplete" in message
    assert "KeyError" in message
    assert "proxy_secret_arn" in message


def test_a_failed_round_stays_non_fatal_for_every_other_round(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An installation where five rounds work must still serve five rounds.

    This is why the swallow exists and why the fix is a log line rather than a
    raise: the same broken Round 5 must not cost Round 4 or Round 6, and the
    loud report must not become an exception that escapes startup.
    """

    manifest = _round5_manifest(tmp_path)

    def unbuildable(*_args, **_kwargs):
        raise ConnectionSpikeLiveConfigurationError("runner instance id is absent")

    monkeypatch.setattr(
        connection_spike_live,
        "connection_spike_setup_config_from_manifest",
        unbuildable,
    )

    with caplog.at_level(logging.DEBUG, logger=ROUND_CONSTRUCTION_LOGGER):
        round5 = app_module.connection_spike_factory_from_manifest(
            manifest, lease_store=_round5_lease_store(manifest)
        )
        round4 = app_module.model_score_factory_from_manifest(manifest)

    assert round5 is None
    assert round4 is not None, "Round 4 must survive a broken Round 5"
    assert len(_failures(caplog)) == 1, "only the round that failed may be reported"


def test_round5_withheld_for_a_drifted_ring_key_names_the_precondition(
    tmp_path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Nothing raised, and the round still vanished.

    Four terms of one boolean returned `None`, so a lease store fencing the
    wrong ring and a developer's in-memory coordinator produced the identical
    silence. The ring key is the one that is a bug rather than a configuration.
    """

    manifest = _round5_manifest(tmp_path)
    drifted = LakebaseLeaseStore("anti-demo/round5/some-other-installation")

    with caplog.at_level(logging.DEBUG, logger=ROUND_CONSTRUCTION_LOGGER):
        factory = app_module.connection_spike_factory_from_manifest(
            manifest, lease_store=drifted
        )

    assert factory is None
    messages = [record.getMessage() for record in caplog.records]
    withheld = [text for text in messages if "ROUND UNAVAILABLE" in text]
    assert len(withheld) == 1
    assert "round=5" in withheld[0]
    assert "some-other-installation" in withheld[0], "say which ring was found"
    assert app_module._round5_lease_ring_key(manifest) in withheld[0], (
        "and which ring was expected"
    )


def test_round5_withheld_without_durable_coordination_says_so(
    tmp_path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    manifest = _round5_manifest(tmp_path)

    class InMemoryStore:
        mode = "memory"
        ring_key = ""

    with caplog.at_level(logging.DEBUG, logger=ROUND_CONSTRUCTION_LOGGER):
        factory = app_module.connection_spike_factory_from_manifest(
            manifest, lease_store=InMemoryStore()
        )

    assert factory is None
    withheld = [
        record.getMessage()
        for record in caplog.records
        if "ROUND UNAVAILABLE" in record.getMessage()
    ]
    assert len(withheld) == 1
    assert "round=5" in withheld[0]
    assert "'memory'" in withheld[0]


class Round4SealRaises:
    """A seal whose Round 4 gate raises, standing in for a manifest shape change."""

    round5_ready = False
    round6_ready = False

    @property
    def manifest_version(self) -> int:
        raise ValueError("sealed Round 4 manifest version is unreadable")


class Round6SealRaises:
    manifest_version = 2
    round4 = None
    round5_ready = False

    @property
    def round6_ready(self) -> bool:
        raise ValueError("sealed Round 6 readiness is unreadable")


@pytest.mark.parametrize(
    ("seal", "factory_name", "round_number", "round_id"),
    [
        (
            Round4SealRaises(),
            "model_score_factory_from_manifest",
            4,
            "put_model_score_in_app",
        ),
        (
            Round6SealRaises(),
            "live_orders_factory_from_manifest",
            6,
            "analyze_live_orders_without_slowing_checkout",
        ),
    ],
)
def test_every_round_gate_reports_its_own_failure(
    seal: object,
    factory_name: str,
    round_number: int,
    round_id: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Round 5 was the one that broke; the clause is the same shape everywhere.

    Rounds 4 and 6 defer their engine construction into the returned closure,
    so their remaining eager work is the seal gate itself -- which is exactly
    what a manifest shape change breaks.
    """

    with caplog.at_level(logging.DEBUG, logger=ROUND_CONSTRUCTION_LOGGER):
        factory = getattr(app_module, factory_name)(seal)

    assert factory is None
    failures = _failures(caplog)
    assert len(failures) == 1
    message = failures[0].getMessage()
    assert f"round={round_number}" in message
    assert round_id in message
    assert "ValueError" in message
    assert "unreadable" in message


def test_a_checkout_with_no_installation_is_loud_only_about_real_failures(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The fix must not become the noise it replaced.

    A developer checkout with no manifest has nothing configured, and saying so
    once per round on every start would bury the failures that matter. So the
    contrast is what gets pinned: no manifest is silent, an unbuildable seal is
    an ERROR.
    """

    with caplog.at_level(logging.DEBUG, logger=ROUND_CONSTRUCTION_LOGGER):
        assert app_module.model_score_factory_from_manifest(None) is None
        assert app_module.live_orders_factory_from_manifest(None) is None
        assert (
            app_module.connection_spike_factory_from_manifest(None, lease_store=None)
            is None
        )
        quiet = list(caplog.records)
        assert app_module.model_score_factory_from_manifest(Round4SealRaises()) is None

    assert quiet == [], "an absent installation is not a failure to report"
    assert len(_failures(caplog)) == 1


def test_probe_reports_a_sealed_round_that_cannot_be_built(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The check `antidemo doctor` is missing.

    Doctor's `_round5_topology_check` validates the seal against live AWS and
    Databricks; it never asks whether the seal still builds a config. So a
    manifest field that only the config builder reads passes every doctor check
    and still removes the round.
    """

    manifest = _round5_manifest(tmp_path)

    assert [(probe.round_number, probe.ok) for probe in probe_round_construction(manifest)] == [
        (5, True)
    ]

    def unbuildable(*_args, **_kwargs):
        raise ConnectionSpikeLiveConfigurationError("proxy subnet ids are empty")

    monkeypatch.setattr(
        connection_spike_live,
        "connection_spike_setup_config_from_manifest",
        unbuildable,
    )

    (probe,) = probe_round_construction(manifest)
    assert probe.ok is False
    assert probe.check_name == "round5_construction"
    assert "ConnectionSpikeLiveConfigurationError" in probe.detail
    assert "proxy subnet ids are empty" in probe.detail


def test_doctor_fails_on_a_sealed_round_that_cannot_be_built(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`antidemo doctor` must exit non-zero, not merely print a line.

    A round that cannot be built is a fault, so it is not advisory the way a
    passed TTL is: the installation genuinely cannot serve what it claims.
    """

    from server.cli import checks_passed, round_construction_checks

    manifest = _round5_manifest(tmp_path)
    monkeypatch.setattr("server.cli.load_manifest", lambda: manifest)

    assert checks_passed(round_construction_checks()) is True

    def unbuildable(*_args, **_kwargs):
        raise ConnectionSpikeLiveConfigurationError("runner subnet id is absent")

    monkeypatch.setattr(
        connection_spike_live,
        "connection_spike_live_config_from_manifest",
        unbuildable,
    )

    checks = round_construction_checks()

    assert [check.name for check in checks] == ["round5_construction"]
    assert checks[0].advisory is False, "an unbuildable round is a fault, not advice"
    assert checks_passed(checks) is False
    assert "runner subnet id is absent" in checks[0].detail


def test_doctor_construction_checks_stay_quiet_without_an_installation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Doctor already reports an unloadable manifest once, under `owned_manifest`."""

    from server.cli import round_construction_checks

    def no_manifest():
        raise RuntimeError("No owned demo manifest exists")

    monkeypatch.setattr("server.cli.load_manifest", no_manifest)

    assert round_construction_checks() == []


def test_probe_is_empty_when_no_round_is_sealed() -> None:
    class NoRound5:
        round5_ready = False

    assert probe_round_construction(NoRound5()) == ()


def test_a_complete_seal_offers_every_round_it_constructs(tmp_path) -> None:
    """The failure the rest of this file could not see: a round that just goes.

    Every other test here drives a construction *failure* and reads the log.
    That is one half of the property, and on its own it is satisfied by a
    factory which returns `None` on a seal that is entirely valid -- no
    exception, so nothing to log, and no test looking. Which is the incident:
    Round 5 absent from an installation that had sealed it.

    So this is the other half, and it is a positive assertion rather than a
    negative one. Every round `CONSTRUCTED_ROUNDS` names must actually be
    offered for a seal that satisfies it, and nothing about a log line can
    substitute for that.
    """

    manifest = _round5_manifest(tmp_path)
    factories = {
        4: app_module.model_score_factory_from_manifest(manifest),
        5: app_module.connection_spike_factory_from_manifest(
            manifest, lease_store=_round5_lease_store(manifest)
        ),
        6: app_module.live_orders_factory_from_manifest(manifest),
    }

    assert set(factories) == set(CONSTRUCTED_ROUNDS), (
        "every constructed round must be exercised here, or its disappearance "
        "is unobserved"
    )
    vanished = sorted(number for number, factory in factories.items() if factory is None)
    assert vanished == [], f"a complete seal lost round(s) {vanished}"


def test_construction_reports_on_every_round_the_catalog_gates_on_a_factory(
    tmp_path,
) -> None:
    """`CONSTRUCTED_ROUNDS` has to keep meaning what it says.

    The catalog decides three rounds' availability from whether their factory
    was built, and those three are exactly the rounds whose construction can
    fail. A fourth gaining a factory without gaining a construction report, or
    one of the three losing its entry, would put a round back in the state this
    module exists to end -- absent, with nothing accounting for it.

    Derived by flipping the availability flags rather than by listing the ids
    again, so this cannot pass by mirroring the same mistake twice.
    """

    gated = {
        item.id
        for item in catalog.ROUNDS
        if catalog.round_by_id(item.id, True, True, True).availability
        != catalog.round_by_id(item.id, False, False, False).availability
    }

    assert gated == set(CONSTRUCTED_ROUNDS.values())
    assert all(
        catalog.round_by_id(round_id, True, True, True).availability
        == Availability.READY
        for round_id in CONSTRUCTED_ROUNDS.values()
    ), "a constructed round must be offerable once its factory exists"

    # And the numbering an operator reads in the log has to be the numbering the
    # catalog orders them by, or "round=5" names the wrong round.
    ordered = [item.id for item in catalog.ROUNDS]
    assert [ordered.index(round_id) + 1 for round_id in CONSTRUCTED_ROUNDS.values()] == list(
        CONSTRUCTED_ROUNDS
    )

    # Round 5 is the only round validated eagerly, and it is validated once per
    # opponent. Narrowing that set silently would leave an opponent it offers
    # but never checked, so it is pinned to the opponents the catalog declares.
    round5 = catalog.round_by_id(RoundId.SURVIVE_CONNECTION_SPIKE)
    assert set(round_construction.ROUND5_COMPETITORS) == set(round5.competitors)


def test_demo_doctor_runs_the_construction_checks(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A check nothing calls is not a check.

    `round_construction_checks` is asserted directly above, which proves it
    computes the right answer and proves nothing about whether `antidemo doctor`
    ever asks it. Unwired, it would report protection that no operator command
    can reach -- the same shape of defect as the silent round.
    """

    from contextlib import nullcontext

    from server import cli as cli_module
    from server.lifecycle import Check

    manifest = _round5_manifest(tmp_path)
    monkeypatch.setattr(cli_module, "load_manifest", lambda: manifest)
    monkeypatch.setattr(cli_module, "doctor", lambda _competitor: [Check("stub", True, "")])
    monkeypatch.setattr(cli_module, "_mutating", lambda _operation: nullcontext())
    monkeypatch.setattr("sys.argv", ["antidemo", "doctor"])

    def unbuildable(*_args, **_kwargs):
        raise ConnectionSpikeLiveConfigurationError("the sealed vpc id is missing")

    monkeypatch.setattr(
        connection_spike_live,
        "connection_spike_setup_config_from_manifest",
        unbuildable,
    )

    exit_code = cli_module.main()
    printed = capsys.readouterr().out

    assert "round5_construction" in printed, "doctor must run the construction checks"
    assert "the sealed vpc id is missing" in printed
    assert exit_code == 1, "an unbuildable round must fail the command, not just print"


def test_exception_diagnostic_terminates_on_a_self_referential_cause() -> None:
    """A cause cycle must not make the diagnostic the failure."""

    first = RuntimeError("outer")
    second = ValueError("inner")
    first.__cause__ = second
    second.__cause__ = first

    diagnostic = exception_diagnostic(first)

    assert diagnostic == "RuntimeError: outer <- ValueError: inner"
