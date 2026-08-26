"""A passed TTL must not brick a long-lived installation, and `antidemo renew` must
move every copy of the timestamp or none of them.

The tests are grouped as: the expiry decoupling (Part A), the renewal command and
its atomicity (Part B), and the `--ttl-hours` no-op that used to look like the
answer to both.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from test_lifecycle import make_manifest, ready_round5_stub

import app as app_module
from server import cli as cli_module
from server import lifecycle
from server.lifecycle import (
    DEFAULT_TTL_HOURS,
    MAX_TTL_HOURS,
    Check,
    _expiry_check,
    _renew_plan_violations,
    _resume_renew_target,
    deployed_renew_followup,
    renew,
    setup,
)
from server.manager import InvalidStateError, RunManager
from server.manifest import DemoManifest

EXPIRED_AT = datetime(2026, 8, 21, 14, 46, 33, tzinfo=UTC)


def expired_manifest(**updates) -> DemoManifest:
    manifest = make_manifest(status=updates.pop("status", "ready"))
    manifest.expires_at = EXPIRED_AT
    for name, value in updates.items():
        setattr(manifest, name, value)
    return manifest


# --------------------------------------------------------------------------
# Part A: an expired timestamp no longer decides whether anything may run.
# --------------------------------------------------------------------------


def test_expired_manifest_no_longer_blocks_app_startup(monkeypatch, caplog) -> None:
    """This is the gate `app.py` uses for deployed startup (`require_v2=True`).

    Before the change this raised, so a deployed app that restarted past its TTL
    never came back up.
    """
    manifest = expired_manifest()
    manifest.manifest_version = 2
    manifest.round4 = SimpleNamespace(app_service_principal_client_id="client")
    monkeypatch.setattr(app_module, "load_manifest", lambda: manifest)

    with caplog.at_level("WARNING"):
        assert app_module._load_ready_manifest(require_v2=True) is manifest

    assert "passed their declared expiry" in caplog.text
    assert "antidemo renew" in caplog.text


def test_expired_manifest_no_longer_blocks_the_six_control_actions(monkeypatch) -> None:
    """`require_ready_manifest` is the single callable wired into every entry point.

    It backs `ShowtimeReadinessGate(manifest_check=...)` for the deployed app and
    all three methods of the local fallback gate, so un-gating it un-gates
    create, start_arm, start_run, start_redo, start_towel and start_cooldown.

    `start_towel` reaches its own validation without consulting the gate at all,
    which is stronger than passing it: the emergency stop is deliberately exempt,
    because the bout it stops is spending money whether the ring is green or not.
    """
    manifest = expired_manifest()
    monkeypatch.setattr(app_module, "load_manifest", lambda: manifest)

    app_module.require_ready_manifest()

    calls: list[str] = []

    def readiness_check() -> None:
        calls.append("checked")
        app_module.require_ready_manifest()

    manager = RunManager(readiness_check=readiness_check)
    entry_points = {
        "create": lambda: manager.create(SimpleNamespace(primary_persona="nobody")),
        "start_arm": lambda: manager.start_arm("no-such-session"),
        "start_run": lambda: manager.start_run("no-such-session"),
        "start_redo": lambda: manager.start_redo("no-such-session"),
        "start_towel": lambda: manager.start_towel("no-such-session"),
        "start_cooldown": lambda: manager.start_cooldown("no-such-session"),
    }
    for name, invoke in entry_points.items():
        with pytest.raises(Exception) as caught:  # noqa: PT011 - any non-expiry failure proves it
            asyncio.run(invoke())
        assert "expired" not in str(caught.value).casefold(), name

    # Four of the six consult the gate before they look the session up. Any of
    # them reaching its own validation is proof the gate passed. `start_redo`
    # gates after the lookup, and `start_towel` never gates.
    assert len(calls) >= 4
    assert manager._readiness_check is readiness_check


def test_expired_manifest_no_longer_blocks_the_local_fallback_gate(monkeypatch) -> None:
    manifest = expired_manifest()
    monkeypatch.setattr(app_module, "load_manifest", lambda: manifest)

    app_module.require_ready_manifest()
    app_module.require_ready_manifest()


def test_a_non_ready_status_still_refuses(monkeypatch) -> None:
    """Decoupling expiry must not weaken the readiness signal that is real."""
    manifest = expired_manifest(status="seeding")
    monkeypatch.setattr(app_module, "load_manifest", lambda: manifest)

    with pytest.raises(InvalidStateError, match="SEEDING"):
        app_module._load_ready_manifest()


def test_reset_and_reconcile_warn_instead_of_refusing(monkeypatch, capsys) -> None:
    """These are what actually broke `antidemo setup`, before doctor was ever reached."""
    manifest = expired_manifest()
    monkeypatch.setattr(lifecycle, "load_manifest", lambda: manifest)
    monkeypatch.setattr(lifecycle, "detect_operator_cidr", lambda: manifest.aws.operator_cidr)
    monkeypatch.setattr(lifecycle, "ensure_coordination", lambda candidate: None)
    monkeypatch.setattr(lifecycle, "apply_manifest_environment", lambda candidate: None)
    monkeypatch.setattr(
        lifecycle,
        "_reset_under_ring_lease",
        lambda candidate, timeout, *, round5_recovery_bout_ids=(): _completed(candidate),
    )

    assert lifecycle.reset(1) is manifest
    assert "passed their declared expiry" in capsys.readouterr().out


async def _completed(value):
    return value


def test_reconcile_infrastructure_warns_instead_of_refusing(monkeypatch, capsys) -> None:
    manifest = expired_manifest()
    monkeypatch.setattr(
        lifecycle,
        "_verify_databricks_identity",
        lambda profile: manifest.databricks.user,
    )
    monkeypatch.setattr(lifecycle, "_verify_aws_identity", lambda *args: None)
    monkeypatch.setattr(
        lifecycle,
        "_refresh_operator_cidr",
        lambda candidate: (_ for _ in ()).throw(_StopHere()),
    )

    with pytest.raises(_StopHere):
        lifecycle.reconcile_infrastructure(manifest)
    assert "passed their declared expiry" in capsys.readouterr().out


class _StopHere(RuntimeError):
    """Marks that a function ran past the gate under test."""


def test_the_doctor_expiry_line_still_prints_but_no_longer_fails_the_run(capsys) -> None:
    expired = _expiry_check(expired_manifest())
    assert expired.ok is False
    assert expired.advisory is True
    assert "HAS PASSED" in expired.detail
    assert "antidemo renew" in expired.detail
    assert "antidemo cleanup --yes" in expired.detail

    live = _expiry_check(make_manifest())
    assert live.ok is True and live.advisory is True

    cli_module.print_checks([expired], False)
    printed = capsys.readouterr().out
    assert printed.startswith("WARN")
    assert "expiry" in printed
    assert "does not fail this run" in printed

    assert cli_module.checks_passed([expired]) is True
    assert cli_module.checks_passed([expired, Check("aws_identity", False, "nope")]) is False


def test_setup_does_not_fail_on_an_advisory_doctor_finding(monkeypatch, tmp_path) -> None:
    """`antidemo setup` proceeds: the expiry line reports, other failures still stop it."""
    manifest = expired_manifest()
    owned = tmp_path / "manifest.json"
    owned.touch()
    monkeypatch.setattr(lifecycle, "manifest_path", lambda: owned)
    monkeypatch.setattr(lifecycle, "load_manifest", lambda: manifest)
    monkeypatch.setattr(lifecycle, "_require_round5_clean_baseline", lambda candidate: None)
    monkeypatch.setattr(lifecycle, "reconcile_infrastructure", lambda candidate: candidate)
    monkeypatch.setattr(lifecycle, "reset", lambda timeout: manifest)
    monkeypatch.setattr(
        lifecycle,
        "_prepare_and_reseal_round5",
        lambda candidate, *, timeout: candidate,
    )
    monkeypatch.setattr(
        lifecycle,
        "_prepare_and_reseal_round6",
        lambda candidate, *, timeout: candidate,
    )
    monkeypatch.setattr(
        lifecycle,
        "doctor",
        lambda competitor, *, timeout_seconds: [_expiry_check(manifest)],
    )

    assert _run_setup(ttl_hours=None) is manifest

    monkeypatch.setattr(
        lifecycle,
        "doctor",
        lambda competitor, *, timeout_seconds: [
            _expiry_check(manifest),
            Check("aws_ownership", False, "tag mismatch"),
        ],
    )
    with pytest.raises(RuntimeError, match="aws_ownership"):
        _run_setup(ttl_hours=None)


def _run_setup(*, ttl_hours):
    return setup(
        databricks_profile="",
        aws_profile="",
        aws_region="",
        expected_account="",
        owner="",
        operator_cidr=None,
        ttl_hours=ttl_hours,
        timeout_seconds=5,
    )


def test_the_refusing_expiry_gate_no_longer_exists_to_be_called() -> None:
    """The trap is gone, not merely unused, and must not be re-added.

    This used to be tested by making `assert_not_expired` detonate and proving no
    control path tripped it. That was the strongest available assertion while the
    method existed, but it also kept it alive: a dead method whose only reason to
    exist was a test asserting it was not called is one edit away from being
    wired back in, and wiring it back in stops an installation working on a timer.

    So the assertion is now about the shape of the type. `expiry_warning` is the
    supported treatment and stays; a raising gate cannot come back without this
    failing first.
    """
    assert not hasattr(DemoManifest, "assert_not_expired"), (
        "assert_not_expired is back on DemoManifest. It raised RuntimeError on a "
        "passed expires_at, every caller swallowed RuntimeError, and the result was "
        "an installation that silently lost Round 5 on a wall-clock comparison. Use "
        "expiry_warning() and let the caller carry on."
    )
    assert callable(DemoManifest.expiry_warning), (
        "expiry_warning is what replaced it; advisory banners depend on it"
    )


def test_no_decoupled_path_consults_expiry_at_all(monkeypatch, capsys) -> None:
    """The gates report a passed TTL; none of them may refuse on one.

    Behavioural, not structural: `require_ready_manifest` and `_warn_if_expired`
    are handed a manifest that is hours past its TTL and must both complete, with
    the warning as their only reaction.
    """
    manifest = expired_manifest()

    monkeypatch.setattr(app_module, "load_manifest", lambda: manifest)
    monkeypatch.setattr(lifecycle, "load_manifest", lambda: manifest)

    app_module.require_ready_manifest()
    lifecycle._warn_if_expired(manifest)
    assert "passed their declared expiry" in capsys.readouterr().out

    # Cleanup deliberately worked regardless of expiry before and must still. It
    # fails here for its own reasons -- this stub manifest has no live Round 5
    # environment to tear down -- and the point is that expiry is not among them.
    from server.safe_change_live import build_safe_change_engine

    with pytest.raises(Exception) as caught:
        build_safe_change_engine(manifest, cleanup_only=True, environment={})
    assert "expire" not in str(caught.value).lower()


# --------------------------------------------------------------------------
# Part B: `antidemo renew` moves every copy, or leaves a diagnosable partial state.
# --------------------------------------------------------------------------


class FakeLease:
    fencing_token = 1
    round_title = "Round 5"
    phase = "round5_bout"
    operator = SimpleNamespace(email="someone@databricks.com", display_name="Someone")


class FakeLeaseStore:
    """Records the renew lease and can pretend a bout already owns the ring."""

    mode = "lakebase"

    def __init__(self, *, held: bool = False) -> None:
        self.held = held
        self.claims: list[str] = []
        self.released = 0
        self.closed = 0

    async def initialize(self) -> None:
        return None

    async def close(self) -> None:
        self.closed += 1

    async def claim(self, *, phase: str, **_kwargs) -> FakeLease:
        from server.coordination import LeaseHeldError

        if self.held:
            raise LeaseHeldError(_held_lease())
        self.claims.append(phase)
        return FakeLease()

    async def renew(self, lease, *, ttl):
        return lease

    async def release(self, lease) -> bool:
        self.released += 1
        return True


def _held_lease():
    from server.models import BoutOperator, SessionState

    now = datetime.now(UTC)
    return SimpleNamespace(
        session_id="live-bout",
        operator=BoutOperator(display_name="Someone", email="someone@databricks.com"),
        phase="round5_bout",
        session_state=SessionState.RUNNING,
        round_id="survive_connection_spike",
        round_title="Round 5",
        competitor_id="aurora",
        competitor_name="Aurora",
        fencing_token=7,
        updated_at=now,
        expires_at=now + timedelta(seconds=90),
    )


def _tag_only_plan(*addresses: str) -> dict:
    return {
        "resource_changes": [
            {
                "address": address,
                "change": {
                    "actions": ["update"],
                    "before": {"tags": {"expires-at": "2026-08-21T14:46:33Z"}},
                    "after": {"tags": {"expires-at": "2026-08-24T00:00:00Z"}},
                    "after_unknown": {"tags_all": True},
                },
            }
            for address in addresses
        ]
    }


def _install_renew_fakes(
    monkeypatch,
    tmp_path,
    manifest,
    *,
    store=None,
    plan=None,
    patch_lease_store: bool = True,
):
    """Wire renew to fakes: no cloud, no Terraform, no coordination database.

    `patch_lease_store=False` leaves the real `build_lease_store` in place. That
    matters for one test below: replacing the factory is exactly what concealed
    renew claiming a process-local ring, because the factory is where the
    environment the manifest establishes is read.
    """
    import server.coordination as coordination

    owned = tmp_path / "manifest.json"
    owned.write_text("{}", encoding="utf-8")
    recorded: dict[str, object] = {"order": []}
    store = store or FakeLeaseStore()

    monkeypatch.setattr(lifecycle, "manifest_path", lambda: owned)
    monkeypatch.setattr(lifecycle, "load_manifest", lambda: manifest)
    monkeypatch.setattr(lifecycle, "_verify_aws_identity", lambda *args: None)
    if patch_lease_store:
        monkeypatch.setattr(coordination, "build_lease_store", lambda **_kwargs: store)
    monkeypatch.setattr(lifecycle, "_require_round5_clean_baseline", lambda candidate: None)
    monkeypatch.setattr(lifecycle, "_terraform_init", lambda candidate: None)

    def fake_plan(candidate, *, expires_at_override=None, **_kwargs):
        recorded["order"].append("plan")
        recorded["override"] = expires_at_override
        return tmp_path / "aws-create.tfplan"

    def fake_apply(candidate, plan_path):
        recorded["order"].append("apply")

    def fake_save(candidate, path=None):
        recorded["order"].append("save")
        recorded["saved_expiry"] = candidate.expires_at
        return owned

    def fake_reseal(candidate, *, timeout):
        recorded["order"].append("reseal5")
        # Mirror what the real path does: rebuild the frozen tag set from the
        # manifest value that is now in place.
        candidate.round5 = ready_round5_stub(
            ownership_tags=SimpleNamespace(expires_at=lifecycle._utc_tag(candidate.expires_at))
        )
        return candidate

    monkeypatch.setattr(lifecycle, "_terraform_plan", fake_plan)
    monkeypatch.setattr(
        lifecycle,
        "_terraform_plan_json",
        lambda candidate, plan_path: plan if plan is not None else _tag_only_plan(),
    )
    monkeypatch.setattr(lifecycle, "_terraform_apply", fake_apply)
    monkeypatch.setattr(lifecycle, "save_manifest", fake_save)
    monkeypatch.setattr(lifecycle, "_prepare_and_reseal_round5", fake_reseal)
    monkeypatch.setattr(
        lifecycle,
        "_prepare_and_reseal_round6",
        lambda candidate, *, timeout: recorded["order"].append("reseal6") or candidate,
    )
    recorded["store"] = store
    recorded["journal"] = tmp_path / lifecycle.RENEW_JOURNAL_NAME
    return recorded


def test_renew_moves_every_copy_and_applies_before_it_writes(monkeypatch, tmp_path) -> None:
    """All three local copies move, and the retag lands before the manifest does.

    Ordering matters because `cleanup` compares manifest expiry against live AWS
    tags and refuses on any mismatch, even under --dry-run. Applying first puts the
    fully-consistent outcome on the most likely failure.
    """
    manifest = expired_manifest()
    manifest.round5 = ready_round5_stub(
        ownership_tags=SimpleNamespace(expires_at="2026-08-21T14:46:33Z")
    )
    monkeypatch.setattr(type(manifest), "round5_ready", property(lambda self: True))
    recorded = _install_renew_fakes(monkeypatch, tmp_path, manifest)

    before = datetime.now(UTC)
    result = renew(ttl_hours=48)

    assert recorded["order"] == ["plan", "apply", "save", "reseal5", "save"]
    # Copy 1: the manifest.
    assert result.expires_at > before + timedelta(hours=47)
    # Copy 2: the frozen Round 5 ownership tag set, re-sealed from live Terraform.
    assert result.round5.ownership_tags.expires_at == lifecycle._utc_tag(result.expires_at)
    # Copy 3: the Terraform variable, overridden so the apply carries the new
    # value while the manifest still held the old one.
    assert recorded["override"] == result.expires_at
    assert recorded["store"].claims == ["maintenance_renew"]
    assert recorded["store"].released == 1
    assert not recorded["journal"].exists()


def test_renew_claims_the_ring_the_manifest_names(monkeypatch, tmp_path) -> None:
    """Renew must establish the manifest environment before it builds the store.

    This test deliberately leaves `build_lease_store` real and fakes the store
    *class* underneath it instead. Patching the factory -- which every other test
    here does -- is precisely what hid the original bug: renew never called
    `apply_manifest_environment`, so the real factory saw no coordination
    endpoint and the ring existed only in this process. Its refusal below could
    then never observe a live bout, and a renew would rewrite the AWS and IAM
    expiry tags underneath one.

    The assertion is positive on purpose. It names the endpoint the manifest
    seals, so it fails both if the store is process-local and if it is durable
    against the wrong ring.
    """
    import server.coordination as coordination

    manifest = expired_manifest()
    # A private mapping, so `apply_manifest_environment` cannot leak the exports
    # it performs into the rest of the suite.
    monkeypatch.setattr(lifecycle.os, "environ", dict(lifecycle.os.environ))
    for name in (
        coordination.COORDINATION_ENDPOINT_ENV,
        coordination.ALLOW_INMEMORY_COORDINATION_ENV,
        "DATABRICKS_APP_NAME",
    ):
        lifecycle.os.environ.pop(name, None)

    built: list[str] = []

    class RecordingLakebaseStore(FakeLeaseStore):
        def __init__(self, *, endpoint_name: str, **_kwargs) -> None:
            super().__init__()
            built.append(endpoint_name)

    monkeypatch.setattr(coordination, "LakebaseBoutLeaseStore", RecordingLakebaseStore)
    _install_renew_fakes(monkeypatch, tmp_path, manifest, patch_lease_store=False)

    renew(ttl_hours=48)

    assert built == ["projects/ad-test-001/branches/coordination/endpoints/primary"]


INSTALLATION_ID = "018f6f50-7d3a-7cc1-9d5d-4d9ac8d107a1"


def _isolated_manifest() -> DemoManifest:
    """A ready v7 install with an installation identity, so round isolation is on.

    `installation_id` is frozen on the model -- it is the one field a generation
    may never re-choose -- so this goes through `model_copy` rather than assigning
    to it.
    """
    return expired_manifest().model_copy(
        update={"manifest_version": 7, "installation_id": INSTALLATION_ID}
    )


class RingFenceStore:
    """A recording stand-in for the Lakebase coordinator, shared across rings.

    Every sibling `for_ring_key` hands back writes into the same ledger, so the
    order rings were claimed, released and closed in is observable from one place.
    That order is the whole subject of the two tests below: a fence built out of
    one row per ring is not atomic, so what has to be proved is that a partial
    sweep is given back.
    """

    mode = "lakebase"

    def __init__(
        self,
        *,
        ring_key: str = "main",
        held: str = "",
        ledger: dict[str, list[str]] | None = None,
        **_kwargs,
    ) -> None:
        self.ring_key = ring_key
        self.held = held
        self.ledger = ledger if ledger is not None else {
            "claimed": [],
            "released": [],
            "initialized": [],
            "closed": [],
        }

    def for_ring_key(self, ring_key: str) -> RingFenceStore:
        return RingFenceStore(ring_key=ring_key, held=self.held, ledger=self.ledger)

    async def initialize(self) -> None:
        self.ledger["initialized"].append(self.ring_key)

    async def close(self) -> None:
        self.ledger["closed"].append(self.ring_key)

    async def claim(self, **_kwargs) -> FakeLease:
        from server.coordination import LeaseHeldError

        if self.ring_key == self.held:
            raise LeaseHeldError(_held_lease())
        self.ledger["claimed"].append(self.ring_key)
        return FakeLease()

    async def renew(self, lease, *, ttl):
        return lease

    async def release(self, lease) -> bool:
        self.ledger["released"].append(self.ring_key)
        return True


def _v7_renew_fixture(monkeypatch, tmp_path, *, held: str = ""):
    """A v7 renew wired to the *real* `build_lease_store`, faking only the store class.

    Patching `build_lease_store` -- which every other renew test does -- would
    hide the defect these tests exist for, because the ring key is chosen on the
    way into that factory. So the factory runs, and the class it constructs is
    replaced instead.
    """
    import server.coordination as coordination

    manifest = _isolated_manifest()
    # A private mapping, so `apply_manifest_environment` cannot leak its exports
    # into the rest of the suite.
    monkeypatch.setattr(lifecycle.os, "environ", dict(lifecycle.os.environ))
    for name in (
        coordination.COORDINATION_ENDPOINT_ENV,
        coordination.ALLOW_INMEMORY_COORDINATION_ENV,
        "DATABRICKS_APP_NAME",
    ):
        lifecycle.os.environ.pop(name, None)

    ledger: dict[str, list[str]] = {
        "claimed": [],
        "released": [],
        "initialized": [],
        "closed": [],
    }

    def build(*, ring_key: str, **_kwargs) -> RingFenceStore:
        return RingFenceStore(ring_key=ring_key, held=held, ledger=ledger)

    monkeypatch.setattr(coordination, "LakebaseBoutLeaseStore", build)
    recorded = _install_renew_fakes(
        monkeypatch, tmp_path, manifest, patch_lease_store=False
    )
    return manifest, ledger, recorded


def _expected_v7_fence_rings() -> list[str]:
    from server.bout_cost import ROUND_ORDER
    from server.coordination import round_ring_key

    keys = [round_ring_key(INSTALLATION_ID, round_id.value) for round_id, _, _ in ROUND_ORDER]
    keys.append(
        round_ring_key(INSTALLATION_ID, "survive_connection_spike", cleanup=True)
    )
    return keys


def test_renew_fences_every_ring_a_v7_bout_can_hold(monkeypatch, tmp_path) -> None:
    """The ring keys, not the endpoint. That distinction is the whole defect.

    `antidemo renew` already reached the right *endpoint*: `apply_manifest_environment`
    puts the sealed coordination endpoint in the environment before the factory
    reads it, and the sibling test above pins that. What it then claimed was
    `build_lease_store()`'s default ring, `main` -- and on a v7 install with an
    `installation_id`, round isolation is on, so `main` is the one ring no bout
    ever holds. `RunManager._lease_store_for_round` returns the unscoped store
    only when isolation is off. So renew's `LeaseHeldError` refusal could not
    observe a live bout, and a renew during a live Round 3 would have rewritten
    the AWS and IAM expiry tags underneath it.

    An endpoint assertion cannot see that. This one can, which is why it names
    all seven scoped keys and asserts `main` is absent.
    """
    _manifest, ledger, _recorded = _v7_renew_fixture(monkeypatch, tmp_path)

    renew(ttl_hours=48)

    expected = _expected_v7_fence_rings()
    assert ledger["claimed"] == expected
    assert "main" not in ledger["claimed"]
    assert ledger["initialized"] == expected
    # Given back newest first, so an interrupted unwind never leaves an older
    # ring fenced while a newer one is free.
    assert ledger["released"] == list(reversed(expected))
    assert sorted(ledger["closed"]) == sorted(expected)


def test_renew_names_the_round_whose_ring_is_held_and_unwinds_the_partial_fence(
    monkeypatch, tmp_path
) -> None:
    """Refusing is half the job; saying which round is the other half.

    Claiming seven rings one row at a time is not atomic, so the refusal arrives
    with rings already held. Both properties are asserted: the message names
    Round 3 by number and title, and the two rings claimed before it are released
    before the refusal reaches the caller.
    """
    expected = _expected_v7_fence_rings()
    round3_ring = expected[2]
    _manifest, ledger, recorded = _v7_renew_fixture(
        monkeypatch, tmp_path, held=round3_ring
    )

    with pytest.raises(RuntimeError) as caught:
        renew(ttl_hours=48)

    message = str(caught.value)
    assert "Round 3" in message
    assert "Recover a deleted order" in message
    assert round3_ring in message
    assert "someone@databricks.com" in message
    # Rounds 1 and 2 were claimed, then given back. Nothing past Round 3 was tried.
    assert ledger["claimed"] == expected[:2]
    assert ledger["released"] == list(reversed(expected[:2]))
    # And no Terraform, no manifest write.
    assert recorded["order"] == []
    assert not recorded["journal"].exists()


def test_a_pre_isolation_install_still_fences_the_ring_its_bouts_use(
    monkeypatch, tmp_path
) -> None:
    """`main` is not wrong everywhere -- it is wrong only under round isolation."""
    from server.lifecycle import renew_fence_ring_keys

    legacy = expired_manifest()
    assert legacy.manifest_version != 7
    assert [key for key, _label in renew_fence_ring_keys(legacy)] == ["main"]

    assert [key for key, _label in renew_fence_ring_keys(_isolated_manifest())] == (
        _expected_v7_fence_rings()
    )


def test_renew_refuses_while_a_bout_holds_the_ring(monkeypatch, tmp_path) -> None:
    manifest = expired_manifest()
    store = FakeLeaseStore(held=True)
    recorded = _install_renew_fakes(monkeypatch, tmp_path, manifest, store=store)

    with pytest.raises(RuntimeError) as caught:
        renew(ttl_hours=48)

    message = str(caught.value)
    assert "Renew refused" in message
    assert "someone@databricks.com" in message
    assert recorded["order"] == []
    assert manifest.expires_at == EXPIRED_AT


def test_renew_refuses_while_round5_per_bout_resources_still_exist(
    monkeypatch, tmp_path
) -> None:
    """Those resources are authorized for cleanup by exact equality against the
    manifest copy of the tag, so rotating it underneath them would strand them."""
    manifest = expired_manifest()
    monkeypatch.setattr(type(manifest), "round5_ready", property(lambda self: True))
    recorded = _install_renew_fakes(monkeypatch, tmp_path, manifest)
    monkeypatch.setattr(
        lifecycle,
        "_require_round5_clean_baseline",
        lambda candidate: (_ for _ in ()).throw(
            RuntimeError("Round 5 per-bout residue: bout-123:proxy")
        ),
    )

    with pytest.raises(RuntimeError, match="residue"):
        renew(ttl_hours=48)

    assert recorded["order"] == []
    assert manifest.expires_at == EXPIRED_AT
    assert not recorded["journal"].exists()


def test_a_failed_apply_changes_nothing_and_says_so(monkeypatch, tmp_path) -> None:
    manifest = expired_manifest()
    recorded = _install_renew_fakes(monkeypatch, tmp_path, manifest)
    monkeypatch.setattr(
        lifecycle,
        "_terraform_apply",
        lambda candidate, plan_path: (_ for _ in ()).throw(RuntimeError("terraform exploded")),
    )

    with pytest.raises(RuntimeError) as caught:
        renew(ttl_hours=48)

    message = str(caught.value)
    assert "did not change anything" in message
    assert "CONSISTENT" in message
    assert "cleanup is unaffected" in message
    # The manifest never moved, so nothing is stranded.
    assert manifest.expires_at == EXPIRED_AT
    assert "save" not in recorded["order"]
    # The journal survives so the retry resumes to the same target.
    assert recorded["journal"].exists()


def test_a_failed_manifest_write_names_the_closed_cleanup_path(monkeypatch, tmp_path) -> None:
    """The narrow window that actually matters: AWS retagged, manifest behind."""
    manifest = expired_manifest()
    recorded = _install_renew_fakes(monkeypatch, tmp_path, manifest)
    monkeypatch.setattr(
        lifecycle,
        "save_manifest",
        lambda candidate, path=None: (_ for _ in ()).throw(OSError("disk full")),
    )

    with pytest.raises(RuntimeError) as caught:
        renew(ttl_hours=48)

    message = str(caught.value)
    assert "INCONSISTENT" in message
    assert "cleanup' will refuse" in message
    assert "--dry-run" in message
    assert "re-run 'antidemo renew'" in message
    assert recorded["journal"].exists()


def test_a_failed_reseal_is_reported_as_bouts_only(monkeypatch, tmp_path) -> None:
    """Cleanup is safe here, because the manifest already matches the live tags."""
    manifest = expired_manifest()
    monkeypatch.setattr(type(manifest), "round5_ready", property(lambda self: True))
    recorded = _install_renew_fakes(monkeypatch, tmp_path, manifest)
    monkeypatch.setattr(
        lifecycle,
        "_prepare_and_reseal_round5",
        lambda candidate, *, timeout: (_ for _ in ()).throw(RuntimeError("runner unreachable")),
    )

    with pytest.raises(RuntimeError) as caught:
        renew(ttl_hours=48)

    message = str(caught.value)
    assert "PARTIAL" in message
    assert "cleanup is safe and unaffected" in message
    assert "denied at creation rather than creating anything un-cleanable" in message
    assert recorded["journal"].exists()


def test_an_interrupted_renew_resumes_to_the_same_target(monkeypatch, tmp_path) -> None:
    """A re-run must converge the copies, not pick a third timestamp."""
    manifest = expired_manifest()
    recorded = _install_renew_fakes(monkeypatch, tmp_path, manifest)
    journal = recorded["journal"]
    journal.write_text(
        json.dumps({"from": "2026-08-21T14:46:33Z", "to": "2026-08-24T09:00:00Z"}),
        encoding="utf-8",
    )

    result = renew(ttl_hours=48)

    resumed = datetime(2026, 8, 24, 9, 0, 0, tzinfo=UTC)
    assert result.expires_at == resumed
    assert recorded["override"] == resumed
    assert not journal.exists()


def test_an_unreadable_journal_is_not_silently_discarded(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(lifecycle, "manifest_path", lambda: tmp_path / "manifest.json")
    (tmp_path / lifecycle.RENEW_JOURNAL_NAME).write_text("{not json", encoding="utf-8")

    with pytest.raises(RuntimeError, match="unreadable renew journal"):
        _resume_renew_target()


def test_renew_refuses_to_move_the_expiry_backwards(monkeypatch, tmp_path) -> None:
    manifest = make_manifest()
    manifest.expires_at = datetime.now(UTC) + timedelta(hours=100)
    _install_renew_fakes(monkeypatch, tmp_path, manifest)

    with pytest.raises(RuntimeError, match="backwards"):
        renew(ttl_hours=1)


def test_renew_respects_the_existing_ttl_maximum(monkeypatch, tmp_path) -> None:
    manifest = expired_manifest()
    _install_renew_fakes(monkeypatch, tmp_path, manifest)

    with pytest.raises(RuntimeError, match="no more than 720"):
        renew(ttl_hours=MAX_TTL_HOURS + 1)
    with pytest.raises(RuntimeError, match="greater than zero"):
        renew(ttl_hours=0)


def test_renew_requires_a_ready_installation(monkeypatch, tmp_path) -> None:
    manifest = expired_manifest(status="seeding")
    _install_renew_fakes(monkeypatch, tmp_path, manifest)

    with pytest.raises(RuntimeError, match="SEEDING"):
        renew(ttl_hours=48)


# --------------------------------------------------------------------------
# The plan gate: derived from configuration, never a memorised resource count.
# --------------------------------------------------------------------------


def test_the_plan_gate_accepts_a_tag_only_change_on_owned_addresses() -> None:
    manifest = make_manifest()
    owned = sorted(lifecycle._expected_aws_state_addresses(manifest))
    assert _renew_plan_violations(manifest, _tag_only_plan(*owned)) == []


def test_the_plan_gate_stops_a_plan_that_touches_instance_class() -> None:
    """Four RDS instances carry a deliberately chosen class; a renew must not resize."""
    manifest = make_manifest()
    address = sorted(lifecycle._expected_aws_state_addresses(manifest))[0]
    plan = {
        "resource_changes": [
            {
                "address": address,
                "change": {
                    "actions": ["update"],
                    "before": {"instance_class": "db.t4g.medium", "tags": {}},
                    "after": {"instance_class": "db.t4g.micro", "tags": {"expires-at": "x"}},
                },
            }
        ]
    }
    assert _renew_plan_violations(manifest, plan) == [f"{address}: changes instance_class"]


def test_the_plan_gate_stops_creation_deletion_and_replacement() -> None:
    manifest = make_manifest()
    address = sorted(lifecycle._expected_aws_state_addresses(manifest))[0]
    for actions in (["create"], ["delete"], ["delete", "create"], ["create", "delete"]):
        plan = {"resource_changes": [{"address": address, "change": {"actions": actions}}]}
        violations = _renew_plan_violations(manifest, plan)
        assert violations == [f"{address}: plans {'+'.join(actions)}, not a tag update"]


def test_the_plan_gate_stops_an_address_outside_the_manifest() -> None:
    manifest = make_manifest()
    plan = _tag_only_plan("aws_db_instance.someone_elses_database")
    assert _renew_plan_violations(manifest, plan) == [
        "aws_db_instance.someone_elses_database: not a manifest-owned address"
    ]


def test_the_plan_gate_allows_the_expected_computed_and_volume_changes() -> None:
    """Four IAM policies re-read as known-after-apply, and the runner tags its root
    volume as well as itself. Both are expected on every plan."""
    manifest = make_manifest()
    plan = {
        "resource_changes": [
            {
                "address": "aws_iam_role_policy.round5_execution",
                "change": {"actions": ["update"], "after_unknown": {"policy": True}},
            },
            {
                "address": "aws_instance.round5_runner",
                "change": {
                    "actions": ["update"],
                    "before": {"volume_tags": {}, "root_block_device": [{"tags": {}}]},
                    "after": {"volume_tags": {"expires-at": "x"}, "root_block_device": [{}]},
                },
            },
            {
                "address": "data.aws_caller_identity.current",
                "change": {"actions": ["read"]},
            },
        ]
    }
    assert _renew_plan_violations(manifest, plan) == []


def test_the_plan_gate_reads_no_hardcoded_resource_count() -> None:
    """Deriving the bound from configuration is what keeps it correct when a round
    is added; a memorised constant would fire spuriously."""
    # _expected_aws_state_addresses reads installation_id, which is frozen on a
    # real manifest, and whether a runtime role was sealed -- the seven free IAM
    # resources that carry it only exist when it was. Both shapes are described
    # directly, with no runtime role, which is what every installation sealed
    # before that role existed looks like.
    #
    # Both halves of the seal, not just the ARN. `AwsManifest` declares
    # `runtime_role_trusted_principal_arns` with a `None` default, so a manifest
    # written before the runtime role existed deserialises with the attribute
    # *present and None* -- never absent. `require_runtime_role_seal_is_whole`
    # then refuses any manifest where only one of the pair is None, so `None`
    # here is the sole legal partner for `runtime_role_arn=None` rather than a
    # value chosen to satisfy an attribute lookup. Naming only the ARN made this
    # namespace a shape no real installation can have, and
    # `anti_demo_runtime_principals` raised AttributeError on the other half.
    unsealed = SimpleNamespace(
        runtime_role_arn=None, runtime_role_trusted_principal_arns=None
    )
    v7 = SimpleNamespace(
        installation_id="018f6f50-7d3a-7cc1-9d5d-4d9ac8d107a1", aws=unsealed
    )
    legacy = SimpleNamespace(installation_id=None, aws=unsealed)
    v7_addresses = lifecycle._expected_aws_state_addresses(v7)

    assert lifecycle._expected_aws_state_addresses(legacy) != v7_addresses
    # The v7 shape spans r1/r2/r3/r5, so the bound grows with the rounds rather
    # than tracking a constant.
    assert sum(1 for address in v7_addresses if '["r5"]' in address) == 6
    assert _renew_plan_violations(v7, _tag_only_plan(*sorted(v7_addresses))) == []


# --------------------------------------------------------------------------
# The deployed app's own copy, and the flag that used to do nothing.
# --------------------------------------------------------------------------


def test_the_deployed_followup_is_reported_and_not_performed(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(lifecycle, "manifest_path", lambda: tmp_path / "manifest.json")
    lines = deployed_renew_followup(make_manifest())
    joined = "\n".join(lines)

    assert "anti-demo-manifest-json" in joined
    assert "restart the app" in joined
    assert "ANTI_DEMO_MANIFEST_JSON" in joined
    assert "a local run needs neither step" in joined


def test_setup_ttl_hours_is_refused_on_an_existing_install(monkeypatch, tmp_path) -> None:
    """It used to be accepted, ignored, and the run proceeded."""
    owned = tmp_path / "manifest.json"
    owned.touch()
    monkeypatch.setattr(lifecycle, "manifest_path", lambda: owned)
    monkeypatch.setattr(
        lifecycle,
        "load_manifest",
        lambda: pytest.fail("the refusal must precede any work"),
    )

    with pytest.raises(RuntimeError) as caught:
        _run_setup(ttl_hours=48)

    message = str(caught.value)
    assert "only to a first provision" in message
    assert "antidemo renew --ttl-hours 48" in message


def test_a_first_provision_defaults_to_the_longer_window(monkeypatch, tmp_path) -> None:
    """72 hours, because the TTL is never re-based off created_at: a 24-hour install
    is already partway expired the first time it is usable."""
    assert DEFAULT_TTL_HOURS == 72.0
    assert MAX_TTL_HOURS == 720.0

    owned = tmp_path / "manifest.json"
    monkeypatch.setattr(lifecycle, "manifest_path", lambda: owned)
    seen: dict[str, float] = {}
    monkeypatch.setattr(
        lifecycle,
        "provision",
        lambda **kwargs: seen.update(ttl_hours=kwargs["ttl_hours"]) or make_manifest(),
    )
    monkeypatch.setattr(
        lifecycle,
        "_prepare_and_reseal_round4",
        lambda candidate, *, timeout: candidate,
    )
    monkeypatch.setattr(
        lifecycle,
        "_prepare_and_reseal_round5",
        lambda candidate, *, timeout: candidate,
    )
    monkeypatch.setattr(
        lifecycle,
        "_prepare_and_reseal_round6",
        lambda candidate, *, timeout: candidate,
    )
    monkeypatch.setattr(lifecycle, "doctor", lambda competitor, *, timeout_seconds: [])

    _run_setup(ttl_hours=None)
    assert seen == {"ttl_hours": 72.0}

    _run_setup(ttl_hours=5)
    assert seen == {"ttl_hours": 5.0}


def test_the_renew_command_is_reachable_and_reports_the_followup(
    monkeypatch, capsys, tmp_path
) -> None:
    # Renew is a mutator, so it now claims the generation lock before running.
    # That needs a selected generation, which every real invocation has and this
    # test previously left unset.
    monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(tmp_path / ".anti-demo-v7" / "manifest.json"))
    manifest = make_manifest()
    monkeypatch.setattr(cli_module, "renew", lambda **kwargs: manifest)
    monkeypatch.setattr(
        cli_module,
        "deployed_renew_followup",
        lambda candidate: ["NEXT  rewrite the secret"],
    )
    monkeypatch.setattr("sys.argv", ["antidemo", "renew", "--ttl-hours", "72"])

    assert cli_module.main() == 0
    printed = capsys.readouterr().out
    assert "RENEWED" in printed
    assert "NEXT  rewrite the secret" in printed
