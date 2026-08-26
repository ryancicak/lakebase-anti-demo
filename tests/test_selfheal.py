"""Browser-initiated recovery: the five D9a conditions, each with a test.

Nothing here provisions anything. The spawn path is proven with a fake mutator
pointed at by `ANTI_DEMO_RECOVERY_COMMAND` -- a shell that writes a marker file
and exits -- which exercises the fork, the environment scrub, the status file
and the log tail without touching AWS, Terraform or a dollar.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from server import generation_lock, selfheal
from server.api import router
from server.manifest import (
    AwsManifest,
    AwsResources,
    DatabricksManifest,
    DemoManifest,
    save_manifest,
)
from server.reconcile import (
    PRESENCE_MISSING,
    PRESENCE_NEVER_CHECKED,
    PRESENCE_PRESENT,
    PRESENCE_UNVERIFIED,
    InstallationPresence,
)

RUN_ID = "ad-selfheal-001"

GONE = InstallationPresence(PRESENCE_MISSING, sealed=7, absent=7)
THERE = InstallationPresence(PRESENCE_PRESENT, sealed=7)
BLIND = InstallationPresence(
    PRESENCE_UNVERIFIED,
    sealed=7,
    reason="the AWS credentials could not be loaded (SSOTokenLoadError)",
)
UNASKED = InstallationPresence(PRESENCE_NEVER_CHECKED)


def _manifest(status: str = "ready") -> DemoManifest:
    return DemoManifest(
        run_id=RUN_ID,
        owner="operator@databricks.com",
        created_at=datetime.now(UTC),
        expires_at=datetime.now(UTC) + timedelta(hours=24),
        status=status,
        aws=AwsManifest(
            profile="sandbox-admin",
            account_id="123456789012",
            region="us-west-2",
            operator_cidr="203.0.113.10/32",
            terraform_state="/tmp/anti-demo-selfheal.tfstate",
            resources=AwsResources(
                aurora_cluster_id="anti-demo-aurora",
                aurora_writer_instance_id="anti-demo-aurora-writer",
                aurora_secret_arn="arn:aws:secretsmanager:us-west-2:123456789012:secret:a",
                rds_instance_id="anti-demo-rds",
                rds_secret_arn="arn:aws:secretsmanager:us-west-2:123456789012:secret:r",
                security_group_id="sg-aurora",
                rds_security_group_id="sg-rds",
                db_subnet_group_name="anti-demo-subnets",
            ),
        ),
        databricks=DatabricksManifest(
            profile="fe-vm-test",
            project_id=RUN_ID,
            endpoint_name=f"projects/{RUN_ID}/branches/production/endpoints/primary",
            user="operator@databricks.com",
        ),
        schema_sha256="abc123",
    )


@pytest.fixture
def installation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """An isolated generation: its own manifest, its own lock, its own journal."""
    path = tmp_path / "manifest.json"
    monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(path))
    monkeypatch.delenv("DATABRICKS_APP_NAME", raising=False)
    monkeypatch.delenv("ANTI_DEMO_ENV", raising=False)
    monkeypatch.delenv(selfheal.RECOVERY_TOKEN_ENV, raising=False)
    save_manifest(_manifest(), path)
    selfheal.reset_observation()
    return path


def _presence(monkeypatch: pytest.MonkeyPatch, presence: InstallationPresence) -> list[bool]:
    """Pin the sweep and record whether each observation was a forced live read."""
    forced: list[bool] = []

    async def observe(*, force: bool = False):
        forced.append(force)
        return presence, 0.0

    monkeypatch.setattr(selfheal, "observe_presence", observe)
    return forced


def _client(**transport: object) -> AsyncClient:
    api = FastAPI()
    api.include_router(router)
    return AsyncClient(
        transport=ASGITransport(app=api, **transport),
        base_url="http://anti-demo.test",
    )


def _fake_mutator(monkeypatch: pytest.MonkeyPatch, marker: Path, *, exit_code: int = 0) -> None:
    """A mutator that provisions nothing and proves it ran.

    It also writes the environment it was handed, which is how the lock-token
    scrub is checked rather than assumed.
    """
    script = (
        f"printf '%s' \"${{{generation_lock.LOCK_TOKEN_ENV}-<scrubbed>}}\" > {marker}; "
        f"echo 'terraform would run here'; exit {exit_code}"
    )
    monkeypatch.setenv(
        selfheal.RECOVERY_COMMAND_ENV, json.dumps(["/bin/sh", "-c", script])
    )


async def _spawned(client: AsyncClient, phrase: str) -> dict:
    response = await client.post("/api/installation/recover", json={"confirm": phrase})
    assert response.status_code == 202, response.text
    return response.json()


async def _settle(attempt_id: str, *, seconds: float = 10.0) -> selfheal.AttemptView:
    """Wait for the detached mutator to record an ending."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        view = selfheal.attempt_view(attempt_id)
        if view is not None and view.phase not in {
            selfheal.PHASE_SPAWNED,
            selfheal.PHASE_RUNNING,
        }:
            return view
        await asyncio.sleep(0.05)
    raise AssertionError(f"attempt {attempt_id} never finished")


# ---------------------------------------------------------------------------
# Condition 2: only a fresh, verified-missing finding may authorise a spend
# ---------------------------------------------------------------------------


async def test_recovery_refuses_when_the_account_could_not_be_read(
    installation: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`unverified` must never spend, and must say why it is not `gone`.

    This is the inversion that makes the whole feature delicate: the reaper
    deletes the IAM users too, so a *real* sweep fails at the credential
    boundary and lands here rather than on a confirmed absence. The commonest
    real trigger for recovery is the one state that must refuse to recover.
    """
    _presence(monkeypatch, BLIND)
    async with _client() as client:
        rendered = (await client.get("/api/installation")).json()
        refused = await client.post(
            "/api/installation/recover", json={"confirm": "anything at all"}
        )

    assert rendered["state"] == "unverified"
    assert rendered["recovery"]["offered"] is False
    assert rendered["recovery"]["code"] == "unverified"
    assert rendered["recovery"]["confirmation_phrase"] == ""

    assert refused.status_code == 409
    detail = refused.json()["detail"]
    # The distinction, in the operator's own words.
    assert "COULD NOT BE READ" in detail
    assert "NOT THE SAME AS THE RESOURCES BEING GONE" in detail
    # It names the thing to fix, and it is a credential, not a database. The
    # command is not pinned to the author's own SSO session name: this refusal is
    # read by whoever installed it, whose credentials come from somewhere else.
    assert "aws sso login" in detail
    assert "databricks-sandbox" not in detail
    # And it warns that this is also what a real sweep looks like, so nobody
    # reads the refusal as "nothing is wrong" -- conditionally, because an
    # account with no sweeping automation still reaches this state another way.
    assert "IAM users as well as the databases" in detail
    assert "if this account has automation" in detail


async def test_recovery_refuses_when_nothing_has_looked_yet(
    installation: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`never_checked` is not `verified_present` and is not a green light."""
    _presence(monkeypatch, UNASKED)
    async with _client() as client:
        rendered = (await client.get("/api/installation")).json()
        refused = await client.post(
            "/api/installation/recover", json={"confirm": "anything at all"}
        )

    assert rendered["state"] == "never_checked"
    assert rendered["checked"] is False
    assert rendered["recovery"]["code"] == "never_checked"
    assert refused.status_code == 409
    assert "HAS NOT BEEN READ YET" in refused.json()["detail"]


async def test_recovery_refuses_when_everything_is_present(
    installation: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _presence(monkeypatch, THERE)
    async with _client() as client:
        refused = await client.post(
            "/api/installation/recover", json={"confirm": "anything at all"}
        )
    assert refused.status_code == 409
    assert "NOTHING TO RECOVER" in refused.json()["detail"]


async def test_the_spend_path_takes_its_own_live_reading(
    installation: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The verdict that authorises a spend is taken at press time, not render time.

    A cached verdict is exactly how a stale finding spends money for no reason,
    which is the failure D9a's second condition names.
    """
    forced = _presence(monkeypatch, GONE)
    _fake_mutator(monkeypatch, tmp_path / "ran")
    async with _client() as client:
        offer = (await client.get("/api/installation")).json()["recovery"]
        await _spawned(client, offer["confirmation_phrase"])

    # The render was allowed to use the cache; the spend was not.
    assert forced == [False, True]


# ---------------------------------------------------------------------------
# Condition 5: no deployed spawn, ever
# ---------------------------------------------------------------------------


async def test_recovery_refuses_in_the_deployed_app(
    installation: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Even with a confirmed absence and a valid confirmation, deployed refuses."""
    monkeypatch.setenv("DATABRICKS_APP_NAME", "lakebase-anti-demo")
    _presence(monkeypatch, GONE)
    _fake_mutator(monkeypatch, tmp_path / "must-not-run")

    async with _client() as client:
        rendered = (await client.get("/api/installation")).json()
        refused = await client.post(
            "/api/installation/recover",
            json={"confirm": selfheal.confirmation_phrase(RUN_ID, "8.35")},
        )

    assert rendered["deployed"] is True
    assert rendered["recovery"]["code"] == "deployed"
    assert refused.status_code == 403
    assert "RECOVERY CANNOT RUN HERE" in refused.json()["detail"]
    assert not (tmp_path / "must-not-run").exists()
    # Nothing was even recorded: a refused spend must not spend a rate-limit slot.
    assert not selfheal.recovery_paths().journal.exists()


async def test_recovery_refuses_with_no_manifest_path_at_all(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The deployed refusal is physical, not a policy flag.

    In the deployed app the manifest arrives as a secret environment variable
    and `ANTI_DEMO_MANIFEST` is unset, so there is nowhere to write a journal,
    no generation lock, and no Terraform state. Unsetting the name alone must
    produce the refusal even with `DATABRICKS_APP_NAME` absent.
    """
    monkeypatch.delenv("ANTI_DEMO_MANIFEST", raising=False)
    monkeypatch.delenv("DATABRICKS_APP_NAME", raising=False)
    _presence(monkeypatch, GONE)

    async with _client() as client:
        refused = await client.post("/api/installation/recover", json={"confirm": "x"})

    assert refused.status_code == 403
    assert "no manifest path at all" in refused.json()["detail"]


# ---------------------------------------------------------------------------
# The deployed refusal names no directory it cannot see
# ---------------------------------------------------------------------------


def test_the_deployed_refusal_never_names_a_superseded_generation(
    installation: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The identical defect `receipts.artifact_root` already fixed, in prose.

    The sentence hardcoded `.anti-demo-v7/` and outlived the generation that
    named it, so on a v8 tree it sent an operator to look in a superseded
    directory. Derived from the same source the receipt root and the server log
    path resolve from, and with no version-stamped fallback for the same reason
    `manifest_path` has no default.
    """
    named = selfheal.deployed_refusal()
    assert ".anti-demo-v7" not in named
    # The generation that is actually selected, not a literal that ages.
    assert str(installation.parent) in named

    # And where there is no manifest path at all -- the deployed app, whose
    # manifest arrives as a secret -- the directory belongs to another machine.
    # Described, which is weaker and true, rather than named and wrong.
    monkeypatch.delenv("ANTI_DEMO_MANIFEST", raising=False)
    described = selfheal.deployed_refusal()
    assert ".anti-demo-v" not in described
    assert "state directory on the operator's machine" in described


# ---------------------------------------------------------------------------
# Who is reading this screen
#
# The defect: a viewer opened the deployed app and was met with an expired-token
# trace, a Terraform state path, a mutation lock and a shell command, above the
# title card. Every sentence of it was true; none of it was theirs to act on.
# ---------------------------------------------------------------------------

OWNER = "operator@databricks.com"
STRANGER = "someone.else@databricks.com"


def _deployed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABRICKS_APP_NAME", "lakebase-anti-demo")


async def test_a_local_caller_is_always_the_operator(
    installation: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The path every bout to date has been run on, unchanged.

    Locally `operator_from_request` synthesises one identity for every caller,
    so there is nothing to discriminate on -- and nothing that should be. A
    local checkout is the machine holding the Terraform state and the only
    context in which any of this advice can be followed.
    """
    _presence(monkeypatch, BLIND)
    async with _client() as client:
        rendered = (await client.get("/api/installation")).json()

    assert rendered["audience"] == selfheal.AUDIENCE_OPERATOR
    # The whole diagnosis, exactly as before: the credential named, the count
    # stated, and the refusal that explains why no button appears.
    assert "SSOTokenLoadError" in rendered["reason"]
    assert "THE ACCOUNT COULD NOT BE READ" in rendered["recovery"]["refusal"]
    assert rendered["recovery"]["plan"]


async def test_the_deployed_app_answers_its_sealed_owner_in_full(
    installation: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reclassifying who reads the diagnosis is not deleting it.

    The owner is knowable in the deployed app even though
    `apply_manifest_environment` never runs there: `load_manifest` reads the
    secret `ANTI_DEMO_MANIFEST_JSON` first, so `manifest.owner` is available
    wherever the app can serve at all.
    """
    _deployed(monkeypatch)
    _presence(monkeypatch, BLIND)
    async with _client() as client:
        rendered = (
            await client.get(
                "/api/installation", headers={"x-forwarded-email": OWNER.upper()}
            )
        ).json()

    # Case-insensitively: an SSO directory is not obliged to agree with a
    # manifest about capitalisation, and a mismatch there would silently
    # reclassify the owner as a stranger.
    assert rendered["audience"] == selfheal.AUDIENCE_OPERATOR
    assert "RECOVERY CANNOT RUN HERE" in rendered["recovery"]["refusal"]
    assert "SSOTokenLoadError" in rendered["reason"]


async def test_the_deployed_app_tells_a_viewer_nothing_it_cannot_act_on(
    installation: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The screenshot, refused at the server rather than hidden in the browser.

    Emptied here and not merely unrendered, because the text names a Terraform
    state file, a mutation lock, a secret environment variable and a shell
    command, and it carries whatever the provider said -- an `ExpiredToken`
    trace, a resource identifier, a log tail. A client cannot leak what it was
    never sent.
    """
    _deployed(monkeypatch)
    _presence(monkeypatch, BLIND)
    async with _client() as client:
        rendered = (
            await client.get(
                "/api/installation", headers={"x-forwarded-email": STRANGER}
            )
        ).json()

    assert rendered["audience"] == selfheal.AUDIENCE_VIEWER
    body = json.dumps(rendered)
    for operator_only in (
        "RECOVERY CANNOT RUN HERE",
        "Terraform",
        "antidemo",
        "mutation lock",
        "SSOTokenLoadError",
        "environment variable",
    ):
        assert operator_only not in body, operator_only

    # What survives is a skeleton of facts with no advice and no identifier --
    # the same signals `/readyz` publishes. Withholding these would be hiding
    # the fault rather than re-addressing it.
    assert rendered["state"] == "unverified"
    assert rendered["deployed"] is True
    assert rendered["sealed_resources"] == 7
    assert rendered["recovery"]["code"] == "deployed"
    # And no viewer is ever one client-side bug away from a spend.
    assert rendered["recovery"]["offered"] is False


async def test_an_unidentified_deployed_caller_is_a_viewer(
    installation: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The two mistakes are not symmetric, so the unknown falls the quiet way.

    Calling the owner a viewer costs him a panel he can also read from
    `/readyz`, from `antidemo status`, and in full from a local checkout.
    Calling a viewer the operator puts a state path and a CLI command on a
    projector.
    """
    _deployed(monkeypatch)
    _presence(monkeypatch, BLIND)
    async with _client() as client:
        rendered = (await client.get("/api/installation")).json()

    assert rendered["audience"] == selfheal.AUDIENCE_VIEWER
    assert rendered["recovery"]["refusal"] == ""


def test_an_unreadable_manifest_leaves_nobody_the_operator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A screen may never fail on the question of who is reading it."""
    monkeypatch.delenv("ANTI_DEMO_MANIFEST", raising=False)
    _deployed(monkeypatch)

    assert selfheal.sealed_owner_email() == ""
    # Empty must never match empty: an unknown owner and an unidentified caller
    # are two absences, not an agreement.
    assert selfheal.audience_for("") == selfheal.AUDIENCE_VIEWER
    assert selfheal.audience_for(None) == selfheal.AUDIENCE_VIEWER
    assert selfheal.audience_for(OWNER) == selfheal.AUDIENCE_VIEWER


# ---------------------------------------------------------------------------
# Condition 3: explicit human confirmation
# ---------------------------------------------------------------------------


async def test_the_confirmation_is_required_rather_than_defaulted(
    installation: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """An empty body, a blank string and a wrong phrase all refuse.

    The phrase is issued by the server and names both the generation and the
    daily cost, so it cannot be produced without having read both -- and it
    cannot be defaulted into a client that never showed them.
    """
    _presence(monkeypatch, GONE)
    marker = tmp_path / "must-not-run"
    _fake_mutator(monkeypatch, marker)

    async with _client() as client:
        offer = (await client.get("/api/installation")).json()["recovery"]
        empty = await client.post("/api/installation/recover", json={})
        blank = await client.post("/api/installation/recover", json={"confirm": ""})
        guessed = await client.post(
            "/api/installation/recover", json={"confirm": "yes"}
        )
        partial = await client.post(
            "/api/installation/recover", json={"confirm": f"recreate {RUN_ID}"}
        )

    assert empty.status_code == 422
    for wrong in (blank, guessed, partial):
        assert wrong.status_code == 409
        assert "the confirmation does not match" in wrong.json()["detail"]
    assert not marker.exists()

    # And the phrase the server issues names what will be created and the money.
    phrase = offer["confirmation_phrase"]
    assert RUN_ID in phrase
    assert offer["usd_per_day"] in phrase
    assert phrase == f"recreate {RUN_ID} for ${offer['usd_per_day']} a day"


async def test_the_offer_names_which_of_the_three_things_setup_would_do(
    installation: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One button meaning three different things is the dishonesty to avoid."""
    _presence(monkeypatch, GONE)
    async with _client() as client:
        ready = (await client.get("/api/installation")).json()["recovery"]["plan"]
        save_manifest(_manifest("seeding"), installation)
        transitional = (await client.get("/api/installation")).json()["recovery"]["plan"]
        await asyncio.to_thread(installation.unlink)
        fresh = (await client.get("/api/installation")).json()["recovery"]["plan"]

    assert "re-apply Terraform" in ready
    assert "destructive to demo state" in ready
    assert "RESUME an interrupted provision" in transitional
    assert "first provision" in fresh


async def test_a_failed_cleanup_is_never_offered_a_retry(
    installation: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`resume_provision` refuses `cleanup_failed` by name, so the UI must not offer it."""
    _presence(monkeypatch, GONE)
    save_manifest(_manifest("cleanup_failed"), installation)
    async with _client() as client:
        rendered = (await client.get("/api/installation")).json()
        refused = await client.post("/api/installation/recover", json={"confirm": "x"})

    assert rendered["recovery"]["code"] == "cleanup_failed"
    assert refused.status_code == 409
    assert "'cleanup_failed'" in refused.json()["detail"]


# ---------------------------------------------------------------------------
# Condition 4: a durable rate limit
# ---------------------------------------------------------------------------


def test_the_rate_limit_is_read_from_disk_by_every_new_process(tmp_path: Path) -> None:
    """A restart must not hand the limiter a fresh budget.

    This is defect 2.4 wearing different clothes: `RestartJournal` carried the
    total forward but not the stamps, so every new supervisor got a full run of
    deaths. Here the equivalent bug would be a provisioning loop.
    """
    journal_path = tmp_path / "recovery-attempts.jsonl"
    now = 1_800_000_000.0

    first = selfheal.RecoveryJournal(journal_path)
    assert first.verdict(now).allowed is True
    first.record({"attempt_id": "a", "at_epoch": now})

    # A brand-new object is what a restarted process has: no memory, only the file.
    restarted = selfheal.RecoveryJournal(journal_path)
    verdict = restarted.verdict(now + 60)
    assert verdict.allowed is False
    assert "restarting the server does not clear it" in verdict.refusal

    # And the interval genuinely expires rather than being permanent.
    assert restarted.verdict(now + selfheal.MIN_SECONDS_BETWEEN_ATTEMPTS + 1).allowed


def test_the_daily_budget_survives_a_restart_too(tmp_path: Path) -> None:
    journal_path = tmp_path / "recovery-attempts.jsonl"
    now = 1_800_000_000.0
    journal = selfheal.RecoveryJournal(journal_path)
    for index in range(selfheal.MAX_ATTEMPTS_PER_WINDOW):
        journal.record(
            {
                "attempt_id": str(index),
                "at_epoch": now - index * selfheal.MIN_SECONDS_BETWEEN_ATTEMPTS * 2,
            }
        )

    restarted = selfheal.RecoveryJournal(journal_path)
    # Past the per-attempt interval, so only the 24-hour budget can refuse it.
    verdict = restarted.verdict(now + selfheal.MIN_SECONDS_BETWEEN_ATTEMPTS + 1)
    assert verdict.allowed is False
    assert verdict.attempts_in_window == selfheal.MAX_ATTEMPTS_PER_WINDOW
    assert "24 hours" in verdict.refusal
    assert restarted.verdict(now + selfheal.RATE_WINDOW_SECONDS + 1).allowed


async def test_a_simulated_restart_does_not_reopen_the_budget_over_http(
    installation: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """End to end: spawn once, throw the process state away, and be refused."""
    _presence(monkeypatch, GONE)
    _fake_mutator(monkeypatch, tmp_path / "ran")

    async with _client() as client:
        offer = (await client.get("/api/installation")).json()["recovery"]
        accepted = await _spawned(client, offer["confirmation_phrase"])
    await _settle(accepted["attempt_id"])

    # Everything this process remembers, forgotten. Only the files remain.
    selfheal.reset_observation()

    async with _client() as client:
        rendered = (await client.get("/api/installation")).json()
        refused = await client.post(
            "/api/installation/recover", json={"confirm": offer["confirmation_phrase"]}
        )

    assert rendered["recovery"]["code"] == "rate_limited"
    assert rendered["recovery"]["attempts_in_window"] == 1
    assert refused.status_code == 429
    assert "RATE LIMITED" in refused.json()["detail"]


# ---------------------------------------------------------------------------
# Condition 1: spawn-only
# ---------------------------------------------------------------------------


def test_the_default_mutator_names_a_launcher_that_is_really_there(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The default argv is checked against the tree, not against a copy of itself.

    `mutator_command()` is reached from one place, and every other test in this
    file replaces it through `ANTI_DEMO_RECOVERY_COMMAND` -- deliberately, since
    that is what keeps the spawn provable without spending money. The cost is
    that the default argv had no coverage at all, and it went stale: the launcher
    was renamed from `demo` to `antidemo` and the whole suite stayed green while
    this path named a file that no longer existed. What that buys an operator is
    the worst available answer -- the button accepts the press, spends a
    rate-limit slot, forks, and the child dies at `execve` while the
    infrastructure it was pressed to restore is still missing.

    So the assertion is against the filesystem rather than against the name.
    Asserting the basename equals `"antidemo"` would just be a second copy of the
    same literal, free to drift in step with the first; a path that has to resolve
    to a real executable cannot survive a rename.

    Executable, not merely present, because `_spawn_detached` reaches it through
    `os.execve` rather than through a shell -- an unexecutable target fails in the
    same silent place as a missing one.

    The override is deleted first. It is set by most of this file and is not in
    `conftest.py`'s ambient scrub list, so an inherited one would make this test
    vacuous -- which is the same blindness that hid the stale literal.
    """
    monkeypatch.delenv(selfheal.RECOVERY_COMMAND_ENV, raising=False)

    command = selfheal.mutator_command()
    launcher = Path(command[0])

    assert launcher.is_file(), (
        f"mutator_command() spawns {command!r}, but {launcher} is not a file in "
        "this checkout. The recovery route would fork a child that dies at "
        "execve, leaving the operator a spent attempt and no infrastructure."
    )
    assert os.access(launcher, os.X_OK), (
        f"{launcher} exists but is not executable, and _spawn_detached execve()s "
        "it directly rather than through a shell."
    )
    # The other way this spawn fails: without `--no-serve` the installer starts a
    # second server on the port this one is already answering on.
    assert command[1:] == ["setup", "--no-serve"]


async def test_the_spawn_scrubs_the_lock_token_and_never_holds_the_lock(
    installation: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The server starts the mutator and stays an observer.

    Two properties, both load-bearing. The child must not inherit
    `ANTI_DEMO_GENERATION_LOCK_TOKEN` -- it is the lock's reentrancy escape
    hatch, so a server launched from a shell that held the lock would hand its
    child a free pass past the exclusion this design rests on. And the serving
    process must hold no lock of its own, or `antidemo setup` becomes impossible
    without stopping the demo.
    """
    monkeypatch.setenv(generation_lock.LOCK_TOKEN_ENV, "a-live-token")
    _presence(monkeypatch, GONE)
    inherited = tmp_path / "inherited-token"
    _fake_mutator(monkeypatch, inherited)

    async with _client() as client:
        offer = (await client.get("/api/installation")).json()["recovery"]
        accepted = await _spawned(client, offer["confirmation_phrase"])

    view = await _settle(accepted["attempt_id"])
    assert view.phase == "succeeded"
    assert view.exit_code == 0
    assert inherited.read_text() == "<scrubbed>"
    assert generation_lock._HELD_BY_THIS_PROCESS == {}
    assert not generation_lock.lock_is_held(
        generation_lock.generation_lock_path(installation)
    )


def test_the_child_environment_drops_the_lock_tokens_reentrancy_pass() -> None:
    child = selfheal.child_environment(
        {generation_lock.LOCK_TOKEN_ENV: "live", "PATH": "/usr/bin"}, "attempt-1"
    )
    assert generation_lock.LOCK_TOKEN_ENV not in child
    assert child["PATH"] == "/usr/bin"
    assert child[selfheal.ATTEMPT_ENV] == "attempt-1"


async def test_recovery_refuses_while_another_process_is_mutating(
    installation: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Two installers over one Terraform state is what the lock exists to stop."""
    _presence(monkeypatch, GONE)
    marker = tmp_path / "must-not-run"
    _fake_mutator(monkeypatch, marker)

    with generation_lock.hold_generation(installation, "antidemo setup"):
        async with _client() as client:
            rendered = (await client.get("/api/installation")).json()
            refused = await client.post(
                "/api/installation/recover", json={"confirm": "x"}
            )

    assert rendered["mutation_in_progress"] is True
    assert rendered["recovery"]["code"] == "mutation_in_progress"
    assert refused.status_code == 409
    assert "A MUTATION IS ALREADY IN FLIGHT" in refused.json()["detail"]
    assert not marker.exists()


# ---------------------------------------------------------------------------
# The progress channel
# ---------------------------------------------------------------------------


async def test_progress_is_pollable_and_carries_the_mutators_output(
    installation: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The SSE stream cannot carry this, so a file the mutator writes does.

    The channel has to outlive the server, because the mutator is detached and
    this process may be restarted while it runs; a file survives that and an
    in-process subscriber does not.
    """
    _presence(monkeypatch, GONE)
    _fake_mutator(monkeypatch, tmp_path / "ran")

    async with _client() as client:
        offer = (await client.get("/api/installation")).json()["recovery"]
        accepted = await _spawned(client, offer["confirmation_phrase"])
        await _settle(accepted["attempt_id"])
        progress = await client.get(accepted["poll"])
        missing = await client.get("/api/installation/recovery/never-happened")

    assert accepted["poll"] == f"/api/installation/recovery/{accepted['attempt_id']}"
    payload = progress.json()
    assert progress.status_code == 200
    assert payload["phase"] == "succeeded"
    assert payload["exit_code"] == 0
    assert "terraform would run here" in payload["log_tail"]
    # A green exit is not a green installation, and the wording must say so.
    assert "Nothing here declares the installation healthy" in payload["detail"]
    assert missing.status_code == 404


async def test_a_failed_mutator_reports_its_code_and_says_a_retry_resumes(
    installation: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _presence(monkeypatch, GONE)
    _fake_mutator(monkeypatch, tmp_path / "ran", exit_code=3)

    async with _client() as client:
        offer = (await client.get("/api/installation")).json()["recovery"]
        accepted = await _spawned(client, offer["confirmation_phrase"])

    view = await _settle(accepted["attempt_id"])
    assert view.phase == "failed"
    assert view.exit_code == 3
    assert "continues from where it stopped" in view.detail


def test_an_attempt_whose_process_vanished_is_reported_as_lost(
    installation: Path,
) -> None:
    """A progress bar that will never move is a surface claiming health again."""
    paths = selfheal.recovery_paths()
    paths.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    (paths.directory / "ghost.json").write_text(
        json.dumps({"attempt_id": "ghost", "phase": "running", "pid": 999_999_999}),
        encoding="utf-8",
    )
    view = selfheal.attempt_view("ghost")
    assert view is not None
    assert view.phase == "lost"
    assert "resumable" in view.detail


def test_a_just_spawned_attempt_is_not_called_lost_before_its_deadline(
    installation: Path,
) -> None:
    """The window between the fork and the child's first write is not a death.

    Calling it one would send an operator to press a button that the generation
    lock -- held by the installer that is in fact running fine -- would refuse.
    """
    paths = selfheal.recovery_paths()
    paths.directory.mkdir(mode=0o700, parents=True, exist_ok=True)

    def write(started_at: datetime) -> None:
        (paths.directory / "young.json").write_text(
            json.dumps(
                {
                    "attempt_id": "young",
                    "phase": selfheal.PHASE_SPAWNED,
                    "pid": None,
                    "started_at": started_at.isoformat(),
                }
            ),
            encoding="utf-8",
        )

    now = datetime.now(UTC)
    write(now - timedelta(seconds=selfheal.SPAWN_REPORT_GRACE_SECONDS / 2))
    young = selfheal.attempt_view("young")
    assert young is not None
    assert young.phase == selfheal.PHASE_SPAWNED

    write(now - timedelta(seconds=selfheal.SPAWN_REPORT_GRACE_SECONDS + 5))
    stale = selfheal.attempt_view("young")
    assert stale is not None
    assert stale.phase == selfheal.PHASE_LOST


# ---------------------------------------------------------------------------
# Authorisation
# ---------------------------------------------------------------------------


async def test_a_request_from_off_this_machine_is_refused(
    installation: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Loopback is the authorisation, and it is stated rather than assumed."""
    _presence(monkeypatch, GONE)
    marker = tmp_path / "must-not-run"
    _fake_mutator(monkeypatch, marker)

    async with _client(client=("10.1.2.3", 4242)) as client:
        refused = await client.post("/api/installation/recover", json={"confirm": "x"})

    assert refused.status_code == 403
    assert "not from this machine" in refused.json()["detail"]
    assert not marker.exists()


async def test_a_configured_token_outranks_loopback(
    installation: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A server bound to a routable address can require the operator's own token."""
    monkeypatch.setenv(selfheal.RECOVERY_TOKEN_ENV, "s3cret")
    _presence(monkeypatch, GONE)
    _fake_mutator(monkeypatch, tmp_path / "ran")

    async with _client() as client:
        offer = (await client.get("/api/installation")).json()["recovery"]
        without = await client.post(
            "/api/installation/recover", json={"confirm": offer["confirmation_phrase"]}
        )
        wrong = await client.post(
            "/api/installation/recover",
            json={"confirm": offer["confirmation_phrase"]},
            headers={selfheal.RECOVERY_TOKEN_HEADER: "guess"},
        )
        accepted = await client.post(
            "/api/installation/recover",
            json={"confirm": offer["confirmation_phrase"]},
            headers={selfheal.RECOVERY_TOKEN_HEADER: "s3cret"},
        )

    assert without.status_code == 403
    assert wrong.status_code == 403
    assert accepted.status_code == 202
    await _settle(accepted.json()["attempt_id"])


# ---------------------------------------------------------------------------
# Bookkeeping
# ---------------------------------------------------------------------------


async def test_the_authorisation_is_journalled_before_the_fork(
    installation: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A crash between recording and forking must cost a slot, not hide a process."""
    _presence(monkeypatch, GONE)
    _fake_mutator(monkeypatch, tmp_path / "ran")
    recorded: list[float] = []
    real_record = selfheal.RecoveryJournal.record

    def watch(self, payload):
        recorded.append(time.monotonic())
        return real_record(self, payload)

    forked: list[float] = []
    real_spawn = selfheal._spawn_detached

    def watch_spawn(*args, **kwargs):
        forked.append(time.monotonic())
        return real_spawn(*args, **kwargs)

    monkeypatch.setattr(selfheal.RecoveryJournal, "record", watch)
    monkeypatch.setattr(selfheal, "_spawn_detached", watch_spawn)

    async with _client() as client:
        offer = (await client.get("/api/installation")).json()["recovery"]
        accepted = await _spawned(client, offer["confirmation_phrase"])
    await _settle(accepted["attempt_id"])

    assert len(recorded) == 1
    assert len(forked) == 1
    assert recorded[0] < forked[0]

    entries = selfheal.RecoveryJournal(selfheal.recovery_paths().journal).entries()
    assert len(entries) == 1
    assert entries[0]["run_id"] == RUN_ID
    assert entries[0]["usd_per_day"] == offer["usd_per_day"]


def test_the_quoted_cost_comes_from_the_cost_model(installation: Path) -> None:
    """Not a new constant, and it says what it counts."""
    from decimal import Decimal

    from server.cost_model import CarryingWindow, EstimateScope, estimate_carrying_cost

    estimate = estimate_carrying_cost(CarryingWindow(seconds=Decimal(86400)))
    expected = estimate.total_usd(EstimateScope.CARRYING) + estimate.total_usd(
        EstimateScope.OVERHEAD
    )
    assert selfheal.daily_cost_usd() == expected
    assert "estimate_carrying_cost" in selfheal.COST_BASIS
