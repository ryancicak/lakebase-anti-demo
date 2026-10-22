"""Round 2's Azure lane against a fake ARM client and a fake connector: no network, no Azure.

Pins what Phase 0 measured (server/azure_safe_change.py): no restore before the first full
backup, "accepted then InternalServerError" is a wait, the restore request is the lane's first
control-plane call, the copy's firewall rules go one at a time, and a copy is owned only by its
tags, its resource group and the installation's Azure scope.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any

import pytest

from server.azure_arm import ArmError, Response, RestoreNotReady
from server.azure_safe_change import (
    RESTORE_VISIBILITY_SECONDS,
    AzurePostgresSafeChangeAdapter,
    AzureSafeChangeConfig,
)
from server.safe_change import SafeChangeOwnershipScope, SafeChangePlan, SafeChangeProvider
from server.safe_change_live import SafeChangeControlPlaneError, SafeChangeLiveConfigurationError

SUB = "00000000-1111-2222-3333-444444444444"
RG = "rg-test"
RULES = (("operator", "192.0.2.10/32"), ("egress-01", "198.51.100.0/24"))
CONFIG = AzureSafeChangeConfig(subscription_id=SUB, resource_group=RG, region="westus2",
                               source_server="r2-src", database="postgres",
                               admin_login="antidemo_admin", firewall_rules=RULES,
                               restore_retry_seconds=0, poll_interval_seconds=0)
SCOPE = SafeChangeOwnershipScope(run_id="ad-test-001", owner="operator@databricks.com",
                                 expires_at="2030-01-01T00:00:00Z", cloud="azure",
                                 azure_subscription_id=SUB, azure_region="westus2")
PLAN = SafeChangePlan(lane_id="azure_postgres", name="Azure Database for PostgreSQL",
                      provider=SafeChangeProvider.AZURE_POSTGRES, source_id="r2-src",
                      artifact_id="r2-src-copy", scope=SCOPE)


def server(name: str, *, tags: dict[str, str] | None = None, group: str = RG,
           state: str = "Ready") -> dict[str, Any]:
    return {"id": f"/subscriptions/{SUB}/resourceGroups/{group}/providers/"
                  f"Microsoft.DBforPostgreSQL/flexibleServers/{name}",
            "name": name, "location": "West US 2", "tags": tags or {},
            "sku": {"name": "Standard_B2s"},
            "properties": {"state": state, "version": "17",
                           "fullyQualifiedDomainName": f"{name}.postgres.example.test",
                           "backup": {"backupRetentionDays": 7}}}


class FakeArm:
    """Behaves as Azure did in Phase 0, including the copy wearing the source's tags."""

    SOURCE_TAGS = {"Owner": "owner@example.com", "spike": "S-Z2"}

    def __init__(self, *, backups: list[str] | None = None, not_ready: int = 0) -> None:
        self.servers = {"r2-src": server("r2-src", tags=dict(self.SOURCE_TAGS))}
        self.backups = ["2026-10-19T03:13:32Z"] if backups is None else backups
        self.not_ready = not_ready
        self.calls: list[str] = []
        self.rules: list[tuple[str, str]] = []
        #: A name Azure still holds after its server reads 404: how many more checks it stays
        #: taken for (2026-10-20: about two minutes after a delete was issued).
        self.reserved: dict[str, int] = {}
        #: The bound each delete was given, in order.
        self.delete_bounds: list[float] = []

    def get_server(self, name: str) -> dict[str, Any] | None:
        self.calls.append(f"get:{name}")
        return self.servers.get(name)

    def server_name_available(self, name: str) -> bool:
        self.calls.append(f"name:{name}")
        if name in self.servers:
            return False
        left = self.reserved.get(name, 0)
        if left > 0:
            self.reserved[name] = left - 1
            return False
        return True

    def list_full_backups(self, name: str) -> list[str]:
        self.calls.append(f"backups:{name}")
        return list(self.backups)

    def begin_restore(self, *, source_server: str, name: str, point_in_time_utc: str,
                      location: str, tags: dict[str, str], source_id: str | None,
                      availability_zone: str | None = None) -> Response:
        self.calls.append(f"begin:{name}")
        self.pending = (name, dict(tags))
        self.restore_zone = availability_zone
        return Response(202, None, {"Azure-AsyncOperation": "https://op"})

    def finish_restore(self, accepted: Response, **_: Any) -> dict[str, Any]:
        self.calls.append("finish")
        if self.not_ready:
            self.not_ready -= 1
            raise RestoreNotReady("not yet", code="InternalServerError")
        name, _requested_tags = self.pending
        # Azure ignores the request's tags: the copy carries the source's.
        self.servers[name] = server(name, tags=dict(self.SOURCE_TAGS))
        return self.servers[name]

    def server_path(self, name: str) -> str:
        return (f"/subscriptions/{SUB}/resourceGroups/{RG}/providers/"
                f"Microsoft.DBforPostgreSQL/flexibleServers/{name}")

    def merge_tags(self, resource_id: str, tags: dict[str, str]) -> None:
        name = resource_id.rsplit("/", 1)[-1]
        self.calls.append(f"tags:{name}")
        self.servers[name]["tags"].update(tags)

    def put_firewall_rules(self, name: str, rules: tuple[tuple[str, str], ...]) -> list:
        self.calls.append(f"rules:{name}")
        self.rules = list(rules)
        return [(rule, 6.3) for rule, _ in rules]

    def delete_server(self, name: str, *, bound_seconds: float = 1800.0) -> str:
        self.calls.append(f"delete:{name}")
        self.delete_bounds.append(bound_seconds)
        self.servers.pop(name, None)
        return "deleted"

    def begin_delete_server(self, name: str) -> Response:
        self.calls.append(f"begin-delete:{name}")
        return Response(202, None, {})


async def connector(**kwargs: Any) -> object:
    return object()


def adapter(arm: FakeArm) -> AzurePostgresSafeChangeAdapter:
    return AzurePostgresSafeChangeAdapter(CONFIG, arm=arm, password=lambda: "pw",  # type: ignore[arg-type]
                                          connector=connector)


class Unanswered(FakeArm):
    """The name check unanswered at ARM's gateway `times` times, then answered."""

    def __init__(self, times: int) -> None:
        super().__init__()
        self.unanswered = times

    def server_name_available(self, name: str) -> bool | None:  # type: ignore[override]
        if self.unanswered:
            self.unanswered -= 1
            self.calls.append(f"name:{name}:unanswered")
            return None
        return super().server_name_available(name)


@pytest.mark.parametrize(("times", "found"), [(2, None), (3, "DELETING")])
async def test_an_unanswered_name_check_is_asked_again_then_counts_as_taken(times, found) -> None:
    # Round 3's Prepare ended twice on a gateway timeout (2026-10-20). Asked again it answers; a
    # name never answered for is waited out as one still taken, never restored to.
    arm = Unanswered(times)
    lane = adapter(arm)
    slept: list[float] = []

    async def counted(seconds: float) -> None:
        slept.append(seconds)

    lane._sleep = counted  # type: ignore[attr-defined]
    inspection = await lane.inspect_artifact(PLAN)
    assert (inspection.state if inspection else None) == found
    assert len(slept) == min(times, 2)


class Creating(FakeArm):
    """A copy whose restore Azure accepted moments ago (the visibility probe, 2026-10-20): it reads
    404 with its name taken for `hidden` name checks, then shows up Provisioning. A delete sent
    while it reads 404 finds nothing; once deleted, its name is held two checks longer."""

    def __init__(self, hidden: int) -> None:
        super().__init__()
        self.hidden = hidden

    def server_name_available(self, name: str) -> bool:
        if name == "r2-src-copy" and self.hidden > 0:
            self.calls.append(f"name:{name}")
            self.hidden -= 1
            if self.hidden == 0:
                self.servers[name] = server(name, tags=dict(self.SOURCE_TAGS),
                                            state="Provisioning")
            return False
        return super().server_name_available(name)

    def delete_server(self, name: str, *, bound_seconds: float = 1800.0) -> str:
        self.calls.append(f"delete:{name}")
        self.delete_bounds.append(bound_seconds)
        if self.servers.pop(name, None) is None:
            return "absent"
        self.reserved[name] = 2
        return "deleted"


async def test_a_copy_still_being_created_is_deleted_once_azure_shows_it() -> None:
    # A towel or a death in a restore's first minute leaves a copy that reads 404 while its name is
    # taken, which the cleanup takes for one already deleting. Its first delete finds nothing; the
    # name wait alone would then have held for the whole bound and failed the cleanup.
    arm = Creating(hidden=3)
    lane = adapter(arm)
    now = [0.0]

    async def passing(seconds: float) -> None:
        now[0] += seconds

    lane._sleep = passing  # type: ignore[attr-defined]
    lane._clock = lambda: now[0]  # type: ignore[attr-defined]
    inspection = await lane.inspect_artifact(PLAN)
    assert inspection is not None and inspection.state == "DELETING" and inspection.owner
    reports: list[str] = []

    async def report(message: str, _wire: str | None = None) -> None:
        reports.append(message)

    await lane.delete_isolated(PLAN, inspection, report)
    assert [call for call in arm.calls if call.startswith("delete:")] == [
        "delete:r2-src-copy", "delete:r2-src-copy"]
    assert "r2-src-copy" not in arm.servers
    assert any("still being created" in message for message in reports)
    assert await lane.inspect_artifact(PLAN) is None
    # The second delete waits inside the name wait's own bound, not delete_server's longer one.
    from server.azure_safe_change import NAME_RELEASE_BOUND_SECONDS

    assert 0 < arm.delete_bounds[1] <= NAME_RELEASE_BOUND_SECONDS


class Dropping(FakeArm):
    """A copy whose delete was accepted reads Dropping, name still taken, for `reads` reads."""

    def __init__(self, reads: int) -> None:
        super().__init__()
        self.reads = reads
        self.dropped = False

    def get_server(self, name: str) -> dict[str, Any] | None:
        self.calls.append(f"get:{name}")
        if self.dropped and self.reads > 0:
            self.reads -= 1
            return server(name, tags=dict(self.SOURCE_TAGS), state="Dropping")
        return self.servers.get(name)

    def server_name_available(self, name: str) -> bool:
        if self.dropped:
            self.calls.append(f"name:{name}")
            return self.reads == 0
        return super().server_name_available(name)

    def delete_server(self, name: str, *, bound_seconds: float = 1800.0) -> str:
        self.calls.append(f"delete:{name}")
        self.servers.pop(name, None)
        self.dropped = True
        return "deleted"


async def test_a_copy_already_deleting_is_waited_out_and_never_deleted_again() -> None:
    # Finding 39's case: a delete was issued and the name is still held, so it is only waited
    # for, whether Azure reads the copy as 404 meanwhile or as Dropping.
    arm = FakeArm()
    arm.reserved["r2-src-copy"] = 3
    lane = adapter(arm)
    lane._sleep = _noop  # type: ignore[attr-defined]
    inspection = await lane.inspect_artifact(PLAN)
    assert inspection is not None and inspection.state == "DELETING"
    await lane.delete_isolated(PLAN, inspection, _noop)
    assert [call for call in arm.calls if call.startswith("delete:")] == ["delete:r2-src-copy"]

    dropping = Dropping(reads=3)
    dropping.servers["r2-src-copy"] = server("r2-src-copy", tags=dict(Dropping.SOURCE_TAGS))
    lane = adapter(dropping)
    lane._sleep = _noop  # type: ignore[attr-defined]
    await lane.delete_isolated(PLAN, await lane.inspect_artifact(PLAN), _noop)
    assert [call for call in dropping.calls if call.startswith("delete:")] == [
        "delete:r2-src-copy"]
    assert dropping.reads == 0  # the Dropping reads were seen, and waited out


class Unseen(FakeArm):
    """A restore Azure accepted as a towel landed (bar 3, 2026-10-21). The lane stopped waiting
    for ARM's answer; the copy reads 404 with its name free for `hidden` reads, then shows up
    Provisioning, holding its name. A delete sent before then finds nothing; once deleted, the
    name is held two checks longer."""

    def __init__(self, hidden: int) -> None:
        super().__init__()
        self.hidden = hidden
        self.sent = threading.Event()
        self.answer = threading.Event()
        self.shown = False

    def begin_restore(self, **kwargs: Any) -> Response:  # type: ignore[override]
        accepted = super().begin_restore(**kwargs)
        self.sent.set()
        self.answer.wait(5)  # ARM's answer, which the towel stops the lane waiting for
        return accepted

    def get_server(self, name: str) -> dict[str, Any] | None:
        if name == "r2-src-copy" and self.sent.is_set() and not self.shown:
            if self.hidden > 0:
                self.hidden -= 1
            else:
                self.shown = True
                self.servers[name] = server(name, tags=dict(self.SOURCE_TAGS),
                                            state="Provisioning")
        return super().get_server(name)

    def delete_server(self, name: str, *, bound_seconds: float = 1800.0) -> str:
        self.calls.append(f"delete:{name}")
        if self.servers.pop(name, None) is None:
            return "absent"
        self.reserved[name] = 2
        return "deleted"


def passing_time(lane: AzurePostgresSafeChangeAdapter) -> list[float]:
    """The lane's clock, moved on by its own sleeps."""

    now = [0.0]

    async def passing(seconds: float) -> None:
        now[0] += seconds

    lane._sleep = passing  # type: ignore[attr-defined]
    lane._clock = lambda: now[0]  # type: ignore[attr-defined]
    return now


async def test_a_restore_a_towel_cut_short_is_deleted_before_the_round_reopens() -> None:
    # Bar 3 (2026-10-21): a towel landed as Azure accepted Round 2's restore. The copy read 404
    # with its name free, so the towel's delete and the cleanup found nothing, the round read
    # READY, and the next bout's restore to the name was refused: RestoreToFlexServerExists.
    arm = Unseen(hidden=4)
    lane = adapter(arm)
    passing_time(lane)
    bout = asyncio.create_task(lane.create_isolated(PLAN, _noop))
    try:
        assert await asyncio.to_thread(arm.sent.wait, 5)
        bout.cancel()  # the towel
        with pytest.raises(asyncio.CancelledError):
            await bout
        # The towel's own teardown, then the cleanup the round waits on before it reopens.
        await lane.abandon_isolated(PLAN)
        inspection = await lane.inspect_artifact(PLAN)
        assert inspection is not None and inspection.state == "REQUESTED" and inspection.owner
        await lane.delete_isolated(PLAN, inspection, _noop)
        assert arm.shown and "r2-src-copy" not in arm.servers
        assert [call for call in arm.calls if call.startswith("delete:")] == [
            "delete:r2-src-copy", "delete:r2-src-copy"]
        assert await lane.inspect_artifact(PLAN) is None
    finally:
        arm.answer.set()


async def test_a_restore_azure_never_shows_is_waited_for_only_until_its_time_to_show() -> None:
    lane = adapter(FakeArm())
    now = passing_time(lane)
    lane._restores[PLAN.artifact_id] = 0.0  # type: ignore[attr-defined]
    inspection = await lane.inspect_artifact(PLAN)
    assert inspection is not None and inspection.state == "REQUESTED"
    await lane.delete_isolated(PLAN, inspection, _noop)
    assert RESTORE_VISIBILITY_SECONDS <= now[0] <= RESTORE_VISIBILITY_SECONDS + 10
    assert await lane.inspect_artifact(PLAN) is None
    # With no restore sent, a 404 with its name free is gone at once, as before.
    assert await adapter(FakeArm()).inspect_artifact(PLAN) is None


async def test_a_source_azure_is_updating_still_serves_and_one_stopped_is_refused() -> None:
    # A backup holds a Flexible Server in Updating for the whole of it, and a restore requested
    # then was accepted and finished (2026-10-20). AWS's rc10 rule: never refuse a source over
    # the cloud's own housekeeping.
    arm = FakeArm()
    arm.servers["r2-src"]["properties"]["state"] = "Updating"
    lane = adapter(arm)
    assert (await lane.preflight(PLAN))["capability"] == "native_pitr_restore"
    artifact = await lane.create_isolated(PLAN, _noop)
    assert artifact.state == "READY"
    for state in ("Stopped", "Disabled", "Dropping", "Starting"):
        refused = FakeArm()
        refused.servers["r2-src"]["properties"]["state"] = state
        with pytest.raises(SafeChangeLiveConfigurationError, match="not ready"):
            await adapter(refused).preflight(PLAN)


class PolicyUpdating(FakeArm):
    """The subscription's policy updates a new copy while its firewall rules go on
    (`deployIfNotExists`, 2026-10-20 7:42 AM): the copy reads `state` from then on."""

    def __init__(self, state: str) -> None:
        super().__init__()
        self.state = state

    def put_firewall_rules(self, name: str, rules: tuple[tuple[str, str], ...]) -> list:
        timings = super().put_firewall_rules(name, rules)
        self.servers[name]["properties"]["state"] = self.state
        return timings


async def test_a_copy_the_subscriptions_policy_is_updating_still_serves() -> None:
    # The check after the restore wanted Ready, and failed a bout when the policy's write landed
    # first: the copy serves while Azure updates it, as an RDS copy does while backing up.
    artifact = await adapter(PolicyUpdating("Updating")).create_isolated(PLAN, _noop)
    assert (artifact.owner, artifact.state) == ("operator@databricks.com", "UPDATING")
    # Any other state is still refused, and the refusal names it.
    with pytest.raises(SafeChangeControlPlaneError, match=r"not a ready copy \(state DISABLED\)"):
        await adapter(PolicyUpdating("Disabled")).create_isolated(PLAN, _noop)


async def test_preflight_refuses_a_source_with_no_full_backup_yet() -> None:
    with pytest.raises(SafeChangeLiveConfigurationError, match="no full backup"):
        await adapter(FakeArm(backups=[])).preflight(PLAN)


async def test_preflight_reports_the_restore_window_and_the_rule_count() -> None:
    evidence = await adapter(FakeArm()).preflight(PLAN)
    assert evidence["capability"] == "native_pitr_restore"
    assert evidence["first_full_backup"] == "2026-10-19T03:13:32Z"
    assert evidence["firewall_rules"] == 2


async def test_create_restores_first_retries_a_restore_azure_could_not_serve_then_opens_rules():
    arm = FakeArm(not_ready=1)
    lane = adapter(arm)
    await lane.preflight(PLAN)
    arm.calls.clear()
    reports: list[tuple[str, str]] = []

    async def report(message: str, wire: str) -> None:
        reports.append((message, wire))
        # The restore request is on the wire before the first presentation I/O.
        assert arm.calls[0] == "begin:r2-src-copy"

    artifact = await lane.create_isolated(PLAN, report)
    assert [c for c in arm.calls if c.startswith(("begin", "finish"))] == [
        "begin:r2-src-copy", "finish", "begin:r2-src-copy", "finish"]
    assert arm.rules == list(RULES)
    assert len([r for r in reports if "accepted" in r[0]]) == 1
    assert (artifact.owner, artifact.state, artifact.azure_region) == (
        "operator@databricks.com", "READY", "westus2")
    assert artifact.azure_subscription_id == SUB


async def test_ownership_is_merged_onto_the_copy_before_its_firewall_rules() -> None:
    # Azure's restore drops the request's tags, so the adapter tags the copy itself, first.
    arm = FakeArm()
    lane = adapter(arm)
    artifact = await lane.create_isolated(PLAN, _noop)
    order = [c for c in arm.calls if c.startswith(("finish", "tags", "rules"))]
    assert order == ["finish", "tags:r2-src-copy", "rules:r2-src-copy"]
    assert artifact.owner == "operator@databricks.com"
    assert arm.servers["r2-src-copy"]["tags"]["spike"] == "S-Z2"  # the source's stay, merged


async def test_the_subscriptions_policy_tags_and_lease_date_go_on_the_copy() -> None:
    # Azure replaces a copy's tags, so a tag the subscription's policy requires must be in the
    # adapter's own set. A policy tag can't take over one of the installation's ownership tags.
    from dataclasses import replace

    config = replace(CONFIG, policy_tags=(("cost-center", "demo"), ("owner", "someone-else")),
                     lease_date_tag="review-by")
    arm = FakeArm()
    lane = AzurePostgresSafeChangeAdapter(config, arm=arm, password=lambda: "pw",  # type: ignore[arg-type]
                                          connector=connector)
    await lane.create_isolated(PLAN, _noop)
    tags = arm.servers["r2-src-copy"]["tags"]
    assert tags["cost-center"] == "demo"
    assert tags["review-by"] == "2030-01-01"
    assert tags["owner"] == "operator@databricks.com"
    # With no policy configured, the copy carries the installation's tags and nothing else.
    assert set(adapter(FakeArm())._tags(PLAN)) == {
        "anti-demo-run-id", "owner", "expires-at", "safe-change-source-id",
        "safe-change-provider", "managed-by"}


async def test_a_policy_tag_in_another_case_is_the_installations_own_tag() -> None:
    # Azure reads tag names in any case, and refused a restore naming "Owner" beside "owner"
    # (AmbiguousPolicyEvaluationPaths, Phase 0, 4:43 AM). The manifest refuses such a policy
    # tag; the adapter still sends each name once, with its own value.
    from dataclasses import replace

    config = replace(CONFIG, policy_tags=(("Owner", "someone-else"), ("cost-center", "demo")),
                     lease_date_tag="review-by")
    arm = FakeArm()
    lane = AzurePostgresSafeChangeAdapter(config, arm=arm, password=lambda: "pw",  # type: ignore[arg-type]
                                          connector=connector)
    await lane.create_isolated(PLAN, _noop)
    _, requested = arm.pending
    assert [name for name in requested if name.lower() == "owner"] == ["owner"]
    assert requested["owner"] == "operator@databricks.com"
    assert (requested["cost-center"], requested["review-by"]) == ("demo", "2030-01-01")


async def test_the_copy_is_restored_into_the_sources_zone() -> None:
    # Phase 0, 4:11 AM: B2s was refused in westus2's zone 3 and offered in 1 and 2, and an
    # unpinned create landed where Azure picked. The source's zone is one its size works in.
    arm = FakeArm()
    arm.servers["r2-src"]["properties"]["availabilityZone"] = "2"
    lane = adapter(arm)
    await lane.preflight(PLAN)
    await lane.create_isolated(PLAN, _noop)
    assert arm.restore_zone == "2"
    # Without a preflight, the restore still reads its source, zone included, first.
    cold = FakeArm()
    cold.servers["r2-src"]["properties"]["availabilityZone"] = "1"
    await adapter(cold).create_isolated(PLAN, _noop)
    assert cold.restore_zone == "1"
    # A source with no zone (a region without zones) leaves the choice to Azure.
    unzoned = FakeArm()
    await adapter(unzoned).create_isolated(PLAN, _noop)
    assert unzoned.restore_zone is None


async def test_every_connection_verifies_the_certificate_and_takes_only_scram(tmp_path) -> None:
    # sslmode=require checks no certificate, and libpq would hand the admin password to any
    # server that asked for it in cleartext (security review, 2026-10-19).
    from server.azure_safe_change import (
        AZURE_POSTGRES_ROOTS,
        AZURE_POSTGRES_ROOTS_SHA256,
        verifying_connector,
    )

    seen: list[dict[str, Any]] = []

    async def recording(**options: Any) -> object:
        seen.append(options)
        return object()

    connect = verifying_connector(recording, AZURE_POSTGRES_ROOTS)
    await connect(host="h", sslmode="require", password="pw")
    assert seen[0]["sslmode"] == "verify-full" and seen[0]["require_auth"] == "scram-sha-256"
    assert seen[0]["sslrootcert"] == str(AZURE_POSTGRES_ROOTS)
    text = AZURE_POSTGRES_ROOTS.read_text()
    assert text.count("BEGIN CERTIFICATE") == 2
    assert "DigiCert Global Root G2" in text and "Microsoft RSA Root" in text
    import hashlib

    assert hashlib.sha256(AZURE_POSTGRES_ROOTS.read_bytes()).hexdigest() == \
        AZURE_POSTGRES_ROOTS_SHA256
    tampered = tmp_path / "roots.pem"
    tampered.write_text(text + "\n")
    with pytest.raises(SafeChangeLiveConfigurationError, match="pinned bundle"):
        verifying_connector(recording, tampered)


async def test_a_restore_that_never_becomes_servable_is_bounded() -> None:
    arm = FakeArm(not_ready=10**6)
    lane = AzurePostgresSafeChangeAdapter(
        AzureSafeChangeConfig(**{**CONFIG.__dict__, "restore_not_ready_bound_seconds": 0}),
        arm=arm, password=lambda: "pw", connector=connector)  # type: ignore[arg-type]
    with pytest.raises(SafeChangeControlPlaneError, match="could not serve"):
        await lane.create_isolated(PLAN, _noop)


async def _noop(*_: Any) -> None:
    return None


async def test_a_copy_with_another_managers_tags_or_outside_the_group_is_not_owned() -> None:
    arm = FakeArm()
    good = adapter(arm)._tags(PLAN)
    arm.servers["r2-src-copy"] = server("r2-src-copy", tags={**good, "managed-by": "someone"})
    assert (await adapter(arm).inspect_artifact(PLAN)).owner == ""
    arm.servers["r2-src-copy"] = server("r2-src-copy", tags=good, group="rg-other")
    assert (await adapter(arm).inspect_artifact(PLAN)).owner == ""
    arm.servers["r2-src-copy"] = server("r2-src-copy", tags=good)
    assert (await adapter(arm).inspect_artifact(PLAN)).owner == "operator@databricks.com"


async def test_a_copy_still_wearing_only_the_sources_tags_is_owned_by_its_name() -> None:
    # Live: the run's tags surfaced many minutes after they were written. Meanwhile the copy
    # carries the source's tags, including an installation source's managed-by = terraform.
    arm = FakeArm()
    arm.servers["r2-src-copy"] = server("r2-src-copy", tags={
        "Owner": "someone@databricks.com", "anti-demo-run-id": "ad-test-001",
        "managed-by": "terraform"})
    artifact = await adapter(arm).inspect_artifact(PLAN)
    assert (artifact.owner, artifact.run_id, artifact.source_id) == (
        "operator@databricks.com", "ad-test-001", "r2-src")


async def test_a_server_with_another_name_is_never_this_runs_copy() -> None:
    arm = FakeArm()
    lane = adapter(arm)
    arm.servers["r2-src-copy"] = server("r2-src-copy-2", tags=lane._tags(PLAN))
    assert (await lane.inspect_artifact(PLAN)).owner == ""


async def test_delete_refuses_a_copy_it_does_not_own_and_deletes_one_it_does() -> None:
    arm = FakeArm()
    lane = adapter(arm)
    arm.servers["r2-src-copy"] = server("r2-src-copy", tags={"anti-demo-run-id": "someone-else"})
    artifact = await lane.inspect_artifact(PLAN)
    with pytest.raises(SafeChangeLiveConfigurationError, match="does not own"):
        await lane.delete_isolated(PLAN, artifact, _noop)
    arm.servers["r2-src-copy"] = server("r2-src-copy", tags=lane._tags(PLAN))
    await lane.delete_isolated(PLAN, await lane.inspect_artifact(PLAN), _noop)
    assert "r2-src-copy" not in arm.servers


async def test_an_aws_scoped_plan_is_refused_before_any_call() -> None:
    aws = SafeChangeOwnershipScope(run_id="ad-test-001", owner="operator@databricks.com",
                                   expires_at="2030-01-01T00:00:00Z",
                                   aws_account_id="123456789012", aws_region="us-west-2")
    arm = FakeArm()
    with pytest.raises(SafeChangeLiveConfigurationError, match="subscription and region"):
        await adapter(arm).preflight(SafeChangePlan(**{**PLAN.__dict__, "scope": aws}))
    assert arm.calls == []


async def test_a_control_plane_refusal_names_no_subscription_or_host() -> None:
    arm = FakeArm()

    def refuse(name: str) -> None:
        raise ArmError(f"reading server {name}: AuthorizationFailed: /subscriptions/{SUB}/x",
                       status=403, code="AuthorizationFailed")

    arm.get_server = refuse  # type: ignore[method-assign]
    with pytest.raises(SafeChangeControlPlaneError) as raised:
        await adapter(arm).preflight(PLAN)
    assert SUB not in str(raised.value)
    assert "example.test" not in str(raised.value)
