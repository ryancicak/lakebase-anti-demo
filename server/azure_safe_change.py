"""Round 2's Azure lane: restore the competitor's server to a copy, then open the copy's firewall.

Like-for-like with RDS, which restores to its latest restorable time (design section 6.2). Azure
restores the r2 Flexible Server to a new server at the request's own instant. It does not copy
firewall rules ("These rules aren't copied from the original server"), so the lane creates the
copy's rules itself, and the deployed app can reach the copy only once they exist. Both steps are
Azure's real workflow, so both are on Azure's clock:

* **The restore** took 451.6 s request-to-Ready in Phase 0, and 460.2 s to a verified read. A fast
  restore, to the newest full backup, was no faster (453.6 s), so the lane takes the latest point
  as RDS does.
* **The firewall rules go one at a time.** Azure serializes them per server: 27 rules (the
  operator plus eastus2's 26 Databricks egress prefixes) took 176 s one by one and 578 s at once.

**Ownership rests on the copy's name, because its tags can't be read back promptly.** Four live
runs of this adapter found, in turn:
- a restore gives the copy the source's tags and ignores the request's;
- tags merged on at once become visible many minutes later, and replace the set rather than
  merging it;
- the merge leaves the server `ServerIsBusy` for the next write;
- a copy reusing a just-deleted name can read 404 right after its restore Succeeded.

So the run's tags are still written the moment the restore completes. But the copy is this
run's because it sits in the installation's resource group under this run's deterministic name
(`adsc-<run>-azure`), and no tag contradicts that: no other run ID, provider or manager. A merely
missing tag proves nothing either way.

**A restore Azure can't serve yet is a wait, not a failure.** Before a server's first full backup
exists (4.2 min after it was Ready, in Phase 0), Azure accepts a restore and then fails it with a
bare ``InternalServerError``. Preflight refuses a source with no full backup, and a bout that
still meets one retries with a fresh point until a bound.

The copy is proved owned the way the AWS adapters prove theirs, with the same tag keys the restore
request wrote and the subscription and region the copy lives in, checked against the
installation's Azure scope. Errors that can reach the audience name the product, never a host, an
address or a subscription (see `DataPlaneConnectionRefusedError`).
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import Callable, Mapping, MutableMapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import psycopg

from .azure_arm import ArmClient, ArmError, RestoreNotReady
from .azure_manifest import AzureManifest
from .safe_change import (
    ArtifactInspection,
    SafeChangePlan,
    SafeChangeProvider,
    SafeChangeSqlConnection,
)
from .safe_change_live import (
    _SAFE_CHANGE_MANAGER,
    _TAG_EXPIRY,
    _TAG_MANAGED_BY,
    _TAG_OWNER,
    _TAG_PROVIDER,
    _TAG_RUN_ID,
    _TAG_SOURCE,
    DatabaseCredentials,
    DatabaseTarget,
    DataPlaneConnectionRefusedError,
    PsycopgConnector,
    SafeChangeControlPlaneError,
    SafeChangeLiveConfigurationError,
    Sleeper,
    _fresh_tls_connection,
)

_LABEL = "Azure Database for PostgreSQL"
#: How long a deleted copy's name may stay taken before its lane gives up, and how often it is
#: asked. Azure released one about two minutes after its delete was issued (2026-10-20).
NAME_RELEASE_BOUND_SECONDS = 900.0
NAME_RELEASE_POLL_SECONDS = 10.0
#: The name check timed out at ARM's gateway twice in one run (2026-10-20): it is asked again this
#: often, this far apart, before an unanswered check counts as a name still taken.
NAME_CHECK_ATTEMPTS = 3
NAME_CHECK_RETRY_SECONDS = 5.0
#: A copy in these states is already on its way out, so a held name is waited out, not deleted
#: again: DELETING is `inspect_artifact`'s word for a 404 whose name is taken.
_BEING_DELETED = frozenset({"DELETING", "DROPPING"})
#: How long a restore this App sent may stay out of sight: no server, and its name free. A towel
#: seconds after one was accepted found nothing to delete, the round read READY, and the next
#: bout's restore to the name was refused, `RestoreToFlexServerExists` (bar 3, 2026-10-21). The
#: visibility probe first saw a copy 64 s after its restore was accepted, behind a
#: read ARM held for a minute.
RESTORE_VISIBILITY_SECONDS = 180.0
#: The restores this process sent and hasn't seen a server for yet, by copy name, each with the
#: clock when it was sent. The App passes this one to every adapter it builds: a towel's cleanup
#: runs on an engine of its own.
RESTORES_SENT: dict[str, float] = {}
#: A server Azure is updating still serves, source or copy. A backup holds a Flexible Server in
#: Updating for the whole of it (365 s on Round 5's General Purpose source, 2026-10-20), and a
#: point-in-time restore requested then was accepted and finished. A new copy is updated by the
#: subscription's own policy too: its `deployIfNotExists` wrote a copy's advanced threat protection
#: settings 90 s after its restore, and the copy check that wanted Ready failed a bout (7:42 AM).
#: As on AWS since rc10 (and for RDS copies in `backing-up`), a check must not refuse over the
#: cloud's own housekeeping. A server that truly can't serve still fails, at its connection.
_SERVING_STATES = frozenset({"ready", "updating"})

#: The public roots Azure Database for PostgreSQL's certificates chain to (DigiCert Global Root G2
#: and Microsoft RSA Root Certificate Authority 2017), shipped with the app and pinned by hash.
AZURE_POSTGRES_ROOTS = Path(__file__).resolve().parent / "certs" / "azure-postgres-roots.pem"
AZURE_POSTGRES_ROOTS_SHA256 = "9f8f6519a38fa1a860507c16d5d5850929e5bdf48646c701cd7a3196be6340ce"


def verifying_connector(connector: PsycopgConnector, ca_file: Path) -> PsycopgConnector:
    """`connector`, with the server's certificate and name checked and only SCRAM accepted.

    `_fresh_tls_connection` asks for ``sslmode=require``, which checks no certificate, and libpq
    sends a cleartext password to any server that asks for one. The admin password opens every
    server and every copy, so the Azure lanes verify the chain and the host name against the
    pinned roots and refuse any authentication but SCRAM. A changed bundle is a refusal, not a
    silent downgrade.
    """

    if hashlib.sha256(ca_file.read_bytes()).hexdigest() != AZURE_POSTGRES_ROOTS_SHA256:
        raise SafeChangeLiveConfigurationError(f"{_LABEL} trust roots are not the pinned bundle")
    root = str(ca_file)

    async def connect(**options: Any) -> Any:
        return await connector(**{**options, "sslmode": "verify-full", "sslrootcert": root,
                                  "require_auth": "scram-sha-256"})

    return connect


@dataclass(frozen=True)
class AzureSafeChangeConfig:
    subscription_id: str
    resource_group: str
    #: Azure's programmatic name for the installation's region, e.g. ``westus2``.
    region: str
    source_server: str
    database: str
    admin_login: str
    #: (rule name, IPv4 CIDR): the operator's /32 and the region's Databricks egress prefixes.
    firewall_rules: tuple[tuple[str, str], ...]
    expected_postgres_major: str = "17"
    #: Tags the subscription's own policy requires on every resource, written on each copy as
    #: given (the installation's `AZURE_POLICY_TAGS`).
    policy_tags: tuple[tuple[str, str], ...] = ()
    #: The name of a tag that carries the lease's date (YYYY-MM-DD) on every resource, for a
    #: subscription whose policy expects one (`AZURE_LEASE_DATE_TAG`). None writes no such tag.
    lease_date_tag: str | None = None
    #: The roots a server's certificate must chain to (`verifying_connector`).
    ca_file: Path = AZURE_POSTGRES_ROOTS
    #: How long a bout keeps re-asking for a restore Azure accepted and could not serve yet.
    restore_not_ready_bound_seconds: float = 900.0
    restore_retry_seconds: float = 20.0
    connect_timeout_seconds: float = 10.0
    connection_ready_timeout_seconds: float = 300.0
    poll_interval_seconds: float = 2.0


def config_from_manifest(
    azure: AzureManifest, round_key: Literal["r2", "r3"], *, database: str
) -> AzureSafeChangeConfig:
    """Round 2's or Round 3's lane config, from the installation's sealed Azure section."""

    seal = azure.servers[round_key]
    return AzureSafeChangeConfig(
        subscription_id=azure.subscription_id, resource_group=azure.resource_group,
        region=azure.region, source_server=seal.server, database=database,
        admin_login=seal.admin_login, firewall_rules=azure.firewall_rules(),
        policy_tags=tuple(sorted(azure.policy_tags.items())), lease_date_tag=azure.lease_date_tag)


def _region(location: object) -> str:
    """ARM answers "East US 2"; the scope says "eastus2"."""

    return str(location or "").replace(" ", "").lower()


def _instant() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _put_tag(tags: dict[str, str], name: str, value: str) -> None:
    """Set one tag the way Azure reads it: by name in any case.

    A request naming one tag twice in different cases (``Owner`` and ``owner``) is refused,
    `AmbiguousPolicyEvaluationPaths`, wherever a policy reads that tag (Phase 0, 2026-10-19).
    """

    for existing in [key for key in tags if key.lower() == name.lower()]:
        del tags[existing]
    tags[name] = value


class AzurePostgresSafeChangeAdapter:
    """The `SafeChangeAdapter` for Azure Database for PostgreSQL Flexible Server."""

    provider = SafeChangeProvider.AZURE_POSTGRES
    name = _LABEL
    #: Tags a subclass adds to every copy it makes, and requires on every copy it calls its own
    #: (Round 3 adds its contract tag, as the AWS recovery adapters do).
    extra_tags: Mapping[str, str] = {}

    def __init__(
        self,
        config: AzureSafeChangeConfig,
        *,
        arm: ArmClient,
        password: Callable[[], str],
        connector: PsycopgConnector = psycopg.AsyncConnection.connect,
        sleep: Sleeper = asyncio.sleep,
        clock: Callable[[], float] = time.monotonic,
        restores: MutableMapping[str, float] | None = None,
    ) -> None:
        self.config = config
        self.source_id = config.source_server
        self._arm = arm
        self._password = password
        self._connector = verifying_connector(connector, config.ca_file)
        self._sleep = sleep
        self._clock = clock
        self._restores: MutableMapping[str, float] = {} if restores is None else restores
        self._source_resource_id: str | None = None
        #: The source's availability zone, where its size is known to be offered: the copy is
        #: restored into it (Phase 0, B2s was refused in one westus2 zone and offered in the rest).
        self._source_zone: str | None = None

    async def _call(self, function: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        try:
            return await asyncio.to_thread(function, *args, **kwargs)
        except RestoreNotReady:
            raise
        except ArmError as exc:
            # ArmError messages carry ARM's code and the resource name only; still, a lane error
            # is quoted to the room, so it says what failed and keeps ARM's words for the log.
            raise SafeChangeControlPlaneError(f"{_LABEL} control plane refused a request "
                                              f"({exc.code or exc.status})") from exc

    def _assert_plan(self, plan: SafeChangePlan) -> None:
        scope = plan.scope
        if plan.provider != self.provider or plan.source_id != self.source_id:
            raise SafeChangeLiveConfigurationError(f"{_LABEL} plan is for another lane")
        if (scope.cloud != "azure" or scope.azure_subscription_id != self.config.subscription_id
                or scope.azure_region != self.config.region):
            raise SafeChangeLiveConfigurationError(
                f"{_LABEL} plan scope does not match the installation's subscription and region")

    async def _source(self) -> Mapping[str, Any]:
        server = await self._call(self._arm.get_server, self.config.source_server)
        if server is None:
            raise SafeChangeLiveConfigurationError(f"{_LABEL} source server does not exist")
        properties = server.get("properties") or {}
        state = str(properties.get("state") or "").lower()
        if state not in _SERVING_STATES:
            raise SafeChangeLiveConfigurationError(
                f"{_LABEL} source server is not ready (state {state or 'unknown'})")
        if str(properties.get("version") or "") != self.config.expected_postgres_major:
            raise SafeChangeLiveConfigurationError(f"{_LABEL} source PostgreSQL major changed")
        if _region(server.get("location")) != self.config.region:
            raise SafeChangeLiveConfigurationError(f"{_LABEL} source is in another region")
        if int((properties.get("backup") or {}).get("backupRetentionDays") or 0) < 1:
            raise SafeChangeLiveConfigurationError(
                f"{_LABEL} source has no point-in-time restore window")
        self._source_resource_id = str(server["id"])
        self._source_zone = str(properties.get("availabilityZone") or "") or None
        return server

    async def preflight(self, plan: SafeChangePlan) -> Mapping[str, object]:
        self._assert_plan(plan)
        server = await self._source()
        backups = await self._call(self._arm.list_full_backups, self.config.source_server)
        if not backups:
            raise SafeChangeLiveConfigurationError(
                f"{_LABEL} source has no full backup yet, so it cannot be restored")
        properties = server.get("properties") or {}
        return {
            "capability": "native_pitr_restore",
            "source_server": self.config.source_server,
            "sku": str((server.get("sku") or {}).get("name") or ""),
            "postgres_version": str(properties.get("version") or ""),
            "first_full_backup": backups[0],
            "firewall_rules": len(self.config.firewall_rules),
            "region": self.config.region,
        }

    def _tags(self, plan: SafeChangePlan) -> dict[str, str]:
        # Azure replaces a copy's tags rather than merging them, so whatever the subscription's
        # policy requires is written here too, with the lease's date if a tag carries it. Each
        # later tag wins over an earlier one of the same name in any case: the installation's
        # own last, so a policy tag can't take one over.
        tags: dict[str, str] = {}
        for name, value in self.config.policy_tags:
            _put_tag(tags, name, value)
        if self.config.lease_date_tag:
            _put_tag(tags, self.config.lease_date_tag, plan.scope.expires_at[:10])
        for name, value in {
            _TAG_RUN_ID: plan.scope.run_id,
            _TAG_OWNER: plan.scope.owner,
            _TAG_EXPIRY: plan.scope.expires_at,
            _TAG_SOURCE: plan.source_id,
            _TAG_PROVIDER: plan.provider.value,
            _TAG_MANAGED_BY: _SAFE_CHANGE_MANAGER,
            **self.extra_tags,
        }.items():
            _put_tag(tags, name, value)
        return tags

    def _tags_contradict(self, plan: SafeChangePlan, tags: Mapping[str, str]) -> bool:
        """Whether a copy's tags say it belongs to someone else.

        Ownership rests on the copy's deterministic name (it carries the run ID) in the
        installation's resource group, because Azure's tags can't be read back promptly: a
        restore gives the copy the *source's* tags, and the run's own tags, merged on at once,
        surfaced many minutes later (Phase 0, fourth live run). So a tag that is merely missing
        proves nothing either way; a tag naming a different run, provider or manager is a refusal.
        The source's own ``managed-by = terraform`` is inherited, and allowed.
        """

        run = tags.get(_TAG_RUN_ID)
        provider = tags.get(_TAG_PROVIDER)
        manager = tags.get(_TAG_MANAGED_BY)
        if run and run != plan.scope.run_id:
            return True
        if provider and provider != plan.provider.value:
            return True
        if manager and manager not in (_SAFE_CHANGE_MANAGER, "terraform"):
            return True
        return any(tags.get(key.lower()) not in (None, value)
                   for key, value in self.extra_tags.items())

    async def _name_free(self, name: str) -> bool:
        """Whether Azure says the copy's name is free. An unanswered check is asked again a few
        times, then counts as taken: a name not known to be free is waited out, never restored
        to. Every caller is before the bell."""

        for attempt in range(NAME_CHECK_ATTEMPTS):
            available = await self._call(self._arm.server_name_available, name)
            if available is not None:
                return available
            if attempt < NAME_CHECK_ATTEMPTS - 1:
                await self._sleep(NAME_CHECK_RETRY_SECONDS)
        return False

    def _unseen(self, name: str) -> bool:
        """Whether a restore to `name` was sent less than `RESTORE_VISIBILITY_SECONDS` ago and
        Azure hasn't shown its server since."""

        sent = self._restores.get(name)
        return sent is not None and self._clock() - sent < RESTORE_VISIBILITY_SECONDS

    async def inspect_artifact(self, plan: SafeChangePlan) -> ArtifactInspection | None:
        self._assert_plan(plan)
        server = await self._call(self._arm.get_server, plan.artifact_id)
        if server is None:
            # A copy Azure is still deleting reads 404 while its name stays taken, and a restore
            # to the name is refused until Azure lets it go. The name is the run's deterministic
            # one, so it is this installation's copy, not gone yet. The shared engine then clears
            # it before the bell, as it clears any owned copy it finds at Prepare.
            #
            # A restore sent moments ago shows neither a server nor a taken name, and is on its
            # way all the same: it is this installation's copy too, until Azure shows it or its
            # time to show passes.
            if self._unseen(plan.artifact_id):
                state = "REQUESTED"
            elif await self._name_free(plan.artifact_id):
                self._restores.pop(plan.artifact_id, None)
                return None
            else:
                state = "DELETING"
            return ArtifactInspection(
                artifact_id=plan.artifact_id, provider=self.provider, source_id=plan.source_id,
                run_id=plan.scope.run_id, owner=plan.scope.owner, state=state,
                azure_subscription_id=self.config.subscription_id.lower(),
                azure_region=self.config.region,
                metadata={"endpoint": "", "port": 5432, "restore_type": "pitr", "sku": ""},
            )
        self._restores.pop(plan.artifact_id, None)
        tags = {str(key).lower(): str(value) for key, value in (server.get("tags") or {}).items()}
        # /subscriptions/<id>/resourceGroups/<rg>/providers/...: a copy outside the
        # installation's resource group is not this installation's, whatever its tags say.
        parts = str(server.get("id") or "").split("/")
        subscription = parts[2] if len(parts) > 4 and parts[1].lower() == "subscriptions" else ""
        group = parts[4] if len(parts) > 4 and parts[3].lower() == "resourcegroups" else ""
        in_group = group.lower() == self.config.resource_group.lower()
        owned = in_group and str(server.get("name") or "") == plan.artifact_id and \
            not self._tags_contradict(plan, tags)
        properties = server.get("properties") or {}
        return ArtifactInspection(
            artifact_id=str(server.get("name") or ""),
            provider=self.provider,
            source_id=plan.source_id if owned else tags.get(_TAG_SOURCE, ""),
            run_id=plan.scope.run_id if owned else tags.get(_TAG_RUN_ID, ""),
            owner=plan.scope.owner if owned else "",
            state=str(properties.get("state") or "").upper(),
            azure_subscription_id=subscription.lower() or None,
            azure_region=_region(server.get("location")) or None,
            metadata={
                "endpoint": str(properties.get("fullyQualifiedDomainName") or ""),
                "port": 5432,
                "restore_type": "pitr",
                "sku": str((server.get("sku") or {}).get("name") or ""),
            },
        )

    async def create_isolated(self, plan: SafeChangePlan, report) -> ArtifactInspection:
        return await self._restore_copy(plan, report, point=None)

    async def _restore_copy(
        self, plan: SafeChangePlan, report, *, point: str | None
    ) -> ArtifactInspection:
        """Restore the source to `point` (None: the request's own instant), then open the copy."""

        self._assert_plan(plan)
        source_id = self._source_resource_id
        if source_id is None:  # preflight normally cached it, which makes the restore call first
            source_id = str((await self._source())["id"])
        started = self._clock()
        reported = False
        fixed_point = point
        while True:
            point = fixed_point or _instant()
            # Noted before it's sent: a towel cancels the wait for ARM's answer, not the request.
            self._restores[plan.artifact_id] = self._clock()
            accepted = await self._call(
                self._arm.begin_restore, source_server=self.config.source_server,
                name=plan.artifact_id, point_in_time_utc=point, location=self.config.region,
                tags=self._tags(plan), source_id=source_id,
                availability_zone=self._source_zone)
            if not reported:
                await report(f"{_LABEL} point-in-time restore request accepted",
                             "ARM.FlexibleServers.PointInTimeRestore")
                reported = True
            try:
                await self._call(self._arm.finish_restore, accepted,
                                 source_server=self.config.source_server, name=plan.artifact_id,
                                 point_in_time_utc=point)
                break
            except RestoreNotReady as exc:
                if self._clock() - started >= self.config.restore_not_ready_bound_seconds:
                    raise SafeChangeControlPlaneError(
                        f"{_LABEL} could not serve a point-in-time restore yet") from exc
                await self._sleep(self.config.restore_retry_seconds)
        # The copy arrives wearing the source's tags, not the request's (Phase 0, live run of
        # this adapter), so ownership is merged on before anything else touches it.
        await self._call(self._arm.merge_tags, self._arm.server_path(plan.artifact_id),
                         self._tags(plan))
        await report(f"Creating the copy's {len(self.config.firewall_rules)} firewall rules, one "
                     "at a time (Azure does not copy them)", "ARM.FlexibleServers.FirewallRules")
        await self._call(self._arm.put_firewall_rules, plan.artifact_id, self.config.firewall_rules)
        artifact = await self.inspect_artifact(plan)
        if artifact is None:
            raise SafeChangeControlPlaneError(f"{_LABEL} restore disappeared after creation")
        if not artifact.owner:
            raise SafeChangeControlPlaneError(f"{_LABEL} restore is not an owned copy")
        if artifact.state.lower() not in _SERVING_STATES:
            raise SafeChangeControlPlaneError(
                f"{_LABEL} restore is not a ready copy (state {artifact.state or 'unknown'})")
        return artifact

    async def _connect(self, host: str) -> SafeChangeSqlConnection:
        if not host:
            raise SafeChangeLiveConfigurationError(
                f"{_LABEL} control plane did not return a database endpoint")
        target = DatabaseTarget(host=host, port=5432, database=self.config.database,
                                credentials=DatabaseCredentials(self.config.admin_login,
                                                                self._password()))
        deadline = self._clock() + self.config.connection_ready_timeout_seconds
        while True:
            try:
                return await _fresh_tls_connection(
                    target, connector=self._connector,
                    connect_timeout_seconds=self.config.connect_timeout_seconds, label=_LABEL)
            except DataPlaneConnectionRefusedError as exc:
                sqlstate = exc.sqlstate
                if not (sqlstate is None or sqlstate.startswith("08") or sqlstate == "57P03"):
                    raise
                last: BaseException = exc
            except (OSError, TimeoutError) as exc:
                last = exc
            if self._clock() >= deadline:
                raise SafeChangeControlPlaneError(
                    f"{_LABEL} data-plane endpoint did not become reachable") from last
            await self._sleep(self.config.poll_interval_seconds)

    async def connect_source(self, plan: SafeChangePlan) -> SafeChangeSqlConnection:
        self._assert_plan(plan)
        server = await self._source()
        return await self._connect(
            str((server.get("properties") or {}).get("fullyQualifiedDomainName") or ""))

    async def connect_isolated(
        self, plan: SafeChangePlan, artifact: ArtifactInspection
    ) -> SafeChangeSqlConnection:
        current = await self.inspect_artifact(plan)
        if current is None or not current.owner or current.artifact_id != artifact.artifact_id:
            raise SafeChangeLiveConfigurationError(f"{_LABEL} copy is no longer the owned one")
        return await self._connect(str(current.metadata.get("endpoint") or ""))

    async def delete_isolated(
        self, plan: SafeChangePlan, artifact: ArtifactInspection, report
    ) -> None:
        current = await self.inspect_artifact(plan)
        if current is None:
            return
        if not current.owner or current.run_id != plan.scope.run_id:
            raise SafeChangeLiveConfigurationError(
                f"{_LABEL} refused to delete a copy this installation does not own")
        await report(f"Deleting the owned {_LABEL} copy", "ARM.FlexibleServers.Delete")
        await self._call(self._arm.delete_server, plan.artifact_id)
        # Gone means its name is free again, and no restore sent to it is still to show: until
        # then the next restore to it is refused.
        deadline = self._clock() + NAME_RELEASE_BOUND_SECONDS
        reported = False
        while self._unseen(plan.artifact_id) or not await self._name_free(plan.artifact_id):
            if self._clock() >= deadline:
                raise SafeChangeControlPlaneError(
                    f"Azure still held the {_LABEL} copy's name {NAME_RELEASE_BOUND_SECONDS:.0f} s "
                    "after its delete")
            if not reported:
                await report(f"Waiting for Azure to release the {_LABEL} copy's name",
                             "ARM.FlexibleServers.CheckNameAvailability")
                reported = True
            await self._sleep(NAME_RELEASE_POLL_SECONDS)
            # A copy still being created holds its name and reads 404 too, for a minute or more
            # after its restore was accepted (the visibility probe, 2026-10-20: 404 at 64 s,
            # Provisioning at 67 s). A delete sent then finds nothing, so the copy is looked up
            # again while its name is held, and deleted once Azure shows it.
            if await self._call(self._arm.get_server, plan.artifact_id) is None:
                continue
            shown = await self.inspect_artifact(plan)
            if shown is not None and shown.state not in _BEING_DELETED:
                if not shown.owner or shown.run_id != plan.scope.run_id:
                    raise SafeChangeLiveConfigurationError(
                        f"{_LABEL} refused to delete a copy this installation does not own")
                await report(f"The {_LABEL} copy was still being created: deleting it now",
                             "ARM.FlexibleServers.Delete")
                # Inside this wait's own bound: `delete_server`'s default is twice as long.
                await self._call(
                    self._arm.delete_server, plan.artifact_id,
                    bound_seconds=max(deadline - self._clock(), NAME_RELEASE_POLL_SECONDS))

    async def abandon_isolated(self, plan: SafeChangePlan) -> None:
        """Issue the delete of a cancelled lane's copy and return at once."""

        current = await self.inspect_artifact(plan)
        if current is not None and current.owner and current.run_id == plan.scope.run_id:
            await self._call(self._arm.begin_delete_server, plan.artifact_id)
