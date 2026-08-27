from __future__ import annotations

import asyncio
import base64
import gzip
import hashlib
import io
import ipaddress
import json
import os
import re
import secrets
import shutil
import subprocess
import tarfile
import threading
import time
import urllib.request
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote
from uuid import uuid4

import boto3
import psycopg
from botocore.exceptions import ClientError
from psycopg import sql

from .aws_auth import (
    AwsAuthMode,
    select_setup_auth,
    selected_subprocess_environment,
    session_arguments,
    validate_runtime_auth,
)
from .capacity import (
    LAKEBASE_MAX_CU,
    LAKEBASE_MIN_CU,
    LAKEBASE_SUSPEND_SECONDS,
    capacity_parity,
    rds_lane_is_scored,
)
from .manifest import (
    PROJECT_ROOT,
    AuroraEnvironmentSeal,
    AwsManifest,
    AwsResources,
    DatabricksManifest,
    DemoManifest,
    LakebaseEnvironmentSeal,
    RdsEnvironmentSeal,
    Round4Resources,
    Round5FrozenConstants,
    Round5OwnershipTags,
    Round5Resources,
    RoundEnvironmentSeal,
    apply_manifest_environment,
    load_manifest,
    manifest_path,
    save_manifest,
)
from .model_score import ModelScoreContract, ModelScoreRow, is_owned_prior_proof
from .model_score_live import (
    SYNCED_TABLE_FAILED_STATES,
    SYNCED_TABLE_HEALTHY_STATES,
    latest_pipeline_update_state,
    synced_table_failure_is_a_stopped_pipeline,
)
from .models import RoundId
from .reconcile import (
    ORPHAN_EPHEMERAL,
    PRESENCE_MISSING,
    PRESENCE_UNVERIFIED,
    TAG_RUN_ID,
    InstallationPresence,
    ReconciliationReport,
    presence_from_report,
    reconcile_live,
)
from .targets import (
    AuroraCredentialProvider,
    ConnectionMaterial,
    LakebaseCredentialProvider,
    RdsCredentialProvider,
    TargetConfigurationError,
    TargetNotArmedError,
    lakebase_region_from_host,
)

AWS_INFRA_DIR = PROJECT_ROOT / "infra" / "aws"
BASE_SCHEMA_PATHS = (
    PROJECT_ROOT / "sql" / "001_probe.sql",
    PROJECT_ROOT / "sql" / "002_orders_base.sql",
)
ANTI_DEMO_RUNTIME_PRINCIPALS_ENV = "ANTI_DEMO_RUNTIME_PRINCIPAL_ARNS"
#: Fixed, and must equal `var.anti_demo_runtime_role_name`'s default. The name is
#: deliberately not generated: the operator's `~/.aws/config` carries this ARN in
#: a `role_arn` key, and a per-install suffix would mean editing that file after
#: every sweep -- the recurring manual step this role exists to remove.
ANTI_DEMO_RUNTIME_ROLE_NAME = "anti-demo-runtime"
#: The bare unique principal ID IAM leaves behind in a trust policy once the
#: principal it named has been deleted. While the principal exists IAM reverse-
#: maps the ID back to an ARN, so seeing one of these in a live trust document is
#: the *only* observable signature of the delete-and-recreate break: a recreated
#: IAM user has an identical name and an identical ARN, so every comparison that
#: works on names -- including `principal_matches` -- reports it as healthy.
_IAM_UNIQUE_PRINCIPAL_ID = re.compile(r"^A(?:ID|RO|NPA|GPA|IPA|NVA|SIA)A[A-Z2-7]{4,}$")
ROUND4_CATALOG_ENV = "ROUND4_CATALOG"
# Only a first provision reads this; afterwards the manifest's seal outranks it,
# so an installation provisioned into some other catalog keeps working unchanged.
#
# `main` is the catalog Databricks itself creates when a workspace is enabled for
# Unity Catalog, which makes it the only name with a real chance of existing in a
# stranger's workspace -- it is a *likely* default, not a guaranteed one. Round 4
# deliberately does not create the catalog if it is absent: cleanup deletes the
# three schemas it made (`_delete_round4_uc_artifacts`) and has no catalog delete,
# so a created catalog would be an orphan nothing reaps. When `main` is absent or
# invisible the operator is told to set ROUND4_CATALOG, before any UC write.
#
# bootstrap.sh parses this assignment with `sed` to run the same resolution at
# zero spend, so the line must stay a plain `NAME = "value"` with a non-empty value.
ROUND4_DEFAULT_CATALOG = "main"
ROUND4_DATABASE = "anti_demo"
ROUND4_SOURCE_TABLE = "model_scores_source"
ROUND4_SYNCED_TABLE = "model_scores"
ROUND4_BASELINE_ENTITY_ID = "customer-0001"
ROUND4_BASELINE_SCORE = 0.25
ROUND4_BASELINE_MODEL_VERSION = "risk-v0"
ROUND4_BASELINE_PROOF_NONCE = "round4-baseline"
ROUND5_NATIVE_ROLE = "anti_demo_burst"
ROUND5_PROBE_IDENTITY = "public.anti_demo_probe"
ROUND5_JOURNAL_TABLE = "anti_demo_coordination.round5_creation_journal"
ROUND5_LEGACY_PARTIAL_ADDRESSES = {
    "aws_db_parameter_group.rds_round5",
    "aws_secretsmanager_secret.round5_lakebase_credentials",
    "aws_secretsmanager_secret.round5_rds_credentials",
    "aws_security_group.round5_proxy",
    "aws_vpc_security_group_egress_rule.round5_proxy_to_rds",
    "aws_vpc_security_group_ingress_rule.round5_runner_to_proxy",
}
ROUND5_LEGACY_REFUSED_ADDRESSES = {
    "aws_db_proxy.round5",
    "aws_db_proxy_default_target_group.round5",
    "aws_db_proxy_target.round5",
    "aws_iam_role.round5_proxy",
    "aws_iam_role_policy.round5_proxy_secret",
}
ROUND5_LEGACY_DYNAMIC_ADDRESSES = {
    "aws_iam_policy.round5_per_bout_role_boundary",
}

_ROUND_NUMBER_IDS = dict(enumerate(tuple(RoundId), start=1))
_V7_LAKEBASE_SUFFIXES = {round_id: f"r{number}" for number, round_id in _ROUND_NUMBER_IDS.items()}
_V7_COORDINATION_SUFFIX = "coord"


@dataclass(frozen=True)
class Check:
    name: str
    ok: bool
    detail: str
    # An advisory check reports but does not decide. It still prints its own
    # line, so the finding stays visible; it just cannot fail `antidemo doctor` or
    # `antidemo setup`. Aggregation lives in `checks_passed` (cli) and `setup`.
    advisory: bool = False


@dataclass(frozen=True)
class _LakebaseBinding:
    project_id: str
    endpoint_name: str


def _v7_lakebase_project_id(manifest: DemoManifest, suffix: str) -> str:
    installation_id = manifest.installation_id
    if not installation_id:
        raise RuntimeError("The per-round Lakebase installation ID is missing")
    compact = installation_id.replace("-", "").casefold()
    return f"anti-demo-{compact}-{suffix}"


def _round_lakebase_binding(manifest: DemoManifest, round_id: RoundId | int) -> _LakebaseBinding:
    if manifest.round_environments is not None:
        sealed = manifest.round_lakebase(round_id)
        return _LakebaseBinding(sealed.project_id, sealed.endpoint_name)
    if manifest.installation_id is not None:
        canonical = _ROUND_NUMBER_IDS[round_id] if isinstance(round_id, int) else round_id
        project_id = _v7_lakebase_project_id(manifest, _V7_LAKEBASE_SUFFIXES[canonical])
        return _LakebaseBinding(
            project_id,
            f"projects/{project_id}/branches/production/endpoints/primary",
        )
    return _LakebaseBinding(
        manifest.databricks.project_id,
        manifest.databricks.endpoint_name,
    )


def _coordination_lakebase_binding(manifest: DemoManifest) -> _LakebaseBinding:
    sealed = manifest.coordination_lakebase
    if sealed is not None:
        return _LakebaseBinding(sealed.project_id, sealed.endpoint_name)
    if manifest.installation_id is not None:
        project_id = _v7_lakebase_project_id(manifest, _V7_COORDINATION_SUFFIX)
        return _LakebaseBinding(
            project_id,
            f"projects/{project_id}/branches/production/endpoints/primary",
        )
    return _LakebaseBinding(
        manifest.databricks.project_id,
        f"projects/{manifest.databricks.project_id}/branches/coordination/endpoints/primary",
    )


def _round_lakebase_provider(
    manifest: DemoManifest,
    round_id: RoundId | int,
    *,
    database: str | None = None,
) -> LakebaseCredentialProvider:
    binding = _round_lakebase_binding(manifest, round_id)
    return LakebaseCredentialProvider(
        endpoint=binding.endpoint_name,
        profile=manifest.databricks.profile,
        database=database or manifest.databricks.database,
        user=manifest.databricks.user,
        expected_region=manifest.aws.region,
    )


def _round_aurora_provider(
    manifest: DemoManifest, round_id: RoundId | int
) -> AuroraCredentialProvider:
    provider = AuroraCredentialProvider()
    if manifest.round_environments is not None:
        environment = manifest.round_environment(round_id)
        if environment.aurora is None:
            raise RuntimeError(f"Round {round_id} has no sealed Aurora environment")
        provider.cluster_id = environment.aurora.cluster_id
        provider.secret_arn = environment.aurora.secret_arn
    provider.database = manifest.databricks.database
    return provider


def _round_rds_provider(manifest: DemoManifest, round_id: RoundId | int) -> RdsCredentialProvider:
    resolved = round_id if isinstance(round_id, RoundId) else _ROUND_NUMBER_IDS[round_id]
    provider = RdsCredentialProvider(round_id=resolved)
    provider.database = manifest.databricks.database
    if manifest.round_environments is not None:
        environment = manifest.round_environment(round_id)
        if environment.rds is None:
            # Not an error. An unscored round seals no RDS instance because
            # Terraform stands none up; the provider still answers, structurally,
            # that RDS cannot scale to zero.
            if rds_lane_is_scored(resolved):
                raise RuntimeError(f"Round {round_id} has no sealed RDS environment")
            return provider
        provider.instance_id = environment.rds.instance_id
        provider.secret_arn = environment.rds.secret_arn
    return provider


def _safe_failure(result: subprocess.CompletedProcess[str]) -> RuntimeError:
    lines = (result.stderr or result.stdout or "command failed").strip().splitlines()
    return RuntimeError(lines[-1] if lines else "command failed")


def _run(
    arguments: list[str],
    *,
    timeout: float = 1200,
    capture: bool = False,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        arguments,
        cwd=PROJECT_ROOT,
        capture_output=capture,
        text=True,
        check=False,
        timeout=timeout,
        env=env,
    )
    if result.returncode != 0:
        raise _safe_failure(result)
    return result


def _run_json(
    arguments: list[str], *, timeout: float = 120, env: dict[str, str] | None = None
) -> dict[str, Any]:
    result = _run(arguments, timeout=timeout, capture=True, env=env)
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError("A control-plane command returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("A control-plane command returned an unexpected JSON shape")
    return payload


def _utc_tag(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _expiry_check(manifest: DemoManifest) -> Check:
    """Report the TTL as advisory: loudly, but without failing the run.

    A passed TTL is worth saying on every doctor run, and the detail carries both
    remedies so the line is actionable rather than just red. It is not a fault in
    the environment, though: nothing reaps the tag, the resources are unaffected by
    it, and making it blocking is what stopped `antidemo setup` from repairing an
    otherwise healthy installation. The honest cost of this is recorded in README:
    an abandoned install no longer announces itself through a failing check.
    """
    expiry_ok = manifest.expires_at > datetime.now(UTC)
    return Check(
        "expiry",
        expiry_ok,
        manifest.expires_at.isoformat()
        if expiry_ok
        else (
            f"{manifest.expires_at.isoformat()} HAS PASSED · nothing reaps this tag · "
            "run 'antidemo renew --ttl-hours N' to move it forward or "
            "'antidemo cleanup --yes' to stop the spend"
        ),
        advisory=True,
    )


def _warn_if_expired(manifest: DemoManifest) -> None:
    """Report a passed TTL on a control path instead of refusing to run.

    Every caller here is doing setup, repair, or reconciliation. An expired
    `expires_at` is a wall-clock comparison against a value written once at
    provision time and never advanced, so it cannot distinguish "abandoned" from
    "in daily use"; refusing on it turned a stale timestamp into an outage and
    left no recovery except teardown. The checks that follow each call site ask
    the resources themselves. Cleanup paths deliberately never consulted expiry
    and still do not.
    """
    warning = manifest.expiry_warning()
    if warning is not None:
        print(f"WARN  {warning}", flush=True)


def _schema_sha256() -> str:
    digest = hashlib.sha256()
    for path in BASE_SCHEMA_PATHS:
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _new_run_id() -> str:
    stamp = datetime.now(UTC).strftime("%Y%m%d-%H%M")
    return f"ad-{stamp}-{secrets.token_hex(2)}"


def detect_operator_cidr(*, timeout_seconds: float = 10.0) -> str:
    """Ask AWS which address it sees this host as, and express it as a /32.

    `timeout_seconds` exists for the runtime drift probe below, which runs beside
    a demo and must not sit on a ten-second socket. Every mutator keeps the
    original default: a provision or a repair is allowed to wait.
    """
    with urllib.request.urlopen(
        "https://checkip.amazonaws.com", timeout=timeout_seconds
    ) as response:
        raw = response.read(128).decode("ascii").strip()
    address = ipaddress.ip_address(raw)
    if address.version != 4:
        raise RuntimeError("Round 1 currently requires an operator public IPv4 address")
    return f"{address}/32"


def _validate_operator_cidr(value: str) -> str:
    network = ipaddress.ip_network(value, strict=True)
    if network.version != 4 or network.prefixlen != 32:
        raise RuntimeError("operator CIDR must be one explicit public IPv4 /32")
    return str(network)


# ---------------------------------------------------------------------------
# The deployed app's own network path, sealed beside the operator's.
# ---------------------------------------------------------------------------
#
# Rounds 1, 2, 3 and 5 all race a live Aurora or RDS opponent over TCP 5432, and
# until this existed every one of those security groups admitted exactly one
# address: the laptop that provisioned the install. That is why the deployed app
# could not run them -- not a private-network problem, because all seven database
# lanes are sealed `publicly_accessible = true` and `_rds_ingress` asserts it as a
# *correctness* condition, but an allowlist with one entry in it.
#
# Databricks publishes the serverless egress prefixes its apps actually leave
# from. Filtered to this installation's region there are four of them totalling
# 401 addresses. The belief that the only option was a /16 of general-purpose EC2
# -- 65,536 addresses, refused outright as a boundary in front of a live database
# -- was true of *AWS's* published ranges and false of Databricks', and Databricks
# publishes its own.
#
# Two properties this must keep, both of which shape the code below more than the
# feature does:
#
# *   **The prefixes never enter this repository.** They are globally routable,
#     and `tests/test_no_live_identifiers_committed.py` refuses a routable IPv4
#     literal in any tracked or untracked file. So there is no Terraform default,
#     no Python constant and no test fixture holding them: they are fetched at
#     reconcile time and sealed into a gitignored manifest. That constraint and
#     the correct design happen to agree -- a hardcoded list would go stale
#     silently, and this one cannot.
# *   **The seal stays exact.** `_postgres_ingress_is_exact` still requires the
#     ingress set to equal precisely what this installation sealed and nothing
#     else. What changed is the size of the sealed set, not the strictness of the
#     comparison; a hand-added rule is still refused and Terraform still revokes
#     it.

#: Databricks' own published feed. The 2026-05-25 decommission retired the legacy
#: per-NCC stable IP list readable from the account console and the Network
#: Connectivity API; this is its documented replacement and the supported method
#: for retrieving these addresses.
SERVERLESS_EGRESS_FEED_URL = "https://www.databricks.com/networking/v1/ip-ranges.json"

#: The feed rows this installation is entitled to admit. `service` and `type`
#: matter as much as the region: an `inbound` row is where Databricks *arrives
#: from*, which is not a path anything here takes, and admitting it would widen
#: the allowlist for nothing.
SERVERLESS_EGRESS_FEED_SERVICE = "Databricks"
SERVERLESS_EGRESS_FEED_PLATFORM = "aws"
SERVERLESS_EGRESS_FEED_TYPE = "outbound"

#: A provision or a repair may wait; nothing on a serving path fetches this.
SERVERLESS_EGRESS_FETCH_TIMEOUT_SECONDS = 15.0

#: The feed is small -- a couple of hundred rows -- and a body far larger than
#: that is a redirect to something else, not the document this parses.
SERVERLESS_EGRESS_FEED_MAX_BYTES = 4 * 1024 * 1024

#: How old a sealed snapshot may be before the re-poll obligation is reported.
#: Databricks publishes as often as every 30 days and new addresses go live 60
#: days after publication, so 30 days leaves two full cycles of slack -- but only
#: if something is watching, which is the entire reason `ServerlessEgressDrift`
#: exists rather than a comment saying "remember to re-run setup".
SERVERLESS_EGRESS_REFRESH_SECONDS = 30 * 24 * 60 * 60


def _validate_serverless_egress_cidrs(values: Sequence[str]) -> tuple[str, ...]:
    """The list's own rule, which is not the operator CIDR's rule.

    `_validate_operator_cidr` above is untouched and still seals one laptop at one
    `/32`. This is a second, weaker claim about a different thing -- published
    provider egress prefixes -- so it gets its own validator rather than widening
    that one's contract. The substance lives on the manifest field, which is where
    the seal is and where `save_manifest` re-validates it; this is the entry point
    a mutator calls before assigning, so a bad fetch is refused before it reaches
    the model rather than at the write.
    """

    normalised = tuple(str(ipaddress.ip_network(value, strict=True)) for value in values)
    AwsManifest.require_narrow_serverless_egress(normalised)
    return normalised


def fetch_serverless_egress_cidrs(
    region: str,
    *,
    timeout_seconds: float = SERVERLESS_EGRESS_FETCH_TIMEOUT_SECONDS,
) -> tuple[tuple[str, ...], int]:
    """The published outbound prefixes for `region`, and the snapshot's timestamp.

    Returns them together because they are one seal: a list that cannot be aged is
    a list nobody will ever re-poll.

    Raises rather than returning an empty tuple when the feed answers but names no
    prefix for the region. An empty allowlist and "this region publishes nothing"
    are different facts, and quietly sealing the first as though it were the
    second would silently un-admit the deployed app on the next reconcile.
    """

    with urllib.request.urlopen(SERVERLESS_EGRESS_FEED_URL, timeout=timeout_seconds) as response:
        document = json.loads(response.read(SERVERLESS_EGRESS_FEED_MAX_BYTES).decode("utf-8"))
    published_at = int(document["timestampSeconds"])
    prefixes: list[str] = []
    for row in document.get("prefixes") or []:
        if (
            row.get("platform") == SERVERLESS_EGRESS_FEED_PLATFORM
            and row.get("region") == region
            and row.get("service") == SERVERLESS_EGRESS_FEED_SERVICE
            and row.get("type") == SERVERLESS_EGRESS_FEED_TYPE
        ):
            # One row carries every prefix for a (platform, region, service,
            # type) tuple, so this is a list per row rather than a row per
            # prefix. Reading it as the latter finds nothing and reports it as
            # "the region publishes none", which is the failure this shape note
            # exists to stop somebody re-introducing.
            prefixes.extend(str(item) for item in row.get("ipv4Prefixes") or [])
    if not prefixes:
        raise RuntimeError(
            f"The Databricks serverless egress feed published no outbound IPv4 prefix for "
            f"{region}. Refusing to seal an empty allowlist, which would read as though the "
            f"deployed app had been deliberately shut out."
        )
    return _validate_serverless_egress_cidrs(sorted(set(prefixes))), published_at


@dataclass(frozen=True)
class ServerlessEgressDrift:
    """The sealed app allowance is old enough that nobody has re-polled the feed.

    Deliberately an *age* rather than a comparison against a freshly fetched feed,
    which is what the design asked for. Three reasons, and the third is the one
    that decided it:

    *   `/readyz` already pays for two blocking socket reads per TTL. A third,
        against a third-party CDN, on the endpoint a monitor hits, buys a signal
        an operator would not act on differently.
    *   A comparison needs the feed to be reachable, so it goes quiet exactly when
        the network is unhealthy. An age cannot.
    *   The repair is identical either way -- `./antidemo setup` re-fetches and
        re-seals -- so "the feed moved" and "nobody has looked in a month" send an
        operator to the same command. The second is answerable for free.

    Carries `detail` and `capabilities` because that is the shape `/readyz`
    consumes, so this reaches the health surface through the seam
    `operator_ingress_drift` already occupies rather than needing a new one.
    """

    sealed_age_days: int
    prefix_count: int

    @property
    def detail(self) -> str:
        return (
            f"THE DEPLOYED APP'S SEALED NETWORK PATH HAS NOT BEEN RE-POLLED: this "
            f"installation admits {self.prefix_count} published Databricks "
            f"serverless egress prefixes to its Aurora and RDS security groups, and "
            f"the snapshot they came from is {self.sealed_age_days} days old. "
            f"Databricks republishes as often as every 30 days, with new addresses "
            f"live 60 days after publication, so a prefix the deployed app starts "
            f"leaving from may not be admitted yet. Nothing is broken right now. "
            f"Run '{OPERATOR_INGRESS_REPAIR_COMMAND}', which re-fetches the feed, "
            f"reseals the list and re-applies the security groups. Nothing "
            f"re-polls this from inside the server, by design."
        )

    @property
    def capabilities(self) -> tuple[str, ...]:
        return (
            f"assurance that the deployed app can still reach Aurora and RDS -- the "
            f"admitted prefixes were published {self.sealed_age_days} days ago and "
            f"have not been re-polled since",
        )


@dataclass(frozen=True)
class DeployedAwsPosture:
    """What this installation has sealed for the deployed app, read from a cache.

    One record rather than three accessors because every consumer wants it at the
    same moment and a manifest read per catalog render is the cost this is
    avoiding. `server.api._availability_signals` reads it to decide whether the
    two deployed refusals still apply, and `operator_ingress_drift` reads it to
    decide whether to report the re-poll obligation.
    """

    egress_prefix_count: int = 0
    egress_published_at: int | None = None
    runtime_role_sealed: bool = False

    @property
    def egress_sealed(self) -> bool:
        return self.egress_prefix_count > 0

    def drift(self, *, now: float) -> ServerlessEgressDrift | None:
        if not self.egress_sealed or self.egress_published_at is None:
            return None
        age = now - float(self.egress_published_at)
        if age < SERVERLESS_EGRESS_REFRESH_SECONDS:
            return None
        return ServerlessEgressDrift(
            sealed_age_days=int(age // 86400),
            prefix_count=self.egress_prefix_count,
        )


#: Monotonic expiry paired with the posture it belongs to. Same TTL as the
#: operator ingress verdict, and for the same reason: a seal only moves when a
#: mutator moves it, but a surface that caches it for the life of the process
#: would keep refusing rounds after `antidemo setup` had already fixed them.
_DEPLOYED_AWS_POSTURE: tuple[float, DeployedAwsPosture] = (0.0, DeployedAwsPosture())


def reset_deployed_aws_posture_cache() -> None:
    """Forget the cached posture. For tests, and after a reseal."""
    global _DEPLOYED_AWS_POSTURE
    _DEPLOYED_AWS_POSTURE = (0.0, DeployedAwsPosture())


def deployed_aws_posture(*, manifest: DemoManifest | None = None) -> DeployedAwsPosture:
    """What is sealed for the deployed app. Free to call, and it cannot raise.

    An unreadable manifest resolves to "nothing is sealed", which is the honest
    answer and also the conservative one: the refusals this feeds can only ever
    *take* readiness away, so the failure mode is a round reported unavailable
    rather than a round shown green that dies at the bell.
    """

    global _DEPLOYED_AWS_POSTURE
    moment = time.monotonic()
    expires_at, cached = _DEPLOYED_AWS_POSTURE
    if moment < expires_at:
        return cached
    try:
        aws = (manifest or load_manifest()).aws
        posture = DeployedAwsPosture(
            egress_prefix_count=len(aws.serverless_egress_cidrs or ()),
            egress_published_at=aws.serverless_egress_published_at,
            runtime_role_sealed=aws.runtime_role_arn is not None,
        )
        ttl = OPERATOR_INGRESS_TTL_SECONDS
    except Exception:
        posture = DeployedAwsPosture()
        ttl = OPERATOR_INGRESS_FAILURE_TTL_SECONDS
    _DEPLOYED_AWS_POSTURE = (moment + ttl, posture)
    return posture


# ---------------------------------------------------------------------------
# Operator ingress drift: detected at runtime, repaired only on command.
# ---------------------------------------------------------------------------
#
# `_refresh_operator_cidr` runs in exactly one place -- `reconcile_infrastructure`,
# so only during `antidemo setup`. Nothing looked at it again afterwards. A laptop
# that changes address between setups (a DHCP lease, a VPN toggle, a different
# network) leaves the AWS security groups allowing an address nobody holds, and
# every round that connects directly to Aurora or RDS starts timing out with
# nothing on screen saying why. On an install meant to stay up for days that is
# the likely silent killer.
#
# Detection is therefore made available to the serving process, and repair is
# deliberately not. The serving process must never mutate: a wrong inference that
# rewrote a security group, reseeded, or wrote the manifest would corrupt a live
# measurement or spend real money, and `antidemo serve` releases the mutation lock
# before serving precisely so it is not a holder. So this observes, caches, and
# names the command an operator runs. The repair already exists in a mutator that
# takes the lock -- `antidemo setup` -> `reconcile_infrastructure` ->
# `_refresh_operator_cidr` -> apply -- and stays the only thing that performs it.

#: The command that actually rebinds the allowance.
OPERATOR_INGRESS_REPAIR_COMMAND = "./antidemo setup"

#: How long one verdict is trusted. The probe is a network round trip, so it must
#: not happen per request; five minutes bounds it to a handful of calls an hour
#: while still noticing a mid-session network change well inside a demo.
OPERATOR_INGRESS_TTL_SECONDS = 300.0

#: A failed or unusable probe is cached too, and for less time. Caching it stops
#: an offline laptop from retrying on every request; keeping it short means the
#: signal comes back promptly once the network does.
OPERATOR_INGRESS_FAILURE_TTL_SECONDS = 30.0

#: Short on purpose: see `detect_operator_cidr`.
OPERATOR_INGRESS_PROBE_TIMEOUT_SECONDS = 3.0


@dataclass(frozen=True)
class OperatorIngressDrift:
    """The sealed database allowance no longer matches this host's address."""

    sealed_cidr: str
    observed_cidr: str

    @property
    def detail(self) -> str:
        return (
            f"OPERATOR INGRESS IS STALE: the AWS security groups allow "
            f"{self.sealed_cidr}, but this host's public address is now "
            f"{self.observed_cidr}. Every round that connects directly to Aurora or "
            f"RDS will fail to connect until the allowance is rebound. Run "
            f"'{OPERATOR_INGRESS_REPAIR_COMMAND}', which re-detects the address, "
            f"rewrites the manifest and re-applies the security groups. Nothing "
            f"repairs this from inside the server, by design."
        )

    @property
    def capabilities(self) -> tuple[str, ...]:
        """What is lost while this stands, in the vocabulary /readyz uses."""
        return (
            f"direct Aurora and RDS connectivity -- the security groups allow "
            f"{self.sealed_cidr} and this host is {self.observed_cidr}, so every "
            f"round that opens one of those connections fails",
        )


#: What the serving process may find wrong with the sealed database allowance.
#: Two findings, one seam, because `/readyz` reads whatever comes back for its
#: `detail` and `capabilities` and has one slot for either.
IngressDrift = OperatorIngressDrift | ServerlessEgressDrift

#: Monotonic expiry paired with the verdict it belongs to.
_OPERATOR_INGRESS_VERDICT: tuple[float, OperatorIngressDrift | None] = (0.0, None)


def reset_operator_ingress_cache() -> None:
    """Forget the cached verdict. For tests, and after a repair."""
    global _OPERATOR_INGRESS_VERDICT
    _OPERATOR_INGRESS_VERDICT = (0.0, None)


def _observe_operator_ingress(
    manifest: DemoManifest | None,
) -> tuple[OperatorIngressDrift | None, float]:
    """One uncached observation, and how long to trust it.

    Never raises, and never claims drift it did not establish. Three cases return
    "no drift" rather than a guess, because a false positive here tells an
    operator to re-apply Terraform for no reason:

    *   The probe failed -- offline, DNS, a captive portal, a timeout.
    *   The probe answered with something that is not a public IPv4 address, which
        is what an IPv6-only network does. `detect_operator_cidr` refuses it, and
        so does this.
    *   The manifest is unreadable or seals no CIDR.

    A NAT or a corporate egress is not a false positive: the sealed value was
    produced by this same probe at provision time, so both sides are the address
    AWS actually sees. That is the address the security group has to name, so
    comparing like with like is what makes the answer meaningful.

    The question is "is this host admitted", not "is this host the operator". It
    used to be able to conflate the two because the sealed set had one member. It
    no longer does: the deployed app leaves from a published Databricks egress
    prefix, which an installation that seals that list *admits*, so comparing the
    app's address against `operator_cidr` alone reports the app's normal,
    working, deliberately-allowed address as a fault -- on the endpoint the round
    list consults, which would take four rounds off the card for being reachable.
    """
    try:
        aws = (manifest or load_manifest()).aws
    except Exception:
        return None, OPERATOR_INGRESS_FAILURE_TTL_SECONDS
    sealed = (aws.operator_cidr or "").strip()
    if not sealed:
        return None, OPERATOR_INGRESS_TTL_SECONDS
    try:
        observed = detect_operator_cidr(timeout_seconds=OPERATOR_INGRESS_PROBE_TIMEOUT_SECONDS)
    except Exception:
        return None, OPERATOR_INGRESS_FAILURE_TTL_SECONDS
    if observed == sealed:
        return None, OPERATOR_INGRESS_TTL_SECONDS
    # `getattr` rather than a plain attribute read: this function promises never
    # to raise, and it is reached from /readyz with whatever manifest the caller
    # holds -- including the stubs that predate this field.
    if _sealed_ingress_admits(observed, getattr(aws, "serverless_egress_cidrs", None) or ()):
        return None, OPERATOR_INGRESS_TTL_SECONDS
    drift = OperatorIngressDrift(sealed_cidr=sealed, observed_cidr=observed)
    return drift, OPERATOR_INGRESS_TTL_SECONDS


def _sealed_ingress_admits(observed: str, egress_cidrs: Sequence[str]) -> bool:
    """Is this host's address inside one of the sealed app prefixes?

    Containment rather than equality, because that is what a security group
    does. The operator's own entry is a `/32` and compares exactly; these are
    ranges, and the whole reason they are sealed is that the address inside them
    varies from one app restart to the next.

    Never raises. A malformed value here must not be able to take down the
    endpoint that reports on it -- the seal is validated where it is written.
    """
    try:
        host = ipaddress.ip_network(observed, strict=False).network_address
        return any(
            host in ipaddress.ip_network(cidr, strict=False) for cidr in egress_cidrs
        )
    except ValueError:
        return False


def operator_ingress_drift(
    *, manifest: DemoManifest | None = None
) -> IngressDrift | None:
    """Whether the sealed database allowance still matches reality.

    Two findings come out of here, and they are reported through one seam
    because `/readyz` consumes this by duck type -- `detail` and `capabilities`
    -- and has exactly one detail slot to give either of them.

    The operator's own address wins when both stand. It is the acute one: rounds
    are failing *now*, and the room is watching them fail. A stale egress
    snapshot breaks nothing yet, which is precisely why it needs somewhere to be
    seen -- an obligation nobody can see is an obligation nobody meets.

    Safe to call from anywhere, including a request path: at most one manifest
    read and one short network probe per `OPERATOR_INGRESS_TTL_SECONDS`, and it
    cannot raise. Reads only -- nothing here writes a manifest, touches AWS, or
    takes the generation lock. The second finding adds no socket at all; see
    `ServerlessEgressDrift`.

    Pass `manifest` when the caller already holds one. Omitting it re-reads the
    seal on each refresh, which is what lets the signal clear itself after a
    repair moves the sealed value instead of reporting drift until a restart.
    """
    global _OPERATOR_INGRESS_VERDICT
    moment = time.monotonic()
    expires_at, cached = _OPERATOR_INGRESS_VERDICT
    if moment >= expires_at:
        cached, ttl = _observe_operator_ingress(manifest)
        _OPERATOR_INGRESS_VERDICT = (moment + ttl, cached)
    if cached is not None:
        return cached
    return deployed_aws_posture(manifest=manifest).drift(now=time.time())


async def operator_ingress_drift_async(
    *, manifest: DemoManifest | None = None
) -> IngressDrift | None:
    """`operator_ingress_drift` off the event loop.

    The probe is a blocking socket read. Called directly from a coroutine it
    would stall every other request on this worker for up to
    `OPERATOR_INGRESS_PROBE_TIMEOUT_SECONDS` -- including the SSE streams a bout
    is riding on.
    """
    return await asyncio.to_thread(operator_ingress_drift, manifest=manifest)


def _operator_cidr_check(manifest: DemoManifest) -> Check:
    """Doctor's own line, which probes directly rather than through the cache.

    A doctor run is an operator asking the question now, so a five-minute-old
    observation would be the wrong answer. The failing detail names what breaks
    and the command that fixes it: a bare pair of addresses left that to be
    inferred by whoever had just watched a round fail for no visible reason.
    """
    try:
        current_cidr = detect_operator_cidr()
    except Exception as exc:
        return Check("operator_cidr", False, str(exc))
    configured = manifest.aws.operator_cidr
    if current_cidr == configured:
        return Check("operator_cidr", True, f"current {current_cidr}; configured {configured}")
    return Check(
        "operator_cidr",
        False,
        f"current {current_cidr}; configured {configured} · every round that "
        f"connects directly to Aurora or RDS will fail until the security groups "
        f"are rebound · run '{OPERATOR_INGRESS_REPAIR_COMMAND}'",
    )


def operator_ingress_check(manifest: DemoManifest | None = None) -> Check:
    """The cached drift as a `Check`, for `antidemo status`.

    `antidemo doctor` uses `_operator_cidr_check` instead: doctor probes directly
    because it is an operator asking now, where `antidemo status` is a cheap read
    that must stay cheap.

    Advisory: it is a real finding an operator must act on, but it is not
    evidence that the command being run has failed, and it must not turn a
    diagnostic command into a non-zero exit on its own.
    """
    drift = operator_ingress_drift(manifest=manifest)
    if drift is None:
        return Check("operator_ingress", True, "SEALED ALLOWANCE MATCHES THIS HOST")
    return Check("operator_ingress", False, drift.detail, advisory=True)


#: How long one presence verdict is trusted. Matched to the ingress TTL above for
#: the same reason: three paginated describes must not happen per request, and a
#: sweep that lands within five minutes still notices a reap well inside a demo.
INSTALLATION_PRESENCE_TTL_SECONDS = 300.0

#: A sweep that could not read the account is cached for less time, so the signal
#: recovers promptly once credentials come back rather than waiting out a full TTL.
INSTALLATION_PRESENCE_FAILURE_TTL_SECONDS = 30.0

#: Monotonic expiry, the verdict, and the report it was derived from. The report
#: is kept so a caller that already has a reason to render drift can read it
#: without provoking a second sweep.
_INSTALLATION_PRESENCE: tuple[float, InstallationPresence | None, ReconciliationReport | None] = (
    0.0,
    None,
    None,
)


def reset_installation_presence_cache() -> None:
    """Forget the cached verdict. For tests, and after a repair."""
    global _INSTALLATION_PRESENCE
    _INSTALLATION_PRESENCE = (0.0, None, None)


def _observe_installation_presence(
    manifest: DemoManifest | None,
) -> tuple[InstallationPresence, ReconciliationReport | None, float]:
    """One uncached sweep, its verdict, and how long to trust it.

    Never raises, and never reports resources gone on the strength of a failed
    read. `reconcile_live` already returns `unavailable` rather than an empty
    inventory when the account cannot be reached, which is what keeps a broken
    credential from looking like a total wipe; everything else that could go
    wrong here lands on the same answer.
    """
    try:
        resolved = manifest or load_manifest()
    except Exception as exc:
        return (
            InstallationPresence(
                PRESENCE_UNVERIFIED,
                reason=f"the manifest could not be read ({type(exc).__name__})",
            ),
            None,
            INSTALLATION_PRESENCE_FAILURE_TTL_SECONDS,
        )
    try:
        report = reconcile_live(resolved, _aws_session)
    except Exception as exc:
        return (
            InstallationPresence(
                PRESENCE_UNVERIFIED,
                reason=f"the account sweep could not be built ({type(exc).__name__})",
            ),
            None,
            INSTALLATION_PRESENCE_FAILURE_TTL_SECONDS,
        )
    presence = presence_from_report(report)
    ttl = (
        INSTALLATION_PRESENCE_FAILURE_TTL_SECONDS
        if presence.state == PRESENCE_UNVERIFIED
        else INSTALLATION_PRESENCE_TTL_SECONDS
    )
    return presence, report, ttl


def installation_presence(*, manifest: DemoManifest | None = None) -> InstallationPresence:
    """Whether the sealed AWS residents are still in the account.

    Safe to call from anywhere, including a request path: at most one sweep per
    `INSTALLATION_PRESENCE_TTL_SECONDS`, and it cannot raise. Reads only --
    nothing here writes a manifest, deletes anything, or takes the generation
    lock, so it stays inside the D9 boundary.
    """
    global _INSTALLATION_PRESENCE
    moment = time.monotonic()
    expires_at, cached, _ = _INSTALLATION_PRESENCE
    if moment < expires_at and cached is not None:
        return cached
    presence, report, ttl = _observe_installation_presence(manifest)
    _INSTALLATION_PRESENCE = (moment + ttl, presence, report)
    return presence


async def installation_presence_async(
    *, manifest: DemoManifest | None = None
) -> InstallationPresence:
    """`installation_presence` off the event loop.

    The sweep is three paginated boto calls. Awaited inline it would stall every
    other request on this worker, including the SSE stream a live bout rides on.
    """
    return await asyncio.to_thread(installation_presence, manifest=manifest)


def cached_installation_report() -> ReconciliationReport | None:
    """The last sweep's report, or None. Never probes, never blocks.

    Deliberately refuses to refresh: the one caller is the standing-cost
    disclosure, which is rendered synchronously on every poll of a session. A
    sweep behind that would put three describes on the request path -- which is
    why the hook it feeds was left unwired rather than pointed at a live read.
    Before the first sweep this returns None, which the disclosure already
    renders as not-read rather than as agreement.
    """
    return _INSTALLATION_PRESENCE[2]


def installation_presence_check(manifest: DemoManifest | None = None) -> Check:
    """The cached presence as a `Check`, for `antidemo status`.

    Only a *verified* loss decides the exit code. A sweep that could not read the
    account is advisory on the same reasoning that makes a handled restart
    advisory: `antidemo status` is run precisely when credentials have lapsed, and
    failing the command because it could not look would make it useless in the
    situation it exists for. A confirmed absence is the opposite case -- it will
    not fix itself and no round can run -- so it fails like a supervisor give-up.
    """
    presence = installation_presence(manifest=manifest)
    if presence.state == PRESENCE_MISSING:
        return Check("installation_presence", False, presence.detail)
    if presence.state == PRESENCE_UNVERIFIED:
        return Check("installation_presence", True, presence.detail, advisory=True)
    return Check("installation_presence", True, presence.detail)


def _terraform_environment(manifest: DemoManifest) -> dict[str, str]:
    selection = validate_runtime_auth(
        manifest.aws.auth_mode,
        manifest.aws.profile,
        os.environ,
    )
    return selected_subprocess_environment(os.environ, selection, manifest.aws.region)


def anti_demo_runtime_principals(manifest: DemoManifest) -> tuple[str, ...]:
    """Resolve the principals the sealed runtime role trusts, seal first.

    Exactly the shape `_round4_catalog` uses, and for the same reason. Only a
    first provision reads the environment; afterwards the manifest is
    authoritative, and an environment value that contradicts it is refused rather
    than silently ignored or silently obeyed. The difference matters more here
    than it does for a catalog name: this list is a trust policy, so quietly
    obeying the environment would let an exported variable widen who can assume
    the installation's principal, and quietly ignoring it would leave an operator
    believing they had.

    An installation that seals nothing -- every installation created before this
    existed -- resolves to the empty tuple, Terraform creates no role, and every
    address check, plan check and doctor check behaves exactly as it always has.
    """

    sealed = manifest.aws.runtime_role_trusted_principal_arns
    configured = tuple(
        item.strip()
        for item in os.environ.get(ANTI_DEMO_RUNTIME_PRINCIPALS_ENV, "").split(",")
        if item.strip()
    )
    if sealed is not None:
        if configured and set(configured) != set(sealed):
            raise RuntimeError(
                f"{ANTI_DEMO_RUNTIME_PRINCIPALS_ENV}={','.join(configured)} disagrees with the "
                f"principals this installation sealed ({','.join(sealed)}). Unset it, or run "
                "'antidemo renew' to re-apply the sealed trust policy. Changing who may assume the "
                "runtime role needs a fresh installation, not an environment variable."
            )
        return tuple(sealed)
    if not configured:
        return ()
    expected_prefix = f"arn:aws:iam::{manifest.aws.account_id}:"
    for arn in configured:
        if not arn.startswith(expected_prefix) or not re.fullmatch(
            r"arn:aws:iam::\d{12}:(?:role|user)/[A-Za-z0-9+=,.@_/-]+", arn
        ):
            raise RuntimeError(
                f"{ANTI_DEMO_RUNTIME_PRINCIPALS_ENV} must be a comma-separated list of exact "
                f"IAM role or user ARNs in {manifest.aws.account_id}; got {arn!r}"
            )
    if len(set(configured)) != len(configured):
        raise RuntimeError(f"{ANTI_DEMO_RUNTIME_PRINCIPALS_ENV} names the same principal twice")
    return configured


def _terraform_variables(
    manifest: DemoManifest,
    *,
    expires_at_override: datetime | None = None,
) -> list[str]:
    """Build the Terraform variables for this manifest.

    `expires_at_override` exists for `renew`, which must apply the *new* expiry to
    AWS before the manifest holds it. Writing the manifest first and retagging
    afterwards is the one ordering that must never be used: `cleanup` compares
    manifest tags against live AWS tags and refuses on any mismatch, even under
    `--dry-run`, so a failure in that order disables the cleanup path.
    """
    runtime_principals = anti_demo_runtime_principals(manifest)
    if runtime_principals:
        # The control role trusts the runtime role, not whoever happens to be
        # running the installer. This is what keeps the operator's input set
        # unchanged: bootstrap still derives ROUND5_APP_PRINCIPAL_ARN from the
        # caller, and it is simply not the answer once a runtime role exists.
        #
        # Knowable before Terraform runs only because the role's name is fixed
        # rather than `name_prefix`d -- see the comment on the resource. Sealed
        # installations read the seal instead, so an overridden name still works.
        round5_app_principal = manifest.aws.runtime_role_arn or (
            f"arn:aws:iam::{manifest.aws.account_id}:role/{ANTI_DEMO_RUNTIME_ROLE_NAME}"
        )
    else:
        round5_app_principal = (
            os.environ.get("ROUND5_APP_PRINCIPAL_ARN", "").strip()
            or os.environ.get("TF_VAR_round5_app_principal_arn", "").strip()
        )
    expected_arn_prefix = f"arn:aws:iam::{manifest.aws.account_id}:"
    if not round5_app_principal.startswith(expected_arn_prefix) or not re.fullmatch(
        r"arn:aws:iam::\d{12}:(?:role|user)/[A-Za-z0-9+=,.@_/-]+",
        round5_app_principal,
    ):
        raise RuntimeError(
            "ROUND5_APP_PRINCIPAL_ARN must bind the app AWS principal in the expected account"
        )
    values = {
        "aws_region": manifest.aws.region,
        "aws_account_id": manifest.aws.account_id,
        "run_id": manifest.run_id,
        "owner": manifest.owner,
        "expires_at": _utc_tag(expires_at_override or manifest.expires_at),
        "operator_cidr": manifest.aws.operator_cidr,
        # Beside the operator CIDR, never instead of it. A list for the same
        # reason as the runtime principals below: it reaches Terraform as HCL
        # through an argv list rather than a shell, so the brackets survive. An
        # empty list is the default and admits only the operator, which is
        # exactly where every installation sealed before this stood.
        "serverless_egress_cidrs": json.dumps(
            list(manifest.aws.serverless_egress_cidrs or ()), separators=(",", ":")
        ),
        "round5_app_principal_arn": round5_app_principal,
        # A list, so it is passed as HCL rather than as a bare string. These
        # arguments go to `subprocess` as an argv list, never through a shell, so
        # the brackets and quotes reach Terraform intact. An empty list is the
        # default and creates nothing.
        "anti_demo_runtime_principal_arns": json.dumps(
            list(runtime_principals), separators=(",", ":")
        ),
        # Pinned from here rather than left to Terraform's own default, so the
        # name this file derives an ARN from and the name Terraform creates can
        # never drift apart.
        "anti_demo_runtime_role_name": (
            manifest.aws.runtime_role_arn or ANTI_DEMO_RUNTIME_ROLE_NAME
        ).rsplit("/", 1)[-1],
    }
    if manifest.installation_id is not None:
        values["installation_id"] = manifest.installation_id
    arguments: list[str] = []
    for name, value in values.items():
        arguments.extend(["-var", f"{name}={value}"])
    return arguments


def _terraform_base() -> list[str]:
    return ["terraform", f"-chdir={AWS_INFRA_DIR}"]


BACKEND_RECORD_NAME = "terraform-backend.json"
BACKEND_OVERRIDE = AWS_INFRA_DIR / "backend_override.tf"


def _backend_record(manifest: DemoManifest) -> dict[str, Any] | None:
    """Read this generation's opt-in remote backend, if it has one.

    Absent -- the default and the only state any existing installation has --
    this returns None and everything below behaves exactly as it always has.
    """
    record = manifest_path().parent / BACKEND_RECORD_NAME
    if not record.is_file():
        return None
    payload = json.loads(record.read_text(encoding="utf-8"))
    if payload.get("backend") != "s3":
        raise RuntimeError(f"{record} names an unsupported backend {payload.get('backend')!r}")
    for key in ("bucket", "key", "region"):
        if not payload.get(key):
            raise RuntimeError(f"{record} is missing {key!r}")
    return payload


def _terraform_init(manifest: DemoManifest) -> None:
    record = _backend_record(manifest)
    if record is None:
        # A stale override from another generation would silently point this
        # init at that generation's state, so it is removed, not left.
        BACKEND_OVERRIDE.unlink(missing_ok=True)
        state = Path(manifest.aws.terraform_state)
        state.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        backend_arguments = [f"-backend-config=path={state}"]
    else:
        # Terraform cannot interpolate a backend block, so the values are
        # written as literals and regenerated on every init.
        BACKEND_OVERRIDE.write_text(
            'terraform {\n'
            '  backend "s3" {\n'
            f'    bucket       = "{record["bucket"]}"\n'
            f'    key          = "{record["key"]}"\n'
            f'    region       = "{record["region"]}"\n'
            '    use_lockfile = true\n'
            '    encrypt      = true\n'
            '  }\n'
            '}\n',
            encoding="utf-8",
        )
        backend_arguments = [
            f"-backend-config=bucket={record['bucket']}",
            f"-backend-config=key={record['key']}",
            f"-backend-config=region={record['region']}",
        ]
    _run(
        _terraform_base() + ["init", "-input=false", "-reconfigure", *backend_arguments],
        env=_terraform_environment(manifest),
    )


ROUND5_PREFLIGHT_TARGETS: tuple[str, ...] = ()


def _terraform_plan(
    manifest: DemoManifest,
    *,
    destroy: bool = False,
    targets: tuple[str, ...] = (),
    expires_at_override: datetime | None = None,
) -> Path:
    plan_name = "aws-destroy.tfplan" if destroy else "aws-create.tfplan"
    plan_path = manifest_path().parent / plan_name
    arguments = _terraform_base() + ["plan", "-input=false"]
    if destroy:
        arguments.append("-destroy")
    for address in targets:
        arguments.append(f"-target={address}")
    arguments.extend(
        [
            f"-out={plan_path}",
            *_terraform_variables(manifest, expires_at_override=expires_at_override),
        ]
    )
    _run(arguments, env=_terraform_environment(manifest))
    return plan_path


def _terraform_apply(manifest: DemoManifest, plan_path: Path) -> None:
    _run(
        _terraform_base() + ["apply", "-input=false", "-auto-approve", str(plan_path)],
        env=_terraform_environment(manifest),
        timeout=2400,
    )


def _terraform_outputs(manifest: DemoManifest) -> dict[str, Any]:
    payload = _run_json(
        _terraform_base() + ["output", "-json"],
        env=_terraform_environment(manifest),
    )
    return {name: item.get("value") for name, item in payload.items()}


EXPECTED_AWS_STATE_ADDRESSES = {
    "aws_db_instance.rds_control_plane_only",
    "aws_db_subnet_group.round1",
    "aws_iam_instance_profile.round5_runner",
    "aws_iam_role.round5_execution",
    "aws_iam_role.round5_proxy_service",
    "aws_iam_role.round5_runner",
    "aws_iam_role_policy.round5_execution",
    "aws_iam_role_policy.round5_proxy_secrets",
    "aws_iam_role_policy.round5_runner_baseline_secret",
    "aws_iam_role_policy_attachment.round5_runner_ssm",
    "aws_iam_policy.round5_runner_boundary",
    "aws_instance.round5_runner",
    "aws_rds_cluster.aurora",
    "aws_rds_cluster_instance.aurora_writer",
    "aws_security_group.aurora",
    "aws_security_group.rds_control_plane_only",
    "aws_security_group.round5_runner",
    "aws_secretsmanager_secret.round5_aurora_proxy_credentials",
    "aws_secretsmanager_secret.round5_rds_proxy_credentials",
    "aws_vpc_security_group_egress_rule.round5_runner_outbound",
    "terraform_data.round5_destroy_guard",
}

_LEGACY_DATABASE_STATE_ADDRESSES = {
    "aws_db_instance.rds_control_plane_only",
    "aws_db_subnet_group.round1",
    "aws_rds_cluster.aurora",
    "aws_rds_cluster_instance.aurora_writer",
    "aws_security_group.aurora",
    "aws_security_group.rds_control_plane_only",
}
_INDEXED_LEGACY_AWS_STATE_ADDRESSES = (
    EXPECTED_AWS_STATE_ADDRESSES - _LEGACY_DATABASE_STATE_ADDRESSES
) | {f"{address}[0]" for address in _LEGACY_DATABASE_STATE_ADDRESSES}
_V7_ROUND_KEYS = ("r1", "r2", "r3", "r5")
# Round 1 stands up no RDS instance, so its instance and security group are absent
# from Terraform state by construction rather than by failure. Aurora, the subnet
# groups and the per-round slugs still cover all four rounds; keeping the two key
# lists separate here mirrors `infra/aws/locals.tf:v7_rds_round_keys`, and folding
# them back together would make `_aws_state_is_complete` demand a resource the
# checked-in Terraform deliberately does not create.
_V7_RDS_ROUND_KEYS = ("r2", "r3", "r5")
_V7_AWS_STATE_ADDRESSES = (
    (EXPECTED_AWS_STATE_ADDRESSES - _LEGACY_DATABASE_STATE_ADDRESSES)
    | {
        f'{address}["{round_key}"]'
        for address in (
            "aws_db_subnet_group.by_round",
            "aws_rds_cluster.aurora_by_round",
            "aws_rds_cluster_instance.aurora_writer_by_round",
            "aws_security_group.aurora_by_round",
        )
        for round_key in _V7_ROUND_KEYS
    }
    | {
        f'{address}["{round_key}"]'
        for address in (
            "aws_db_instance.rds_by_round",
            "aws_security_group.rds_by_round",
        )
        for round_key in _V7_RDS_ROUND_KEYS
    }
)


# Conditional on the seal rather than unconditional, because `_aws_state_is_complete`
# demands exact set equality: adding these to the base set would make every
# installation that predates the runtime role read as incomplete state.
_ANTI_DEMO_RUNTIME_POLICY_KEYS = ("1-network", "2-databases", "3-identity")
_ANTI_DEMO_RUNTIME_STATE_ADDRESSES = {
    "aws_iam_role.anti_demo_runtime[0]",
    *(
        f'aws_iam_policy.anti_demo_runtime["{key}"]'
        for key in _ANTI_DEMO_RUNTIME_POLICY_KEYS
    ),
    *(
        f'aws_iam_role_policy_attachment.anti_demo_runtime["{key}"]'
        for key in _ANTI_DEMO_RUNTIME_POLICY_KEYS
    ),
}


def _expected_aws_state_addresses(manifest: DemoManifest) -> set[str]:
    base = (
        _V7_AWS_STATE_ADDRESSES
        if manifest.installation_id is not None
        else EXPECTED_AWS_STATE_ADDRESSES
    )
    # Resolve through the same helper that builds the Terraform variables rather
    # than through the sealed field: on a first provision the seal is written
    # *after* this check runs, so reading the manifest alone expects 37 addresses
    # while Terraform has already created 44 and the check can never pass.
    if not anti_demo_runtime_principals(manifest):
        return base
    return base | _ANTI_DEMO_RUNTIME_STATE_ADDRESSES


def _aws_state_is_complete(manifest: DemoManifest, addresses: set[str]) -> bool:
    expected = _expected_aws_state_addresses(manifest)
    if addresses == expected:
        return True
    if manifest.aws.runtime_role_arn is not None:
        return False
    return manifest.installation_id is None and addresses == _INDEXED_LEGACY_AWS_STATE_ADDRESSES


def _terraform_managed_addresses(manifest: DemoManifest) -> set[str]:
    result = _run(
        _terraform_base() + ["state", "list"],
        capture=True,
        env=_terraform_environment(manifest),
    )
    addresses = {
        line.strip()
        for line in result.stdout.splitlines()
        if line.strip() and not line.strip().startswith("data.")
    }
    recognized = (
        EXPECTED_AWS_STATE_ADDRESSES
        | _INDEXED_LEGACY_AWS_STATE_ADDRESSES
        | _V7_AWS_STATE_ADDRESSES
        | _ANTI_DEMO_RUNTIME_STATE_ADDRESSES
        | ROUND5_LEGACY_PARTIAL_ADDRESSES
        | ROUND5_LEGACY_REFUSED_ADDRESSES
        | ROUND5_LEGACY_DYNAMIC_ADDRESSES
    )
    unexpected = addresses - recognized
    if unexpected:
        raise RuntimeError(
            "Cleanup refused: Terraform state contains unexpected managed resources: "
            + ", ".join(sorted(unexpected))
        )
    return addresses


def _reconcile_legacy_round5_partial_state(
    manifest: DemoManifest,
    managed_addresses: set[str],
    *,
    timeout: float = 1800,
) -> bool:
    """Restore the source before Terraform removes the exact failed v3 partial apply."""
    legacy = managed_addresses & (ROUND5_LEGACY_PARTIAL_ADDRESSES | ROUND5_LEGACY_REFUSED_ADDRESSES)
    if not legacy:
        return False
    refused = legacy & ROUND5_LEGACY_REFUSED_ADDRESSES
    if refused:
        raise RuntimeError(
            "Legacy Round 5 reconciliation refused: the partial state unexpectedly "
            "contains IAM, runner, Proxy, target, or ingress resources: "
            + ", ".join(sorted(refused))
        )
    if "aws_db_parameter_group.rds_round5" not in legacy:
        raise RuntimeError(
            "Legacy Round 5 reconciliation refused: the partial state has add-ons "
            "without its owned SCRAM parameter group"
        )

    state_values = _terraform_state_resource_values(manifest, managed_addresses)
    required_tags = _required_tags(manifest)
    for address in legacy:
        values = state_values[address]
        tags = values.get("tags_all") or values.get("tags") or {}
        if not isinstance(tags, dict) or any(
            str(tags.get(key) or "") != value for key, value in required_tags.items()
        ):
            raise RuntimeError(
                f"Legacy Round 5 reconciliation refused: ownership differs for {address}"
            )

    ingress_address = "aws_vpc_security_group_ingress_rule.round5_runner_to_proxy"
    if ingress_address in legacy:
        proxy_values = state_values.get("aws_security_group.round5_proxy") or {}
        runner_values = state_values.get("aws_security_group.round5_runner") or {}
        ingress = state_values[ingress_address]
        if not (
            proxy_values.get("id")
            and runner_values.get("id")
            and ingress.get("security_group_id") == proxy_values.get("id")
            and ingress.get("referenced_security_group_id") == runner_values.get("id")
            and ingress.get("ip_protocol") == "tcp"
            and ingress.get("from_port") == 5432
            and ingress.get("to_port") == 5432
        ):
            raise RuntimeError(
                "Legacy Round 5 reconciliation refused: runner-to-Proxy ingress "
                "identity is not exact"
            )

    parameter_values = state_values["aws_db_parameter_group.rds_round5"]
    parameter_name = str(parameter_values.get("name") or "")
    if not parameter_name:
        raise RuntimeError(
            "Legacy Round 5 reconciliation refused: parameter-group identity is missing"
        )
    if not _aws_ownership(manifest).ok:
        raise RuntimeError(
            "Legacy Round 5 reconciliation refused: source database ownership differs"
        )

    rds = _aws_session(manifest).client("rds")
    parameter_groups = rds.describe_db_parameter_groups(DBParameterGroupName=parameter_name).get(
        "DBParameterGroups", []
    )
    if len(parameter_groups) != 1:
        raise RuntimeError(
            "Legacy Round 5 reconciliation refused: custom parameter group is not exact"
        )
    parameter_group = parameter_groups[0]
    if str(parameter_group.get("DBParameterGroupName") or "") != parameter_name:
        raise RuntimeError(
            "Legacy Round 5 reconciliation refused: custom parameter-group identity differs"
        )
    parameter_arn = str(parameter_group.get("DBParameterGroupArn") or "")
    actual_tags = {
        str(tag.get("Key") or ""): str(tag.get("Value") or "")
        for tag in rds.list_tags_for_resource(ResourceName=parameter_arn).get("TagList", [])
    }
    if not parameter_arn or any(
        actual_tags.get(key) != value for key, value in required_tags.items()
    ):
        raise RuntimeError(
            "Legacy Round 5 reconciliation refused: custom parameter-group tags differ"
        )

    databases = rds.describe_db_instances(
        DBInstanceIdentifier=manifest.aws.resources.rds_instance_id
    ).get("DBInstances", [])
    if len(databases) != 1:
        raise RuntimeError("Legacy Round 5 reconciliation could not resolve the owned RDS source")
    attached = databases[0].get("DBParameterGroups") or []
    custom_attached = any(
        str(group.get("DBParameterGroupName") or "") == parameter_name for group in attached
    )
    already_restored = (
        str(databases[0].get("DBInstanceStatus") or "").lower() == "available"
        and len(attached) == 1
        and attached[0].get("DBParameterGroupName") == "default.postgres17"
        and str(attached[0].get("ParameterApplyStatus") or "").lower() == "in-sync"
        and not (databases[0].get("PendingModifiedValues") or {}).get("DBParameterGroupName")
    )
    if already_restored:
        return True
    if not custom_attached:
        raise RuntimeError(
            "Legacy Round 5 reconciliation refused: the owned custom parameter group "
            "is neither attached nor already restored to default.postgres17"
        )

    print("RESTORE RDS source to default.postgres17 before legacy add-on removal", flush=True)
    rds.modify_db_instance(
        DBInstanceIdentifier=manifest.aws.resources.rds_instance_id,
        DBParameterGroupName="default.postgres17",
        ApplyImmediately=True,
    )
    deadline = time.monotonic() + timeout
    reboot_requested = False
    while True:
        databases = rds.describe_db_instances(
            DBInstanceIdentifier=manifest.aws.resources.rds_instance_id
        ).get("DBInstances", [])
        if len(databases) != 1:
            raise RuntimeError(
                "Legacy Round 5 reconciliation lost the exact owned RDS source "
                "while restoring default.postgres17"
            )
        database = databases[0]
        groups = database.get("DBParameterGroups") or []
        status = str(database.get("DBInstanceStatus") or "").lower()
        pending_parameter_group = (database.get("PendingModifiedValues") or {}).get(
            "DBParameterGroupName"
        )
        restored = (
            status == "available"
            and len(groups) == 1
            and groups[0].get("DBParameterGroupName") == "default.postgres17"
            and str(groups[0].get("ParameterApplyStatus") or "").lower() == "in-sync"
            and not pending_parameter_group
        )
        if restored:
            return True
        default_pending_reboot = (
            status == "available"
            and len(groups) == 1
            and groups[0].get("DBParameterGroupName") == "default.postgres17"
            and str(groups[0].get("ParameterApplyStatus") or "").lower() == "pending-reboot"
            and not pending_parameter_group
        )
        if default_pending_reboot and not reboot_requested:
            print(
                "REBOOT RDS source to finish applying default.postgres17",
                flush=True,
            )
            rds.reboot_db_instance(DBInstanceIdentifier=manifest.aws.resources.rds_instance_id)
            reboot_requested = True
            continue
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError(
                "Legacy Round 5 reconciliation timed out waiting for "
                "default.postgres17 to become available and in-sync"
            )
        time.sleep(min(15.0, remaining))


def _aws_resources_from_outputs(outputs: dict[str, Any]) -> AwsResources:
    fields = {
        "aurora_cluster_id": "aurora_cluster_id",
        "aurora_writer_instance_id": "aurora_writer_instance_id",
        "aurora_secret_arn": "aurora_secret_arn",
        "security_group_id": "aurora_security_group_id",
        "db_subnet_group_name": "db_subnet_group_name",
    }
    # The Round 1 RDS mirror. Round 1 stands up no RDS instance, so these outputs
    # are null by construction and the mirror fields are left empty. They are not
    # re-pointed at another round's box: a field named for Round 1 that held r2's
    # instance would make the seal describe a resource Round 1 does not have, and
    # the arming path would then try to describe it. `manifest.py`'s Round 1
    # legacy-mirror check enforces the same emptiness from the other side.
    optional_fields = {
        "rds_instance_id": "rds_instance_id",
        "rds_secret_arn": "rds_secret_arn",
        "rds_security_group_id": "rds_security_group_id",
    }
    values = {field: str(outputs.get(output_name) or "") for field, output_name in fields.items()}
    missing = [field for field, value in values.items() if not value]
    if missing:
        raise RuntimeError(
            "Terraform state is missing required owned-resource outputs: " + ", ".join(missing)
        )
    values.update(
        {
            field: str(outputs.get(output_name) or "")
            for field, output_name in optional_fields.items()
        }
    )
    return AwsResources(**values)


# The round-keyed RDS outputs are keyed by the RDS round list, not the full one,
# because Round 1 stands up no RDS instance. Naming them explicitly keeps the
# expected key set a property of the output rather than of the caller.
_V7_RDS_ROUND_OUTPUTS = frozenset(
    {
        "rds_security_group_ids",
        "rds_instance_ids",
        "rds_addresses",
        "rds_resource_ids",
        "rds_secret_arns",
    }
)


def _required_round_output_map(outputs: dict[str, Any], name: str) -> dict[str, str]:
    payload = outputs.get(name)
    if not isinstance(payload, dict):
        raise RuntimeError(f"Terraform output {name} is not a round-keyed map")
    expected = set(
        _V7_RDS_ROUND_KEYS if name in _V7_RDS_ROUND_OUTPUTS else _V7_ROUND_KEYS
    )
    if set(payload) != expected:
        raise RuntimeError(f"Terraform output {name} has unexpected round keys")
    values = {key: str(value or "") for key, value in payload.items()}
    if any(not value for value in values.values()):
        raise RuntimeError(f"Terraform output {name} contains an empty binding")
    return values


def _v7_aws_environment_seals(
    outputs: dict[str, Any],
) -> dict[RoundId, tuple[AuroraEnvironmentSeal, RdsEnvironmentSeal | None]]:
    maps = {
        name: _required_round_output_map(outputs, name)
        for name in (
            "db_subnet_group_names",
            "aurora_security_group_ids",
            "aurora_cluster_ids",
            "aurora_cluster_resource_ids",
            "aurora_writer_instance_ids",
            "aurora_writer_endpoints",
            "aurora_secret_arns",
            "rds_security_group_ids",
            "rds_instance_ids",
            "rds_addresses",
            "rds_resource_ids",
            "rds_secret_arns",
        )
    }
    round_ids = {
        "r1": RoundId.WAKE_IDLE_APP,
        "r2": RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
        "r3": RoundId.RECOVER_DELETED_ORDER,
        "r5": RoundId.SURVIVE_CONNECTION_SPIKE,
    }
    sealed: dict[RoundId, tuple[AuroraEnvironmentSeal, RdsEnvironmentSeal | None]] = {}
    for key, round_id in round_ids.items():
        subnet = maps["db_subnet_group_names"][key]
        aurora = AuroraEnvironmentSeal(
            cluster_id=maps["aurora_cluster_ids"][key],
            cluster_resource_id=maps["aurora_cluster_resource_ids"][key],
            writer_instance_id=maps["aurora_writer_instance_ids"][key],
            direct_host=maps["aurora_writer_endpoints"][key],
            secret_arn=maps["aurora_secret_arns"][key],
            security_group_id=maps["aurora_security_group_ids"][key],
            db_subnet_group_name=subnet,
        )
        # A round with no RDS instance seals no RDS environment. Round 1 is the
        # only such AWS round today: its RDS lane refuses to enter on engine
        # semantics and is never timed, so Terraform stands no instance up and
        # there is nothing here to seal.
        rds = (
            RdsEnvironmentSeal(
                instance_id=maps["rds_instance_ids"][key],
                resource_id=maps["rds_resource_ids"][key],
                direct_host=maps["rds_addresses"][key],
                secret_arn=maps["rds_secret_arns"][key],
                security_group_id=maps["rds_security_group_ids"][key],
                db_subnet_group_name=subnet,
            )
            if key in maps["rds_instance_ids"]
            else None
        )
        sealed[round_id] = (aurora, rds)
    return sealed


def _reseal_v7_aws_round_environments(
    manifest: DemoManifest, outputs: dict[str, Any]
) -> None:
    """Re-derive the AWS half of each round seal from the applied Terraform outputs.

    Reconciliation never used to change *which* resources exist, so refreshing the
    flat Round 1 mirror was enough to keep the manifest self-consistent. Deleting
    Round 1's RDS instance makes that false: the flat mirror empties while the
    round seal still names the instance, and Round 1's legacy-mirror check
    correctly rejects that pair. Re-reading both from one set of outputs is what
    keeps them from disagreeing.

    Lakebase is deliberately untouched. Only the Aurora and RDS halves are
    re-read, so a reseal after an AWS change cannot disturb the Lakebase side.
    """

    if manifest.installation_id is None or manifest.round_environments is None:
        return
    aws = _v7_aws_environment_seals(outputs)
    manifest.round_environments = {
        round_id: (
            seal.model_copy(update={"aurora": aws[round_id][0], "rds": aws[round_id][1]})
            if round_id in aws
            else seal
        )
        for round_id, seal in manifest.round_environments.items()
    }


def _hydrate_aws_resources(
    manifest: DemoManifest,
    outputs: dict[str, Any] | None = None,
    *,
    persist: bool = True,
) -> None:
    resolved = outputs if outputs is not None else _terraform_outputs(manifest)
    manifest.aws.resources = _aws_resources_from_outputs(resolved)
    _seal_anti_demo_runtime(manifest, resolved)
    if persist:
        save_manifest(manifest)


def _seal_anti_demo_runtime(manifest: DemoManifest, outputs: dict[str, Any]) -> None:
    """Record what Terraform actually built, once, and never move it afterwards.

    The seal outranks the environment from here on, exactly as Round 4's catalog
    does. It is written only on the provision that first creates the role: a
    later apply re-reads the same two outputs and must agree with them, because
    the alternative -- silently re-sealing whatever AWS currently says -- would
    turn a drifted trust policy into the new definition of correct and remove the
    only thing `antidemo doctor` has to compare against.
    """

    role_arn = str(outputs.get("anti_demo_runtime_role_arn") or "")
    trusted = tuple(
        str(item) for item in (outputs.get("anti_demo_runtime_trusted_principal_arns") or [])
    )
    sealed = manifest.aws.runtime_role_arn
    if not role_arn:
        if sealed is not None:
            raise RuntimeError(
                f"This installation sealed the runtime role {sealed}, but Terraform reports none. "
                "Re-run 'antidemo renew' to re-apply it; do not re-provision, which would "
                "re-seal the absence."
            )
        return
    if sealed is None:
        manifest.aws.runtime_role_arn = role_arn
        manifest.aws.runtime_role_trusted_principal_arns = trusted
        return
    expected = manifest.aws.runtime_role_trusted_principal_arns or ()
    if role_arn != sealed or set(trusted) != set(expected):
        raise RuntimeError(
            f"Terraform's runtime role ({role_arn} trusting {','.join(trusted)}) differs from the "
            f"seal ({sealed} trusting {','.join(expected)}). The seal outranks Terraform here; "
            "clean up and re-provision to move the installation to another principal."
        )


def _terraform_state_resource_values(
    manifest: DemoManifest,
    expected_addresses: set[str],
) -> dict[str, dict[str, Any]]:
    payload = _run_json(
        _terraform_base() + ["show", "-json"],
        env=_terraform_environment(manifest),
    )
    values = payload.get("values") or {}
    root_module = values.get("root_module") or {}
    resources = root_module.get("resources") or []
    found: dict[str, dict[str, Any]] = {}
    for resource in resources:
        if not isinstance(resource, dict) or resource.get("mode") == "data":
            continue
        address = str(resource.get("address") or "")
        resource_values = resource.get("values")
        if not address or not isinstance(resource_values, dict) or address in found:
            raise RuntimeError("Cleanup refused: Terraform state resource data is ambiguous")
        found[address] = resource_values
    if set(found) != expected_addresses:
        raise RuntimeError(
            "Cleanup refused: Terraform state identities differ from its managed address list"
        )
    return found


def _validate_partial_aws_destroy_retry(
    manifest: DemoManifest,
    managed_addresses: set[str],
) -> None:
    intentional_round5_stage = manifest.round5 is None and manifest.manifest_version in (1, 2)
    if manifest.status != "cleanup_failed" and not intentional_round5_stage:
        raise RuntimeError(
            "Cleanup cannot safely inventory an incomplete AWS apply; run ./antidemo resume first"
        )
    state_values = _terraform_state_resource_values(manifest, managed_addresses)
    resources = manifest.aws.resources
    expected_identities = {
        "aws_db_instance.rds_control_plane_only": ("identifier", resources.rds_instance_id),
        "aws_db_subnet_group.round1": ("name", resources.db_subnet_group_name),
        "aws_rds_cluster.aurora": ("cluster_identifier", resources.aurora_cluster_id),
        "aws_rds_cluster_instance.aurora_writer": (
            "identifier",
            resources.aurora_writer_instance_id,
        ),
        "aws_security_group.aurora": ("id", resources.security_group_id),
        "aws_security_group.rds_control_plane_only": (
            "id",
            resources.rds_security_group_id,
        ),
    }
    round5_addresses = managed_addresses - set(expected_identities)
    allowed_round5_children = {
        "aws_db_proxy_default_target_group.round5",
        "aws_db_proxy_target.round5",
        "aws_iam_role_policy.round5_execution",
        "aws_iam_role_policy.round5_runner_baseline_secret",
        "aws_iam_role_policy.round5_proxy_secret",
        # The name Terraform actually gives the proxy secret-reader policy is
        # plural; the singular above matches no resource in `infra/` and never
        # did. An inline role policy cannot carry tags at all -- IAM exposes no
        # tagging for them -- so it can only ever be recognised this way, and
        # its ownership is proven by its parent `aws_iam_role.round5_proxy_service`,
        # which is tagged and is checked by this same loop.
        "aws_iam_role_policy.round5_proxy_secrets",
        "aws_iam_role_policy.round5_runner_secrets",
        "aws_iam_role_policy_attachment.round5_runner_ssm",
        "aws_vpc_security_group_egress_rule.round5_proxy_to_rds",
        "aws_vpc_security_group_egress_rule.round5_runner_outbound",
        "aws_vpc_security_group_ingress_rule.round5_runner_to_proxy",
    }
    for address in round5_addresses:
        values = state_values[address]
        tags = values.get("tags_all") or values.get("tags") or {}
        if isinstance(tags, dict) and tags:
            required_tags = _required_tags_for_address(manifest, address)
            if any(str(tags.get(key) or "") != value for key, value in required_tags.items()):
                raise RuntimeError(
                    f"Cleanup refused: Terraform state ownership tags differ for {address}"
                )
        elif address not in allowed_round5_children:
            raise RuntimeError(
                f"Cleanup refused: remaining Round 5 resource has no ownership tags: {address}"
            )
    for address in managed_addresses & set(expected_identities):
        field, expected = expected_identities[address]
        if not expected or str(state_values[address].get(field) or "") != expected:
            raise RuntimeError(f"Cleanup refused: Terraform state identity mismatch for {address}")

    expected_tags = _required_tags(manifest)

    def require_tags(address: str, tags: list[dict[str, Any]]) -> None:
        actual = {
            str(tag.get("Key") or ""): str(tag.get("Value") or "")
            for tag in tags
            if isinstance(tag, dict)
        }
        if any(actual.get(key) != value for key, value in expected_tags.items()):
            raise RuntimeError(f"Cleanup refused: AWS ownership tag mismatch for {address}")

    try:
        session = _aws_session(manifest)
        rds = session.client("rds")
        ec2 = session.client("ec2")
        for address in managed_addresses & set(expected_identities):
            _, expected = expected_identities[address]
            if address == "aws_db_subnet_group.round1":
                item = rds.describe_db_subnet_groups(DBSubnetGroupName=expected)["DBSubnetGroups"][
                    0
                ]
                if item.get("DBSubnetGroupName") != expected:
                    raise RuntimeError("subnet group identity mismatch")
                arn = str(item.get("DBSubnetGroupArn") or "")
                if not arn:
                    raise RuntimeError("subnet group ARN is missing")
                require_tags(
                    address,
                    rds.list_tags_for_resource(ResourceName=arn).get("TagList", []),
                )
            elif address == "aws_rds_cluster.aurora":
                item = rds.describe_db_clusters(DBClusterIdentifier=expected)["DBClusters"][0]
                if item.get("DBClusterIdentifier") != expected:
                    raise RuntimeError("Aurora cluster identity mismatch")
                require_tags(
                    address,
                    rds.list_tags_for_resource(ResourceName=item["DBClusterArn"]).get(
                        "TagList", []
                    ),
                )
            elif address in {
                "aws_db_instance.rds_control_plane_only",
                "aws_rds_cluster_instance.aurora_writer",
            }:
                item = rds.describe_db_instances(DBInstanceIdentifier=expected)["DBInstances"][0]
                if item.get("DBInstanceIdentifier") != expected:
                    raise RuntimeError("database instance identity mismatch")
                require_tags(
                    address,
                    rds.list_tags_for_resource(ResourceName=item["DBInstanceArn"]).get(
                        "TagList", []
                    ),
                )
            else:
                item = ec2.describe_security_groups(GroupIds=[expected])["SecurityGroups"][0]
                if item.get("GroupId") != expected:
                    raise RuntimeError("security group identity mismatch")
                require_tags(address, item.get("Tags") or [])
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(
            "Cleanup refused: could not validate remaining AWS resource ownership"
        ) from exc


def _verify_aws_identity(
    profile: str,
    region: str,
    expected_account: str,
    auth_mode: AwsAuthMode = "profile",
) -> None:
    selection = validate_runtime_auth(auth_mode, profile, os.environ)
    arguments = ["aws", "sts", "get-caller-identity"]
    if selection.mode == "profile":
        arguments.extend(["--profile", selection.profile])
    arguments.extend(["--region", region, "--output", "json"])
    payload = _run_json(
        arguments,
        env=selected_subprocess_environment(os.environ, selection, region),
    )
    actual = str(payload.get("Account") or "")
    if actual != expected_account:
        raise RuntimeError(
            f"AWS credentials resolved to account {actual or 'UNKNOWN'}, "
            f"expected {expected_account}"
        )


def _aws_session(manifest: DemoManifest) -> boto3.Session:
    selection = validate_runtime_auth(
        manifest.aws.auth_mode,
        manifest.aws.profile,
        os.environ,
    )
    return boto3.Session(
        **session_arguments(selection.mode, selection.profile, manifest.aws.region)
    )


def _databricks_json(profile: str, *arguments: str, timeout: float = 600) -> dict[str, Any]:
    return _run_json(
        ["databricks", *arguments, "-p", profile, "-o", "json"],
        timeout=timeout,
    )


def _databricks_api(
    profile: str,
    method: str,
    path: str,
    *,
    body: dict[str, Any] | None = None,
    timeout: float = 600,
) -> dict[str, Any]:
    arguments = ["databricks", "api", method.lower(), path]
    if body is not None:
        arguments.extend(["--json", json.dumps(body)])
    arguments.extend(["-p", profile, "-o", "json"])
    return _run_json(arguments, timeout=timeout)


def _databricks_api_optional(profile: str, path: str) -> dict[str, Any] | None:
    result = subprocess.run(
        ["databricks", "api", "get", path, "-p", profile, "-o", "json"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    if result.returncode != 0:
        detail = f"{result.stderr}\n{result.stdout}".lower()
        if any(
            marker in detail
            for marker in ("not found", "does not exist", "resource_does_not_exist", "404")
        ):
            return None
        raise _safe_failure(result)
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError("A Databricks API lookup returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("A Databricks API lookup returned an unexpected JSON shape")
    return payload


def _databricks_api_delete_no_response(profile: str, path: str) -> None:
    _run(
        ["databricks", "api", "delete", path, "-p", profile, "-o", "json"],
        capture=True,
        timeout=600,
    )


def _sql_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    columns = [
        str(column.get("name") or "")
        for column in (((payload.get("manifest") or {}).get("schema") or {}).get("columns") or [])
    ]
    data = (payload.get("result") or {}).get("data_array") or []
    return [dict(zip(columns, row, strict=False)) for row in data]


def _sql_statement(
    profile: str,
    warehouse_id: str,
    statement: str,
    *,
    timeout: float = 600,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    payload = _databricks_api(
        profile,
        "post",
        "/api/2.0/sql/statements",
        body={
            "warehouse_id": warehouse_id,
            "statement": statement,
            "wait_timeout": "50s",
            "on_wait_timeout": "CONTINUE",
            "disposition": "INLINE",
        },
        timeout=min(timeout, 120),
    )
    while True:
        state = str(((payload.get("status") or {}).get("state")) or "")
        if state == "SUCCEEDED":
            return payload
        if state in {"FAILED", "CANCELED", "CLOSED"}:
            raise RuntimeError(f"Databricks SQL statement failed with state {state}")
        statement_id = str(payload.get("statement_id") or "")
        if not statement_id:
            raise RuntimeError("Databricks SQL statement did not return an ID")
        if time.monotonic() >= deadline:
            raise RuntimeError("Databricks SQL statement timed out")
        time.sleep(min(2, max(0, deadline - time.monotonic())))
        payload = _databricks_api(
            profile,
            "get",
            f"/api/2.0/sql/statements/{quote(statement_id, safe='')}",
            timeout=120,
        )


def _round4_catalog(manifest: DemoManifest) -> str:
    """Resolve Round 4's Unity Catalog, preferring the value the manifest sealed.

    A constant read once at import time can disagree with what a sealed
    installation actually provisioned, and Round 4's catalog reaches CREATE
    SCHEMA, GRANT and cleanup DELETE statements, so the disagreement would not
    be harmless. Only a first provision reads the environment; afterwards the
    seal is authoritative, and an environment value that contradicts it is
    refused rather than silently ignored or silently obeyed.
    """

    configured = os.environ.get(ROUND4_CATALOG_ENV, "").strip()
    sealed = manifest.round4
    if sealed is not None:
        if configured and configured != sealed.storage_catalog:
            raise RuntimeError(
                f"{ROUND4_CATALOG_ENV}={configured} disagrees with the Unity Catalog this "
                f"installation sealed ({sealed.storage_catalog}). Unset it, or clean up and "
                "re-provision to move Round 4 to another catalog."
            )
        return sealed.storage_catalog
    if not configured:
        return ROUND4_DEFAULT_CATALOG
    # The resolved name is interpolated into backtick-quoted SQL identifiers and
    # into dotted Unity Catalog full names, so refuse anything that could break
    # out of either rather than quoting it and hoping.
    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_-]*", configured):
        raise RuntimeError(
            f"{ROUND4_CATALOG_ENV} must be a bare Unity Catalog name of letters, digits, "
            f"underscores and dashes; got {configured!r}"
        )
    return configured


def _require_round4_catalog(manifest: DemoManifest, catalog: str) -> None:
    """Refuse a first provision into a Unity Catalog this principal cannot see.

    Round 4 does not create the catalog -- cleanup has no catalog delete, so it
    would leave an orphan nothing reaps -- and the compiled-in default is only
    *likely* to exist rather than certain. Without this the absence surfaces as
    a raw Databricks error from `CREATE SCHEMA` partway through provisioning,
    which names neither the variable that fixes it nor the fact that a default
    was guessed at all.
    """

    if _round4_get_uc_object(manifest, "catalogs", catalog) is not None:
        return
    hint = (
        f"Set {ROUND4_CATALOG_ENV} to a catalog it can create schemas in"
        if catalog == ROUND4_DEFAULT_CATALOG
        else f"Point {ROUND4_CATALOG_ENV} at a catalog it can create schemas in"
    )
    suffix = (
        f" ({ROUND4_DEFAULT_CATALOG} is only the default because Unity Catalog usually "
        "creates it; this workspace does not have it.)"
        if catalog == ROUND4_DEFAULT_CATALOG
        else ""
    )
    raise RuntimeError(
        f"Round 4 needs Unity Catalog {catalog!r} and Databricks principal "
        f"{manifest.databricks.user} cannot see it. {hint}. Round 4 will not create a "
        f"catalog, because cleanup does not delete one.{suffix}"
    )


def _round4_names(manifest: DemoManifest) -> dict[str, str]:
    token = manifest.run_id.replace("-", "_")
    if not re.fullmatch(r"[A-Za-z0-9_]+", token):
        raise RuntimeError("Run ID cannot be converted to safe Round 4 identifiers")
    catalog = _round4_catalog(manifest)
    source_schema = f"anti_demo_{token}"
    storage_schema = f"anti_demo_sync_{token}"
    online_schema = f"anti_demo_online_{token}"
    source_table = f"{catalog}.{source_schema}.{ROUND4_SOURCE_TABLE}"
    synced_table_id = f"{catalog}.{online_schema}.{ROUND4_SYNCED_TABLE}"
    project_id = _round_lakebase_binding(manifest, 4).project_id
    project = f"projects/{project_id}"
    branch = f"{project}/branches/production"
    return {
        "catalog": catalog,
        "project": project,
        "source_schema": source_schema,
        "storage_schema": storage_schema,
        "online_schema": online_schema,
        "source_table": source_table,
        "synced_table_id": synced_table_id,
        "resource_name": f"synced_tables/{synced_table_id}",
        "branch": branch,
        "database": f"{branch}/databases/{ROUND4_DATABASE}",
        "endpoint_name": f"{branch}/endpoints/primary",
    }


def _round4_synced_spec(names: dict[str, str]) -> dict[str, Any]:
    return {
        "source_table_full_name": names["source_table"],
        "primary_key_columns": ["entity_id"],
        "scheduling_policy": "CONTINUOUS",
        "new_pipeline_spec": {
            "storage_catalog": names["catalog"],
            "storage_schema": names["storage_schema"],
        },
        "branch": names["branch"],
        "postgres_database": ROUND4_DATABASE,
        "create_database_objects_if_missing": True,
    }


def _round4_get_synced_table(
    manifest: DemoManifest, names: dict[str, str]
) -> dict[str, Any] | None:
    resource = quote(names["resource_name"], safe="/")
    return _databricks_api_optional(
        manifest.databricks.profile,
        f"/api/2.0/postgres/{resource}",
    )


def _round4_get_database_synced_table(
    manifest: DemoManifest, names: dict[str, str]
) -> dict[str, Any] | None:
    synced_table_id = quote(names["synced_table_id"], safe="")
    return _databricks_api_optional(
        manifest.databricks.profile,
        f"/api/2.0/database/synced_tables/{synced_table_id}",
    )


def _round4_get_branch(manifest: DemoManifest, names: dict[str, str]) -> dict[str, Any] | None:
    return _databricks_api_optional(
        manifest.databricks.profile,
        f"/api/2.0/postgres/{quote(names['branch'], safe='/')}",
    )


def _round4_get_pipeline(manifest: DemoManifest, pipeline_id: str) -> dict[str, Any]:
    return _databricks_api(
        manifest.databricks.profile,
        "get",
        f"/api/2.0/pipelines/{quote(pipeline_id, safe='')}",
        timeout=120,
    )


#: How an operator puts the Round 4 pipeline back up, spelled once.
#:
#: Every refusal below has to name it, and `pipeline_power.owed_stop_sentence`
#: exists because a sentence about this pipeline acquired two copies that
#: disagreed about how to spell the command inside a single afternoon.
ROUND4_PIPELINE_START_COMMAND = "antidemo pipeline start"
ROUND4_PIPELINE_STATUS_COMMAND = "antidemo pipeline status"
ROUND4_PIPELINE_STOP_COMMAND = "antidemo pipeline stop"


def _round4_sync_failure(
    manifest: DemoManifest,
    pipeline_id: str,
    observed_states: Iterable[str],
) -> RuntimeError:
    """The error a failed Round 4 synced-table state actually deserves.

    Built rather than raised, so each caller keeps its own ``raise`` and its own
    stack. Called only once a failed state has been seen, so the one pipeline read
    below costs the healthy polling path nothing.

    **The defect this exists to close.** A deliberately stopped pipeline and a
    broken one are indistinguishable at the synced table -- `app.py` says the same
    thing about `doctor` -- and the waits below asserted the broken reading. The
    sequence that produced it is the ordinary, thrifty one: stop the pipeline to
    stop paying the standing rate, then `antidemo reset`. Reset had already
    flipped the manifest to ``seeding`` on its way in, then aborted naming a
    pipeline failure that had never happened, so an installation doing exactly
    what it was told refused every round until somebody restored it by hand.

    **Why this refuses instead of starting the pipeline itself.** Reset could
    start it, restore the baseline and put it back down -- that is the sequence an
    operator performs by hand. It is rejected because the "put it back down" half
    cannot be guaranteed. A ``finally`` survives an exception but not a killed
    process, a closed laptop or a lost lease, and the state left behind by a start
    with no matching stop is the forgotten stop: the most expensive failure mode
    in this installation, and the one an entire durable stop-record mechanism
    exists to *detect* precisely because nothing can prevent it. A refusal that
    spends nothing beats a recovery that can leak the standing rate.

    There is a second reason, read off the code rather than argued: nothing here
    can converge against a stopped pipeline anyway. `_repair_round4_baseline`
    commits a fresh Delta version and the wait then requires the synced table's
    cursor to reach exactly it, which a pipeline with no continuous update never
    does -- a stopped pipeline omits ``continuous_update_status`` entirely. So
    relaxing the terminal check without refusing would only trade a fast wrong
    error for a slow one at the far end of the timeout.
    """

    observed = sorted(
        {str(state).strip().upper() for state in observed_states if state}
        & SYNCED_TABLE_FAILED_STATES
    )
    try:
        pipeline = _round4_get_pipeline(manifest, pipeline_id)
    except Exception:
        # An unreadable pipeline cannot exonerate a failed state, so this falls
        # through to the terminal reading below. Failing toward the noisier
        # answer is the same direction `pipeline_power.read_stop_marker` fails
        # in, and for the same reason.
        pipeline = {}
    pipeline_state = str(pipeline.get("state") or "")
    update_state = latest_pipeline_update_state(pipeline)
    if not synced_table_failure_is_a_stopped_pipeline(observed, pipeline_state, update_state):
        return RuntimeError(
            f"Round 4 synced table entered terminal state {', '.join(observed) or 'UNKNOWN'} "
            f"· pipeline {pipeline_state or 'unreadable'} · newest update "
            f"{update_state or 'NONE'}"
        )
    from .pipeline_power import PIPELINE_USD_PER_DAY

    return RuntimeError(
        f"Round 4 needs its Managed Sync pipeline running, and pipeline {pipeline_id} "
        f"is switched off rather than broken: its newest update is {update_state} -- "
        f"somebody ended it, it did not fail -- and the pipeline itself reads "
        f"{pipeline_state or 'unreadable'}. That is why the synced table reports "
        f"{', '.join(observed)}, whose own description is that the table is online "
        f"but its latest pipeline update failed. Nothing is damaged and no data was "
        f"lost. Start it with '{ROUND4_PIPELINE_START_COMMAND}', wait for "
        f"'{ROUND4_PIPELINE_STATUS_COMMAND}' to read RUNNING, then re-run this "
        f"command. This refuses rather than starting the pipeline for you because "
        f"running it costs ${PIPELINE_USD_PER_DAY:.2f}/day and a command that spent "
        f"that on your behalf could also fail before switching it back off."
    )


def _round4_get_uc_object(
    manifest: DemoManifest, kind: str, full_name: str
) -> dict[str, Any] | None:
    return _databricks_api_optional(
        manifest.databricks.profile,
        f"/api/2.1/unity-catalog/{kind}/{quote(full_name, safe='')}",
    )


def _round4_list_uc_tables(
    manifest: DemoManifest, schema_name: str, *, catalog: str
) -> list[dict[str, Any]]:
    payload = _databricks_api(
        manifest.databricks.profile,
        "get",
        "/api/2.1/unity-catalog/tables"
        f"?catalog_name={quote(catalog, safe='')}"
        f"&schema_name={quote(schema_name, safe='')}",
        timeout=120,
    )
    tables = payload.get("tables") or []
    if not isinstance(tables, list) or any(not isinstance(item, dict) for item in tables):
        raise RuntimeError("Round 4 Unity Catalog table listing has an invalid shape")
    if payload.get("next_page_token"):
        raise RuntimeError("Round 4 Unity Catalog table listing is unexpectedly paginated")
    return tables


def _validate_round4_synced_table(
    manifest: DemoManifest,
    payload: dict[str, Any],
    names: dict[str, str],
    *,
    sealed: Round4Resources | None = None,
    require_identity: bool = True,
) -> tuple[str, str]:
    if payload.get("name") != names["resource_name"]:
        raise RuntimeError("Round 4 synced table resource name does not match the owned run")
    if payload.get("synced_table_id") != names["synced_table_id"]:
        raise RuntimeError("Round 4 synced table ID does not match the owned run")
    spec = payload.get("spec")
    expected_spec = _round4_synced_spec(names)
    if spec is not None:
        if not isinstance(spec, dict):
            raise RuntimeError("Round 4 /postgres synced table spec has an invalid shape")
        for field in (
            "source_table_full_name",
            "primary_key_columns",
            "scheduling_policy",
            "branch",
            "postgres_database",
        ):
            if field in spec and spec[field] != expected_spec[field]:
                raise RuntimeError(f"Round 4 synced table has an unexpected {field}")
        # Create-only inputs are omitted by some live GET responses.
        if (
            "create_database_objects_if_missing" in spec
            and spec["create_database_objects_if_missing"] is not True
        ):
            raise RuntimeError(
                "Round 4 synced table has an unexpected create_database_objects_if_missing"
            )
        pipeline_spec = spec.get("new_pipeline_spec")
        if pipeline_spec is not None and pipeline_spec != expected_spec["new_pipeline_spec"]:
            raise RuntimeError("Round 4 synced table has unexpected pipeline storage")
    status = payload.get("status") or {}
    if status.get("project") != names["project"]:
        raise RuntimeError("Round 4 synced table is not owned by the manifest Lakebase project")
    uid = str(payload.get("uid") or "")
    pipeline_id = str(status.get("pipeline_id") or "")
    if require_identity and (not uid or not pipeline_id):
        raise RuntimeError("Round 4 synced table did not return its UID and pipeline identity")
    if sealed is not None and (uid != sealed.synced_table_uid or pipeline_id != sealed.pipeline_id):
        raise RuntimeError("Round 4 synced table identity differs from the sealed manifest")
    return uid, pipeline_id


def _validate_round4_database_synced_table(
    payload: dict[str, Any],
    names: dict[str, str],
    *,
    project_uid: str,
    branch_uid: str,
    pipeline_id: str,
    require_sync_position: bool = False,
) -> None:
    if payload.get("name") != names["synced_table_id"]:
        raise RuntimeError("Round 4 /database synced table name is not exact")
    expected_identity = {
        "effective_database_project_id": project_uid,
        "effective_database_branch_id": branch_uid,
        "effective_logical_database_name": ROUND4_DATABASE,
    }
    for field, expected in expected_identity.items():
        if payload.get(field) != expected:
            raise RuntimeError(f"Round 4 /database synced table has an unexpected {field}")
    for field, expected in (
        ("database_project_id", project_uid),
        ("database_branch_id", branch_uid),
        ("logical_database_name", ROUND4_DATABASE),
    ):
        if field in payload and payload[field] not in (None, "", expected):
            raise RuntimeError(f"Round 4 /database synced table has an unexpected {field}")
    spec = payload.get("spec") or {}
    expected_spec = _round4_synced_spec(names)
    for field in ("source_table_full_name", "primary_key_columns", "scheduling_policy"):
        if spec.get(field) != expected_spec[field]:
            raise RuntimeError(f"Round 4 /database synced table has an unexpected {field}")
    if (
        "create_database_objects_if_missing" in spec
        and spec["create_database_objects_if_missing"] is not True
    ):
        raise RuntimeError("Round 4 /database synced table changed its create contract")
    pipeline_spec = spec.get("new_pipeline_spec")
    if pipeline_spec is not None and pipeline_spec != expected_spec["new_pipeline_spec"]:
        raise RuntimeError("Round 4 /database synced table has unexpected pipeline storage")
    existing_pipeline_id = spec.get("existing_pipeline_id")
    if existing_pipeline_id not in (None, "", pipeline_id):
        raise RuntimeError("Round 4 /database synced table refers to a different pipeline")
    status = payload.get("data_synchronization_status") or {}
    if not isinstance(status, dict):
        raise RuntimeError("Round 4 /database synchronization status has an invalid shape")
    if status.get("pipeline_id") != pipeline_id:
        raise RuntimeError("Round 4 /database synced table pipeline identity is not exact")
    if require_sync_position:
        continuous = status.get("continuous_update_status") or {}
        last_sync = status.get("last_sync") or {}
        if not isinstance(continuous, dict) or not isinstance(last_sync, dict):
            raise RuntimeError("Round 4 /database synchronization position has an invalid shape")
        delta_info = last_sync.get("delta_table_sync_info") or {}
        if not isinstance(delta_info, dict):
            raise RuntimeError("Round 4 /database synchronization position has an invalid shape")
        try:
            int(continuous["last_processed_commit_version"])
            int(delta_info["delta_commit_version"])
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError(
                "Round 4 /database synced table has no exact synchronization position"
            ) from exc
        for container, field in (
            (continuous, "timestamp"),
            (delta_info, "delta_commit_timestamp"),
            (last_sync, "sync_start_timestamp"),
            (last_sync, "sync_end_timestamp"),
        ):
            value = container.get(field)
            try:
                parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"Round 4 /database synced table has an invalid {field}"
                ) from exc
            if parsed.tzinfo is None or parsed.utcoffset() is None:
                raise RuntimeError(f"Round 4 /database synced table has an invalid {field}")


def _validate_round4_project_and_branch(
    manifest: DemoManifest,
    names: dict[str, str],
    project: dict[str, Any],
    branch: dict[str, Any],
    *,
    sealed: Round4Resources | None,
) -> tuple[str, str]:
    project_id = names["project"].removeprefix("projects/")
    _validate_lakebase_project(manifest, project, project_id=project_id)
    project_uid = str(project.get("uid") or "")
    branch_uid = str(branch.get("uid") or "")
    if not project_uid:
        raise RuntimeError("Round 4 Lakebase project returned no UID")
    if (
        branch.get("name") != names["branch"]
        or branch.get("branch_id") != "production"
        or branch.get("parent") != names["project"]
        or not branch_uid
    ):
        raise RuntimeError("Round 4 Lakebase branch identity is not exact")
    if sealed is not None and (
        project_uid != sealed.project_uid or branch_uid != sealed.branch_uid
    ):
        raise RuntimeError("Round 4 Lakebase project or branch UID changed")
    return project_uid, branch_uid


def _validate_round4_pipeline(
    payload: dict[str, Any],
    *,
    pipeline_id: str,
    synced_table_uid: str,
    setup_principal: str,
    names: dict[str, str],
) -> None:
    if payload.get("pipeline_id") != pipeline_id:
        raise RuntimeError("Round 4 pipeline identity is not exact")
    if payload.get("creator_user_name") != setup_principal:
        raise RuntimeError("Round 4 pipeline creator is not the setup principal")
    spec = payload.get("spec") or {}
    if (
        spec.get("catalog") != names["catalog"]
        or spec.get("schema") != names["online_schema"]
        or spec.get("pipeline_type") != "DATABASE_TABLE_SYNC"
        or spec.get("continuous") is not True
    ):
        raise RuntimeError("Round 4 pipeline is not continuous")
    managed = spec.get("managed_definition") or {}
    sync = managed.get("database_table_sync") or {}
    sinks = sync.get("sinks")
    if not isinstance(sinks, list) or len(sinks) != 1 or not isinstance(sinks[0], dict):
        raise RuntimeError("Round 4 pipeline does not have exactly one sink")
    sink = sinks[0]
    expected_sink = {
        "src_table": names["source_table"],
        "dest_table": f"{ROUND4_DATABASE}.{names['online_schema']}.{ROUND4_SYNCED_TABLE}",
        "dest_table_uc_name": names["synced_table_id"],
        "dest_table_id": synced_table_uid,
        "primary_key": ["entity_id"],
        "creator": setup_principal,
    }
    for field, expected in expected_sink.items():
        if sink.get(field) != expected:
            raise RuntimeError(f"Round 4 pipeline sink has an unexpected {field}")
    if sink.get("online_catalog_name") not in (None, names["catalog"]):
        raise RuntimeError("Round 4 pipeline sink has an unexpected online catalog")


def _validate_round4_uc_object(
    item: dict[str, Any] | None,
    *,
    full_name: str,
    setup_principal: str,
    label: str,
) -> dict[str, Any]:
    if item is None:
        raise RuntimeError(f"Round 4 {label} does not exist")
    if item.get("full_name") != full_name:
        raise RuntimeError(f"Round 4 {label} name is not exact")
    if item.get("owner") != setup_principal or item.get("created_by") != setup_principal:
        raise RuntimeError(f"Round 4 {label} ownership is not exact")
    return item


def _validate_round4_uc_contract(
    manifest: DemoManifest,
    names: dict[str, str],
    *,
    setup_principal: str,
    pipeline_id: str,
    require_storage_schema: bool,
) -> None:
    for label, kind, full_name in (
        ("source schema", "schemas", f"{names['catalog']}.{names['source_schema']}"),
        ("online schema", "schemas", f"{names['catalog']}.{names['online_schema']}"),
        ("source table", "tables", names["source_table"]),
        ("synced table", "tables", names["synced_table_id"]),
    ):
        item = _validate_round4_uc_object(
            _round4_get_uc_object(manifest, kind, full_name),
            full_name=full_name,
            setup_principal=setup_principal,
            label=label,
        )
        if label == "synced table" and item.get("pipeline_id") != pipeline_id:
            raise RuntimeError("Round 4 Unity Catalog sink pipeline is not exact")
    storage_full_name = f"{names['catalog']}.{names['storage_schema']}"
    storage = _round4_get_uc_object(manifest, "schemas", storage_full_name)
    if require_storage_schema:
        _validate_round4_uc_object(
            storage,
            full_name=storage_full_name,
            setup_principal=setup_principal,
            label="storage schema",
        )
        if _round4_list_uc_tables(manifest, names["storage_schema"], catalog=names["catalog"]):
            raise RuntimeError("Round 4 auxiliary storage schema is not empty")
    elif storage is not None:
        raise RuntimeError("Round 4 auxiliary storage schema still exists")
    online_tables = _round4_list_uc_tables(
        manifest, names["online_schema"], catalog=names["catalog"]
    )
    if [item.get("full_name") for item in online_tables] != [names["synced_table_id"]]:
        raise RuntimeError("Round 4 pipeline does not have exactly one owned sink")


def _wait_round4_operation(profile: str, operation: dict[str, Any], *, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    payload = operation
    while not payload.get("done"):
        name = str(payload.get("name") or "")
        if not name:
            raise RuntimeError("Round 4 operation did not return a resource name")
        if time.monotonic() >= deadline:
            raise RuntimeError("Round 4 control-plane operation timed out")
        time.sleep(min(2, max(0, deadline - time.monotonic())))
        payload = _databricks_api(
            profile,
            "get",
            f"/api/2.0/postgres/{quote(name, safe='/')}",
            timeout=120,
        )
    if payload.get("error"):
        raise RuntimeError("Round 4 control-plane operation failed")


def _verify_databricks_identity(profile: str) -> str:
    current_user = _databricks_json(profile, "current-user", "me")
    user = str(current_user.get("userName") or "")
    if not user:
        raise RuntimeError("Databricks profile did not resolve to a workspace user")
    # This is a capability check as well as an authentication check.
    _run(
        ["databricks", "postgres", "list-projects", "-p", profile, "-o", "json"],
        capture=True,
    )
    return user


def _create_lakebase(
    manifest: DemoManifest,
    *,
    project_id: str | None = None,
    display_label: str | None = None,
) -> None:
    profile = manifest.databricks.profile
    project_id = project_id or manifest.databricks.project_id
    display_name = display_label or f"Lakebase Anti-Demo {manifest.run_id}"
    _run(
        [
            "databricks",
            "postgres",
            "create-project",
            project_id,
            "--json",
            json.dumps({"spec": {"display_name": display_name, "pg_version": 17}}),
            "--timeout",
            "10m",
            "-p",
            profile,
            "-o",
            "json",
        ],
        capture=True,
        timeout=700,
    )


def _get_lakebase_project_or_none(
    manifest: DemoManifest, *, project_id: str | None = None
) -> dict[str, Any] | None:
    project_id = project_id or manifest.databricks.project_id
    arguments = [
        "databricks",
        "postgres",
        "get-project",
        f"projects/{project_id}",
        "-p",
        manifest.databricks.profile,
        "-o",
        "json",
    ]
    result = subprocess.run(
        arguments,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    if result.returncode != 0:
        detail = f"{result.stderr}\n{result.stdout}".lower()
        missing_markers = ("not found", "does not exist", "resource_does_not_exist", "404")
        if any(marker in detail for marker in missing_markers):
            return None
        raise _safe_failure(result)
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError("The Lakebase project lookup returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("The Lakebase project lookup returned an unexpected JSON shape")
    return payload


def _validate_lakebase_project(
    manifest: DemoManifest,
    project: dict[str, Any],
    *,
    project_id: str | None = None,
) -> None:
    project_id = project_id or manifest.databricks.project_id
    status = project.get("status") or {}
    actual_id = str(project.get("project_id") or status.get("project_id") or "")
    actual_name = str(project.get("name") or f"projects/{actual_id}")
    expected_name = f"projects/{project_id}"
    if actual_id != project_id or actual_name != expected_name:
        raise RuntimeError("Lakebase project identity does not match the owned manifest")
    pg_version = int(status.get("pg_version") or 0)
    if pg_version != 17:
        raise RuntimeError(f"Lakebase project is PostgreSQL {pg_version}, expected 17")


def _region_parity(manifest: DemoManifest) -> Check:
    try:
        endpoint = _databricks_json(
            manifest.databricks.profile,
            "postgres",
            "get-endpoint",
            manifest.databricks.endpoint_name,
        )
        host = str((((endpoint.get("status") or {}).get("hosts") or {}).get("host")) or "")
        lakebase_region = lakebase_region_from_host(host)
    except Exception as exc:
        return Check("region_parity", False, str(exc))
    matches = lakebase_region == manifest.aws.region
    return Check(
        "region_parity",
        matches,
        f"Lakebase {lakebase_region}; AWS competitors {manifest.aws.region}",
    )


def _endpoint_capacity(profile: str, endpoint_name: str) -> tuple[float | None, float | None]:
    """``(max_cu, min_cu)`` as one named endpoint's control plane reports them."""

    endpoint = _databricks_json(profile, "postgres", "get-endpoint", endpoint_name)
    status = endpoint.get("status") or {}
    maximum = status.get("autoscaling_limit_max_cu")
    minimum = status.get("autoscaling_limit_min_cu")
    return (
        None if maximum is None else float(maximum),
        None if minimum is None else float(minimum),
    )


def _lakebase_max_cu(manifest: DemoManifest) -> float | None:
    """The single pre-v7 endpoint's ceiling.

    Correct only where there *is* one endpoint, which is the pre-v7 layout. v7
    seals one endpoint per round and must use :func:`_lakebase_capacity`.
    """

    maximum, _ = _endpoint_capacity(
        manifest.databricks.profile, manifest.databricks.endpoint_name
    )
    return maximum


def _lakebase_capacity(
    manifest: DemoManifest, round_id: RoundId | int
) -> tuple[float | None, float | None]:
    """``(max_cu, min_cu)`` for one round's own endpoint.

    Reading `manifest.databricks.endpoint_name` here compared every round against
    Round 1's endpoint. Seven endpoints exist; they agree today at 2 CU and would
    stop agreeing the moment any round's CU is changed, at which point the check
    would have passed a lane it never looked at. Latent, not harmless.

    The floor comes back alongside the ceiling because the floors are disclosed
    too, and reading them from separate calls is how the pair would drift.
    """

    environment = manifest.round_environment(round_id)
    return _endpoint_capacity(
        manifest.databricks.profile, environment.lakebase.endpoint_name
    )


def _capacity_parity(manifest: DemoManifest) -> Check:
    """Fail when either side is no longer configured to the matched ceiling.

    Region parity above stops the lanes drifting apart geographically. This stops
    them drifting apart on compute, which is the axis that would silently turn a
    load-sensitive result unfair: Round 5 ranks lanes by successful clients,
    client errors and p99, so a smaller AWS box would read as a worse product.
    Every provisioned AWS lane is checked, not just the round under test.
    """

    try:
        session = _aws_session(manifest)
        rds = session.client("rds")
        if manifest.manifest_version == 7:
            lanes = [
                (
                    f"r{number}",
                    _ROUND_NUMBER_IDS[number],
                    manifest.round_environment(number).aurora,
                    # Round 1 seals no RDS instance: its lane refuses to enter on
                    # engine semantics and is never timed, so there is nothing here
                    # to compare and nothing to report as missing. Reading the seal
                    # unconditionally validated a lane that does not compete.
                    (
                        manifest.round_environment(number).rds
                        if rds_lane_is_scored(_ROUND_NUMBER_IDS[number])
                        else None
                    ),
                )
                for number in (1, 2, 3, 5)
            ]
        else:
            # The pre-v7 layout has one endpoint and one instance for the whole
            # installation, mirrored into fields named for Round 1. That single
            # instance served every round, so the per-round scored-lane rule above
            # does not apply to it and it is still checked.
            resources = manifest.aws.resources
            lanes = [
                (
                    "r1",
                    None,
                    AuroraEnvironmentSeal.model_construct(
                        cluster_id=resources.aurora_cluster_id
                    ),
                    RdsEnvironmentSeal.model_construct(
                        instance_id=resources.rds_instance_id
                    ),
                )
            ]
        details: list[str] = []
        for round_key, round_id, aurora, postgres in lanes:
            if round_id is None:
                lakebase_max_cu = _lakebase_max_cu(manifest)
                lakebase_min_cu = None
            else:
                lakebase_max_cu, lakebase_min_cu = _lakebase_capacity(manifest, round_id)
            aurora_max_acu: float | None = None
            aurora_min_acu: float | None = None
            if aurora is not None:
                cluster = rds.describe_db_clusters(DBClusterIdentifier=aurora.cluster_id)[
                    "DBClusters"
                ][0]
                scaling = cluster.get("ServerlessV2ScalingConfiguration") or {}
                raw_max = scaling.get("MaxCapacity")
                aurora_max_acu = None if raw_max is None else float(raw_max)
                raw_min = scaling.get("MinCapacity")
                aurora_min_acu = None if raw_min is None else float(raw_min)
            instance_class: str | None = None
            # Blank identifier, unfiltered describe, stranger's instance class --
            # the same trap as in `_aws_ownership`, but here it would be reported
            # as a parity verdict rather than refused.
            if postgres is not None and postgres.instance_id:
                instance = rds.describe_db_instances(
                    DBInstanceIdentifier=postgres.instance_id
                )["DBInstances"][0]
                instance_class = str(instance.get("DBInstanceClass") or "") or None
            result = capacity_parity(
                lakebase_max_cu=lakebase_max_cu,
                aurora_max_acu=aurora_max_acu,
                rds_instance_class=instance_class,
                lakebase_min_cu=lakebase_min_cu,
                aurora_min_acu=aurora_min_acu,
                round_id=round_id,
            )
            if not result.ok:
                return Check("capacity_parity", False, f"{round_key}: {result.detail}")
            details.append(f"{round_key} {result.detail}")
    except Exception as exc:
        return Check("capacity_parity", False, str(exc))
    if not details:
        return Check("capacity_parity", False, "no AWS competitor lane was found to compare")
    return Check("capacity_parity", True, "; ".join(details))


def _configure_lakebase(
    manifest: DemoManifest,
    *,
    project_id: str | None = None,
    endpoint_name: str | None = None,
    max_cu: float = LAKEBASE_MAX_CU,
) -> None:
    profile = manifest.databricks.profile
    project_id = project_id or manifest.databricks.project_id
    endpoint = endpoint_name or manifest.databricks.endpoint_name
    _run(
        [
            "databricks",
            "postgres",
            "update-endpoint",
            endpoint,
            "spec.autoscaling_limit_min_cu,spec.autoscaling_limit_max_cu",
            "--json",
            json.dumps(
                {
                    "spec": {
                        "autoscaling_limit_min_cu": LAKEBASE_MIN_CU,
                        "autoscaling_limit_max_cu": max_cu,
                    }
                }
            ),
            "--timeout",
            "10m",
            "-p",
            profile,
            "-o",
            "json",
        ],
        capture=True,
        timeout=700,
    )
    _run(
        [
            "databricks",
            "postgres",
            "update-endpoint",
            endpoint,
            "spec.suspension",
            "--json",
            # Lakebase supports a one-minute automatic scale-to-zero timeout.
            # Use each product's shortest native auto-suspend setting so the
            # re-do round measures the capability instead of normalizing away
            # Lakebase's faster idle policy.
            json.dumps(
                {"spec": {"suspend_timeout_duration": f"{LAKEBASE_SUSPEND_SECONDS}s"}}
            ),
            "--timeout",
            "10m",
            "-p",
            profile,
            "-o",
            "json",
        ],
        capture=True,
        timeout=700,
    )
    project = _get_lakebase_project_or_none(manifest, project_id=project_id)
    if project is None:
        raise RuntimeError("Lakebase project disappeared while it was being configured")
    _validate_lakebase_project(manifest, project, project_id=project_id)


def _ensure_lakebase(
    manifest: DemoManifest,
    *,
    project_id: str | None = None,
    endpoint_name: str | None = None,
    display_label: str | None = None,
    max_cu: float = 2,
) -> None:
    project_id = project_id or manifest.databricks.project_id
    endpoint_name = endpoint_name or manifest.databricks.endpoint_name
    project = _get_lakebase_project_or_none(manifest, project_id=project_id)
    if project is None:
        _create_lakebase(
            manifest,
            project_id=project_id,
            display_label=display_label,
        )
    else:
        _validate_lakebase_project(manifest, project, project_id=project_id)
    _configure_lakebase(
        manifest,
        project_id=project_id,
        endpoint_name=endpoint_name,
        max_cu=max_cu,
    )


def _seal_lakebase_environment(
    manifest: DemoManifest, binding: _LakebaseBinding
) -> LakebaseEnvironmentSeal:
    project_name = f"projects/{binding.project_id}"
    branch_name = f"{project_name}/branches/production"
    project = _get_lakebase_project_or_none(manifest, project_id=binding.project_id)
    if project is None:
        raise RuntimeError(f"Lakebase project {project_name} disappeared before sealing")
    _validate_lakebase_project(manifest, project, project_id=binding.project_id)
    branch = _databricks_json(
        manifest.databricks.profile,
        "postgres",
        "get-branch",
        branch_name,
    )
    endpoint = _databricks_json(
        manifest.databricks.profile,
        "postgres",
        "get-endpoint",
        binding.endpoint_name,
    )
    branch_status = branch.get("status") or {}
    endpoint_status = endpoint.get("status") or {}
    hosts = endpoint_status.get("hosts") or {}
    values = {
        "project_id": binding.project_id,
        "project_uid": str(project.get("uid") or ""),
        "branch_name": branch_name,
        "branch_uid": str(branch.get("uid") or ""),
        "endpoint_name": binding.endpoint_name,
        "endpoint_uid": str(endpoint.get("uid") or ""),
        "direct_host": str(hosts.get("host") or ""),
        "pooled_host": str(hosts.get("read_write_pooled_host") or ""),
    }
    if (
        branch.get("name") != branch_name
        or branch.get("branch_id") != "production"
        or branch.get("parent") not in (None, project_name)
        or branch_status.get("branch_id") not in (None, "production")
        or endpoint.get("name") != binding.endpoint_name
        or endpoint.get("endpoint_id") != "primary"
        or endpoint.get("parent") not in (None, branch_name)
        or endpoint_status.get("endpoint_id") not in (None, "primary")
        or endpoint_status.get("disabled") is True
    ):
        raise RuntimeError(
            f"Lakebase production endpoint identity is not exact for {binding.project_id}"
        )
    sealed = LakebaseEnvironmentSeal.model_validate(values)
    actual_region = lakebase_region_from_host(sealed.direct_host)
    if actual_region != manifest.aws.region:
        raise RuntimeError(
            f"Lakebase project {binding.project_id} is in {actual_region}, "
            f"expected {manifest.aws.region}"
        )
    return sealed


def _ensure_v7_lakebase_projects(
    manifest: DemoManifest,
) -> tuple[dict[RoundId, LakebaseEnvironmentSeal], LakebaseEnvironmentSeal]:
    rounds: dict[RoundId, LakebaseEnvironmentSeal] = {}
    for number, round_id in _ROUND_NUMBER_IDS.items():
        binding = _round_lakebase_binding(manifest, round_id)
        print(
            f"CREATE/VERIFY Round {number} Lakebase project {binding.project_id}",
            flush=True,
        )
        _ensure_lakebase(
            manifest,
            project_id=binding.project_id,
            endpoint_name=binding.endpoint_name,
            display_label=f"Lakebase Anti-Demo {manifest.run_id} Round {number}",
        )
        rounds[round_id] = _seal_lakebase_environment(manifest, binding)

    coordination = _coordination_lakebase_binding(manifest)
    print(
        f"CREATE/VERIFY installation coordination Lakebase project {coordination.project_id}",
        flush=True,
    )
    _ensure_lakebase(
        manifest,
        project_id=coordination.project_id,
        endpoint_name=coordination.endpoint_name,
        display_label=f"Lakebase Anti-Demo {manifest.run_id} Coordination",
        max_cu=1,
    )
    return rounds, _seal_lakebase_environment(manifest, coordination)


def _prepare_v7_round_environments(manifest: DemoManifest, outputs: dict[str, Any]) -> None:
    lakebase, coordination = _ensure_v7_lakebase_projects(manifest)
    aws = _v7_aws_environment_seals(outputs)
    environments: dict[RoundId, RoundEnvironmentSeal] = {}
    for round_id in RoundId:
        competitor_seals = aws.get(round_id)
        environments[round_id] = RoundEnvironmentSeal(
            lakebase=lakebase[round_id],
            aurora=competitor_seals[0] if competitor_seals else None,
            rds=competitor_seals[1] if competitor_seals else None,
        )
    manifest.round_environments = environments
    manifest.coordination_environment = coordination
    manifest.databricks.coordination_endpoint_name = coordination.endpoint_name
    # Validate the complete staged inventory before it can be persisted. The
    # legacy top-level Databricks/AWS bindings remain the exact Round 1 mirror.
    DemoManifest.model_validate(manifest.model_dump())


def _round5_lakebase_hosts(manifest: DemoManifest) -> tuple[str, str]:
    binding = _round_lakebase_binding(manifest, 5)
    endpoint = _databricks_json(
        manifest.databricks.profile,
        "postgres",
        "get-endpoint",
        binding.endpoint_name,
    )
    if endpoint.get("name") != binding.endpoint_name:
        raise RuntimeError("Round 5 Lakebase endpoint identity is not exact")
    hosts = (endpoint.get("status") or {}).get("hosts") or {}
    direct = str(hosts.get("host") or "")
    pooled = str(hosts.get("read_write_pooled_host") or "")
    if not direct or not pooled:
        raise RuntimeError("Round 5 Lakebase direct and pooled hosts are unavailable")
    # The pooled hostname is deliberately never derived from the direct hostname.
    # Only the control-plane value returned above is accepted into the v3 seal.
    return direct, pooled


def _enable_round5_lakebase_native_login(manifest: DemoManifest) -> tuple[str, str]:
    binding = _round_lakebase_binding(manifest, 5)
    project_name = f"projects/{binding.project_id}"
    project = (
        _get_lakebase_project_or_none(manifest, project_id=binding.project_id)
        if manifest.round_environments is not None
        else _get_lakebase_project_or_none(manifest)
    )
    if project is None:
        raise RuntimeError("Round 5 Lakebase project disappeared during native-login setup")
    _validate_lakebase_project(manifest, project, project_id=binding.project_id)
    if (project.get("status") or {}).get("enable_pg_native_login") is not True:
        _run(
            [
                "databricks",
                "postgres",
                "update-project",
                project_name,
                "spec.enable_pg_native_login",
                "--json",
                json.dumps({"spec": {"enable_pg_native_login": True}}),
                "--timeout",
                "10m",
                "-p",
                manifest.databricks.profile,
                "-o",
                "json",
            ],
            capture=True,
            timeout=700,
        )
        project = (
            _get_lakebase_project_or_none(manifest, project_id=binding.project_id)
            if manifest.round_environments is not None
            else _get_lakebase_project_or_none(manifest)
        )
        if project is None:
            raise RuntimeError("Round 5 Lakebase project disappeared during native-login setup")
        _validate_lakebase_project(manifest, project, project_id=binding.project_id)
    if (project.get("status") or {}).get("enable_pg_native_login") is not True:
        raise RuntimeError("Round 5 Lakebase native password login is not enabled")
    return _round5_lakebase_hosts(manifest)


def _round5_runner_archive() -> str:
    from .connection_spike_live import RUNNER_ASSETS

    runner_root = PROJECT_ROOT / "runner"
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w:gz") as archive:
        for name in RUNNER_ASSETS:
            path = runner_root / name
            if not path.is_file():
                raise RuntimeError(f"Round 5 runner asset is missing: {name}")
            archive.add(path, arcname=name, recursive=False)
    return base64.b64encode(stream.getvalue()).decode("ascii")


#: Budget for one bounded command on the Round 5 runner, in seconds.
#:
#: This single number is spent twice, which is what makes a small value wrong
#: rather than merely tight. It is `SendCommand`'s `TimeoutSeconds` -- how long
#: SSM may leave the command pending before the agent picks it up -- and it is
#: also the local `get_command_invocation` poll deadline below, covering agent
#: pickup, execution, and a poll loop that sleeps in two-second steps. Under
#: that budget the command is cancelled and the caller is told it timed out.
#:
#: There is a hard floor as well: botocore models `TimeoutSeconds` with
#: `min=30`, so anything smaller is rejected by parameter validation before the
#: request is signed and the runner is unreachable rather than slow. Both of the
#: paths that spend this budget -- installing the runner harness and, more
#: pointedly, `_require_round5_runner_idle`, which gates teardown -- fail closed,
#: so a value chosen at the floor buys nothing and risks refusing a cleanup the
#: runner would have permitted.
ROUND5_SSM_COMMAND_TIMEOUT_SECONDS = 120
# SSM limits the complete command document plus parameters to 100 KB. The
# compressed base64 runner archive is one parameter entry; reserve 20 KB for the
# document wrapper and install commands instead of carrying the old arbitrary
# 20,000-character cliff.
ROUND5_RUNNER_SSM_ARCHIVE_MAX_CHARS = 80_000


def _run_round5_ssm_command(
    ssm: Any,
    *,
    runner_instance_id: str,
    commands: list[str],
    timeout: float,
) -> str:
    response = ssm.send_command(
        InstanceIds=[runner_instance_id],
        DocumentName="AWS-RunShellScript",
        TimeoutSeconds=int(timeout),
        Parameters={
            "commands": commands,
            "executionTimeout": [str(int(timeout))],
        },
        CloudWatchOutputConfig={"CloudWatchOutputEnabled": False},
    )
    command_id = str((response.get("Command") or {}).get("CommandId") or "")
    if not command_id:
        raise RuntimeError("Round 5 runner configuration returned no SSM command ID")
    deadline = time.monotonic() + timeout
    terminal = {"Success", "Cancelled", "Failed", "TimedOut", "Cancelling"}
    while True:
        try:
            invocation = ssm.get_command_invocation(
                CommandId=command_id,
                InstanceId=runner_instance_id,
            )
        except Exception:
            invocation = {}
        status = str(invocation.get("Status") or "")
        if status in terminal:
            if status != "Success":
                standard_error = str(invocation.get("StandardErrorContent") or "")
                stages = re.findall(r"ANTI_DEMO_STAGE=([a-z_]{1,32})", standard_error)
                stage = f" at stage {stages[-1]}" if stages else ""
                raise RuntimeError(f"Round 5 runner configuration command failed{stage}")
            return str(invocation.get("StandardOutputContent") or "")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            try:
                ssm.cancel_command(
                    CommandId=command_id,
                    InstanceIds=[runner_instance_id],
                )
            finally:
                raise RuntimeError("Round 5 runner configuration command timed out") from None
        time.sleep(min(2.0, remaining))


def _round5_setup_request(
    ssm: Any,
    *,
    runner_instance_id: str,
    payload: dict[str, Any],
    timeout: float = ROUND5_SSM_COMMAND_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    encoded = base64.b64encode(
        gzip.compress(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8"))
    ).decode("ascii")
    output = _run_round5_ssm_command(
        ssm,
        runner_instance_id=runner_instance_id,
        commands=[
            "set -euo pipefail",
            f"/opt/lakebase-anti-demo/round5/run_connection_spike.sh '{encoded}'",
        ],
        timeout=timeout,
    )
    result_lines = [line for line in output.splitlines() if line.startswith("SETUP_RESULT:")]
    settled = f"SETUP_SETTLED:{payload['nonce']}"
    if len(result_lines) != 1 or settled not in output.splitlines():
        raise RuntimeError("Round 5 baseline runner returned no exact settled result")
    try:
        result = json.loads(result_lines[0].split(":", 1)[1])
    except json.JSONDecodeError as exc:
        raise RuntimeError("Round 5 baseline runner returned invalid JSON") from exc
    expected = {
        "protocol": "connection-spike-setup-v1",
        "action": payload["action"],
        "nonce": payload["nonce"],
        "status": "verified",
    }
    if not isinstance(result, dict) or any(
        result.get(key) != value for key, value in expected.items()
    ):
        raise RuntimeError("Round 5 baseline runner result did not match its request")
    return result


def _seal_round5_admin(public_key: str, material: ConnectionMaterial) -> str:
    """Seal short-lived Lakebase OAuth material without persisting plaintext."""
    try:
        from nacl.public import PublicKey, SealedBox

        decoded_key = base64.b64decode(public_key, validate=True)
        plaintext = json.dumps(
            {
                "host": material.host,
                "port": material.port,
                "dbname": material.database,
                "username": material.user,
                "password": material.password,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return base64.b64encode(SealedBox(PublicKey(decoded_key)).encrypt(plaintext)).decode(
            "ascii"
        )
    except Exception:
        raise RuntimeError("Round 5 Lakebase admin credential sealing failed") from None


def _wait_round5_runner_ready(
    session: boto3.Session,
    *,
    runner_instance_id: str,
    timeout: float,
) -> None:
    ssm = session.client("ssm")
    deadline = time.monotonic() + timeout
    while True:
        instances = ssm.describe_instance_information(
            Filters=[{"Key": "InstanceIds", "Values": [runner_instance_id]}]
        ).get("InstanceInformationList", [])
        if (
            len(instances) == 1
            and instances[0].get("InstanceId") == runner_instance_id
            and str(instances[0].get("PingStatus") or "").upper() == "ONLINE"
        ):
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError("Round 5 readiness timed out waiting for the neutral SSM runner")
        time.sleep(min(5.0, remaining))


def _configure_round5_runner(
    session: boto3.Session,
    *,
    runner_instance_id: str,
    expected_harness_sha256: str,
) -> str:
    from .connection_spike_live import RUNNER_PATH, runner_asset_sha256s

    install_root = str(Path(RUNNER_PATH).parent)
    trust_bundle_path = f"{install_root}/round5-ca.pem"
    expected_assets = runner_asset_sha256s()
    _install_round5_runner_assets(session, runner_instance_id=runner_instance_id)
    commands = [
        "set -euo pipefail",
        "test -s /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
        "curl --fail --silent --show-error --location --proto '=https' --tlsv1.2 "
        "https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem "
        f"--output {install_root}/aws-rds-global.pem.tmp",
        f"grep -q -- '-----BEGIN CERTIFICATE-----' {install_root}/aws-rds-global.pem.tmp",
        f"openssl crl2pkcs7 -nocrl -certfile {install_root}/aws-rds-global.pem.tmp "
        "| openssl pkcs7 -print_certs -noout | grep -q '^subject='",
        f"cat /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem "
        f"{install_root}/aws-rds-global.pem.tmp > {trust_bundle_path}.tmp",
        f"chmod 0644 {trust_bundle_path}.tmp",
        f"mv {trust_bundle_path}.tmp {trust_bundle_path}",
        f"rm -f {install_root}/aws-rds-global.pem.tmp",
    ]
    _run_round5_ssm_command(
        session.client("ssm"),
        runner_instance_id=runner_instance_id,
        commands=commands,
        timeout=ROUND5_SSM_COMMAND_TIMEOUT_SECONDS,
    )

    installed_assets, harness_checksum, trust_checksum = _round5_runner_asset_checksums(
        session,
        runner_instance_id=runner_instance_id,
        trust_bundle_path=trust_bundle_path,
    )
    if installed_assets != expected_assets:
        raise RuntimeError("Round 5 runner per-file checksums differ from the local assets")
    if harness_checksum != expected_harness_sha256:
        raise RuntimeError("Round 5 runner checksum differs from the sealed local harness")
    return trust_checksum


def _install_round5_runner_assets(
    session: boto3.Session,
    *,
    runner_instance_id: str,
) -> None:
    """Install only the three versioned runner assets and their Python environment."""
    from .connection_spike_live import RUNNER_PATH

    install_root = str(Path(RUNNER_PATH).parent)
    archive = _round5_runner_archive()
    if len(archive) > ROUND5_RUNNER_SSM_ARCHIVE_MAX_CHARS:
        raise RuntimeError("Round 5 runner bundle exceeds the bounded SSM install payload")
    commands = [
        "set -euo pipefail",
        "stage=bootstrap",
        "trap 'echo ANTI_DEMO_STAGE=$stage >&2' ERR",
        "stage=packages",
        "dnf install -y python3.12",
        "stage=directory",
        f"install -d -m 0755 {install_root}",
        "stage=archive",
        f"printf '%s' '{archive}' | base64 -d | tar -xz -C {install_root}",
        "stage=venv",
        f"python3.12 -m venv {install_root}/venv",
        "stage=dependencies",
        f"{install_root}/venv/bin/pip install --disable-pip-version-check "
        f"--no-cache-dir -r {install_root}/requirements-round5.txt",
        "stage=permissions",
        f"chmod 0755 {RUNNER_PATH} {install_root}/connection_spike_runner.py",
    ]
    _run_round5_ssm_command(
        session.client("ssm"),
        runner_instance_id=runner_instance_id,
        commands=commands,
        timeout=ROUND5_SSM_COMMAND_TIMEOUT_SECONDS,
    )


def _round5_runner_asset_checksums(
    session: boto3.Session,
    *,
    runner_instance_id: str,
    trust_bundle_path: str,
) -> tuple[dict[str, str], str, str]:
    from .connection_spike_live import RUNNER_ASSETS, RUNNER_PATH

    install_root = str(Path(RUNNER_PATH).parent)
    asset_literal = repr(RUNNER_ASSETS)
    # Output is deliberately digest-only. No path, host, account, credential, or
    # provider message is returned to the caller.
    output = _run_round5_ssm_command(
        session.client("ssm"),
        runner_instance_id=runner_instance_id,
        commands=[
            "set -euo pipefail",
            f"{install_root}/venv/bin/python3.12 - <<'PY'",
            "import hashlib, pathlib",
            f"root = pathlib.Path('{install_root}')",
            f"names = {asset_literal}",
            "digest = hashlib.sha256()",
            "for name in names:",
            "    value = (root / name).read_bytes()",
            "    digest.update(name.encode()); digest.update(b'\\0')",
            "    digest.update(value); digest.update(b'\\0')",
            "    print('ASSET=' + name + ':' + hashlib.sha256(value).hexdigest())",
            "print('HARNESS=' + digest.hexdigest())",
            f"print('TRUST=' + hashlib.sha256(pathlib.Path('{trust_bundle_path}')"
            ".read_bytes()).hexdigest())",
            "PY",
        ],
        timeout=ROUND5_SSM_COMMAND_TIMEOUT_SECONDS,
    )
    assets: dict[str, str] = {}
    harness = ""
    trust = ""
    for line in output.splitlines():
        if line.startswith("ASSET="):
            name, separator, checksum = line.removeprefix("ASSET=").partition(":")
            if (
                separator
                and name in RUNNER_ASSETS
                and re.fullmatch(r"[0-9a-f]{64}", checksum)
            ):
                assets[name] = checksum
        elif line.startswith("HARNESS="):
            harness = line.removeprefix("HARNESS=")
        elif line.startswith("TRUST="):
            trust = line.removeprefix("TRUST=")
    if (
        set(assets) != set(RUNNER_ASSETS)
        or not re.fullmatch(r"[0-9a-f]{64}", harness)
        or not re.fullmatch(r"[0-9a-f]{64}", trust)
    ):
        raise RuntimeError("Round 5 secret-free checksum doctor returned invalid output")
    return assets, harness, trust


def _round5_runner_checksums(
    session: boto3.Session,
    *,
    runner_instance_id: str,
    trust_bundle_path: str,
) -> tuple[str, str]:
    _assets, harness, trust = _round5_runner_asset_checksums(
        session,
        runner_instance_id=runner_instance_id,
        trust_bundle_path=trust_bundle_path,
    )
    return harness, trust


def _round5_runner_credential_digests(
    session: boto3.Session,
    *,
    runner_instance_id: str,
) -> dict[str, str]:
    """Hash the runner's baseline credentials with the runner's own code.

    The drift this closes is the one that cost 2026-08-24: a re-seal rotates the
    runner's credentials, the manifest write that should have recorded the new
    digests does not happen, and the installation carries a seal describing files
    that no longer exist. Nothing detected it. Every surface said Round 5 was
    ready, and the only symptom was a lane failing `baseline_auth_hash_invalid`
    after the bell, seven minutes into a bout.

    Comparing the two ends is cheap -- one SSM round trip, no database connection,
    no Aurora wake -- which is why it belongs in `doctor` rather than only in the
    bout that discovers it too late.

    The digest is computed **by importing the installed runner** rather than by
    reimplementing its rules here. `_canonical_json`, `BASELINE_CREDENTIAL_PATHS`
    and the per-lane key sets all belong to that module, and a check that retyped
    them would agree with a stale copy of itself instead of with the code that
    writes the files -- the same hardcoded-vs-hardcoded failure
    `_coordination_runtime_grants` exists to avoid. Renaming any of them breaks
    this loudly instead of silently passing.

    Digests only. No credential value reaches the command, the runner's stdout,
    the app logs or any browser payload, exactly as for the checksum doctor above.
    """

    from .connection_spike_live import RUNNER_PATH

    install_root = str(Path(RUNNER_PATH).parent)
    output = _run_round5_ssm_command(
        session.client("ssm"),
        runner_instance_id=runner_instance_id,
        commands=[
            "set -euo pipefail",
            f"{install_root}/venv/bin/python3.12 - <<'PY'",
            "import hashlib, importlib.util, pathlib, sys",
            f"root = pathlib.Path('{install_root}')",
            "spec = importlib.util.spec_from_file_location(",
            "    'anti_demo_round5_runner', root / 'connection_spike_runner.py'",
            ")",
            "module = importlib.util.module_from_spec(spec)",
            "sys.modules[spec.name] = module",
            "spec.loader.exec_module(module)",
            "for lane in sorted(module.BASELINE_CREDENTIAL_PATHS):",
            "    keys = (",
            "        module.RDS_BASELINE_KEYS",
            "        if lane in module.AWS_CREDENTIAL_IDS",
            "        else module.BASELINE_DATABASE_KEYS",
            "    )",
            "    value = module._read_root_json(module.BASELINE_CREDENTIAL_PATHS[lane], keys)",
            "    encoded = module._canonical_json(value)",
            "    print('DIGEST_' + lane.upper() + '=' + hashlib.sha256(encoded).hexdigest())",
            "PY",
        ],
        timeout=ROUND5_SSM_COMMAND_TIMEOUT_SECONDS,
    )
    digests = {
        key.removeprefix("DIGEST_").lower(): value
        for line in output.splitlines()
        if line.strip().startswith("DIGEST_") and "=" in line
        for key, value in [line.strip().split("=", 1)]
    }
    if not digests or any(
        not re.fullmatch(r"[0-9a-f]{64}", digest) for digest in digests.values()
    ):
        raise RuntimeError("Round 5 secret-free credential doctor returned invalid output")
    return digests


def _required_round5_outputs(outputs: dict[str, Any]) -> dict[str, Any]:
    fields = {
        "aurora_direct_host": "round5_aurora_direct_host",
        "aurora_cluster_id": "aurora_cluster_id",
        "aurora_cluster_resource_id": "round5_aurora_cluster_resource_id",
        "aurora_writer_instance_id": "aurora_writer_instance_id",
        "aurora_master_secret_arn": "aurora_secret_arn",
        "rds_direct_host": "round5_rds_direct_host",
        "rds_master_secret_arn": "rds_secret_arn",
        "rds_resource_id": "round5_rds_resource_id",
        "vpc_id": "vpc_id",
        "proxy_subnet_ids": "subnet_ids",
        "control_role_arn": "round5_control_role_arn",
        "control_role_trusted_principal_arn": "round5_app_principal_arn",
        "proxy_service_role_arn": "round5_proxy_service_role_arn",
        "proxy_service_policy_name": "round5_proxy_service_policy_name",
        "aurora_proxy_secret_arn": "round5_aurora_proxy_secret_arn",
        "rds_proxy_secret_arn": "round5_rds_proxy_secret_arn",
        "runner_permissions_boundary_arn": "round5_runner_permissions_boundary_arn",
        "runner_instance_id": "round5_runner_instance_id",
        "runner_instance_profile_arn": "round5_runner_instance_profile_arn",
        "runner_role_arn": "round5_runner_role_arn",
        "runner_subnet_id": "round5_runner_subnet_id",
        "runner_security_group_id": "round5_runner_security_group_id",
        "runner_egress_rule_id": "round5_runner_egress_rule_id",
        "bout_name_prefix": "round5_bout_name_prefix",
        "ownership_tags": "round5_bout_base_tags",
    }
    values: dict[str, Any] = {field: outputs.get(key) for field, key in fields.items()}
    if outputs.get("round_installation_slugs"):
        round5_maps = {
            "aurora_cluster_id": "aurora_cluster_ids",
            "aurora_writer_instance_id": "aurora_writer_instance_ids",
            "aurora_master_secret_arn": "aurora_secret_arns",
            "rds_master_secret_arn": "rds_secret_arns",
        }
        for field, output_name in round5_maps.items():
            round_values = outputs.get(output_name)
            values[field] = round_values.get("r5") if isinstance(round_values, dict) else None
    missing = [field for field, value in values.items() if value in (None, "", {}, [])]
    if missing:
        raise RuntimeError(
            "Terraform state is missing required Round 5 baseline outputs: " + ", ".join(missing)
        )
    for field in fields:
        if field not in {"ownership_tags", "proxy_subnet_ids"}:
            values[field] = str(values[field])
    values["proxy_subnet_ids"] = tuple(str(item) for item in values["proxy_subnet_ids"])
    return values


def _round5_aurora_cluster_resource_id(
    manifest: DemoManifest,
    rds: Any,
    *,
    direct_host: str,
    cluster_id: str,
    writer_instance_id: str,
    master_secret_arn: str,
    expected_resource_id: str | None = None,
) -> str:
    """Resolve and verify the exact Terraform-owned Aurora writer identity."""
    top_level = manifest.aws.resources
    if manifest.round_environments is not None:
        round5 = manifest.round_environment(5)
        if round5.aurora is None:
            raise RuntimeError("Round 5 Aurora environment is not sealed")
        expected_cluster_id = round5.aurora.cluster_id
        expected_writer_id = round5.aurora.writer_instance_id
        expected_secret_arn = round5.aurora.secret_arn
    else:
        expected_cluster_id = top_level.aurora_cluster_id
        expected_writer_id = top_level.aurora_writer_instance_id
        expected_secret_arn = top_level.aurora_secret_arn
    if (
        cluster_id != expected_cluster_id
        or writer_instance_id != expected_writer_id
        or master_secret_arn != expected_secret_arn
    ):
        raise RuntimeError("Round 5 Aurora outputs differ from the owned AWS resources")
    clusters = rds.describe_db_clusters(DBClusterIdentifier=cluster_id).get("DBClusters", [])
    if len(clusters) != 1:
        raise RuntimeError("Round 5 Aurora cluster did not resolve exactly once")
    cluster = clusters[0]
    resource_id = str(cluster.get("DbClusterResourceId") or "")
    members = cluster.get("DBClusterMembers") or []
    if (
        cluster.get("DBClusterIdentifier") != cluster_id
        or str(cluster.get("Status") or "").lower() != "available"
        or str(cluster.get("Endpoint") or "") != direct_host
        or str((cluster.get("MasterUserSecret") or {}).get("SecretArn") or "") != master_secret_arn
        or not resource_id
        or (expected_resource_id is not None and resource_id != expected_resource_id)
        or len(members) != 1
        or members[0].get("DBInstanceIdentifier") != writer_instance_id
        or members[0].get("IsClusterWriter") is not True
    ):
        raise RuntimeError("Round 5 Aurora cluster identity differs from the seal")
    writers = rds.describe_db_instances(DBInstanceIdentifier=writer_instance_id).get(
        "DBInstances", []
    )
    if len(writers) != 1:
        raise RuntimeError("Round 5 Aurora writer did not resolve exactly once")
    writer = writers[0]
    if (
        writer.get("DBInstanceIdentifier") != writer_instance_id
        or writer.get("DBClusterIdentifier") != cluster_id
        or str(writer.get("DBInstanceStatus") or "").lower() != "available"
    ):
        raise RuntimeError("Round 5 Aurora writer identity differs from the seal")
    return resource_id


def _canonical_iam_policy(document: Any) -> str:
    """Canonicalize a complete IAM policy without discarding any statement."""
    if isinstance(document, str):
        try:
            document = json.loads(unquote(document))
        except (TypeError, ValueError) as exc:
            raise RuntimeError("IAM trust policy is not valid JSON") from exc
    if not isinstance(document, dict):
        raise RuntimeError("IAM trust policy is not a document")

    def normalize(value: Any, *, key: str = "") -> Any:
        if key in {"Action", "NotAction", "Resource", "NotResource"}:
            members = value if isinstance(value, list) else [value]
            return sorted(normalize(member) for member in members)
        if key == "Principal" and isinstance(value, dict):
            return {
                principal_type: sorted(
                    normalize(member)
                    for member in (principals if isinstance(principals, list) else [principals])
                )
                for principal_type, principals in sorted(value.items())
            }
        if isinstance(value, dict):
            return {
                child_key: normalize(child_value, key=child_key)
                for child_key, child_value in sorted(value.items())
            }
        if isinstance(value, list):
            normalized = [normalize(member) for member in value]
            return sorted(
                normalized,
                key=lambda member: json.dumps(member, sort_keys=True, separators=(",", ":")),
            )
        return value

    return json.dumps(normalize(document), sort_keys=True, separators=(",", ":"))


def _iam_trust_principals(document: Any) -> tuple[str, ...]:
    """Every AWS principal a live `sts:AssumeRole` trust document actually names.

    Returned verbatim, which is the whole point. IAM stores a named principal by
    its unique ID and reverse-maps that ID back to an ARN only while the
    principal exists, so the *strings in this list* are the one place the
    delete-and-recreate break becomes visible. Canonicalising them, resolving
    them, or comparing them by name would all destroy the evidence.
    """

    if isinstance(document, str):
        try:
            document = json.loads(unquote(document))
        except (TypeError, ValueError) as exc:
            raise RuntimeError("IAM trust policy is not valid JSON") from exc
    if not isinstance(document, dict):
        raise RuntimeError("IAM trust policy is not a document")
    statements = document.get("Statement")
    statements = statements if isinstance(statements, list) else [statements]
    principals: list[str] = []
    for statement in statements:
        if not isinstance(statement, dict) or statement.get("Effect") != "Allow":
            continue
        actions = statement.get("Action")
        actions = actions if isinstance(actions, list) else [actions]
        if not any(str(action) == "sts:AssumeRole" for action in actions):
            continue
        entry = statement.get("Principal")
        if not isinstance(entry, dict):
            continue
        aws = entry.get("AWS")
        if aws is None:
            continue
        principals.extend(str(item) for item in (aws if isinstance(aws, list) else [aws]))
    return tuple(principals)


def _iam_principal_exists(iam: Any, arn: str) -> bool:
    name = arn.rsplit("/", 1)[-1]
    try:
        if ":user/" in arn:
            iam.get_user(UserName=name)
        else:
            iam.get_role(RoleName=name)
    except ClientError as exc:
        if str((exc.response or {}).get("Error", {}).get("Code") or "") == "NoSuchEntity":
            return False
        raise
    return True


_RUNTIME_TRUST_UNRESOLVED = (
    "THE SEALED RUNTIME ROLE NO LONGER TRUSTS WHAT IT SAYS IT TRUSTS. Its trust policy "
    "holds {orphans}, which is a bare IAM unique principal ID rather than an ARN. IAM "
    "shows that only when the principal the policy named has been deleted -- it stores "
    "the unique ID, not the ARN, so a principal recreated with an identical name and an "
    "identical ARN is a different principal to this policy and cannot assume the role. "
    "Nothing that compares names can see this: the credential probe reports 'ok', "
    "/readyz stays clean, the catalog offers Round 5, and the AssumeRole fails after the "
    "bell. Run 'antidemo renew' to re-apply the trust policy; it re-resolves the ARN to the "
    "new unique ID and does not touch the seal."
)


def _anti_demo_runtime_trust_check(manifest: DemoManifest) -> Check:
    """Read the live trust policy of the sealed runtime role and believe only it.

    Deliberately not a comparison of one name against another. `principal_matches`
    already does that, correctly, and it is precisely why the fortnightly sweep's
    delete-and-recreate of the app's IAM user is invisible to every automated
    surface in this project: the recreated user has the same name and the same
    ARN. The only observable difference lives in the trust document AWS actually
    holds, so this reads that document and nothing else.
    """

    sealed_role = manifest.aws.runtime_role_arn
    sealed_principals = manifest.aws.runtime_role_trusted_principal_arns
    if sealed_role is None or sealed_principals is None:
        return Check(
            "anti_demo_runtime_trust",
            True,
            "not installed: this installation seals no runtime role, so both the operator "
            "and the deployed app authenticate as themselves",
            advisory=True,
        )
    try:
        iam = _aws_session(manifest).client("iam")
        role = iam.get_role(RoleName=sealed_role.rsplit("/", 1)[-1]).get("Role") or {}
        if role.get("Arn") != sealed_role:
            raise RuntimeError(
                f"the runtime role resolves to {role.get('Arn')!r}, not the sealed {sealed_role}"
            )
        live = _iam_trust_principals(role.get("AssumeRolePolicyDocument"))
        orphans = sorted(item for item in live if _IAM_UNIQUE_PRINCIPAL_ID.fullmatch(item))
        if orphans:
            raise RuntimeError(_RUNTIME_TRUST_UNRESOLVED.format(orphans=", ".join(orphans)))
        missing = sorted(set(sealed_principals) - set(live))
        if missing:
            gone = [arn for arn in missing if not _iam_principal_exists(iam, arn)]
            raise RuntimeError(
                f"the runtime role no longer trusts {', '.join(missing)}"
                + (
                    f"; {', '.join(gone)} does not exist in IAM at all, which is what the "
                    "fortnightly sweep looks like"
                    if gone
                    else ""
                )
                + ". Run 'antidemo renew' to re-apply the sealed trust policy."
            )
        widened = sorted(set(live) - set(sealed_principals))
        if widened:
            raise RuntimeError(
                f"the runtime role trusts {', '.join(widened)}, which this installation never "
                "sealed. Someone widened who can assume this installation's principal outside "
                "Terraform. Run 'antidemo renew' to restore the sealed trust policy."
            )
        return Check(
            "anti_demo_runtime_trust",
            True,
            f"{sealed_role.rsplit('/', 1)[-1]} trusts exactly {len(live)} sealed "
            f"principal{'s' if len(live) != 1 else ''}, all resolving",
        )
    except Exception as exc:
        return Check("anti_demo_runtime_trust", False, str(exc))


def _round5_topology_check(
    manifest: DemoManifest,
    resources: Round5Resources | None = None,
) -> Check:
    sealed = resources or (manifest.round5 if manifest.round5_ready else None)
    if not isinstance(sealed, Round5Resources):
        return Check("round5_secret_free_topology", False, "manifest has no Round 5 seal")
    try:
        round5_binding = _round_lakebase_binding(manifest, 5)
        project = (
            _get_lakebase_project_or_none(manifest, project_id=round5_binding.project_id)
            if manifest.round_environments is not None
            else _get_lakebase_project_or_none(manifest)
        )
        if (
            project is None
            or (project.get("status") or {}).get("enable_pg_native_login") is not True
        ):
            raise RuntimeError("Lakebase native login is not enabled")
        direct_host, pooled_host = _round5_lakebase_hosts(manifest)
        if direct_host != sealed.lakebase_direct_host or pooled_host != sealed.lakebase_pooled_host:
            raise RuntimeError("Lakebase hosts differ from the sealed control-plane values")

        session = _aws_session(manifest)
        rds = session.client("rds")
        _round5_aurora_cluster_resource_id(
            manifest,
            rds,
            direct_host=str(sealed.aurora_direct_host),
            cluster_id=str(sealed.aurora_cluster_id),
            writer_instance_id=str(sealed.aurora_writer_instance_id),
            master_secret_arn=str(sealed.aurora_master_secret_arn),
            expected_resource_id=str(sealed.aurora_cluster_resource_id),
        )
        round5_rds_id = (
            manifest.round_environment(5).rds.instance_id
            if manifest.round_environments is not None
            and manifest.round_environment(5).rds is not None
            else manifest.aws.resources.rds_instance_id
        )
        databases = rds.describe_db_instances(DBInstanceIdentifier=round5_rds_id).get(
            "DBInstances", []
        )
        if len(databases) != 1:
            raise RuntimeError("Round 5 direct RDS instance did not resolve exactly once")
        database = databases[0]
        if (
            str((database.get("Endpoint") or {}).get("Address") or "") != sealed.rds_direct_host
            or str(database.get("DbiResourceId") or "") != sealed.rds_resource_id
            or str((database.get("MasterUserSecret") or {}).get("SecretArn") or "")
            != sealed.rds_master_secret_arn
        ):
            raise RuntimeError("Round 5 direct RDS identity differs from the seal")
        database_subnets = tuple(
            sorted(
                str(item.get("SubnetIdentifier") or "")
                for item in (database.get("DBSubnetGroup") or {}).get("Subnets", [])
            )
        )
        if database_subnets != tuple(sorted(sealed.proxy_subnet_ids)):
            raise RuntimeError("Round 5 RDS subnets differ from the baseline seal")
        parameter_groups = database.get("DBParameterGroups") or []
        if (
            str(database.get("DBInstanceStatus") or "").lower() != "available"
            or len(parameter_groups) != 1
            or parameter_groups[0].get("DBParameterGroupName") != "default.postgres17"
            or str(parameter_groups[0].get("ParameterApplyStatus") or "").lower() != "in-sync"
        ):
            raise RuntimeError("RDS source is not on default.postgres17, available, and in-sync")

        ec2 = session.client("ec2")
        instances = ec2.describe_instances(InstanceIds=[sealed.runner_instance_id]).get(
            "Reservations", []
        )
        runners = [
            instance for reservation in instances for instance in reservation.get("Instances", [])
        ]
        if len(runners) != 1:
            raise RuntimeError("Round 5 runner did not resolve exactly once")
        runner = runners[0]
        groups = [item.get("GroupId") for item in runner.get("SecurityGroups", [])]
        if (
            runner.get("InstanceId") != sealed.runner_instance_id
            or (runner.get("State") or {}).get("Name") != "running"
            or runner.get("InstanceType") != "m6i.large"
            or not runner.get("PublicIpAddress")
            or runner.get("SubnetId") != sealed.runner_subnet_id
            or runner.get("VpcId") != sealed.vpc_id
            or groups != [sealed.runner_security_group_id]
            or (runner.get("IamInstanceProfile") or {}).get("Arn")
            != sealed.runner_instance_profile_arn
            or (runner.get("MetadataOptions") or {}).get("HttpTokens") != "required"
        ):
            raise RuntimeError("Round 5 runner topology differs from the sealed contract")
        runner_groups = ec2.describe_security_groups(
            GroupIds=[sealed.runner_security_group_id]
        ).get("SecurityGroups", [])
        if len(runner_groups) != 1 or runner_groups[0].get("IpPermissions"):
            raise RuntimeError("Round 5 runner security group permits inbound traffic")
        rules = ec2.describe_security_group_rules(
            Filters=[
                {
                    "Name": "group-id",
                    "Values": [sealed.runner_security_group_id],
                }
            ]
        ).get("SecurityGroupRules", [])
        if len(rules) != 1 or not (
            rules[0].get("SecurityGroupRuleId") == sealed.runner_egress_rule_id
            and rules[0].get("IsEgress") is True
            and rules[0].get("IpProtocol") == "-1"
            and rules[0].get("CidrIpv4") == "0.0.0.0/0"
        ):
            raise RuntimeError("Round 5 runner egress differs from the baseline seal")

        iam = session.client("iam")
        runner_role_name = sealed.runner_role_arn.rsplit("/", 1)[-1]
        runner_role = iam.get_role(RoleName=runner_role_name).get("Role") or {}
        if (
            runner_role.get("Arn") != sealed.runner_role_arn
            or (runner_role.get("PermissionsBoundary") or {}).get("PermissionsBoundaryArn")
            != sealed.runner_permissions_boundary_arn
            or _canonical_iam_policy(runner_role.get("AssumeRolePolicyDocument"))
            != _canonical_iam_policy(sealed.runner_trust_policy)
        ):
            raise RuntimeError("Round 5 runner role, trust, or boundary differs from the seal")
        profile_name = sealed.runner_instance_profile_arn.rsplit("/", 1)[-1]
        profile = (
            iam.get_instance_profile(InstanceProfileName=profile_name).get("InstanceProfile") or {}
        )
        if profile.get("Arn") != sealed.runner_instance_profile_arn or [
            role.get("Arn") for role in profile.get("Roles", [])
        ] != [sealed.runner_role_arn]:
            raise RuntimeError("Round 5 runner instance profile differs from the seal")
        control_name = sealed.control_role_arn.rsplit("/", 1)[-1]
        control_role = iam.get_role(RoleName=control_name).get("Role") or {}
        if control_role.get("Arn") != sealed.control_role_arn or _canonical_iam_policy(
            control_role.get("AssumeRolePolicyDocument")
        ) != _canonical_iam_policy(sealed.control_trust_policy):
            raise RuntimeError("Round 5 control role or trust differs from the seal")
        if (
            iam.get_policy(PolicyArn=sealed.runner_permissions_boundary_arn).get("Policy") or {}
        ).get("Arn") != sealed.runner_permissions_boundary_arn:
            raise RuntimeError("Round 5 runner permissions boundary differs from the seal")

        proxy_role_arn = str(sealed.proxy_service_role_arn)
        proxy_role_name = proxy_role_arn.rsplit("/", 1)[-1]
        proxy_role = iam.get_role(RoleName=proxy_role_name).get("Role") or {}
        proxy_role_tags = {
            str(tag.get("Key") or ""): str(tag.get("Value") or "")
            for tag in proxy_role.get("Tags", [])
        }
        expected_proxy_role_tags = _required_tags_for_address(
            manifest, "aws_iam_role.round5_proxy_service"
        )
        if (
            proxy_role.get("Arn") != proxy_role_arn
            or _canonical_iam_policy(proxy_role.get("AssumeRolePolicyDocument"))
            != _canonical_iam_policy(sealed.proxy_service_trust_policy)
            or any(
                proxy_role_tags.get(key) != value for key, value in expected_proxy_role_tags.items()
            )
        ):
            raise RuntimeError("Round 5 static Proxy service role differs from the seal")
        proxy_policy_name = str(sealed.proxy_service_policy_name)
        if iam.list_role_policies(RoleName=proxy_role_name).get("PolicyNames", []) != [
            proxy_policy_name
        ]:
            raise RuntimeError("Round 5 static Proxy role policy identity differs from the seal")
        proxy_policy = iam.get_role_policy(
            RoleName=proxy_role_name,
            PolicyName=proxy_policy_name,
        )
        expected_proxy_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "secretsmanager:DescribeSecret",
                        "secretsmanager:GetSecretValue",
                    ],
                    "Resource": [
                        str(sealed.aurora_proxy_secret_arn),
                        str(sealed.rds_proxy_secret_arn),
                    ],
                }
            ],
        }
        if _canonical_iam_policy(proxy_policy.get("PolicyDocument")) != _canonical_iam_policy(
            expected_proxy_policy
        ):
            raise RuntimeError("Round 5 static Proxy role policy differs from the seal")

        secrets_manager = session.client("secretsmanager")
        expected_secret_tags = _required_tags(manifest)
        for lane_id, secret_arn in (
            ("aurora", str(sealed.aurora_proxy_secret_arn)),
            ("rds", str(sealed.rds_proxy_secret_arn)),
        ):
            secret = secrets_manager.describe_secret(SecretId=secret_arn)
            secret_tags = {
                str(tag.get("Key") or ""): str(tag.get("Value") or "")
                for tag in secret.get("Tags", [])
            }
            version_stages = secret.get("VersionIdsToStages") or {}
            has_current = any(
                "AWSCURRENT" in stages
                for stages in version_stages.values()
                if isinstance(stages, list)
            )
            if (
                secret.get("ARN") != secret_arn
                or not has_current
                or any(secret_tags.get(key) != value for key, value in expected_secret_tags.items())
            ):
                raise RuntimeError(f"Round 5 static {lane_id} Proxy secret differs from the seal")
        managed = (
            session.client("ssm")
            .describe_instance_information(
                Filters=[{"Key": "InstanceIds", "Values": [sealed.runner_instance_id]}]
            )
            .get("InstanceInformationList", [])
        )
        if (
            len(managed) != 1
            or managed[0].get("InstanceId") != sealed.runner_instance_id
            or str(managed[0].get("PingStatus") or "").upper() != "ONLINE"
        ):
            raise RuntimeError("Round 5 runner is not online in SSM")
        harness, trust = _round5_runner_checksums(
            session,
            runner_instance_id=sealed.runner_instance_id,
            trust_bundle_path=sealed.trust_bundle_path,
        )
        if harness != sealed.harness_sha256 or trust != sealed.trust_bundle_sha256:
            raise RuntimeError("Round 5 runner or trust-bundle checksum differs from v5")
        digests = _round5_runner_credential_digests(
            session, runner_instance_id=sealed.runner_instance_id
        )
        # `getattr` without a default on purpose: if a sealed field is ever
        # renamed, this raises rather than quietly comparing nothing.
        drifted = sorted(
            lane
            for lane, digest in digests.items()
            if digest != getattr(sealed, f"{lane}_credential_sha256")
        )
        if drifted:
            raise RuntimeError(
                "Round 5 sealed credential digests differ from the runner's baseline "
                f"files for {', '.join(drifted)}: the seal names credentials the runner "
                "has already replaced, so those lanes fail baseline_auth_hash_invalid "
                "after the bell. Re-seal Round 5 to record what is on disk."
            )
        _require_round5_tags_the_control_role_allows(iam, sealed)
        _require_round5_clean_baseline(manifest)
        return Check(
            "round5_secret_free_topology",
            True,
            "clean baseline: Lakebase pooled host and Aurora/RDS sources are prepared",
        )
    except Exception as exc:
        return Check("round5_secret_free_topology", False, str(exc))


def _require_round5_tags_the_control_role_allows(iam: Any, sealed: Round5Resources) -> None:
    """Refuse a seal whose per-bout tags its own control role would deny.

    The companion to the credential-digest comparison above, and the same shape
    of fault: two ends that have to agree, one of which can move without the
    other noticing. `renew` re-applies `round5_control.tf`, which conditions
    `ec2:CreateTags` on `security-group-rule/*` on the exact `expires-at` --
    among other tags -- that Terraform was given. Nothing re-derived the sealed
    copy, so after a renewal the manifest tagged every per-bout rule with the
    superseded expiry and the grant became an implicit deny.

    What that looked like is why this is worth an IAM read: the bout armed, the
    per-bout security group was created, and the third journaled mutation failed
    with `UnauthorizedOperation ... no identity-based policy allows the
    ec2:CreateTags action`, recorded as the bare string `provider_create_failed`
    and surfaced to the screen as "The Round 5 setup phase failed". Read-only,
    one `GetRolePolicy`, and it names the drifted tag instead.

    Only `StringEquals` on `aws:RequestTag/*` is compared, because that is the
    test that has to match a value exactly. `Null`, `ForAllValues` and the rest
    constrain presence or shape rather than content, and asserting a reading of
    them here would be this check inventing policy semantics it does not own.
    """

    role_name = sealed.control_role_arn.rsplit("/", 1)[-1]
    sent = sealed.ownership_tags.as_aws_tags()
    demanded: dict[str, str] = {}
    for policy_name in iam.list_role_policies(RoleName=role_name).get("PolicyNames", []):
        # IAM returns the document percent-encoded over the wire, and already
        # decoded through some clients. Accept both rather than assume one.
        document = iam.get_role_policy(RoleName=role_name, PolicyName=policy_name).get(
            "PolicyDocument"
        )
        if isinstance(document, str):
            document = json.loads(unquote(document))
        if not isinstance(document, dict):
            raise RuntimeError("Round 5 control role policy is not a document")
        for statement in document.get("Statement") or []:
            actions = statement.get("Action") or []
            if isinstance(actions, str):
                actions = [actions]
            resources = statement.get("Resource") or []
            if isinstance(resources, str):
                resources = [resources]
            if statement.get("Effect") != "Allow" or "ec2:CreateTags" not in actions:
                continue
            if not any(str(item).endswith(":security-group-rule/*") for item in resources):
                continue
            equals = (statement.get("Condition") or {}).get("StringEquals") or {}
            for key, value in equals.items():
                if str(key).startswith("aws:RequestTag/") and isinstance(value, str):
                    demanded[str(key).removeprefix("aws:RequestTag/")] = value

    refused = sorted(key for key, value in demanded.items() if sent.get(key) != value)
    if refused:
        detail = ", ".join(
            f"{key}: seal sends {sent.get(key)!r}, policy allows {demanded[key]!r}"
            for key in refused
        )
        raise RuntimeError(
            "Round 5 per-bout ownership tags are not the ones its control role allows "
            f"({detail}): every per-bout security-group rule would be refused at "
            "ec2:CreateTags, so the bout dies in setup rather than reaching its stop "
            "gate. Re-seal Round 5 to record the tags the applied policy expects."
        )


def _require_round5_runner_idle(manifest: DemoManifest) -> None:
    """Fail cleanup closed while a bounded Round 5 command still owns the runner."""
    if not manifest.round5_ready:
        return
    round5 = manifest.require_round5_resources()
    output = _run_round5_ssm_command(
        _aws_session(manifest).client("ssm"),
        runner_instance_id=round5.runner_instance_id,
        commands=[
            "set -euo pipefail",
            "flock -n /run/lock/lakebase-anti-demo-round5.lock -c 'echo RUNNER_IDLE'",
        ],
        timeout=ROUND5_SSM_COMMAND_TIMEOUT_SECONDS,
    )
    if output.strip() != "RUNNER_IDLE":
        raise RuntimeError("Cleanup refused: the Round 5 runner lock is not free")


def _reseal_round5_harness(sealed: Round5Resources, harness_sha256: str) -> Round5Resources:
    """Return a canonical v5 seal for an installed, verified runner harness."""
    return _reseal_round5(sealed, harness_sha256=harness_sha256)


def _round5_runner_refresh_session(manifest: DemoManifest) -> boto3.Session:
    """Reach the sealed Round 5 control role from the verified ambient principal."""
    from .connection_spike_live import (
        _control_role_source_session,
        connection_spike_live_config_from_manifest,
    )

    config = connection_spike_live_config_from_manifest(
        manifest,
        "aurora_serverless_v2",
    )
    ambient = _aws_session(manifest)
    identity = ambient.client("sts", region_name=config.region).get_caller_identity()
    account = str(identity.get("Account") or "")
    principal = str(identity.get("Arn") or "")
    if account != config.expected_account_id or f"::{account}:" not in principal:
        raise RuntimeError(
            "Runner refresh refused: the ambient AWS principal is not in the sealed account"
        )
    try:
        source = _control_role_source_session(
            lambda **_kwargs: ambient,
            region=config.region,
            expected_account_id=config.expected_account_id,
            runtime_role_arn=config.runtime_role_arn,
            session_name="anti-demo-runner-refresh-runtime",
        )
        response = source.client("sts", region_name=config.region).assume_role(
            RoleArn=config.execution_role_arn,
            RoleSessionName="anti-demo-runner-refresh",
            DurationSeconds=900,
        )
    except ClientError:
        # FE sandbox administrators may be sealed as trusted operators while an
        # independently swept/recreated role chain is temporarily unavailable.
        # Their SSO role is already bounded to the sealed account and region, so
        # use it directly rather than forcing a broad reset merely to repair the
        # runner files. Ordinary IAM users do not get this fallback.
        assumed_role = re.fullmatch(
            rf"arn:aws:sts::{re.escape(account)}:assumed-role/([^/]+)/[^/]+",
            principal,
        )
        trusted_role_names = {
            arn.rsplit("/", 1)[-1]
            for arn in (manifest.aws.runtime_role_trusted_principal_arns or ())
            if ":role/" in arn
        }
        if (
            assumed_role is None
            or assumed_role.group(1) not in trusted_role_names
            or "sandbox-admin" not in assumed_role.group(1)
        ):
            raise RuntimeError(
                "Runner refresh refused: the sealed role chain denied AssumeRole "
                "(category=runner_role_chain_denied)"
            ) from None
        print(
            "RUNNER using the sealed sandbox administrator directly because the "
            "least-privilege role chain denied AssumeRole",
            flush=True,
        )
        return ambient
    credentials = response.get("Credentials") or {}
    required = ("AccessKeyId", "SecretAccessKey", "SessionToken")
    if any(not credentials.get(key) for key in required):
        raise RuntimeError("Runner refresh refused: the sealed control role returned no session")
    assumed = boto3.Session(
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
        region_name=config.region,
    )
    assumed_identity = assumed.client("sts", region_name=config.region).get_caller_identity()
    if str(assumed_identity.get("Account") or "") != config.expected_account_id:
        raise RuntimeError("Runner refresh refused: the control role left the sealed account")
    return assumed


def _round5_refresh_ring_keys(manifest: DemoManifest) -> list[tuple[str, str]]:
    from .coordination import ROUND5_RING_KEY, round_ring_key

    if manifest.manifest_version != 7 or not manifest.installation_id:
        return [(ROUND5_RING_KEY, "Round 5")]
    return [
        (
            round_ring_key(
                manifest.installation_id,
                RoundId.SURVIVE_CONNECTION_SPIKE.value,
            ),
            "Round 5 main ring",
        ),
        (
            round_ring_key(
                manifest.installation_id,
                RoundId.SURVIVE_CONNECTION_SPIKE.value,
                cleanup=True,
            ),
            "Round 5 cleanup ring",
        ),
    ]


def _verify_round5_runner_instance(
    manifest: DemoManifest,
    session: boto3.Session,
) -> None:
    sealed = manifest.require_round5_resources()
    reservations = session.client("ec2", region_name=manifest.aws.region).describe_instances(
        InstanceIds=[sealed.runner_instance_id]
    ).get("Reservations", [])
    instances = [
        instance for reservation in reservations for instance in reservation.get("Instances", [])
    ]
    if len(instances) != 1:
        raise RuntimeError("Runner refresh refused: the sealed EC2 runner did not resolve once")
    instance = instances[0]
    availability_zone = str((instance.get("Placement") or {}).get("AvailabilityZone") or "")
    profile_arn = str((instance.get("IamInstanceProfile") or {}).get("Arn") or "")
    if (
        instance.get("InstanceId") != sealed.runner_instance_id
        or (instance.get("State") or {}).get("Name") != "running"
        or not availability_zone.startswith(manifest.aws.region)
        or profile_arn != sealed.runner_instance_profile_arn
    ):
        raise RuntimeError(
            "Runner refresh refused: the EC2 runner no longer matches its sealed "
            "account, region, state, or instance profile"
        )


def _refresh_round5_runner_locked(
    manifest: DemoManifest,
    session: boto3.Session,
    *,
    commit_allowed: Callable[[], bool] = lambda: True,
) -> DemoManifest:
    """Install, verify, then atomically advance only the runner-related seal."""
    from .connection_spike_live import runner_asset_sha256s, runner_harness_sha256

    sealed = manifest.require_round5_resources()
    source_assets = runner_asset_sha256s()
    source_harness = runner_harness_sha256()
    try:
        installed_assets, installed_harness, installed_trust = (
            _round5_runner_asset_checksums(
                session,
                runner_instance_id=sealed.runner_instance_id,
                trust_bundle_path=sealed.trust_bundle_path,
            )
        )
    except Exception:
        # A missing/corrupt runner file is the main condition this command exists
        # to repair. The post-install read below remains mandatory and fail-closed.
        installed_assets, installed_harness, installed_trust = {}, "", ""
    aligned = (
        installed_assets == source_assets
        and installed_harness == source_harness
        and installed_trust == sealed.trust_bundle_sha256
    )
    if not aligned:
        try:
            _install_round5_runner_assets(
                session,
                runner_instance_id=sealed.runner_instance_id,
            )
        except Exception as exc:
            stage = re.search(r" at stage ([a-z_]{1,32})", str(exc))
            stage_detail = f"; stage={stage.group(1)}" if stage else ""
            raise RuntimeError(
                "Runner refresh failed during the bounded SSM install; "
                "the manifest seal was not changed "
                f"(category=runner_install_failed{stage_detail})"
            ) from None
        try:
            installed_assets, installed_harness, installed_trust = (
                _round5_runner_asset_checksums(
                    session,
                    runner_instance_id=sealed.runner_instance_id,
                    trust_bundle_path=sealed.trust_bundle_path,
                )
            )
        except Exception:
            raise RuntimeError(
                "Runner refresh could not verify the installed files; "
                "the manifest seal was not changed (category=runner_verify_failed)"
            ) from None
    if installed_assets != source_assets or installed_harness != source_harness:
        raise RuntimeError(
            "Runner refresh installed bytes that differ from source; "
            "the manifest seal was not changed (category=runner_hash_mismatch)"
        )
    if installed_trust != sealed.trust_bundle_sha256:
        raise RuntimeError(
            "Runner refresh found trust-bundle drift; the manifest seal was not changed "
            "(category=runner_trust_mismatch)"
        )
    if runner_asset_sha256s() != source_assets or runner_harness_sha256() != source_harness:
        raise RuntimeError(
            "Runner source changed during refresh; the manifest seal was not changed "
            "(category=runner_source_changed)"
        )
    if not commit_allowed():
        raise RuntimeError(
            "Runner refresh lost its Round 5 fence before commit; "
            "the manifest seal was not changed (category=runner_fence_lost)"
        )
    candidate = manifest.model_copy(deep=True)
    candidate.round5 = _reseal_round5_harness(sealed, source_harness)
    candidate.status = manifest.status
    try:
        save_manifest(candidate)
    except Exception:
        raise RuntimeError(
            "Runner refresh verified EC2 but could not atomically save the new seal "
            "(category=runner_seal_write_failed); run './antidemo runner refresh' again"
        ) from None
    print(f"RUNNER source   sha256:{source_harness}", flush=True)
    print(f"RUNNER installed sha256:{installed_harness}", flush=True)
    sealed_harness = candidate.require_round5_resources().harness_sha256
    print(f"RUNNER sealed    sha256:{sealed_harness}", flush=True)
    return candidate


async def _refresh_round5_runner_under_fence(
    manifest: DemoManifest,
    session: boto3.Session,
    *,
    timeout_seconds: float,
) -> DemoManifest:
    from .coordination import LeaseHeldError, build_lease_store
    from .models import BoutOperator, SessionState

    apply_manifest_environment(manifest)
    ring_keys = _round5_refresh_ring_keys(manifest)
    primary = build_lease_store(ring_key=ring_keys[0][0])
    await primary.initialize()
    rings: list[tuple[Any, str]] = [(primary, ring_keys[0][1])]
    for ring_key, label in ring_keys[1:]:
        sibling = primary.for_ring_key(ring_key)
        await sibling.initialize()
        rings.append((sibling, label))
    operator = BoutOperator(
        display_name=manifest.owner.split("@", 1)[0].replace(".", " ").title()
        or "Demo operator",
        email=manifest.owner if "@" in manifest.owner else None,
        subject=f"maintenance:{manifest.owner.casefold()}",
    )
    ttl = timedelta(seconds=max(90.0, min(timeout_seconds + 30.0, 900.0)))
    held: list[tuple[Any, Any]] = []
    stop = asyncio.Event()
    fence_lost = threading.Event()
    try:
        for store, label in rings:
            try:
                lease = await store.claim(
                    session_id=f"maintenance-runner-refresh-{manifest.run_id}",
                    operator=operator,
                    phase="maintenance_runner_refresh",
                    session_state=SessionState.RUNNING,
                    round_id="maintenance_runner_refresh",
                    round_title="Round 5 runner refresh",
                    competitor_id="all",
                    competitor_name="Round 5 runner",
                    ttl=ttl,
                )
            except LeaseHeldError as exc:
                owner = exc.lease.operator.email or exc.lease.operator.display_name
                raise RuntimeError(
                    f"Runner refresh refused: {label} is active; {owner} owns "
                    f"phase {exc.lease.phase}"
                ) from None
            held.append((store, lease))

        async def heartbeat() -> None:
            try:
                while not stop.is_set():
                    await asyncio.sleep(15)
                    for index, (store, lease) in enumerate(held):
                        held[index] = (store, await store.renew(lease, ttl=ttl))
            except BaseException:
                fence_lost.set()
                raise

        heartbeat_task = asyncio.create_task(heartbeat(), name="runner-refresh-heartbeat")
        operation_task = asyncio.create_task(
            asyncio.to_thread(
                _refresh_round5_runner_locked,
                manifest,
                session,
                commit_allowed=lambda: not fence_lost.is_set(),
            ),
            name="round5-runner-refresh",
        )
        done, _pending = await asyncio.wait(
            {heartbeat_task, operation_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if heartbeat_task in done:
            failure = heartbeat_task.exception()
            fence_lost.set()
            await operation_task
            raise RuntimeError(
                "Runner refresh stopped because its Round 5 fence was lost"
            ) from failure
        result = operation_task.result()
        stop.set()
        heartbeat_task.cancel()
        await asyncio.gather(heartbeat_task, return_exceptions=True)
        return result
    finally:
        stop.set()
        for store, lease in reversed(held):
            try:
                await store.release(lease)
            except Exception:
                print("WARN  a runner-refresh ring will expire on its bounded TTL", flush=True)
        for store, _label in rings:
            try:
                await store.close()
            except Exception:
                print("WARN  a runner-refresh coordinator could not be closed", flush=True)


def refresh_round5_runner(*, timeout: float = 300.0) -> DemoManifest:
    """Refresh only the sealed Round 5 runner; never run Terraform or reset a lane."""
    manifest = load_manifest()
    if manifest.status != "ready" or not manifest.round5_ready:
        raise RuntimeError("Runner refresh requires a ready installation with Round 5 sealed")
    if timeout < ROUND5_SSM_COMMAND_TIMEOUT_SECONDS or timeout > 900:
        raise RuntimeError("Runner refresh timeout must be between 120 and 900 seconds")
    actual_user = _verify_databricks_identity(manifest.databricks.profile)
    if actual_user != manifest.databricks.user:
        raise RuntimeError("Runner refresh refused: the Databricks principal changed")
    _verify_aws_identity(
        manifest.aws.profile,
        manifest.aws.region,
        manifest.aws.account_id,
        manifest.aws.auth_mode,
    )
    session = _round5_runner_refresh_session(manifest)
    _verify_round5_runner_instance(manifest, session)
    _wait_round5_runner_ready(
        session,
        runner_instance_id=manifest.require_round5_resources().runner_instance_id,
        timeout=timeout,
    )
    return asyncio.run(
        _refresh_round5_runner_under_fence(
            manifest,
            session,
            timeout_seconds=timeout,
        )
    )


def _round5_ownership_tags(
    manifest: DemoManifest, outputs: Mapping[str, Any]
) -> Round5OwnershipTags:
    """The exact tag set every per-bout Round 5 resource must carry.

    Round 5 is the only round whose IAM policy *conditions* on these values
    rather than merely recording them: `round5_control.tf` grants
    `ec2:CreateTags` on `security-group-rule/*` only when the request carries
    `expires-at` -- and the run id, the slug, both spellings of owner -- equal
    to the Terraform variables. So this set is not a label, it is half of a
    credential, and the sealed copy and the policy have to agree exactly or the
    grant evaluates to an implicit deny.

    Built here rather than in either caller because `renew` moves `expires_at`
    and re-applies Terraform, which rewrites that condition. A re-seal that
    carried the previous tags forward left the manifest naming an expiry the
    policy no longer allowed, and the only symptom was `AuthorizeSecurityGroup*`
    failing with `UnauthorizedOperation ... no identity-based policy allows
    ec2:CreateTags` two seconds into a bout, journaled as the bare string
    `provider_create_failed`. That cost a bout on 2026-08-24. One definition,
    used by the first seal and by every re-seal, is what keeps the two ends from
    drifting apart again.

    Validated against the live Terraform output rather than trusted: the policy
    is generated from the same variables as `round5_bout_base_tags`, so an
    output that disagrees with the manifest means the apply and the seal are
    describing different installations, and that must fail here rather than
    after the bell.
    """

    expected = _required_round_tags(manifest, "r5")
    expected["managed-by"] = "round5-lifecycle"
    if outputs["ownership_tags"] != expected:
        raise RuntimeError("Terraform Round 5 per-bout ownership tags are not exact")
    return Round5OwnershipTags(
        anti_demo_run_id=manifest.run_id,
        owner=manifest.owner,
        expires_at=_utc_tag(manifest.expires_at),
        anti_demo_installation_slug=expected.get("anti-demo-installation-slug"),
        anti_demo_round=expected.get("anti-demo-round"),
    )


def _reseal_round5(sealed: Round5Resources, **updates: Any) -> Round5Resources:
    """Return a canonical v5 seal after changing non-hash baseline bindings."""
    values = sealed.model_dump(
        mode="json",
        exclude={"baseline_sha256", "config_sha256"},
        exclude_none=True,
    )
    values.update(updates)
    baseline_sha256 = hashlib.sha256(
        json.dumps(values, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    config_sha256 = hashlib.sha256(
        json.dumps(
            {
                "baseline_sha256": baseline_sha256,
                "contract_sha256": sealed.contract_sha256,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return Round5Resources(
        **values,
        baseline_sha256=baseline_sha256,
        config_sha256=config_sha256,
    )


def _prepare_and_reassert_round5_aws_credentials(
    ssm: Any,
    *,
    runner_instance_id: str,
    common: dict[str, Any],
    lanes: tuple[tuple[str, str, str, str], ...],
) -> dict[str, str]:
    """Prepare ordinary source roles, then populate stable Proxy secrets."""
    digests: dict[str, str] = {}
    for lane_id, direct_host, master_secret_arn, destination_secret_arn in lanes:
        prepared = _round5_setup_request(
            ssm,
            runner_instance_id=runner_instance_id,
            payload={
                **common,
                "action": "prepare_rds_baseline",
                "nonce": secrets.token_hex(16),
                "lane_id": lane_id,
                "endpoint_host": direct_host,
                "credential_host": direct_host,
                "master_secret_arn": master_secret_arn,
            },
        )
        credential_sha256 = str(prepared.get("credential_sha256") or "")
        if re.fullmatch(r"[0-9a-f]{64}", credential_sha256) is None:
            raise RuntimeError(f"Round 5 runner returned an invalid {lane_id} credential digest")
        _round5_setup_request(
            ssm,
            runner_instance_id=runner_instance_id,
            payload={
                **common,
                "action": "reassert_rds_credentials",
                "nonce": secrets.token_hex(16),
                "lane_id": lane_id,
                "endpoint_host": direct_host,
                "credential_host": direct_host,
                "master_secret_arn": master_secret_arn,
                "destination_secret_arn": destination_secret_arn,
                "credential_sha256": credential_sha256,
            },
        )
        digests[lane_id] = credential_sha256
    return digests


def _reassert_round5_aws_credentials(
    ssm: Any,
    *,
    runner_instance_id: str,
    common: dict[str, Any],
    lanes: tuple[tuple[str, str, str, str, str], ...],
) -> None:
    """Restore static Proxy secrets without rotating an existing baseline.

    A running app and a running local server each hold the immutable manifest
    snapshot they started with. An ordinary reset used to mint new Aurora/RDS
    passwords and replace the runner files, which made both processes stale
    immediately: their next bout sent the old digest and failed
    ``baseline_auth_hash_invalid`` only after the slow Proxy provisioning path.

    Reasserting is the operation reset actually needs. The runner proves the
    sealed digest still matches its root-owned file, re-applies that same
    ordinary-role password, and copies it into the static Proxy secret. Any real
    drift still fails closed instead of silently blessing a new credential.
    """

    for (
        lane_id,
        direct_host,
        master_secret_arn,
        destination_secret_arn,
        credential_sha256,
    ) in lanes:
        _round5_setup_request(
            ssm,
            runner_instance_id=runner_instance_id,
            payload={
                **common,
                "action": "reassert_rds_credentials",
                "nonce": secrets.token_hex(16),
                "lane_id": lane_id,
                "endpoint_host": direct_host,
                "credential_host": direct_host,
                "master_secret_arn": master_secret_arn,
                "destination_secret_arn": destination_secret_arn,
                "credential_sha256": credential_sha256,
            },
        )


def _prepare_and_reseal_round5(manifest: DemoManifest, *, timeout: float) -> DemoManifest:
    from .connection_spike import ConnectionSpikeContract
    from .connection_spike_live import runner_harness_sha256

    if manifest.round4 is None or manifest.manifest_version not in (2, 3, 4, 5, 6, 7):
        raise RuntimeError("Round 5 provisioning requires a complete sealed Round 4")
    outputs = _required_round5_outputs(_terraform_outputs(manifest))
    session = _aws_session(manifest)
    _round5_aurora_cluster_resource_id(
        manifest,
        session.client("rds"),
        direct_host=outputs["aurora_direct_host"],
        cluster_id=outputs["aurora_cluster_id"],
        writer_instance_id=outputs["aurora_writer_instance_id"],
        master_secret_arn=outputs["aurora_master_secret_arn"],
        expected_resource_id=outputs["aurora_cluster_resource_id"],
    )
    harness_sha256 = runner_harness_sha256()
    _wait_round5_runner_ready(
        session, runner_instance_id=outputs["runner_instance_id"], timeout=timeout
    )

    if manifest.round5_ready:
        sealed = manifest.require_round5_resources()
        for field in (
            "aurora_direct_host",
            "aurora_cluster_id",
            "aurora_cluster_resource_id",
            "aurora_writer_instance_id",
            "aurora_master_secret_arn",
            "rds_direct_host",
            "rds_master_secret_arn",
            "rds_resource_id",
            "vpc_id",
            "proxy_subnet_ids",
            "control_role_arn",
            "control_role_trusted_principal_arn",
            "proxy_service_role_arn",
            "proxy_service_policy_name",
            "aurora_proxy_secret_arn",
            "rds_proxy_secret_arn",
            "runner_permissions_boundary_arn",
            "runner_instance_id",
            "runner_instance_profile_arn",
            "runner_role_arn",
            "runner_subnet_id",
            "runner_security_group_id",
            "runner_egress_rule_id",
            "bout_name_prefix",
        ):
            if outputs[field] != getattr(sealed, field):
                raise RuntimeError(f"Round 5 Terraform output {field} differs from the v5 seal")
        trust_bundle_sha256 = _configure_round5_runner(
            session,
            runner_instance_id=outputs["runner_instance_id"],
            expected_harness_sha256=harness_sha256,
        )
        if trust_bundle_sha256 != sealed.trust_bundle_sha256:
            raise RuntimeError("Round 5 trust bundle differs from the existing v5 seal")
        ssm = session.client("ssm")
        public_key_result = _round5_setup_request(
            ssm,
            runner_instance_id=sealed.runner_instance_id,
            payload={
                "protocol": "connection-spike-setup-v1",
                "action": "public_key",
                "nonce": secrets.token_hex(16),
            },
        )
        if public_key_result.get("public_key_sha256") != sealed.runner_public_key_sha256:
            raise RuntimeError("Round 5 runner public key differs from the v5 seal")
        common = {
            "protocol": "connection-spike-setup-v1",
            "bout_id": f"baseline-{manifest.run_id}",
            "port": 5432,
            "dbname": manifest.databricks.database,
            "username": ROUND5_NATIVE_ROLE,
            "trust_bundle_path": sealed.trust_bundle_path,
            "trust_bundle_sha256": sealed.trust_bundle_sha256,
        }
        _round5_setup_request(
            ssm,
            runner_instance_id=sealed.runner_instance_id,
            payload={
                **common,
                "action": "verify",
                "nonce": secrets.token_hex(16),
                "lane_id": "lakebase",
                "endpoint_host": sealed.lakebase_pooled_host,
                "credential_host": sealed.lakebase_direct_host,
                "credential_sha256": sealed.lakebase_credential_sha256,
            },
        )
        _reassert_round5_aws_credentials(
            ssm,
            runner_instance_id=sealed.runner_instance_id,
            common=common,
            lanes=(
                (
                    "aurora",
                    str(sealed.aurora_direct_host),
                    str(sealed.aurora_master_secret_arn),
                    str(sealed.aurora_proxy_secret_arn),
                    sealed.aurora_credential_sha256,
                ),
                (
                    "rds",
                    sealed.rds_direct_host,
                    sealed.rds_master_secret_arn,
                    str(sealed.rds_proxy_secret_arn),
                    sealed.rds_credential_sha256,
                ),
            ),
        )
        candidate = _reseal_round5(
            sealed,
            harness_sha256=harness_sha256,
            # Rebuilt, never carried forward. `renew` moves the installation
            # expiry and re-applies the Terraform that conditions Round 5's
            # `ec2:CreateTags` grant on it, so the previous tag set is exactly
            # the one the policy has stopped allowing.
            ownership_tags=_round5_ownership_tags(manifest, outputs).model_dump(mode="json"),
        )
    else:
        direct_host, pooled_host = _enable_round5_lakebase_native_login(manifest)
        apply_manifest_environment(manifest)
        lakebase_admin = asyncio.run(_round_lakebase_provider(manifest, 5).connection_material())
        if lakebase_admin.host != direct_host:
            raise RuntimeError("Round 5 Lakebase admin host differs from the control plane")
        trust_bundle_sha256 = _configure_round5_runner(
            session,
            runner_instance_id=outputs["runner_instance_id"],
            expected_harness_sha256=harness_sha256,
        )
        ssm = session.client("ssm")
        baseline_bout = f"baseline-{manifest.run_id}"
        public_key_result = _round5_setup_request(
            ssm,
            runner_instance_id=outputs["runner_instance_id"],
            payload={
                "protocol": "connection-spike-setup-v1",
                "action": "public_key",
                "nonce": secrets.token_hex(16),
            },
        )
        public_key = str(public_key_result.get("public_key") or "")
        public_key_sha256 = str(public_key_result.get("public_key_sha256") or "")
        try:
            actual_public_key_sha256 = hashlib.sha256(
                base64.b64decode(public_key, validate=True)
            ).hexdigest()
        except ValueError as exc:
            raise RuntimeError("Round 5 runner public key is invalid") from exc
        if actual_public_key_sha256 != public_key_sha256:
            raise RuntimeError("Round 5 runner public-key digest is invalid")

        common = {
            "protocol": "connection-spike-setup-v1",
            "bout_id": baseline_bout,
            "port": 5432,
            "dbname": manifest.databricks.database,
            "username": ROUND5_NATIVE_ROLE,
            "trust_bundle_path": "/opt/lakebase-anti-demo/round5/round5-ca.pem",
            "trust_bundle_sha256": trust_bundle_sha256,
        }
        lakebase_result = _round5_setup_request(
            ssm,
            runner_instance_id=outputs["runner_instance_id"],
            payload={
                **common,
                "action": "prepare_lakebase",
                "nonce": secrets.token_hex(16),
                "lane_id": "lakebase",
                "endpoint_host": pooled_host,
                "credential_host": direct_host,
                "sealed_admin": _seal_round5_admin(public_key, lakebase_admin),
                "public_key_sha256": public_key_sha256,
            },
        )
        aws_digests = _prepare_and_reassert_round5_aws_credentials(
            ssm,
            runner_instance_id=outputs["runner_instance_id"],
            common=common,
            lanes=(
                (
                    "aurora",
                    outputs["aurora_direct_host"],
                    outputs["aurora_master_secret_arn"],
                    outputs["aurora_proxy_secret_arn"],
                ),
                (
                    "rds",
                    outputs["rds_direct_host"],
                    outputs["rds_master_secret_arn"],
                    outputs["rds_proxy_secret_arn"],
                ),
            ),
        )
        lakebase_credential_sha256 = str(lakebase_result.get("credential_sha256") or "")
        aurora_credential_sha256 = aws_digests["aurora"]
        rds_credential_sha256 = aws_digests["rds"]
        if not all(
            re.fullmatch(r"[0-9a-f]{64}", digest)
            for digest in (
                lakebase_credential_sha256,
                aurora_credential_sha256,
                rds_credential_sha256,
            )
        ):
            raise RuntimeError("Round 5 runner returned invalid baseline credential digests")

        ownership_tags = _round5_ownership_tags(manifest, outputs)
        values = {
            "lakebase_direct_host": direct_host,
            "lakebase_pooled_host": pooled_host,
            "aurora_direct_host": outputs["aurora_direct_host"],
            "aurora_cluster_id": outputs["aurora_cluster_id"],
            "aurora_cluster_resource_id": outputs["aurora_cluster_resource_id"],
            "aurora_writer_instance_id": outputs["aurora_writer_instance_id"],
            "aurora_master_secret_arn": outputs["aurora_master_secret_arn"],
            "rds_direct_host": outputs["rds_direct_host"],
            "rds_master_secret_arn": outputs["rds_master_secret_arn"],
            "rds_resource_id": outputs["rds_resource_id"],
            "vpc_id": outputs["vpc_id"],
            "proxy_subnet_ids": outputs["proxy_subnet_ids"],
            "control_role_arn": outputs["control_role_arn"],
            "control_role_trusted_principal_arn": outputs["control_role_trusted_principal_arn"],
            "proxy_service_role_arn": outputs["proxy_service_role_arn"],
            "proxy_service_policy_name": outputs["proxy_service_policy_name"],
            "aurora_proxy_secret_arn": outputs["aurora_proxy_secret_arn"],
            "rds_proxy_secret_arn": outputs["rds_proxy_secret_arn"],
            "runner_permissions_boundary_arn": outputs["runner_permissions_boundary_arn"],
            "runner_instance_id": outputs["runner_instance_id"],
            "runner_instance_profile_arn": outputs["runner_instance_profile_arn"],
            "runner_role_arn": outputs["runner_role_arn"],
            "runner_subnet_id": outputs["runner_subnet_id"],
            "runner_security_group_id": outputs["runner_security_group_id"],
            "runner_egress_rule_id": outputs["runner_egress_rule_id"],
            "runner_public_key_sha256": public_key_sha256,
            "lakebase_credential_sha256": lakebase_credential_sha256,
            "aurora_credential_sha256": aurora_credential_sha256,
            "rds_credential_sha256": rds_credential_sha256,
            "bout_name_prefix": outputs["bout_name_prefix"],
            "ownership_tags": ownership_tags.model_dump(mode="json"),
            "credential_root": "/var/lib/lakebase-anti-demo/credentials",
            "journal_table": ROUND5_JOURNAL_TABLE,
            "native_role": ROUND5_NATIVE_ROLE,
            "probe_identity": ROUND5_PROBE_IDENTITY,
            "ssm_document_name": "AWS-RunShellScript",
            "runner_path": "/opt/lakebase-anti-demo/round5/run_connection_spike.sh",
            "trust_bundle_path": "/opt/lakebase-anti-demo/round5/round5-ca.pem",
            "trust_bundle_sha256": trust_bundle_sha256,
            "harness_sha256": harness_sha256,
            "frozen_constants": Round5FrozenConstants().model_dump(mode="json"),
            "contract_sha256": ConnectionSpikeContract().sha256,
        }
        baseline_sha256 = hashlib.sha256(
            json.dumps(values, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        config_sha256 = hashlib.sha256(
            json.dumps(
                {
                    "baseline_sha256": baseline_sha256,
                    "contract_sha256": ConnectionSpikeContract().sha256,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        candidate = Round5Resources(
            **values,
            baseline_sha256=baseline_sha256,
            config_sha256=config_sha256,
        )

        # Preparing the sealed Round 5 credentials necessarily opens fresh
        # baseline connections.  Do not couple Round 5 availability to the
        # unrelated Round 1 auto-pause clock here: Round 1's arm/doctor path
        # still requires live scale-zero evidence before it can be timed.

    # Record the digests before the gate, because the rotation above has already
    # replaced the runner's baseline credential files and that cannot be undone.
    # A manifest that omits the digests it just minted is not the cautious
    # outcome, it is the false one: the runner keeps the new credentials, the
    # seal keeps naming the superseded hashes, and nothing notices until a bout
    # dies after the bell on `baseline_auth_hash_invalid`. That is what a failure
    # between this rotation and this write cost on 2026-08-24 -- two seven-minute
    # bouts, with the Lakebase lane winning in 2.6s while its opponent could not
    # authenticate at all. Writing first makes the seal describe what is on disk;
    # the gate below still refuses, just as loudly, and `status` is deliberately
    # left short of `ready` so a failed topology can never advertise itself.
    manifest.round5 = candidate
    manifest.status = "seeding"
    save_manifest(manifest)

    topology = _round5_topology_check(manifest, candidate)
    if not topology.ok:
        raise RuntimeError(f"Round 5 secret-free doctor failed: {topology.detail}")
    # The v5 manifest seals only the clean baseline. Every per-bout mutation is
    # journaled after T0 and must be absent again before another setup/destroy.
    if manifest.round6 is None:
        manifest.manifest_version = 5
        manifest.status = "ready"
    else:
        # Round 5 credential preparation wakes Lakebase. Preserve the exact
        # Round 6 cleanup seal, but revoke READY until its live canary and the
        # final scale-zero observation have both run again.
        manifest.manifest_version = 7 if manifest.round_environments is not None else 6
        manifest.status = "seeding"
    save_manifest(manifest)
    return manifest


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _prepare_round4_source_artifacts(
    manifest: DemoManifest, names: dict[str, str], warehouse_id: str
) -> None:
    profile = manifest.databricks.profile
    for schema_name in (
        names["source_schema"],
        names["storage_schema"],
        names["online_schema"],
    ):
        _sql_statement(
            profile,
            warehouse_id,
            f"CREATE SCHEMA IF NOT EXISTS `{names['catalog']}`.`{schema_name}`",
        )
    _sql_statement(
        profile,
        warehouse_id,
        f"""
CREATE TABLE IF NOT EXISTS `{names["catalog"]}`.`{names["source_schema"]}`.`{ROUND4_SOURCE_TABLE}` (
  entity_id STRING NOT NULL,
  score DOUBLE NOT NULL,
  model_version STRING NOT NULL,
  proof_nonce STRING NOT NULL,
  updated_at TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""".strip(),
    )
    _sql_statement(
        profile,
        warehouse_id,
        "ALTER TABLE "
        f"`{names['catalog']}`.`{names['source_schema']}`.`{ROUND4_SOURCE_TABLE}` "
        "SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')",
    )


def _repair_round4_baseline(
    manifest: DemoManifest, names: dict[str, str], warehouse_id: str
) -> int:
    profile = manifest.databricks.profile
    _sql_statement(
        profile,
        warehouse_id,
        f"""
MERGE INTO `{names["catalog"]}`.`{names["source_schema"]}`.`{ROUND4_SOURCE_TABLE}` AS target
USING (SELECT {_sql_string(ROUND4_BASELINE_ENTITY_ID)} AS entity_id,
              {ROUND4_BASELINE_SCORE!r} AS score,
              {_sql_string(ROUND4_BASELINE_MODEL_VERSION)} AS model_version,
              {_sql_string(ROUND4_BASELINE_PROOF_NONCE)} AS proof_nonce,
              current_timestamp() AS updated_at) AS baseline
ON target.entity_id = baseline.entity_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""".strip(),
    )
    history = _sql_statement(
        profile,
        warehouse_id,
        "DESCRIBE HISTORY "
        f"`{names['catalog']}`.`{names['source_schema']}`.`{ROUND4_SOURCE_TABLE}` LIMIT 1",
    )
    rows = _sql_rows(history)
    try:
        return int(rows[0]["version"])
    except (IndexError, KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(
            "Round 4 source table did not return its committed Delta version"
        ) from exc


def _prepare_round4_source(manifest: DemoManifest, names: dict[str, str], warehouse_id: str) -> int:
    _prepare_round4_source_artifacts(manifest, names, warehouse_id)
    return _repair_round4_baseline(manifest, names, warehouse_id)


ROUND4_BASELINE_ROW = ModelScoreRow(
    entity_id=ROUND4_BASELINE_ENTITY_ID,
    score=ROUND4_BASELINE_SCORE,
    model_version=ROUND4_BASELINE_MODEL_VERSION,
    proof_nonce=ROUND4_BASELINE_PROOF_NONCE,
)


def _read_round4_source_row(
    manifest: DemoManifest, names: dict[str, str], warehouse_id: str
) -> ModelScoreRow | None:
    rows = _sql_rows(
        _sql_statement(
            manifest.databricks.profile,
            warehouse_id,
            "SELECT entity_id, score, model_version, proof_nonce FROM "
            f"`{names['catalog']}`.`{names['source_schema']}`.`{ROUND4_SOURCE_TABLE}` "
            f"WHERE entity_id = {_sql_string(ROUND4_BASELINE_ENTITY_ID)}",
        )
    )
    if len(rows) != 1:
        return None
    row = rows[0]
    try:
        return ModelScoreRow(
            entity_id=str(row.get("entity_id") or ""),
            score=float(row.get("score")),
            model_version=str(row.get("model_version") or ""),
            proof_nonce=str(row.get("proof_nonce") or ""),
        )
    except (TypeError, ValueError):
        return None


def _wait_round4_sync_position(
    manifest: DemoManifest,
    names: dict[str, str],
    source_version: int,
    *,
    pipeline_id: str,
    timeout: float,
) -> None:
    """Block until Managed Sync reports it applied the given source Delta version.

    Restoring the source alone is not enough: an arm that runs inside the sync
    window sees a baseline source against a residue application row and refuses.

    ``pipeline_id`` is threaded in for :func:`_round4_sync_failure`'s sake and for
    no other reason. It is what lets this wait tell a pipeline somebody switched
    off from one that fell over, which the synced table alone cannot say.
    """

    deadline = time.monotonic() + timeout
    while True:
        database = _round4_get_database_synced_table(manifest, names)
        status = (database or {}).get("data_synchronization_status") or {}
        state = str(status.get("detailed_state") or "")
        if state in SYNCED_TABLE_FAILED_STATES:
            raise _round4_sync_failure(manifest, pipeline_id, (state,))
        delta_info = (status.get("last_sync") or {}).get("delta_table_sync_info") or {}
        try:
            applied = int(delta_info["delta_commit_version"])
        except (KeyError, TypeError, ValueError):
            applied = -1
        if applied >= source_version:
            return
        if time.monotonic() >= deadline:
            raise RuntimeError(
                "Round 4 baseline restore did not reach the synced table before timeout"
            )
        time.sleep(min(2, max(0, deadline - time.monotonic())))


def _restore_round4_baseline_if_owned(
    manifest: DemoManifest,
    names: dict[str, str],
    warehouse_id: str,
    *,
    pipeline_id: str,
    timeout: float,
) -> bool:
    """Re-seed the sealed Round 4 baseline when the source holds demo-owned residue.

    A completed Round 4 run leaves its own run-owned proof row behind; the round
    only restored it when an operator threw in the towel mid-run. Returns True
    when a restore was performed, False when the source was already the exact
    baseline, and raises when the row is not this demo's to overwrite.
    """

    row = _read_round4_source_row(manifest, names, warehouse_id)
    if row == ROUND4_BASELINE_ROW:
        return False
    if not is_owned_prior_proof(row):
        raise RuntimeError("Round 4 source table does not contain the exact baseline")
    source_version = _repair_round4_baseline(manifest, names, warehouse_id)
    if _read_round4_source_row(manifest, names, warehouse_id) != ROUND4_BASELINE_ROW:
        raise RuntimeError("Round 4 baseline restore did not produce the exact source row")
    _wait_round4_sync_position(
        manifest, names, source_version, pipeline_id=pipeline_id, timeout=timeout
    )
    return True


def _create_or_get_round4_synced_table(
    manifest: DemoManifest, names: dict[str, str], *, timeout: float
) -> dict[str, Any]:
    existing = _round4_get_synced_table(manifest, names)
    if existing is not None:
        _validate_round4_synced_table(manifest, existing, names, require_identity=False)
        return existing
    path = "/api/2.0/postgres/synced_tables?synced_table_id=" + quote(
        names["synced_table_id"], safe=""
    )
    operation = _databricks_api(
        manifest.databricks.profile,
        "post",
        path,
        body={"spec": _round4_synced_spec(names)},
        timeout=120,
    )
    _wait_round4_operation(manifest.databricks.profile, operation, timeout=timeout)
    deadline = time.monotonic() + timeout
    while True:
        created = _round4_get_synced_table(manifest, names)
        if created is not None:
            _validate_round4_synced_table(manifest, created, names)
            return created
        if time.monotonic() >= deadline:
            raise RuntimeError("Round 4 synced table disappeared after creation")
        time.sleep(min(2, max(0, deadline - time.monotonic())))


def _wait_round4_cross_endpoint_table(
    manifest: DemoManifest,
    names: dict[str, str],
    *,
    timeout: float,
) -> tuple[dict[str, Any], dict[str, Any]]:
    deadline = time.monotonic() + timeout
    while True:
        postgres = _round4_get_synced_table(manifest, names)
        database = _round4_get_database_synced_table(manifest, names)
        if postgres is not None and database is not None:
            return postgres, database
        if time.monotonic() >= deadline:
            raise RuntimeError(
                "Round 4 synced table did not converge across /postgres and /database"
            )
        time.sleep(min(2, max(0, deadline - time.monotonic())))


async def _connect(material: ConnectionMaterial, *, autocommit: bool = False):
    deadline = time.monotonic() + 120
    retry_delays = (2, 4, 8, 10)
    failure_count = 0

    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError(
                "PostgreSQL setup connection did not become ready within 120 seconds"
            )

        try:
            async with asyncio.timeout(remaining):
                return await psycopg.AsyncConnection.connect(
                    host=material.host,
                    port=material.port,
                    dbname=material.database,
                    user=material.user,
                    password=material.password,
                    sslmode="require",
                    application_name="lakebase-anti-demo-setup",
                    connect_timeout=max(1, min(15, int(remaining))),
                    autocommit=autocommit,
                )
        except TimeoutError:
            raise RuntimeError(
                "PostgreSQL setup connection did not become ready within 120 seconds"
            ) from None
        except psycopg.OperationalError as exc:
            sqlstate = exc.sqlstate
            retryable = sqlstate is None or sqlstate.startswith("08") or sqlstate == "57P03"
            if not retryable:
                raise RuntimeError(
                    f"PostgreSQL setup connection failed (SQLSTATE {sqlstate})"
                ) from None

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError(
                    "PostgreSQL setup connection did not become ready within 120 seconds"
                ) from None
            delay = min(retry_delays[min(failure_count, len(retry_delays) - 1)], remaining)
            print(
                f"WAIT PostgreSQL setup connection not ready; retrying in {max(1, round(delay))}s"
            )
            await asyncio.sleep(delay)
            failure_count += 1


async def _read_round4_baseline(
    manifest: DemoManifest, names: dict[str, str]
) -> tuple[str, float, str, str] | None:
    apply_manifest_environment(manifest)
    material = await _round_lakebase_provider(
        manifest, 4, database=ROUND4_DATABASE
    ).connection_material()
    connection = await _connect(material)
    async with connection:
        async with connection.cursor() as cursor:
            await cursor.execute(
                sql.SQL(
                    "SELECT entity_id, score, model_version, proof_nonce "
                    "FROM {}.{} WHERE entity_id = %s"
                ).format(
                    sql.Identifier(names["online_schema"]),
                    sql.Identifier(ROUND4_SYNCED_TABLE),
                ),
                (ROUND4_BASELINE_ENTITY_ID,),
            )
            row = await cursor.fetchone()
    if row is None:
        return None
    return str(row[0]), float(row[1]), str(row[2]), str(row[3])


def _wait_round4_baseline(
    manifest: DemoManifest,
    names: dict[str, str],
    source_version: int,
    *,
    project_uid: str,
    branch_uid: str,
    pipeline_id: str,
    timeout: float,
) -> tuple[dict[str, Any], dict[str, Any]]:
    deadline = time.monotonic() + timeout
    expected_row = (
        ROUND4_BASELINE_ENTITY_ID,
        ROUND4_BASELINE_SCORE,
        ROUND4_BASELINE_MODEL_VERSION,
        ROUND4_BASELINE_PROOF_NONCE,
    )
    while True:
        postgres = _round4_get_synced_table(manifest, names)
        database = _round4_get_database_synced_table(manifest, names)
        if postgres is None or database is None:
            raise RuntimeError("Round 4 synced table disappeared while waiting for baseline")
        _validate_round4_synced_table(manifest, postgres, names, require_identity=False)
        _validate_round4_database_synced_table(
            database,
            names,
            project_uid=project_uid,
            branch_uid=branch_uid,
            pipeline_id=pipeline_id,
        )
        postgres_status = postgres.get("status") or {}
        database_status = database.get("data_synchronization_status") or {}
        postgres_state = str(postgres_status.get("detailed_state") or "")
        state = str(database_status.get("detailed_state") or "")
        if state in SYNCED_TABLE_FAILED_STATES or postgres_state in SYNCED_TABLE_FAILED_STATES:
            raise _round4_sync_failure(manifest, pipeline_id, (state, postgres_state))
        continuous = database_status.get("continuous_update_status") or {}
        last_sync = database_status.get("last_sync") or {}
        delta_info = last_sync.get("delta_table_sync_info") or {}
        try:
            processed = int(continuous["last_processed_commit_version"])
            last_sync_version = int(delta_info["delta_commit_version"])
        except (KeyError, TypeError, ValueError):
            processed = last_sync_version = -1
        if processed > source_version or last_sync_version > source_version:
            raise RuntimeError("Round 4 sync advanced beyond the source Delta head")
        postgres_last_sync = postgres_status.get("last_sync") or {}
        postgres_delta = postgres_last_sync.get("delta_table_sync_info") or {}
        postgres_version = postgres_delta.get("delta_commit_version")
        if postgres_version is not None:
            try:
                if int(postgres_version) > source_version:
                    raise RuntimeError("Round 4 /postgres last_sync exceeds the source Delta head")
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    "Round 4 /postgres last_sync has an invalid Delta version"
                ) from exc
        if (
            state in SYNCED_TABLE_HEALTHY_STATES
            and postgres_state in SYNCED_TABLE_HEALTHY_STATES
            and processed == source_version
            and last_sync_version == source_version
        ):
            _validate_round4_database_synced_table(
                database,
                names,
                project_uid=project_uid,
                branch_uid=branch_uid,
                pipeline_id=pipeline_id,
                require_sync_position=True,
            )
            row = asyncio.run(_read_round4_baseline(manifest, names))
            if row == expected_row:
                return postgres, database
        if time.monotonic() >= deadline:
            raise RuntimeError("Round 4 exact baseline did not become available before timeout")
        time.sleep(min(2, max(0, deadline - time.monotonic())))


#: Every Unity Catalog privilege the deployed app is permitted to hold. `MODIFY`
#: is in the set because Round 4's whole point is the app writing a score into
#: the source Delta table and watching it arrive; `ALL PRIVILEGES` is absent for
#: the same reason it is absent from the coordination plan, and `MANAGE` because
#: nothing in the app owns a securable.
_UNITY_CATALOG_APP_PRIVILEGES = frozenset({"USE CATALOG", "USE SCHEMA", "SELECT", "MODIFY"})


@dataclass(frozen=True)
class UnityCatalogAppGrant:
    """One Unity Catalog securable the deployed app reaches, and the least it needs.

    ``name`` is always derived from the value the round seals and the runtime
    adapter later reads -- `_round4_names` for Round 4, the CDF status's own
    ``uc_table`` for Round 6 -- never composed from fresh literals here. Writing
    a name by hand is what produced the defect this class exists to stop: the
    Round 4 plan granted ``…model_scores_source``, a real and plausible table,
    while `inspect_sync` read the synced ``model_scores`` next to it. The app
    held `USE SCHEMA` on the online schema and nothing at all on the one table
    inside it, and `USE SCHEMA` is traversal that never implies `SELECT`.
    """

    securable: str
    name: str
    privileges: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.securable not in {"CATALOG", "SCHEMA", "TABLE"}:
            raise ValueError(f"{self.name} has an unsupported securable {self.securable!r}")
        expected_parts = {"CATALOG": 1, "SCHEMA": 2, "TABLE": 3}[self.securable]
        parts = self.name.split(".")
        if len(parts) != expected_parts or not all(parts):
            raise ValueError(f"{self.name!r} is not a {self.securable.casefold()} name")
        if not self.privileges:
            raise ValueError(f"{self.name} needs at least one privilege")
        unknown = sorted(set(self.privileges) - _UNITY_CATALOG_APP_PRIVILEGES)
        if unknown:
            raise ValueError(f"{self.name} may not be granted {', '.join(unknown)}")

    def statement(self, principal: str) -> str:
        identifier = "`" + self.name.replace(".", "`.`") + "`"
        return (
            f"GRANT {', '.join(self.privileges)} ON {self.securable} {identifier} "
            f"TO `{principal.replace('`', '``')}`"
        )


def _round4_unity_catalog_grants(names: Mapping[str, str]) -> tuple[UnityCatalogAppGrant, ...]:
    """Every Unity Catalog securable Round 4 touches at runtime, and its privileges.

    `tests/test_lifecycle.py` records the URLs `LiveModelScoreAdapter.inspect_sync`
    actually requests and fails if any Unity Catalog object among them has no
    entry here, so the covering set is derived from the reads rather than from
    another list of names. A read added to the adapter without a grant is a red
    test, not a `PermissionDenied` in front of an audience.
    """

    catalog = names["catalog"]
    return (
        UnityCatalogAppGrant("CATALOG", catalog, ("USE CATALOG",)),
        # The three schemas the adapter validates ownership of. `USE SCHEMA` is
        # traversal only and confers nothing on what is inside them.
        UnityCatalogAppGrant(
            "SCHEMA", f"{catalog}.{names['source_schema']}", ("USE SCHEMA",)
        ),
        UnityCatalogAppGrant(
            "SCHEMA", f"{catalog}.{names['storage_schema']}", ("USE SCHEMA",)
        ),
        UnityCatalogAppGrant(
            "SCHEMA", f"{catalog}.{names['online_schema']}", ("USE SCHEMA",)
        ),
        # The source Delta table: read by `read_source` and `_source_head`,
        # written by the MERGE in `commit_source_update`, and read again through
        # `table_changes(...)` for the CDF proof.
        UnityCatalogAppGrant("TABLE", names["source_table"], ("SELECT", "MODIFY")),
        # The synced table, which is a distinct securable from the source and the
        # one the round is named after. `inspect_sync` GETs
        # `/api/2.0/database/synced_tables/{synced_table_id}`, and that read is
        # authorized as `SELECT` on the Unity Catalog table of the same name --
        # not as anything on the schema around it. Read-only: the pipeline writes
        # this table, never the app.
        UnityCatalogAppGrant("TABLE", names["synced_table_id"], ("SELECT",)),
    )


def _round6_unity_catalog_grants(
    destination_table_full_name: str,
) -> tuple[UnityCatalogAppGrant, ...]:
    """Every Unity Catalog securable Round 6 touches at runtime, and its privileges.

    Round 6's destination is created by the native CDF feed rather than by
    setup, so its name is only knowable from the feed's own ``uc_table``. That
    one string is what `prepare_round6` seals as ``destination_table_full_name``
    and what `LiveOrdersLiveAdapter.read_history` selects from, and the catalog
    and schema are split back out of it here rather than re-read from the
    environment -- so the grant cannot name a different schema from the one the
    feed actually wrote to.

    Round 6 had no Unity Catalog grants at all: setup created the app's Lakebase
    branch role and stopped, and the deployed app was refused at arm with
    `PermissionDenied: User does not have USE SCHEMA on Schema
    '<catalog>.<destination schema>'` from inside `get_cdf_config`.
    """

    parts = destination_table_full_name.split(".")
    if len(parts) != 3 or not all(parts):
        raise RuntimeError("Round 6 destination table is not a three-level Unity Catalog name")
    catalog, schema, _table = parts
    return (
        UnityCatalogAppGrant("CATALOG", catalog, ("USE CATALOG",)),
        # `get_cdf_config` and `get_cdf_status` resolve the destination schema
        # and refuse without traversal on it, before any row is read.
        UnityCatalogAppGrant("SCHEMA", f"{catalog}.{schema}", ("USE SCHEMA",)),
        # `read_history` selects the proof row. Read-only: the CDF feed writes
        # this history table and the app only ever looks at it.
        UnityCatalogAppGrant("TABLE", destination_table_full_name, ("SELECT",)),
    )


def _grant_round6_unity_catalog(
    manifest: DemoManifest,
    destination_table_full_name: str,
    warehouse_id: str,
    app_client_id: str | None,
) -> None:
    """Issue Round 6's Unity Catalog grants. Called by `round6_lifecycle`."""

    if not app_client_id:
        return
    for grant in _round6_unity_catalog_grants(destination_table_full_name):
        _sql_statement(manifest.databricks.profile, warehouse_id, grant.statement(app_client_id))


def _grant_round4_uc_and_warehouse(
    manifest: DemoManifest,
    names: dict[str, str],
    warehouse_id: str,
    pipeline_id: str,
    app_client_id: str | None,
) -> None:
    if app_client_id is None:
        return
    profile = manifest.databricks.profile
    _run(
        [
            "databricks",
            "permissions",
            "update",
            "warehouses",
            warehouse_id,
            "--json",
            json.dumps(
                {
                    "access_control_list": [
                        {
                            "service_principal_name": app_client_id,
                            "permission_level": "CAN_USE",
                        }
                    ]
                }
            ),
            "-p",
            profile,
            "-o",
            "json",
        ],
        capture=True,
    )
    # CAN_RUN rather than CAN_VIEW, and the difference is one word for a reason
    # worth writing down. CAN_VIEW let the app read pipeline state, which is all
    # arm ever needed while the pipeline was resident around the clock. Under the
    # narrow amendment to D9a and D20a the app now also starts this pipeline at
    # arm and stops it once a bout has settled, and CAN_RUN is the least
    # privilege that permits those two calls. It is deliberately not CAN_MANAGE:
    # nothing the app does may edit the pipeline's specification, and a
    # `scheduling_policy` edit is the one mutation that would recreate the synced
    # table and take Rounds 5 and 6 with it.
    _run(
        [
            "databricks",
            "permissions",
            "update",
            "pipelines",
            pipeline_id,
            "--json",
            json.dumps(
                {
                    "access_control_list": [
                        {
                            "service_principal_name": app_client_id,
                            "permission_level": "CAN_RUN",
                        }
                    ]
                }
            ),
            "-p",
            profile,
            "-o",
            "json",
        ],
        capture=True,
    )
    for grant in _round4_unity_catalog_grants(names):
        _sql_statement(profile, warehouse_id, grant.statement(app_client_id))


def _lakebase_app_role_id(app_client_id: str) -> str:
    role_id = f"app-{app_client_id.casefold()}"
    if not re.fullmatch(r"[a-z](?:[a-z0-9-]{0,61}[a-z0-9])?", role_id):
        raise RuntimeError("Lakebase application client ID cannot form a valid role resource ID")
    return role_id


def _validate_lakebase_app_role(payload: dict[str, Any], branch: str, app_client_id: str) -> None:
    expected_role_id = _lakebase_app_role_id(app_client_id)
    expected_name = f"{branch}/roles/{expected_role_id}"
    status = payload.get("status") or {}
    parent = payload.get("parent")
    role_id = payload.get("role_id") or status.get("role_id")
    if payload.get("name") != expected_name or role_id != expected_role_id:
        raise RuntimeError("Lakebase application role identity is not exact")
    if parent is not None and parent != branch:
        raise RuntimeError("Lakebase application role parent is not exact")
    if not status:
        raise RuntimeError("Lakebase application role returned no authoritative status")
    for role in (status, payload.get("spec")):
        if role is None:
            continue
        if (
            role.get("identity_type") != "SERVICE_PRINCIPAL"
            or role.get("auth_method") != "LAKEBASE_OAUTH_V1"
            or role.get("postgres_role") != app_client_id
        ):
            raise RuntimeError("Lakebase application role authentication is incompatible")
        if role.get("membership_roles"):
            raise RuntimeError("Lakebase application role has forbidden broad membership")
        attributes = role.get("attributes") or {}
        if any(bool(value) for value in attributes.values()):
            raise RuntimeError("Lakebase application role has forbidden elevated attributes")


#: The Lakebase project permission the deployed app holds, and it must be this one.
#:
#: The `database-projects` API offers exactly two levels -- there is no narrower
#: third to reach for. `CAN_USE` grants "the permission to create catalogs and
#: tables within the database project": catalogs and tables, not *branches*. Round
#: 2 and Round 3 build their isolated environment by creating a branch
#: (`LakebaseSafeChangeAdapter.create_isolated`), so on `CAN_USE` the deployed app
#: cannot run those rounds at all -- it answered "Databricks control-plane request
#: was refused" in front of an audience while the local server, whose operator
#: principal holds `CAN_MANAGE`, passed the identical source.
#:
#: The accepted cost, stated plainly rather than left for the next reader to
#: discover: this principal can now DELETE the Lakebase project. That is a real
#: escalation for a public-facing app identity and it was weighed against the
#: alternative, which is two rounds that are advertised and then refuse. The
#: owner chose the rounds working.
_LAKEBASE_APP_PROJECT_PERMISSION = "CAN_MANAGE"


def _lakebase_project_id(branch_name: str) -> str:
    """The permissions-API project ID behind a Lakebase branch resource name.

    Branch names are ``projects/<project-id>/branches/<branch-id>``, and the
    `database-projects` permissions endpoint is keyed on ``<project-id>`` rather
    than on the project UID the refusal message quotes. Derived from the same
    string the role creation posts to, so a role and its project permission
    cannot end up on different projects.
    """

    parts = branch_name.split("/")
    if len(parts) != 4 or parts[0] != "projects" or parts[2] != "branches" or not parts[1]:
        raise RuntimeError("Lakebase branch name is not a project-scoped resource name")
    return parts[1]


def _grant_lakebase_app_project_permission(
    manifest: DemoManifest, app_client_id: str, project_id: str
) -> None:
    """Give the app its control-plane standing on one Lakebase project.

    A branch role authenticates a Postgres connection; it authorizes nothing on
    the control plane. Every `/api/2.0/postgres/...` call the rounds make --
    reading the project, the branch, the endpoint, the synced table, the CDF
    status, and *creating the branch* Rounds 2 and 3 isolate into -- is checked
    against the project's own ACL, and a service principal absent from it is
    refused with "assign the user ... 'Can Use' or 'Can Manage' for Database
    project <uid>". `update` rather than `set`, so the operator's own
    `CAN_MANAGE` entry is preserved rather than replaced.
    """

    _run(
        [
            "databricks",
            "permissions",
            "update",
            "database-projects",
            project_id,
            "--json",
            json.dumps(
                {
                    "access_control_list": [
                        {
                            "service_principal_name": app_client_id,
                            "permission_level": _LAKEBASE_APP_PROJECT_PERMISSION,
                        }
                    ]
                }
            ),
            "-p",
            manifest.databricks.profile,
            "-o",
            "json",
        ],
        capture=True,
    )


def _lakebase_app_project_ids(manifest: DemoManifest) -> tuple[str, ...]:
    """Every Lakebase project the deployed app must reach, derived from the seal.

    Enumerated from the same ``round_environments`` mapping the runtime resolves
    its own connections through, plus the coordination endpoint, rather than
    written out here: a round repointed at a different project, or a seventh
    round, changes this set without this function being edited. That is the
    bargain `_coordination_runtime_grants` struck for table names, applied to
    projects.

    Enumerating is the whole fix. The project permission used to be issued only
    where a branch role is created, and only Rounds 4 and 6 create one -- so r1,
    r2, r3 and r5 had no ACL entry at all, and each was discovered broken
    separately, in front of an audience, over several days.
    """

    branches: list[str] = []
    environments = manifest.round_environments
    if environments is not None:
        branches.extend(seal.lakebase.branch_name for seal in environments.values())
    coordination = manifest.coordination_environment
    coordination_endpoint = (
        coordination.endpoint_name
        if coordination is not None
        else manifest.databricks.coordination_endpoint_name
    )
    if coordination_endpoint:
        branches.append(coordination_endpoint.rsplit("/endpoints/", 1)[0])
    # Ordered and de-duplicated: Round 1's project is also the legacy
    # `databricks.endpoint_name` mirror, so the same project arrives twice.
    return tuple(sorted({_lakebase_project_id(branch) for branch in branches}))


def _grant_lakebase_app_projects(manifest: DemoManifest, app_client_id: str) -> tuple[str, ...]:
    """Grant the app its project permission on every project it must reach.

    Returns the failures rather than raising on the first one. A gate that stops
    at the first problem has repeatedly hidden a second one here, and the caller
    wants to tell an operator that four projects are unreachable, not one.
    """

    failures: list[str] = []
    for project_id in _lakebase_app_project_ids(manifest):
        try:
            _grant_lakebase_app_project_permission(manifest, app_client_id, project_id)
        except Exception as error:  # noqa: BLE001 - every failure is reported, not the first
            failures.append(f"{project_id}: {error}")
    return tuple(failures)


def _ensure_lakebase_app_roles(
    manifest: DemoManifest,
    app_client_id: str,
    branches: tuple[str, ...],
    *,
    timeout: float,
    create: bool = True,
) -> None:
    if not branches or len(set(branches)) != len(branches):
        raise RuntimeError("Lakebase application role branches must be unique and non-empty")
    for branch in branches:
        if create:
            # Kept, but no longer the authority. Pairing the grant with role
            # creation is exactly what left r1, r2, r3 and r5 with no ACL entry
            # at all -- only Rounds 4 and 6 create a role, so only their projects
            # were reached. `_grant_lakebase_app_projects` now covers every sealed
            # project unconditionally; this stays because it costs one idempotent
            # call and guarantees the ordering a role needs, whichever caller
            # arrives first.
            _grant_lakebase_app_project_permission(
                manifest, app_client_id, _lakebase_project_id(branch)
            )
        role_id = _lakebase_app_role_id(app_client_id)
        role_name = f"{branch}/roles/{role_id}"
        role = _databricks_api_optional(
            manifest.databricks.profile,
            f"/api/2.0/postgres/{quote(role_name, safe='/')}",
        )
        if role is None:
            if not create:
                raise RuntimeError(f"Lakebase application role is missing on {branch}")
            operation = _databricks_api(
                manifest.databricks.profile,
                "post",
                f"/api/2.0/postgres/{quote(branch, safe='/')}/roles?role_id="
                f"{quote(role_id, safe='')}",
                body={
                    "spec": {
                        "identity_type": "SERVICE_PRINCIPAL",
                        "auth_method": "LAKEBASE_OAUTH_V1",
                        "postgres_role": app_client_id,
                        "membership_roles": [],
                    }
                },
                timeout=120,
            )
            _wait_round4_operation(manifest.databricks.profile, operation, timeout=timeout)
            role = _databricks_api_optional(
                manifest.databricks.profile,
                f"/api/2.0/postgres/{quote(role_name, safe='/')}",
            )
            if role is None:
                raise RuntimeError("Lakebase application role disappeared after creation")
        _validate_lakebase_app_role(role, branch, app_client_id)


def _ensure_round4_app_roles(
    manifest: DemoManifest,
    app_client_id: str,
    *,
    timeout: float,
    create: bool = True,
) -> None:
    if manifest.round_environments is not None:
        branches = (
            _round_lakebase_binding(manifest, 4).endpoint_name.rsplit("/endpoints/", 1)[0],
            _coordination_lakebase_binding(manifest).endpoint_name.rsplit("/endpoints/", 1)[0],
        )
    else:
        branches = (
            f"projects/{manifest.run_id}/branches/production",
            f"projects/{manifest.run_id}/branches/coordination",
        )
    _ensure_lakebase_app_roles(
        manifest,
        app_client_id,
        branches,
        timeout=timeout,
        create=create,
    )


def _measured_lakebase_app_role_branches(manifest: DemoManifest) -> tuple[str, ...]:
    """The branches the app needs a Postgres role on to be granted in Rounds 1-3.

    Deduplicated because a pre-``round_environments`` installation puts every
    round on one shared branch, and ``_ensure_lakebase_app_roles`` refuses a
    tuple that names the same branch twice.
    """

    if manifest.round_environments is None:
        return (f"projects/{manifest.run_id}/branches/production",)
    return tuple(
        dict.fromkeys(
            _round_lakebase_binding(manifest, number).endpoint_name.rsplit(
                "/endpoints/", 1
            )[0]
            for number in MEASURED_LAKEBASE_ROUNDS
        )
    )


#: Every privilege the deployed app is permitted to hold on a coordination
#: relation. `sql.SQL` does not escape what it is handed, so the plan below is
#: checked against this rather than trusted; `CREATE` is absent on purpose, and
#: `ALL` is absent because it would grant `TRUNCATE` and `REFERENCES` on tables
#: the app is only supposed to append to.
_COORDINATION_PRIVILEGES = frozenset({"SELECT", "INSERT", "UPDATE", "DELETE"})


@dataclass(frozen=True)
class CoordinationRuntimeGrant:
    """One coordination relation the deployed app touches, and the least it needs.

    ``table`` is schema-qualified and comes from the module that owns the SQL
    using it, never from a literal written here: a grant naming a table by hand
    can outlive the table, and the symptom is a `/readyz` 503 on a fresh install
    weeks later rather than anything visible at the rename.

    ``sequences`` are bare names inside the same schema. They exist because a
    `bigserial` column makes an INSERT that omits it read the sequence, so the
    INSERT privilege alone is not enough to append a row.
    """

    table: str
    privileges: tuple[str, ...]
    sequences: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.table.count(".") != 1 or not all(self.table.partition(".")):
            raise ValueError(f"{self.table!r} must be a schema-qualified relation")
        if not self.privileges:
            raise ValueError(f"{self.table} needs at least one privilege")
        unknown = sorted(set(self.privileges) - _COORDINATION_PRIVILEGES)
        if unknown:
            raise ValueError(f"{self.table} may not be granted {', '.join(unknown)}")

    @property
    def schema(self) -> str:
        return self.table.partition(".")[0]

    @property
    def name(self) -> str:
        return self.table.rpartition(".")[2]


def _coordination_runtime_grants() -> tuple[CoordinationRuntimeGrant, ...]:
    """The complete runtime privilege set, one entry per durable store.

    `docs/DEPLOY.md` is the source of truth for the privileges themselves; this
    is that block, expressed so that setup can issue it.

    The shape matters as much as the contents. Four of these tables were missing
    for a release: the app held `arw` on the ring lease and nothing at all on
    `startup_readiness`, so `StartupReadinessStore.read()` was refused, the
    readiness gate gave up permanently, and the deployed app sat at `/readyz`
    503. Nothing caught it because the list of grants was written out by hand
    next to nothing that could disagree with it.

    Two things disagree with it now. Every name is imported from the store's own
    module, so a rename breaks the import instead of silently orphaning a grant;
    and `tests/test_lifecycle.py` reads the `CREATE TABLE` statements out of
    `server/` and fails if a durable coordination table -- or a sequence behind a
    `serial` column -- has no entry here. Adding a table without a grant is
    therefore a red test at review time, not a 503 in front of an audience.
    """

    from .connection_spike_journal import ROUND5_CREATION_JOURNAL_TABLE
    from .coordination import COORDINATION_TABLE
    from .cost_ledger import CALIBRATION_TABLE, COST_LEDGER_TABLE, RECONCILIATION_TABLE
    from .pipeline_power import PIPELINE_POWER_SEQUENCE, PIPELINE_POWER_TABLE
    from .readiness import READINESS_TABLE
    from .receipts import BOUT_RECEIPT_TABLE

    return (
        # Claiming the ring is INSERT ... ON CONFLICT DO UPDATE, which needs both
        # halves; renew and release are UPDATE; `current()` and the staleness
        # diagnosis are SELECT. No DELETE: a lease is released by clearing its
        # columns, never by removing the row that carries the fencing token.
        CoordinationRuntimeGrant(COORDINATION_TABLE, ("SELECT", "INSERT", "UPDATE")),
        # Same upsert shape, one row per ring key. SELECT is the one that took the
        # app out of rotation: `initialize()` returns early when the table already
        # exists, so the missing privilege surfaced on `read()` instead, where
        # there was no InsufficientPrivilege handler to explain it.
        CoordinationRuntimeGrant(READINESS_TABLE, ("SELECT", "INSERT", "UPDATE")),
        # Append-only by contract, so no UPDATE and no DELETE -- the journal is
        # evidence, and a correction is a new row. `event_id` is bigserial.
        CoordinationRuntimeGrant(
            ROUND5_CREATION_JOURNAL_TABLE,
            ("SELECT", "INSERT"),
            ("round5_creation_journal_event_id_seq",),
        ),
        # Estimates are INSERT; `close_bout` and `reconcile_window` are UPDATE;
        # every read path is SELECT. `SELECT ... FOR UPDATE` needs the UPDATE
        # privilege too, which the same grant supplies. The immutability of an
        # original estimate is enforced by a trigger, not by withholding UPDATE.
        CoordinationRuntimeGrant(COST_LEDGER_TABLE, ("SELECT", "INSERT", "UPDATE")),
        # An immutable revision history: written once, never amended.
        CoordinationRuntimeGrant(RECONCILIATION_TABLE, ("SELECT", "INSERT")),
        # Recomputed rather than amended: `reconcile_window` DELETEs the row for
        # the affected key and INSERTs a freshly aggregated one. The only DELETE
        # the app holds anywhere, and the reason this table cannot borrow the
        # ledger's privilege set.
        CoordinationRuntimeGrant(CALIBRATION_TABLE, ("SELECT", "INSERT", "DELETE")),
        # The bout receipt history: append-only by construction. A second terminal
        # event after a declared one is a new row that loses the read, never an
        # overwrite, so there is no UPDATE and no DELETE. No sequence: the key is
        # (session_id, round_id, sealing_event), all supplied by the writer.
        CoordinationRuntimeGrant(BOUT_RECEIPT_TABLE, ("SELECT", "INSERT")),
        # Deliberate stops and starts of the Round 4 pipeline. Append-only for
        # the same reason the journal above is -- a correction is a new row and
        # the newest wins the read -- so no UPDATE and no DELETE. `event_id` is
        # bigserial, hence the sequence: without it the app's first stop fails at
        # the INSERT, and a stop that succeeded in the cloud but recorded nothing
        # is exactly the ambiguity this table exists to remove.
        CoordinationRuntimeGrant(
            PIPELINE_POWER_TABLE,
            ("SELECT", "INSERT"),
            (PIPELINE_POWER_SEQUENCE,),
        ),
    )


def _measured_lakebase_runtime_grants() -> tuple[CoordinationRuntimeGrant, ...]:
    """Every relation the deployed app touches in a *measured* Lakebase database.

    Separate from ``_coordination_runtime_grants`` because these are different
    databases, not different tables: coordination lives in one endpoint the app
    reads for leases and receipts, and Rounds 1, 2, 3 and 5 each race their own
    Lakebase endpoint carrying their own copy of these two tables. A grant on the
    coordination database says nothing about any of them, and a grant on Round 4's
    online schema -- the only measured grant setup ever issued -- says nothing
    either.

    WHY THIS EXISTS, and it is the fifth instance of one defect. Setup granted the
    app's principal on Round 4's schema and on the coordination tables and stopped.
    On the deployed app that left ``CONNECT`` on each measured database and
    ``USAGE`` on ``public`` -- both present, which is what made this look
    configured -- and *no table privilege at all*. Round 1 got past arming and was
    refused at the probe INSERT with SQLSTATE 42501, after the bell. Round 3 was
    refused at arm with `permission denied for table orders`. Both rounds were
    published as ``ready`` first.

    THE PRIVILEGES ARE LEAST-PRIVILEGE AND DELIBERATELY NOT ``ALL``, and each set
    is the union of the verbs that round's own SQL issues:

    * ``anti_demo_probe`` -- ``PsycopgPreparedTarget.attempt`` upserts a nonce and
      reads it back, so SELECT, INSERT and (for ``ON CONFLICT DO UPDATE``) UPDATE.
    * ``orders`` -- ``RecoveryContract`` selects, inserts and DELETEs it (Round 3);
      ``SafeChangeContract`` selects and inserts it (Round 2). The union is SELECT,
      INSERT, DELETE. No UPDATE: neither contract issues one.

    ``ALTER`` IS ABSENT AND CANNOT BE ADDED. Round 2's ``migration_sql`` is an
    ``ALTER TABLE public.orders ADD COLUMN``, and PostgreSQL has no grantable ALTER
    privilege -- it requires table ownership. This plan therefore cannot make Round
    2's migration step succeed for the app's principal, and pretending otherwise by
    granting something adjacent would put a green round on the card that dies
    mid-bout. See ``_grant_measured_lakebase_postgres`` for what is reported.

    Every table name is imported from the module that issues the statements
    (``server.targets`` and ``server.safe_change``), so a rename breaks an import
    here instead of silently orphaning a grant, and
    ``tests/test_lifecycle.py`` parses the contract SQL and fails if this plan does
    not cover a relation or a verb the code actually uses.
    """

    from .safe_change import ORDERS_TABLE
    from .targets import PROBE_PRIVILEGES, PROBE_TABLE

    return (
        CoordinationRuntimeGrant(PROBE_TABLE, PROBE_PRIVILEGES),
        CoordinationRuntimeGrant(ORDERS_TABLE, ("SELECT", "INSERT", "DELETE")),
    )


#: The measured Lakebase rounds whose lane connects AS THE APP'S OAUTH PRINCIPAL,
#: and therefore the only ones a grant to that principal can help.
#:
#: Round 4 is absent because ``_grant_round4_postgres`` owns its online schema and
#: Round 6 because it reaches Unity Catalog rather than a measured Postgres lane.
#: ROUND 5 IS ABSENT FOR A DIFFERENT REASON and it is worth stating, because it
#: was in this tuple and made a from-scratch provision fail: Round 5 authenticates
#: with a NATIVE Postgres login (``ROUND5_NATIVE_ROLE``, enabled by
#: ``_enable_round5_lakebase_native_login``), not with the app's service-principal
#: OAuth role. The app has no Postgres role in Round 5's database at all -- live
#: reads confirm it is the only round where that role is absent -- so every
#: statement here fails on it with "role does not exist", and the round is
#: unaffected either way. Its own receipt records the lane as "Built-in Lakebase
#: pool verified" with a null error while that role was missing, which is the
#: evidence that it never needed it.
MEASURED_LAKEBASE_ROUNDS = (1, 2, 3)

#: The group role that owns the relations the runtime issues ownership-requiring
#: DDL against, and of which BOTH the operator and the app are members.
#:
#: A group rather than either principal, and this is the whole design:
#: PostgreSQL satisfies an ownership check for any role holding the privileges of
#: the owner (``has_privs_of_role``), so two members of one owning role both pass
#: while neither has to take ownership away from the other. The local server keeps
#: running its DDL as the operator and the deployed app can run its migration,
#: from the same table, with no transfer and nothing to undo.
MEASURED_TABLE_OWNER_ROLE = "anti_demo_table_owner"


def _measured_lakebase_owned_relations() -> dict[int, tuple[str, ...]]:
    """Per round, the relations the app must pass an OWNERSHIP check on.

    THE SIXTH INSTANCE OF THIS FAMILY, AND A DISTINCT SUB-SPECIES. The five
    before it were missing *privileges*, and the repair was always a ``GRANT``.
    This one cannot be repaired that way at all: Round 2's ``migration_sql`` is
    an ``ALTER TABLE public.orders ADD COLUMN``, and ``ALTER TABLE`` -- like
    ``DROP``, ``TRUNCATE``, ``CREATE INDEX`` and ``COMMENT`` -- is checked
    against *ownership*, for which PostgreSQL has no grantable privilege. Every
    privilege in ``_measured_lakebase_runtime_grants`` was correctly granted and
    the deployed app still answered "must be owner of table orders".

    WHY THE APP DID NOT ALREADY OWN IT. ``_apply_schema`` seeds these tables as
    the operator principal, so the operator owns them; a Lakebase branch is a
    copy of its parent, so every per-bout branch inherits that ownership. The
    app can now create the branch (it holds ``CAN_MANAGE``) and still owns
    nothing inside it.

    SCOPE IS DERIVED, NOT LISTED. Only Round 2 appears because only
    ``SafeChangeContract`` issues ownership-requiring DDL;
    ``RecoveryContract`` (Round 3) selects, inserts and deletes, which are
    privileges it already holds, and Round 1's probe is an upsert. Round 3
    reuses Round 2's *adapter* to create its branch, which is why it needed
    ``CAN_MANAGE``, but it issues no DDL and therefore needs no ownership.
    ``tests/test_lifecycle.py`` parses the contracts for the whole DDL class and
    fails if a statement is added that this mapping does not cover, so a
    migration added to Round 3 is caught here rather than on stage.
    """

    from .safe_change import ORDERS_TABLE

    return {2: (ORDERS_TABLE,)}


async def _own_measured_lakebase_relations(
    manifest: DemoManifest, app_client_id: str | None
) -> list[str]:
    """Put the DDL-receiving relations under a role the app is a member of.

    APPLIED TO THE PARENT, WHICH IS THE ONLY REASON IT LASTS. These statements
    run against each round's *production* branch -- the same endpoint
    ``_round_lakebase_provider`` seeds and the same one every per-bout branch is
    cloned from. Roles, role memberships and table ownership are all copied by
    the clone, so a branch created after this runs inherits it. Applying the
    same repair inside a per-bout branch would be thrown away with the branch
    and the next bout would fail identically.

    Every statement is idempotent and every lane is attempted, with failures
    returned rather than raised, for the reason spelled out in
    ``_grant_measured_lakebase_postgres``.
    """

    if app_client_id is None:
        return []
    apply_manifest_environment(manifest)
    owned = _measured_lakebase_owned_relations()
    if manifest.round_environments is None:
        return []
    failures: list[str] = []
    group = sql.Identifier(MEASURED_TABLE_OWNER_ROLE)
    for number, relations in sorted(owned.items()):
        try:
            material = await _round_lakebase_provider(manifest, number).connection_material()
            connection = await _connect(material)
            async with connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(
                        "SELECT 1 FROM pg_roles WHERE rolname = %s",
                        (MEASURED_TABLE_OWNER_ROLE,),
                    )
                    if await cursor.fetchone() is None:
                        # NOLOGIN: nothing authenticates as this role; it exists
                        # only to be owned by, and to be a member of.
                        await cursor.execute(
                            sql.SQL("CREATE ROLE {} NOLOGIN").format(group)
                        )
                    # The operator keeps its DDL standing through membership
                    # rather than through ownership. Without this line the
                    # reassignment below would lock the local server, `resume`
                    # and `cleanup` out of their own tables -- and Lakebase
                    # refuses to grant a principal role back, so there would be
                    # no way to recover it.
                    for member in (material.user, app_client_id):
                        await cursor.execute(
                            sql.SQL("GRANT {} TO {}").format(group, sql.Identifier(member))
                        )
                    # `ALTER TABLE ... OWNER TO` requires the INCOMING owner to
                    # hold CREATE on the containing schema.
                    for relation in relations:
                        schema, _, name = relation.rpartition(".")
                        await cursor.execute(
                            sql.SQL("GRANT USAGE, CREATE ON SCHEMA {} TO {}").format(
                                sql.Identifier(schema or "public"), group
                            )
                        )
                        await cursor.execute(
                            sql.SQL("ALTER TABLE {}.{} OWNER TO {}").format(
                                sql.Identifier(schema or "public"),
                                sql.Identifier(name),
                                group,
                            )
                        )
                await connection.commit()
        except Exception as exc:  # noqa: BLE001 - every lane is reported, see docstring
            failures.append(f"Round {number}: {type(exc).__name__}: {exc}")
    return failures


async def _grant_measured_lakebase_postgres(
    manifest: DemoManifest, app_client_id: str | None
) -> list[str]:
    """Issue :func:`_measured_lakebase_runtime_grants` on every measured lane.

    Issued for the same round numbers ``seed_identical_schema`` seeds and through
    the same ``_round_lakebase_provider`` accessor it seeds them with, so a round
    that gains a Lakebase lane cannot receive a schema without also receiving the
    privileges that make it usable from the deployed app.

    EVERY LANE IS ATTEMPTED AND EVERY FAILURE IS RETURNED. Raising on the first one
    is what hid the shape of this bug for five iterations: the two failures
    available here -- a relation that does not exist yet, and a principal that has
    no role in this database -- are indistinguishable from one another when seen
    alone, and an operator who fixes the one they were shown then discovers the
    next. Both need to be visible at once. Round 5 used to demonstrate the second
    failure and is now correctly outside ``MEASURED_LAKEBASE_ROUNDS`` entirely,
    because it authenticates natively rather than as the app; see that constant.

    Returns the human-readable failures rather than raising them, so a caller can
    decide whether an un-grantable lane is fatal to a provision or is a round that
    should simply not be offered. The caller in ``_prepare_and_reseal_round4``
    treats them as fatal; the report is what makes that decision reviewable.
    """

    if app_client_id is None:
        return []
    apply_manifest_environment(manifest)
    grants = _measured_lakebase_runtime_grants()
    per_round = manifest.round_environments is not None
    round_numbers = MEASURED_LAKEBASE_ROUNDS if per_round else (1,)
    failures: list[str] = []
    for number in round_numbers:
        provider = (
            _round_lakebase_provider(manifest, number)
            if per_round
            else LakebaseCredentialProvider()
        )
        try:
            material = await provider.connection_material()
            connection = await _connect(material)
            async with connection:
                async with connection.cursor() as cursor:
                    role = sql.Identifier(app_client_id)
                    await cursor.execute(
                        sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                            sql.Identifier(material.database), role
                        )
                    )
                    for grant in grants:
                        await cursor.execute(
                            sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                                sql.Identifier(grant.schema), role
                            )
                        )
                        await cursor.execute(
                            sql.SQL("GRANT {} ON TABLE {}.{} TO {}").format(
                                sql.SQL(", ").join(
                                    sql.SQL(privilege) for privilege in grant.privileges
                                ),
                                sql.Identifier(grant.schema),
                                sql.Identifier(grant.name),
                                role,
                            )
                        )
                await connection.commit()
        except Exception as exc:  # noqa: BLE001 - every lane is reported, see docstring
            failures.append(f"Round {number}: {type(exc).__name__}: {exc}")
    return failures


async def _grant_round4_postgres(
    manifest: DemoManifest, names: dict[str, str], app_client_id: str | None
) -> None:
    from .coordination import COORDINATION_SCHEMA, read_coordination_objects

    if app_client_id is None:
        return
    apply_manifest_environment(manifest)
    material = await _round_lakebase_provider(
        manifest, 4, database=ROUND4_DATABASE
    ).connection_material()
    connection = await _connect(material)
    async with connection:
        async with connection.cursor() as cursor:
            role = sql.Identifier(app_client_id)
            await cursor.execute(
                sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                    sql.Identifier(ROUND4_DATABASE), role
                )
            )
            await cursor.execute(
                sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                    sql.Identifier(names["online_schema"]), role
                )
            )
            await cursor.execute(
                sql.SQL("GRANT SELECT ON TABLE {}.{} TO {}").format(
                    sql.Identifier(names["online_schema"]),
                    sql.Identifier(ROUND4_SYNCED_TABLE),
                    role,
                )
            )
        await connection.commit()

    coordination_endpoint = _coordination_endpoint_name(manifest)
    endpoint = _databricks_json(
        manifest.databricks.profile,
        "postgres",
        "get-endpoint",
        coordination_endpoint,
    )
    if endpoint.get("name") != coordination_endpoint:
        raise RuntimeError("Round 4 coordination endpoint identity is not exact")
    host = str((((endpoint.get("status") or {}).get("hosts") or {}).get("host")) or "")
    credential = _databricks_json(
        manifest.databricks.profile,
        "postgres",
        "generate-database-credential",
        coordination_endpoint,
    )
    token = str(credential.get("token") or "")
    if not host or not token:
        raise RuntimeError("Round 4 coordination setup credential is unavailable")
    coordination_material = ConnectionMaterial(
        host=host,
        port=5432,
        database=manifest.databricks.database,
        user=manifest.databricks.user,
        password=token,
    )
    coordination = await _connect(coordination_material)
    async with coordination:
        async with coordination.cursor() as cursor:
            role = sql.Identifier(app_client_id)
            await cursor.execute(
                sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                    sql.Identifier(manifest.databricks.database), role
                )
            )
            await cursor.execute(
                sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                    sql.Identifier(COORDINATION_SCHEMA), role
                )
            )
            grants = _coordination_runtime_grants()
            # A GRANT cannot name a relation that does not exist, and the raw
            # `UndefinedTable` it raises names one table with no hint about which
            # side of the split is wrong. `ensure_coordination` provisions all of
            # them; the one provision path that can skip it is a resume of an
            # installation whose Round 4 was already sealed.
            objects = await read_coordination_objects(
                cursor, [grant.table for grant in grants]
            )
            if not objects.complete:
                raise RuntimeError(
                    "The app's runtime privileges cannot be granted: the coordination "
                    f"schema is missing {objects.describe_missing()}. Setup creates "
                    "these in `ensure_coordination` as the schema owner, so re-run "
                    "'antidemo setup' rather than granting by hand -- the app holds no "
                    "CREATE and cannot make them itself."
                )
            for grant in grants:
                await cursor.execute(
                    sql.SQL("GRANT {} ON TABLE {}.{} TO {}").format(
                        sql.SQL(", ").join(
                            sql.SQL(privilege) for privilege in grant.privileges
                        ),
                        sql.Identifier(grant.schema),
                        sql.Identifier(grant.name),
                        role,
                    )
                )
                for sequence in grant.sequences:
                    await cursor.execute(
                        sql.SQL("GRANT USAGE, SELECT ON SEQUENCE {}.{} TO {}").format(
                            sql.Identifier(grant.schema),
                            sql.Identifier(sequence),
                            role,
                        )
                    )
        await coordination.commit()


def _ensure_round4(manifest: DemoManifest, *, timeout: float) -> DemoManifest:
    names = _round4_names(manifest)
    sealed = manifest.round4
    actual_principal = _verify_databricks_identity(manifest.databricks.profile)
    if actual_principal != manifest.databricks.user:
        raise RuntimeError("Round 4 setup principal differs from the owned manifest user")
    if sealed is None:
        warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "").strip()
        if not warehouse_id:
            raise RuntimeError("DATABRICKS_WAREHOUSE_ID is required to provision Round 4")
        setup_principal = actual_principal
        app_client_id = os.environ.get("DATABRICKS_APP_CLIENT_ID", "").strip()
        if not app_client_id:
            raise RuntimeError("DATABRICKS_APP_CLIENT_ID is required to provision Round 4")
        _require_round4_catalog(manifest, names["catalog"])
    else:
        warehouse_id = sealed.warehouse_id
        setup_principal = sealed.setup_principal
        app_client_id = sealed.app_service_principal_client_id
        if not app_client_id:
            raise RuntimeError("Round 4 manifest is missing its application principal")
        if setup_principal != actual_principal:
            raise RuntimeError("Round 4 setup principal differs from its sealed identity")
        expected = {
            "source_table_full_name": names["source_table"],
            "storage_catalog": names["catalog"],
            "storage_schema": names["storage_schema"],
            "synced_table_id": names["synced_table_id"],
            "synced_table_resource_name": names["resource_name"],
            "physical_database": ROUND4_DATABASE,
            "physical_schema": names["online_schema"],
            "physical_table": ROUND4_SYNCED_TABLE,
            "branch": names["branch"],
            "endpoint_name": names["endpoint_name"],
        }
        for field, value in expected.items():
            if getattr(sealed, field) != value:
                raise RuntimeError(f"Round 4 manifest has an unexpected {field}")
        sealed_contract = ModelScoreContract(
            pipeline_id=sealed.pipeline_id,
            source_table=sealed.source_table_full_name,
            synced_table=(
                f"{sealed.physical_database}.{sealed.physical_schema}.{sealed.physical_table}"
            ),
        )
        if sealed.contract_sha256 != sealed_contract.sha256:
            raise RuntimeError("Round 4 manifest contract hash does not match its resources")

    project = (
        _get_lakebase_project_or_none(
            manifest,
            project_id=names["project"].removeprefix("projects/"),
        )
        if manifest.round_environments is not None
        else _get_lakebase_project_or_none(manifest)
    )
    branch = _round4_get_branch(manifest, names)
    if project is None or branch is None:
        raise RuntimeError("Round 4 Lakebase project or production branch does not exist")
    project_uid, branch_uid = _validate_round4_project_and_branch(
        manifest, names, project, branch, sealed=sealed
    )

    postgres = _round4_get_synced_table(manifest, names)
    database = _round4_get_database_synced_table(manifest, names)
    existing = postgres is not None or database is not None
    if existing:
        # An interrupted create can be visible from one API before the other.
        # Seeing either representation permanently forbids a second POST.
        postgres, database = _wait_round4_cross_endpoint_table(manifest, names, timeout=timeout)
    else:
        _prepare_round4_source_artifacts(manifest, names, warehouse_id)
        for label, kind, full_name in (
            ("source schema", "schemas", f"{names['catalog']}.{names['source_schema']}"),
            ("storage schema", "schemas", f"{names['catalog']}.{names['storage_schema']}"),
            ("online schema", "schemas", f"{names['catalog']}.{names['online_schema']}"),
            ("source table", "tables", names["source_table"]),
        ):
            _validate_round4_uc_object(
                _round4_get_uc_object(manifest, kind, full_name),
                full_name=full_name,
                setup_principal=setup_principal,
                label=label,
            )
        postgres = _create_or_get_round4_synced_table(manifest, names, timeout=timeout)
        postgres, database = _wait_round4_cross_endpoint_table(manifest, names, timeout=timeout)

    uid, pipeline_id = _validate_round4_synced_table(manifest, postgres, names, sealed=sealed)
    _validate_round4_database_synced_table(
        database,
        names,
        project_uid=project_uid,
        branch_uid=branch_uid,
        pipeline_id=pipeline_id,
    )
    _validate_round4_pipeline(
        _round4_get_pipeline(manifest, pipeline_id),
        pipeline_id=pipeline_id,
        synced_table_uid=uid,
        setup_principal=setup_principal,
        names=names,
    )
    _validate_round4_uc_contract(
        manifest,
        names,
        setup_principal=setup_principal,
        pipeline_id=pipeline_id,
        require_storage_schema=True,
    )

    # Only after all pre-existing cross-endpoint resources pass validation may
    # resume repair the source baseline.
    source_version = _repair_round4_baseline(manifest, names, warehouse_id)
    ready, ready_database = _wait_round4_baseline(
        manifest,
        names,
        source_version,
        project_uid=project_uid,
        branch_uid=branch_uid,
        pipeline_id=pipeline_id,
        timeout=timeout,
    )
    uid, pipeline_id = _validate_round4_synced_table(manifest, ready, names, sealed=sealed)
    _validate_round4_database_synced_table(
        ready_database,
        names,
        project_uid=project_uid,
        branch_uid=branch_uid,
        pipeline_id=pipeline_id,
        require_sync_position=True,
    )
    resource_name = str(ready.get("name") or "")
    # Every Lakebase project the app must reach, before any role work needs one
    # of them to already be reachable. This sits in `_ensure_round4`, which
    # `_prepare_and_reseal_round4` calls and `resume_provision` runs
    # UNCONDITIONALLY -- deliberately not the Round 6 side, which `resume` skips
    # on `round6_ready` and which is how a grant has been missed here before.
    project_failures = _grant_lakebase_app_projects(manifest, app_client_id)
    if project_failures:
        raise RuntimeError(
            "The deployed app could not be granted "
            f"{_LAKEBASE_APP_PROJECT_PERMISSION} on "
            f"{len(project_failures)} Lakebase project(s), so the rounds behind them "
            "would be published as ready and then refused on the control plane: "
            + "; ".join(project_failures)
        )
    _ensure_round4_app_roles(
        manifest,
        app_client_id,
        timeout=timeout,
    )
    _grant_round4_uc_and_warehouse(manifest, names, warehouse_id, pipeline_id, app_client_id)
    asyncio.run(_grant_round4_postgres(manifest, names, app_client_id))
    # A Lakebase Postgres role is created per branch, and the call above reaches
    # only Round 4's branch and coordination's. Both measured steps below name the
    # app as a grantee inside the measured lanes' own databases, which PostgreSQL
    # refuses outright when no role of that name exists there, so a first provision
    # cannot get past them unless the role is created on those branches first.
    _ensure_lakebase_app_roles(
        manifest,
        app_client_id,
        _measured_lakebase_app_role_branches(manifest),
        timeout=timeout,
    )
    # The measured lanes, which are different databases from either of the two the
    # call above grants on. Sequenced after it so a failure here cannot leave Round
    # 4 half-granted, and before the contract below so a provision that cannot make
    # Rounds 1/2/3 usable from the deployed app fails at setup rather than in front
    # of an audience.
    # Ownership before privileges. `ALTER TABLE ... OWNER TO` preserves the ACL
    # it finds, but sequencing the grants afterwards means the privileges are
    # re-asserted against the final owner rather than an intermediate one.
    ownership_failures = asyncio.run(
        _own_measured_lakebase_relations(manifest, app_client_id)
    )
    if ownership_failures:
        raise RuntimeError(
            "The deployed app could not be made a member of the role owning the "
            "relations it issues DDL against, so Round 2's migration would be "
            "published as ready and then refused with 'must be owner of table': "
            + "; ".join(ownership_failures)
        )
    measured_failures = asyncio.run(
        _grant_measured_lakebase_postgres(manifest, app_client_id)
    )
    if measured_failures:
        raise RuntimeError(
            "The deployed app could not be granted the measured Lakebase privileges "
            f"on {len(measured_failures)} of {len(MEASURED_LAKEBASE_ROUNDS)} lanes, "
            "so those rounds would be published as ready and then refused on a "
            "permission: " + "; ".join(measured_failures)
        )
    contract = ModelScoreContract(
        pipeline_id=pipeline_id,
        source_table=names["source_table"],
        synced_table=(f"{ROUND4_DATABASE}.{names['online_schema']}.{ROUND4_SYNCED_TABLE}"),
    )
    resealed_round4 = Round4Resources(
        warehouse_id=warehouse_id,
        setup_principal=setup_principal,
        app_service_principal_client_id=app_client_id,
        source_table_full_name=names["source_table"],
        storage_catalog=names["catalog"],
        storage_schema=names["storage_schema"],
        synced_table_id=names["synced_table_id"],
        synced_table_resource_name=resource_name,
        synced_table_uid=uid,
        pipeline_id=pipeline_id,
        project_uid=project_uid,
        branch_uid=branch_uid,
        physical_database=ROUND4_DATABASE,
        physical_schema=names["online_schema"],
        physical_table=ROUND4_SYNCED_TABLE,
        branch=names["branch"],
        endpoint_name=names["endpoint_name"],
        contract_sha256=contract.sha256,
    )
    _commit_round4_reseal(manifest, resealed_round4)
    manifest.status = "waiting_for_zero"
    save_manifest(manifest)
    return manifest


def _commit_round4_reseal(manifest: DemoManifest, resealed_round4: Round4Resources) -> None:
    preserve_round6 = manifest.round6_ready and manifest.round4 == resealed_round4
    preserve_round5 = manifest.round5_ready and manifest.round4 == resealed_round4
    manifest.round4 = resealed_round4
    if preserve_round6:
        manifest.manifest_version = (
            7
            if manifest.installation_id is not None
            and manifest.round_environments is not None
            and manifest.coordination_environment is not None
            else 6
        )
        return
    if preserve_round5:
        manifest.round6 = None
        manifest.manifest_version = 5
        return
    manifest.round6 = None
    manifest.round5 = None
    manifest.manifest_version = 2


def _prepare_and_reseal_round4(manifest: DemoManifest, *, timeout: float) -> DemoManifest:
    manifest = _ensure_round4(manifest, timeout=timeout)
    print(
        "SEAL  Round 4 connections closed; waiting for Round 1 Lakebase scale zero",
        flush=True,
    )
    asyncio.run(wait_for_scale_zero(manifest, timeout))
    manifest.status = "ready"
    save_manifest(manifest)
    return manifest


async def _ensure_lakebase_database(
    database: str, provider: LakebaseCredentialProvider | None = None
) -> None:
    if provider is None:
        provider = LakebaseCredentialProvider(database="postgres")
    original_database = provider.database
    provider.database = "postgres"
    try:
        admin_material = await provider.connection_material()
    finally:
        provider.database = original_database
    connection = await _connect(admin_material, autocommit=True)
    async with connection:
        async with connection.cursor() as cursor:
            await cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database,))
            if await cursor.fetchone() is None:
                await cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))


async def _apply_schema(material: ConnectionMaterial) -> str:
    connection = await _connect(material)
    async with connection:
        async with connection.cursor() as cursor:
            for path in BASE_SCHEMA_PATHS:
                await cursor.execute(path.read_text(encoding="utf-8"))
            await cursor.execute("TRUNCATE TABLE public.anti_demo_probe")
            await cursor.execute(
                "SELECT current_setting('server_version_num')::int, "
                "to_regclass('public.anti_demo_probe')::text, "
                "(SELECT count(*) FROM public.anti_demo_probe), "
                "to_regclass('public.orders')::text, "
                "(SELECT count(*) FROM public.orders), "
                "EXISTS ("
                "  SELECT 1 FROM information_schema.columns "
                "  WHERE table_schema = 'public' AND table_name = 'orders' "
                "    AND column_name = 'delivery_instructions'"
                ")"
            )
            row = await cursor.fetchone()
        await connection.commit()
    if (
        row is None
        or row[1] != "anti_demo_probe"
        or row[2] != 0
        or row[3] != "orders"
        or row[4] != 1
        or row[5] is True
    ):
        raise RuntimeError("Base schema verification failed")
    major = int(row[0]) // 10_000
    if major != 17:
        raise RuntimeError(f"PostgreSQL major version is {major}, expected 17")
    return str(row[0])


async def seed_identical_schema(manifest: DemoManifest) -> tuple[str, str, str]:
    apply_manifest_environment(manifest)
    if manifest.round_environments is not None:
        round_numbers = (1, 2, 3, 5)
        # Round 1 stands up no RDS instance, so there is nothing there to seed.
        # Lakebase and Aurora keep all four rounds; only the RDS fleet is short
        # one. The three material tuples are therefore no longer the same length,
        # which is why the return below indexes by cumulative offset rather than
        # assuming a common width.
        rds_round_numbers = tuple(
            number for number in round_numbers if rds_lane_is_scored(_ROUND_NUMBER_IDS[number])
        )
        lakebase_providers = tuple(
            _round_lakebase_provider(manifest, number) for number in round_numbers
        )
        aurora_providers = tuple(
            _round_aurora_provider(manifest, number) for number in round_numbers
        )
        rds_providers = tuple(
            _round_rds_provider(manifest, number) for number in rds_round_numbers
        )
        await asyncio.gather(
            *(
                _ensure_lakebase_database(manifest.databricks.database, provider)
                for provider in lakebase_providers
            )
        )
        lakebase_materials = await asyncio.gather(
            *(provider.connection_material() for provider in lakebase_providers)
        )
        aurora_materials = await asyncio.gather(
            *(provider.connection_material() for provider in aurora_providers)
        )
        rds_materials = await asyncio.gather(
            *(provider.connection_material() for provider in rds_providers)
        )
    else:
        await _ensure_lakebase_database(manifest.databricks.database)
        lakebase_materials = (await LakebaseCredentialProvider().connection_material(),)
        aurora_materials = (await AuroraCredentialProvider().connection_material(),)
        rds_materials = (await RdsCredentialProvider().connection_material(),)
    versions = await asyncio.gather(
        *(_apply_schema(material) for material in lakebase_materials),
        *(_apply_schema(material) for material in aurora_materials),
        *(_apply_schema(material) for material in rds_materials),
    )
    return (
        versions[0],
        versions[len(lakebase_materials)],
        versions[len(lakebase_materials) + len(aurora_materials)],
    )


def _coordination_endpoint_name(manifest: DemoManifest) -> str:
    return _coordination_lakebase_binding(manifest).endpoint_name


def _coordination_branch_or_none(manifest: DemoManifest) -> dict[str, Any] | None:
    branch_name = f"projects/{manifest.databricks.project_id}/branches/coordination"
    result = subprocess.run(
        [
            "databricks",
            "postgres",
            "get-branch",
            branch_name,
            "-p",
            manifest.databricks.profile,
            "-o",
            "json",
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    if result.returncode != 0:
        detail = f"{result.stderr}\n{result.stdout}".lower()
        if any(
            marker in detail
            for marker in ("not found", "does not exist", "resource_does_not_exist", "404")
        ):
            return None
        raise _safe_failure(result)
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError("The coordination branch lookup returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("The coordination branch lookup returned an unexpected shape")
    return payload


def ensure_coordination(manifest: DemoManifest) -> DemoManifest:
    """Create the dedicated, non-measured Lakebase lease endpoint and schema."""
    expected_endpoint = _coordination_endpoint_name(manifest)
    if _round_lakebase_binding(manifest, 1).endpoint_name == expected_endpoint:
        raise RuntimeError("Coordination cannot use the measured Lakebase endpoint")
    configured = manifest.databricks.coordination_endpoint_name
    if configured and configured != expected_endpoint:
        raise RuntimeError("The coordination endpoint does not match the owned project")

    if manifest.round_environments is None:
        branch_name = f"projects/{manifest.databricks.project_id}/branches/coordination"
        source_branch = f"projects/{manifest.databricks.project_id}/branches/production"
        branch = _coordination_branch_or_none(manifest)
        if branch is None:
            print("CREATE dedicated Lakebase coordination branch", flush=True)
            _run(
                [
                    "databricks",
                    "postgres",
                    "create-branch",
                    f"projects/{manifest.databricks.project_id}",
                    "coordination",
                    "--json",
                    json.dumps(
                        {
                            "spec": {
                                "source_branch": source_branch,
                                "no_expiry": True,
                            }
                        }
                    ),
                    "--timeout",
                    "10m",
                    "-p",
                    manifest.databricks.profile,
                    "-o",
                    "json",
                ],
                capture=True,
                timeout=700,
            )
            branch = _coordination_branch_or_none(manifest)
        if branch is None or str(branch.get("name") or "") != branch_name:
            raise RuntimeError("The dedicated coordination branch could not be verified")
        branch_status = branch.get("status") or {}
        returned_source = str(branch_status.get("source_branch") or "")
        if returned_source and returned_source != source_branch:
            raise RuntimeError("The coordination branch has unexpected source lineage")
    else:
        sealed = manifest.coordination_lakebase
        if sealed is None or sealed.endpoint_name != expected_endpoint:
            raise RuntimeError("The installation coordination seal is incomplete")

    for mask, spec in (
        (
            "spec.autoscaling_limit_min_cu,spec.autoscaling_limit_max_cu",
            {"autoscaling_limit_min_cu": 0.5, "autoscaling_limit_max_cu": 1},
        ),
        ("spec.suspension", {"suspend_timeout_duration": "60s"}),
    ):
        _run(
            [
                "databricks",
                "postgres",
                "update-endpoint",
                expected_endpoint,
                mask,
                "--json",
                json.dumps({"spec": spec}),
                "--timeout",
                "10m",
                "-p",
                manifest.databricks.profile,
                "-o",
                "json",
            ],
            capture=True,
            timeout=700,
        )
    endpoint = _databricks_json(
        manifest.databricks.profile,
        "postgres",
        "get-endpoint",
        expected_endpoint,
    )
    returned_endpoint = str(endpoint.get("name") or "")
    host = str((((endpoint.get("status") or {}).get("hosts") or {}).get("host")) or "")
    if returned_endpoint != expected_endpoint or not host:
        raise RuntimeError("The dedicated coordination endpoint could not be verified")
    if (
        manifest.coordination_lakebase is not None
        and host != manifest.coordination_lakebase.direct_host
    ):
        raise RuntimeError("The installation coordination host changed after sealing")

    manifest.databricks.coordination_endpoint_name = expected_endpoint
    apply_manifest_environment(manifest)

    async def initialize_table() -> None:
        from .coordination import LakebaseBoutLeaseStore, read_coordination_objects
        from .cost_ledger import LakebaseCostLedgerStore
        from .pipeline_power import DurablePipelinePowerStore
        from .readiness import StartupReadinessStore
        from .receipts import DurableReceiptStore

        if manifest.round_environments is not None:
            coordination_provider = LakebaseCredentialProvider(
                endpoint=expected_endpoint,
                profile=manifest.databricks.profile,
                database=manifest.databricks.database,
                user=manifest.databricks.user,
                expected_region=manifest.aws.region,
            )
            await _ensure_lakebase_database(manifest.databricks.database, coordination_provider)

        store = LakebaseBoutLeaseStore(
            endpoint_name=expected_endpoint,
            database=manifest.databricks.database,
            profile=manifest.databricks.profile,
            host=host,
            user=manifest.databricks.user,
        )
        await store.initialize()
        # The remaining durable objects, created here by the store that owns each
        # one's DDL rather than by a copy of it in this module.
        #
        # This is not a convenience. Setup is the only place the app's privileges
        # are granted (`_grant_round4_postgres`, one provision earlier than a
        # first serve), and a GRANT names a relation that has to already exist.
        # Left to the app, `startup_readiness` and the three cost tables are
        # created by whichever process first runs as an identity holding CREATE --
        # the operator's local server -- which on a fresh install is *after* the
        # grants have already been issued and sealed. `docs/DEPLOY.md` has always
        # said setup provisions every object in the coordination schema as the
        # operator; for four of them it did not, and the app inherited a schema it
        # could neither create nor be granted.
        #
        # `_run` is the store's own connection runner, reached the same way
        # `readiness.py` reaches it when it builds this store for the app.
        await StartupReadinessStore(store._run).initialize()
        await DurableReceiptStore(store._run).initialize()
        await DurablePipelinePowerStore(store._run).initialize()
        await store.close()

        ledger = LakebaseCostLedgerStore(
            endpoint_name=expected_endpoint,
            database=manifest.databricks.database,
            profile=manifest.databricks.profile,
            host=host,
            user=manifest.databricks.user,
        )
        try:
            await ledger.initialize()
        finally:
            await ledger.close()

        credential = _databricks_json(
            manifest.databricks.profile,
            "postgres",
            "generate-database-credential",
            expected_endpoint,
        )
        token = str(credential.get("token") or "")
        if not token:
            raise RuntimeError("Round 5 journal setup credential is unavailable")
        connection = await _connect(
            ConnectionMaterial(
                host=host,
                port=5432,
                database=manifest.databricks.database,
                user=manifest.databricks.user,
                password=token,
            )
        )
        async with connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {ROUND5_JOURNAL_TABLE} (
                        event_id bigserial PRIMARY KEY,
                        bout_id text NOT NULL,
                        fencing_token bigint NOT NULL CHECK (fencing_token > 0),
                        ordinal integer NOT NULL CHECK (ordinal > 0),
                        resource_kind text NOT NULL CHECK (resource_kind <> ''),
                        deterministic_name text,
                        client_token text,
                        provider_id text,
                        lifecycle_state text NOT NULL CHECK (lifecycle_state IN (
                            'create_intent', 'created', 'create_failed',
                            'delete_intent', 'deleted', 'delete_failed', 'refused'
                        )),
                        metadata jsonb NOT NULL CHECK (jsonb_typeof(metadata) = 'object'),
                        runtime_seal_sha256 char(64) NOT NULL CHECK (
                            runtime_seal_sha256 ~ '^[0-9a-f]{{64}}$'
                        ),
                        intent_at timestamptz NOT NULL,
                        occurred_at timestamptz NOT NULL,
                        completed_at timestamptz,
                        error text,
                        CHECK (deterministic_name IS NOT NULL OR client_token IS NOT NULL)
                    )
                    """
                )
                await cursor.execute(
                    "CREATE INDEX IF NOT EXISTS round5_creation_journal_scope_idx "
                    f"ON {ROUND5_JOURNAL_TABLE} (bout_id, fencing_token, event_id)"
                )
                # Prove the schema now holds every relation the app will be
                # granted, before any grant is issued. A GRANT on an absent table
                # is a hard error mid-provision that names one table; this names
                # all of them, and it is the check that fails if a store gains a
                # durable table whose creation never reached setup.
                objects = await read_coordination_objects(
                    cursor, [grant.table for grant in _coordination_runtime_grants()]
                )
                if not objects.complete:
                    raise RuntimeError(
                        "The coordination schema is missing "
                        f"{objects.describe_missing()} after setup created it. The "
                        "deployed app is granted privileges on these and can create "
                        "none of them, so provisioning cannot continue."
                    )
            await connection.commit()

    asyncio.run(initialize_table())
    save_manifest(manifest)
    return manifest


def _round5_active_journal_addons(manifest: DemoManifest) -> list[str]:
    """Return unresolved append-only journal identities from the coordination DB."""
    endpoint_name = _coordination_endpoint_name(manifest)
    endpoint = _databricks_json(
        manifest.databricks.profile, "postgres", "get-endpoint", endpoint_name
    )
    host = str((((endpoint.get("status") or {}).get("hosts") or {}).get("host")) or "")
    credential = _databricks_json(
        manifest.databricks.profile,
        "postgres",
        "generate-database-credential",
        endpoint_name,
    )
    token = str(credential.get("token") or "")
    if endpoint.get("name") != endpoint_name or not host or not token:
        raise RuntimeError("Round 5 journal inspection could not bind the exact endpoint")

    async def inspect() -> list[str]:
        connection = await _connect(
            ConnectionMaterial(
                host=host,
                port=5432,
                database=manifest.databricks.database,
                user=manifest.databricks.user,
                password=token,
            )
        )
        async with connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    f"""
                    SELECT bout_id, fencing_token, ordinal, lifecycle_state
                    FROM (
                        SELECT bout_id, fencing_token, ordinal, lifecycle_state,
                               row_number() OVER (
                                   PARTITION BY bout_id, fencing_token, ordinal
                                   ORDER BY event_id DESC
                               ) AS newest
                        FROM {ROUND5_JOURNAL_TABLE}
                    ) AS journal
                    WHERE newest = 1 AND lifecycle_state <> 'deleted'
                    ORDER BY bout_id, fencing_token, ordinal
                    """
                )
                rows = await cursor.fetchall()
        return [f"{row[0]}:{row[1]}:{row[2]}:{row[3]}" for row in rows]

    try:
        return asyncio.run(inspect())
    except (psycopg.errors.InvalidSchemaName, psycopg.errors.UndefinedTable) as exc:
        if manifest.manifest_version < 4:
            return []
        raise RuntimeError("Round 5 journal table is missing from the sealed baseline") from exc


def _round5_runtime_tag_inventory(manifest: DemoManifest) -> list[str]:
    """Discover deterministic names first, then fail closed on exact ownership."""
    candidate = getattr(manifest, "round5", None)
    sealed = candidate if isinstance(candidate, Round5Resources) else None
    if sealed is None and getattr(manifest, "round5_ready", False):
        require = getattr(manifest, "require_round5_resources", None)
        if callable(require):
            sealed = require()
    expected = (
        sealed.ownership_tags.as_aws_tags()
        if sealed is not None
        else {
            "anti-demo-run-id": manifest.run_id,
            "Owner": manifest.owner,
            "owner": manifest.owner,
            "expires-at": _utc_tag(manifest.expires_at),
            "managed-by": "round5-lifecycle",
        }
    )
    name_prefix = sealed.bout_name_prefix[:40].rstrip("-") + "-" if sealed else ""
    secret_prefix = (
        sealed.secret_name_prefix.rstrip("/") + "/"
        if sealed is not None and sealed.secret_name_prefix
        else ""
    )
    found: list[str] = []

    def tags_for(values: list[dict[str, Any]]) -> dict[str, str]:
        return {
            str(tag.get("Key") or ""): str(tag.get("Value") or "")
            for tag in values
            if isinstance(tag, dict)
        }

    def is_legacy_candidate(tags: list[dict[str, Any]]) -> bool:
        actual = tags_for(tags)
        return (
            actual.get("anti-demo-run-id") == manifest.run_id
            and actual.get("managed-by") == "round5-lifecycle"
        )

    def accept(
        identity: str,
        deterministic_name: str,
        tags: list[dict[str, Any]],
        *,
        iam: bool = False,
    ) -> None:
        actual = tags_for(tags)
        bout_id = actual.get("anti-demo-bout-id", "")
        token = actual.get("anti-demo:bout-token", "")
        resource_expected = (
            {
                key: value
                for key, value in expected.items()
                if key.casefold() != "owner" or key == "owner"
            }
            if iam
            else expected
        )
        exact = {
            **resource_expected,
            "anti-demo-bout-id": bout_id,
            "anti-demo:bout-token": token,
        }
        expected_token = hashlib.sha256(bout_id.encode()).hexdigest()[:16] if bout_id else ""
        if (
            actual != exact
            or not bout_id
            or token != expected_token
            or (sealed is not None and token not in deterministic_name)
        ):
            raise RuntimeError(f"Round 5 cleanup refused: ownership tags differ for {identity}")
        found.append(identity)

    def matching_name(name: str, *, secret: bool = False) -> bool:
        if sealed is None:
            return False
        prefix = secret_prefix if secret else name_prefix
        if not prefix or not name.startswith(prefix):
            return False
        suffix = name[len(prefix) :]
        return bool(
            re.fullmatch(r"[0-9a-f]{16}", suffix)
            if secret
            else re.fullmatch(r"[0-9a-f]{16}-.+", suffix)
        )

    session = _aws_session(manifest)
    secrets_manager = session.client("secretsmanager")
    token: str | None = None
    while True:
        arguments: dict[str, Any] = {"IncludePlannedDeletion": True}
        arguments["Filters"] = (
            [{"Key": "name", "Values": [secret_prefix]}]
            if secret_prefix
            else [
                {"Key": "tag-key", "Values": ["anti-demo-run-id"]},
                {"Key": "tag-value", "Values": [manifest.run_id]},
            ]
        )
        if token:
            arguments["NextToken"] = token
        page = secrets_manager.list_secrets(**arguments)
        for secret in page.get("SecretList", []):
            name = str(secret.get("Name") or "")
            tags = secret.get("Tags") or []
            if matching_name(name, secret=True):
                # A deterministic per-bout name. Planned deletions are asked for
                # on purpose here: a secret inside its recovery window still
                # holds its name, so the next bout that tried to create it would
                # collide, and that is a real obstruction whether or not the
                # secret can be read.
                accept(str(secret.get("ARN") or name or "secret"), name, tags)
            elif not secret_prefix and is_legacy_candidate(tags):
                # The tag-only sweep, and here a tag hit is not existence.
                # Nothing recreates a legacy secret by name, so a scheduled
                # deletion obstructs nothing -- it cannot be read and it is not
                # billing. Everything this function returns makes
                # `_require_round5_clean_baseline` raise, so counting one would
                # refuse every teardown of this installation for the length of
                # the recovery window, naming a resource whose only remedy is to
                # wait. `DeletedDate` is the owning service's own answer to "does
                # this still exist", which is the distinction
                # `reconcile._RETIRING` already draws for databases and
                # instances.
                if secret.get("DeletedDate"):
                    continue
                accept(str(secret.get("ARN") or name or "secret"), name, tags)
        token = str(page.get("NextToken") or "") or None
        if token is None:
            break

    ec2 = session.client("ec2")
    group_filters = (
        [{"Name": "vpc-id", "Values": [sealed.vpc_id]}]
        if sealed is not None
        else [{"Name": "tag:anti-demo-run-id", "Values": [manifest.run_id]}]
    )
    groups = ec2.describe_security_groups(Filters=group_filters).get("SecurityGroups", [])
    for group in groups:
        name = str(group.get("GroupName") or "")
        tags = group.get("Tags") or []
        if matching_name(name) or (sealed is None and is_legacy_candidate(tags)):
            accept(str(group.get("GroupId") or name or "security-group"), name, tags)
    # Round 5's own RDS security group, not the flat `aws.resources` mirror. That
    # mirror is named for Round 1, and Round 1 now stands up no RDS instance, so it
    # mirrors an empty string -- which DescribeSecurityGroupRules rejects as a
    # malformed group ID. On a pre-v7 manifest the mirror is still the right value,
    # because there one instance served every round.
    round5_rds_group_id = manifest.aws.resources.rds_security_group_id
    if getattr(manifest, "round_environments", None) is not None:
        round5_rds = manifest.round_environment(RoundId.SURVIVE_CONNECTION_SPIKE).rds
        round5_rds_group_id = round5_rds.security_group_id if round5_rds is not None else ""
    rule_group_ids = (
        [
            identifier
            for identifier in (
                sealed.runner_security_group_id,
                round5_rds_group_id,
                *[
                    str(group.get("GroupId") or "")
                    for group in groups
                    if matching_name(str(group.get("GroupName") or ""))
                ],
            )
            # An absent identifier is dropped rather than passed through: a filter
            # containing "" fails the whole call, so one missing binding would
            # otherwise take out the entire Round 5 residue scan.
            if identifier
        ]
        if sealed is not None
        else []
    )
    for rule in ec2.describe_security_group_rules(
        Filters=(
            [{"Name": "group-id", "Values": rule_group_ids}]
            if sealed is not None
            else [{"Name": "tag:anti-demo-run-id", "Values": [manifest.run_id]}]
        )
    ).get("SecurityGroupRules", []):
        description = str(rule.get("Description") or "")
        tags = rule.get("Tags") or []
        if matching_name(description) or (sealed is None and is_legacy_candidate(tags)):
            accept(
                str(rule.get("SecurityGroupRuleId") or "security-group-rule"),
                description,
                tags,
            )

    rds = session.client("rds")
    marker: str | None = None
    while True:
        arguments = {"Marker": marker} if marker else {}
        page = rds.describe_db_proxies(**arguments)
        for proxy in page.get("DBProxies", []):
            arn = str(proxy.get("DBProxyArn") or "")
            name = str(proxy.get("DBProxyName") or "")
            if arn:
                tags = rds.list_tags_for_resource(ResourceName=arn).get("TagList", [])
                if matching_name(name) or (sealed is None and is_legacy_candidate(tags)):
                    accept(arn, name, tags)
        marker = str(page.get("Marker") or "") or None
        if marker is None:
            break

    iam = session.client("iam")
    static_iam_roles = {
        str(role_arn)
        for role_arn in (
            getattr(sealed, "control_role_arn", None),
            getattr(sealed, "runner_role_arn", None),
            getattr(sealed, "proxy_service_role_arn", None),
        )
        if role_arn
    }
    expected_static_iam_tags = _required_round_tags(manifest, "r5")
    # IAM tag keys are case-insensitive, so Terraform intentionally omits the
    # duplicate title-cased Owner key on roles.
    expected_static_iam_tags.pop("Owner")
    marker = None
    while True:
        arguments = {"Marker": marker} if marker else {}
        page = iam.list_roles(**arguments)
        for role in page.get("Roles", []):
            role_name = str(role.get("RoleName") or "")
            tags = role.get("Tags")
            if tags is None and role_name:
                tags = iam.list_role_tags(RoleName=role_name).get("Tags", [])
            identity = str(role.get("Arn") or role_name)
            if identity in static_iam_roles:
                if tags_for(tags or []) != expected_static_iam_tags:
                    raise RuntimeError(
                        "Round 5 cleanup refused: static Terraform ownership tags differ for "
                        f"{identity}"
                    )
                continue
            if matching_name(role_name) or (sealed is None and is_legacy_candidate(tags or [])):
                accept(
                    identity,
                    role_name,
                    tags or [],
                    iam=True,
                )
        marker = str(page.get("Marker") or "") if page.get("IsTruncated") else ""
        if not marker:
            break
    if sealed is not None:
        runner_role_name = sealed.runner_role_arn.rsplit("/", 1)[-1]
        policies = iam.list_role_policies(RoleName=runner_role_name).get("PolicyNames", [])
        found.extend(
            f"iam-inline:{runner_role_name}/{policy_name}"
            for policy_name in policies
            if matching_name(str(policy_name))
            and str(policy_name).endswith("-runner-secret")
        )
    return sorted(set(found))


def _require_round5_clean_baseline(manifest: DemoManifest) -> None:
    journaled = _round5_active_journal_addons(manifest)
    discovered = _round5_runtime_tag_inventory(manifest)
    if journaled or discovered:
        detail = sorted({*journaled, *discovered})
        raise RuntimeError(
            "Round 5 clean baseline required; reconcile journaled per-bout add-ons: "
            + ", ".join(detail)
        )


def _write_round5_clean_receipt(manifest: DemoManifest) -> Path:
    payload = {
        "schema_version": 1,
        "run_id": manifest.run_id,
        "journal_table": ROUND5_JOURNAL_TABLE,
        "journaled_addons": [],
        "tag_discovered_addons": [],
        "verified_at": datetime.now(UTC).isoformat(),
    }
    payload["receipt_sha256"] = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    receipt = manifest_path().parent / "round5-clean-receipt.json"
    temporary = receipt.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    temporary.chmod(0o600)
    os.replace(temporary, receipt)
    return receipt


async def wait_for_scale_zero(manifest: DemoManifest, timeout_seconds: float) -> None:
    apply_manifest_environment(manifest)
    targets = (LakebaseCredentialProvider(), AuroraCredentialProvider())
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    last_status = ""
    while loop.time() < deadline:
        results = await asyncio.gather(
            *(target.assert_armed() for target in targets),
            return_exceptions=True,
        )
        configuration_error = next(
            (item for item in results if isinstance(item, TargetConfigurationError)), None
        )
        if configuration_error is not None:
            raise configuration_error
        unexpected = next(
            (
                item
                for item in results
                if isinstance(item, BaseException) and not isinstance(item, TargetNotArmedError)
            ),
            None,
        )
        if unexpected is not None:
            raise RuntimeError("Scale-zero verification failed unexpectedly") from unexpected
        if all(isinstance(item, dict) for item in results):
            return
        status = " · ".join(str(item) for item in results if isinstance(item, TargetNotArmedError))
        if status != last_status:
            print(f"WAIT  {status}", flush=True)
            last_status = status
        await asyncio.sleep(5)
    raise RuntimeError(f"Timed out after {timeout_seconds:.0f}s waiting for both systems to sleep")


async def reset_safe_change_only_artifacts(manifest: DemoManifest) -> None:
    """Remove deterministic safe-change children without opening source databases."""
    apply_manifest_environment(manifest)
    from .safe_change_live import build_safe_change_engine

    engine = build_safe_change_engine(manifest, cleanup_only=True)
    await engine.reset_all()


async def reset_recovery_artifacts(manifest: DemoManifest) -> None:
    """Remove deterministic recovery children while their sources are available."""
    apply_manifest_environment(manifest)
    from .recovery_live import build_recovery_engine

    recovery = build_recovery_engine(manifest, cleanup_only=True)
    await recovery.reset_all()


async def reset_safe_change_artifacts(manifest: DemoManifest) -> None:
    """Remove all deterministic, ownership-verified Round 2 and 3 children."""
    await reset_safe_change_only_artifacts(manifest)
    await reset_recovery_artifacts(manifest)


async def _reconcile_round5_failed_cleanups(
    manifest: DemoManifest,
    store: Any,
    bout_ids: tuple[str, ...],
) -> None:
    """Explicitly recover old journal scopes under a newly claimed durable fence."""
    if not bout_ids:
        return
    if (
        getattr(store, "mode", None) != "lakebase"
        or not callable(getattr(store, "_run", None))
        or not callable(getattr(store, "current", None))
    ):
        raise RuntimeError(
            "Round 5 cleanup recovery requires the durable Lakebase coordination store"
        )

    from .connection_spike_live import (
        LakebaseCreationJournalStore,
        build_connection_spike_live_engine,
    )
    from .coordination import LeaseHeldError
    from .models import BoutOperator, SessionState

    class ActiveLeaseFence:
        async def assert_current(self, scope: Any) -> None:
            active = await store.current()
            if (
                active is None
                or active.session_id != scope.bout_id
                or active.fencing_token != scope.fencing_token
            ):
                raise RuntimeError("Round 5 cleanup-recovery fence is no longer current")

    async def fresh_lakebase_host() -> str:
        _, pooled = await asyncio.to_thread(_round5_lakebase_hosts, manifest)
        return pooled

    journal = LakebaseCreationJournalStore(
        store._run,
        authority_ring_key=store.ring_key,
    )
    fence = ActiveLeaseFence()

    async def journal_competitor_id(bout_id: str) -> str:
        scopes = tuple(await journal.scopes(bout_id))
        if not scopes:
            raise RuntimeError("Round 5 cleanup recovery has no durable journal scope for the bout")
        competitor_ids: set[str] = set()
        for scope in scopes:
            if scope.bout_id != bout_id:
                raise RuntimeError(
                    "Round 5 cleanup recovery journal returned a different bout scope"
                )
            events = tuple(await journal.events(scope))
            if not events:
                raise RuntimeError(
                    "Round 5 cleanup recovery journal scope contains no resource events"
                )
            for event in events:
                if (
                    event.bout_id != scope.bout_id
                    or event.fencing_token != scope.fencing_token
                    or event.runtime_seal_sha256 != scope.runtime_seal_sha256
                ):
                    raise RuntimeError(
                        "Round 5 cleanup recovery journal event differs from its scope"
                    )
                competitor_id = event.metadata.get("competitor_id")
                if not isinstance(competitor_id, str) or competitor_id not in {
                    "rds_postgres",
                    "aurora_serverless_v2",
                }:
                    raise RuntimeError(
                        "Round 5 cleanup recovery journal is missing exact competitor metadata"
                    )
                competitor_ids.add(str(competitor_id))
        if len(competitor_ids) != 1:
            raise RuntimeError("Round 5 cleanup recovery journal mixes competitor metadata")
        return competitor_ids.pop()

    display_name = manifest.owner.split("@", 1)[0].replace(".", " ").title()
    operator = BoutOperator(
        display_name=display_name or "Demo operator",
        email=manifest.owner if "@" in manifest.owner else None,
        subject=f"maintenance:{manifest.owner.casefold()}",
    )
    ttl = timedelta(seconds=90)

    for bout_id in bout_ids:
        competitor_id = await journal_competitor_id(bout_id)
        competitor_name = (
            "Amazon Aurora PostgreSQL Serverless v2"
            if competitor_id == "aurora_serverless_v2"
            else "Amazon RDS for PostgreSQL"
        )
        engine = build_connection_spike_live_engine(
            manifest,
            competitor_id=competitor_id,
            journal=journal,
            fence=fence,
            fresh_lakebase_host=fresh_lakebase_host,
        )
        try:
            lease = await store.claim(
                session_id=bout_id,
                operator=operator,
                phase="round5_cleanup_recovery",
                session_state=SessionState.FAILED,
                round_id="survive_connection_spike",
                round_title="Round 5 cleanup recovery",
                competitor_id=competitor_id,
                competitor_name=competitor_name,
                ttl=ttl,
            )
        except LeaseHeldError as exc:
            active = exc.lease
            owner = active.operator.email or active.operator.display_name
            raise RuntimeError(
                f"Round 5 cleanup recovery refused: {owner} owns "
                f"{active.round_title} ({active.phase})"
            ) from exc

        lease_holder = [lease]

        async def heartbeat(holder: list[Any] = lease_holder) -> None:
            while True:
                await asyncio.sleep(15)
                holder[0] = await store.renew(holder[0], ttl=ttl)

        heartbeat_task = asyncio.create_task(
            heartbeat(), name=f"round5-cleanup-heartbeat-{bout_id}"
        )
        recovery_task = asyncio.create_task(
            engine.reconcile_failed_cleanup(bout_id, lease.fencing_token),
            name=f"round5-cleanup-recovery-{bout_id}",
        )
        try:
            done, _ = await asyncio.wait(
                {heartbeat_task, recovery_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if heartbeat_task in done:
                failure = heartbeat_task.exception()
                recovery_task.cancel()
                await asyncio.gather(recovery_task, return_exceptions=True)
                raise RuntimeError(
                    "Round 5 cleanup recovery stopped because the ring fence was lost"
                ) from failure
            recovery_task.result()
        finally:
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)
            await store.release(lease_holder[0])

    await asyncio.to_thread(_require_round5_clean_baseline, manifest)


def _round5_cleanup_ring_key(manifest: DemoManifest) -> str:
    """Select the cleanup fence used by this manifest generation."""
    from .coordination import ROUND5_RING_KEY, round_ring_key

    if manifest.manifest_version != 7:
        return ROUND5_RING_KEY
    if manifest.installation_id is None:
        raise RuntimeError("Manifest v7 is missing its installation identity")
    return round_ring_key(
        manifest.installation_id,
        RoundId.SURVIVE_CONNECTION_SPIKE.value,
        cleanup=True,
    )


async def _reset_under_ring_lease(
    manifest: DemoManifest,
    timeout_seconds: float,
    *,
    round5_recovery_bout_ids: tuple[str, ...] = (),
) -> DemoManifest:
    """Reset the environment while atomically owning the shared demo ring."""
    from .coordination import LeaseHeldError, build_lease_store
    from .models import BoutOperator, SessionState

    store = build_lease_store()
    had_round6_seal = manifest.round6 is not None
    await store.initialize()
    display_name = manifest.owner.split("@", 1)[0].replace(".", " ").title()
    operator = BoutOperator(
        display_name=display_name or "Demo operator",
        email=manifest.owner if "@" in manifest.owner else None,
        subject=f"maintenance:{manifest.owner.casefold()}",
    )
    ttl = timedelta(seconds=90)
    try:
        if manifest.round5_ready:
            round5_store = store.for_ring_key(_round5_cleanup_ring_key(manifest))
            try:
                await round5_store.initialize()
                if (
                    getattr(round5_store, "mode", None) != "lakebase"
                    or not callable(getattr(round5_store, "_run", None))
                    or not callable(getattr(round5_store, "current", None))
                ):
                    raise RuntimeError(
                        "Round 5 reset requires the durable Lakebase coordination store"
                    )
                await _reconcile_round5_failed_cleanups(
                    manifest,
                    round5_store,
                    round5_recovery_bout_ids,
                )
            finally:
                await round5_store.close()
        try:
            lease = await store.claim(
                session_id=f"maintenance-reset-{manifest.run_id}",
                operator=operator,
                phase="maintenance_reset",
                session_state=SessionState.RUNNING,
                round_id="maintenance_reset",
                round_title="Backstage environment reset",
                competitor_id="all",
                competitor_name="Lakebase + AWS competitors",
                ttl=ttl,
            )
        except LeaseHeldError as exc:
            active = exc.lease
            owner = active.operator.email or active.operator.display_name
            raise RuntimeError(
                f"Reset refused: {owner} owns {active.round_title} ({active.phase})"
            ) from exc

        lease_holder = [lease]

        async def heartbeat() -> None:
            while True:
                await asyncio.sleep(15)
                lease_holder[0] = await store.renew(lease_holder[0], ttl=ttl)

        async def operation() -> DemoManifest:
            print("RESET remove owned Round 2 and Round 3 environments", flush=True)
            await reset_safe_change_artifacts(manifest)
            manifest.round3_anchor = None
            manifest.status = "seeding"
            manifest.last_reset_at = datetime.now(UTC)
            manifest.schema_sha256 = _schema_sha256()
            save_manifest(manifest)
            print("RESET verify identical source schemas and wake the Round 1 lanes", flush=True)
            await seed_identical_schema(manifest)
            await asyncio.to_thread(ensure_coordination, manifest)
            print("RESET restore and verify the exact Round 4 baseline", flush=True)
            await asyncio.to_thread(
                _ensure_round4,
                manifest,
                timeout=timeout_seconds,
            )
            print(
                "SEAL  all Round 1 and Round 4 setup connections closed; waiting for scale zero",
                flush=True,
            )
            await wait_for_scale_zero(manifest, timeout_seconds)
            if manifest.round5_ready:
                print(
                    "RESET prepare and seal stable Round 5 Proxy credentials",
                    flush=True,
                )
                await asyncio.to_thread(
                    _prepare_and_reseal_round5,
                    manifest,
                    timeout=timeout_seconds,
                )
            if had_round6_seal:
                print(
                    "RESET revalidate Round 6 native CDF canary and final scale zero",
                    flush=True,
                )
                await asyncio.to_thread(
                    _prepare_and_reseal_round6,
                    manifest,
                    timeout=timeout_seconds,
                )
            manifest.status = "ready"
            save_manifest(manifest)
            return manifest

        heartbeat_task = asyncio.create_task(heartbeat(), name="maintenance-lease-heartbeat")
        operation_task = asyncio.create_task(operation(), name="owned-environment-reset")
        try:
            done, _ = await asyncio.wait(
                {heartbeat_task, operation_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if heartbeat_task in done:
                failure = heartbeat_task.exception()
                operation_task.cancel()
                await asyncio.gather(operation_task, return_exceptions=True)
                raise RuntimeError(
                    "Reset stopped because the shared ring lease was lost"
                ) from failure
            return operation_task.result()
        finally:
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)
            await store.release(lease_holder[0])
    finally:
        await store.close()


def _complete_provision(manifest: DemoManifest, zero_timeout_seconds: float) -> DemoManifest:
    if manifest.status == "provisioning":
        print(
            f"CREATE/VERIFY AWS Round 1 resources in "
            f"{manifest.aws.account_id}/{manifest.aws.region}",
            flush=True,
        )
        _terraform_init(manifest)
        _run(_terraform_base() + ["validate"], env=_terraform_environment(manifest))
        # Proxy creation is intentionally staged after lifecycle writes the
        # anti_demo_burst AWSCURRENT secret versions outside Terraform.
        create_plan = _terraform_plan(manifest, targets=ROUND5_PREFLIGHT_TARGETS)
        _terraform_apply(manifest, create_plan)
        if not _aws_state_is_complete(manifest, _terraform_managed_addresses(manifest)):
            raise RuntimeError("Terraform did not converge to the exact Round 5 baseline state")
        outputs = _terraform_outputs(manifest)
        _hydrate_aws_resources(manifest, outputs)

        if manifest.installation_id is not None:
            _prepare_v7_round_environments(manifest, outputs)
            save_manifest(manifest)
            print(
                "CHECK six isolated Lakebase round projects and one coordination "
                "project match AWS region",
                flush=True,
            )
        else:
            print(f"CREATE/VERIFY owned Lakebase project {manifest.run_id}", flush=True)
            _ensure_lakebase(manifest)
            region = _region_parity(manifest)
            if not region.ok:
                raise RuntimeError(f"Region parity failed: {region.detail}")
            print(f"CHECK {region.detail}", flush=True)
        manifest.status = "seeding"
        save_manifest(manifest)

    if manifest.status == "waiting_for_zero" and manifest.round3_anchor is not None:
        manifest.round3_anchor = None
        save_manifest(manifest)

    if manifest.status == "seeding":
        manifest.round3_anchor = None
        manifest.last_reset_at = datetime.now(UTC)
        manifest.schema_sha256 = _schema_sha256()
        save_manifest(manifest)
        print("SEED  identical PostgreSQL 17 proof schemas", flush=True)
        asyncio.run(seed_identical_schema(manifest))
        ensure_coordination(manifest)
        manifest.status = "waiting_for_zero"
        save_manifest(manifest)

    if manifest.status == "waiting_for_zero":
        ensure_coordination(manifest)
        print("SEAL  all setup connections closed; waiting for scale zero", flush=True)
        asyncio.run(wait_for_scale_zero(manifest, zero_timeout_seconds))
        manifest.status = "ready"
        save_manifest(manifest)
    return manifest


def provision(
    *,
    databricks_profile: str,
    aws_profile: str,
    aws_region: str,
    expected_account: str,
    owner: str,
    operator_cidr: str | None,
    ttl_hours: float,
    zero_timeout_seconds: float,
) -> DemoManifest:
    if manifest_path().exists():
        raise RuntimeError(
            f"An owned run already exists at {manifest_path()}; clean it up before provisioning"
        )
    if not databricks_profile:
        raise RuntimeError("DATABRICKS_PROFILE or --databricks-profile is required")
    if not expected_account or len(expected_account) != 12 or not expected_account.isdigit():
        raise RuntimeError("AWS_EXPECTED_ACCOUNT_ID or --expected-account must be 12 digits")
    if ttl_hours <= 0 or ttl_hours > MAX_TTL_HOURS:
        raise RuntimeError(
            f"ttl-hours must be greater than zero and no more than {MAX_TTL_HOURS:.0f}"
        )

    auth = select_setup_auth(os.environ, aws_profile)

    print("CHECK Databricks FEVM identity and Lakebase API", flush=True)
    databricks_user = _verify_databricks_identity(databricks_profile)
    print("CHECK explicit AWS account binding", flush=True)
    _verify_aws_identity(auth.profile, aws_region, expected_account, auth.mode)
    cidr = _validate_operator_cidr(operator_cidr or detect_operator_cidr())
    run_id = _new_run_id()
    installation_id = str(uuid4())
    compact_installation_id = installation_id.replace("-", "")
    round1_project_id = f"anti-demo-{compact_installation_id}-r1"
    coordination_project_id = f"anti-demo-{compact_installation_id}-coord"
    created_at = datetime.now(UTC)
    expires_at = created_at + timedelta(hours=ttl_hours)
    state_path = (manifest_path().parent / "terraform.tfstate").resolve()
    manifest = DemoManifest(
        installation_id=installation_id,
        run_id=run_id,
        owner=owner or databricks_user,
        created_at=created_at,
        expires_at=expires_at,
        status="provisioning",
        aws=AwsManifest(
            auth_mode=auth.mode,
            profile=auth.profile,
            account_id=expected_account,
            region=aws_region,
            operator_cidr=cidr,
            terraform_state=str(state_path),
        ),
        databricks=DatabricksManifest(
            profile=databricks_profile,
            project_id=round1_project_id,
            endpoint_name=(f"projects/{round1_project_id}/branches/production/endpoints/primary"),
            coordination_endpoint_name=(
                f"projects/{coordination_project_id}/branches/production/endpoints/primary"
            ),
            user=databricks_user,
        ),
        schema_sha256=_schema_sha256(),
    )
    save_manifest(manifest)
    return _complete_provision(manifest, zero_timeout_seconds)


def resume_provision(zero_timeout_seconds: float = 900) -> DemoManifest:
    manifest = load_manifest()
    # Resuming an interrupted provision is repair work. Refusing it because the
    # declared TTL passed while the provision was interrupted is exactly
    # backwards: it strands a half-built install that `antidemo setup` reaches
    # through this path whenever status is not yet ready.
    _warn_if_expired(manifest)
    if manifest.status == "cleanup_failed":
        raise RuntimeError("Cleanup previously failed; inspect it before changing resources")

    print("CHECK stored Databricks and AWS identities before resuming", flush=True)
    actual_user = _verify_databricks_identity(manifest.databricks.profile)
    if actual_user != manifest.databricks.user:
        raise RuntimeError(
            f"Databricks profile resolved to {actual_user}, expected {manifest.databricks.user}"
        )
    _verify_aws_identity(
        manifest.aws.profile,
        manifest.aws.region,
        manifest.aws.account_id,
        manifest.aws.auth_mode,
    )
    current_cidr = detect_operator_cidr()
    if current_cidr != manifest.aws.operator_cidr:
        raise RuntimeError(
            f"Operator public IP changed to {current_cidr}; provisioned ingress is "
            f"{manifest.aws.operator_cidr}"
        )
    round4_waiting_for_final_seal = (
        manifest.status == "waiting_for_zero"
        and manifest.manifest_version == 2
        and manifest.round4 is not None
    )
    round6_seal_retry = (
        manifest.status == "seeding"
        and manifest.manifest_version == 5
        and manifest.round5_ready
        and manifest.round6 is None
    )
    if round6_seal_retry:
        print(
            "RESUME interrupted Round 6 seal without repeating completed base seeding",
            flush=True,
        )
        return _prepare_and_reseal_round6(manifest, timeout=zero_timeout_seconds)
    if manifest.status != "ready" and not round4_waiting_for_final_seal:
        manifest = _complete_provision(manifest, zero_timeout_seconds)
    manifest = _prepare_and_reseal_round4(manifest, timeout=zero_timeout_seconds)
    if not manifest.round5_ready:
        manifest = _prepare_and_reseal_round5(manifest, timeout=zero_timeout_seconds)
    if manifest.round6_ready:
        return manifest
    return _prepare_and_reseal_round6(manifest, timeout=zero_timeout_seconds)


def reset(timeout_seconds: float = 900) -> DemoManifest:
    manifest = load_manifest()
    # `antidemo setup` on an existing ready install lands here via
    # reconcile_infrastructure -> reset. Asserting non-expiry made the documented
    # "set up once, use anytime" workflow fail 24 hours in, with no supported
    # recovery short of a full teardown and re-provision. Reset verifies real
    # state (operator CIDR, ownership tags, Round 5 residue) immediately below;
    # a provision-time wall-clock value adds nothing to that.
    _warn_if_expired(manifest)
    recovery_bout_ids: tuple[str, ...] = ()
    if manifest.round5_ready:
        journaled = _round5_active_journal_addons(manifest)
        discovered = _round5_runtime_tag_inventory(manifest)
        recovery_bout_ids = tuple(sorted({identity.partition(":")[0] for identity in journaled}))
        if discovered and not recovery_bout_ids:
            raise RuntimeError(
                "Round 5 reset refused: deterministic-prefix residue has no "
                "ownership-authorizing journal scope"
            )
    current_cidr = detect_operator_cidr()
    if current_cidr != manifest.aws.operator_cidr:
        raise RuntimeError(
            f"Operator public IP changed to {current_cidr}; provisioned ingress is "
            f"{manifest.aws.operator_cidr}"
        )
    ensure_coordination(manifest)
    apply_manifest_environment(manifest)
    return asyncio.run(
        _reset_under_ring_lease(
            manifest,
            timeout_seconds,
            round5_recovery_bout_ids=recovery_bout_ids,
        )
    )


def _refresh_operator_cidr(manifest: DemoManifest) -> None:
    """Rebind owned database ingress when the local operator's public IP changes."""
    current_cidr = detect_operator_cidr()
    if current_cidr == manifest.aws.operator_cidr:
        return
    ownership = _aws_ownership(manifest)
    # An absence is not an ownership failure worth refusing over. The refusal
    # protects an ingress rule on a resource that might not be ours; once the
    # account says the sealed databases do not exist, there is no rule left to
    # protect and no stranger's security group to rewrite, so the property it
    # defends has already evaporated. Refusing anyway is what deadlocked recovery
    # from the sandbox reaper: it deletes the databases, an operator's address
    # drifts inside the fortnight before anyone notices, and then the repair is
    # gated on describing the very resources that are gone. `_aws_ownership`
    # cannot make this distinction itself because it collapses every failure into
    # one boolean, and widening it would loosen the check for real callers.
    if not ownership.ok and not _sealed_databases_absent(manifest):
        raise RuntimeError(
            "Refusing to change database ingress because owned AWS resources could not be verified"
        )
    previous_cidr = manifest.aws.operator_cidr
    manifest.aws.operator_cidr = current_cidr
    save_manifest(manifest)
    print(
        f"REBIND operator ingress {previous_cidr} -> {current_cidr}",
        flush=True,
    )


def _refresh_serverless_egress_cidrs(manifest: DemoManifest) -> None:
    """Re-poll the published app egress prefixes and reseal them beside the laptop's.

    The counterpart of `_refresh_operator_cidr`, and it keeps that function's
    ownership refusal for the same reason: this rewrites ingress on database
    security groups, and it must not do so on resources this manifest cannot
    prove it owns.

    A feed that cannot be read is a warning rather than a failure, and the
    asymmetry is deliberate. `antidemo setup` is the repair path -- it is what an
    operator runs when something is already wrong -- so letting a third party's
    CDN being briefly unreachable abort a reconcile would make the repair
    conditional on the internet being well. Whatever is already sealed stays
    sealed and is re-applied unchanged; nothing is widened and nothing is
    silently dropped. An installation that has never sealed a list simply carries
    on admitting only the operator, which is exactly where it was before.

    Count printed rather than the addresses. They are globally routable, and
    `antidemo setup` output ends up pasted into notes, issues and this
    repository -- which is the shape of leak `tests/
    test_no_live_identifiers_committed.py` exists to catch and cannot catch once
    it is in somebody's terminal scrollback.
    """

    try:
        cidrs, published_at = fetch_serverless_egress_cidrs(manifest.aws.region)
    except Exception as exc:
        print(
            f"WARN  Could not re-poll the Databricks serverless egress feed "
            f"({type(exc).__name__}). Keeping the "
            f"{len(manifest.aws.serverless_egress_cidrs or ())} prefix(es) this installation "
            f"already sealed. The deployed app's access is unchanged; re-run "
            f"'{OPERATOR_INGRESS_REPAIR_COMMAND}' once the feed is reachable.",
            flush=True,
        )
        return
    if (
        cidrs == (manifest.aws.serverless_egress_cidrs or None)
        and published_at == manifest.aws.serverless_egress_published_at
    ):
        return
    ownership = _aws_ownership(manifest)
    if not ownership.ok and not _sealed_databases_absent(manifest):
        raise RuntimeError(
            "Refusing to change database ingress because owned AWS resources could not be verified"
        )
    previous = len(manifest.aws.serverless_egress_cidrs or ())
    manifest.aws.serverless_egress_cidrs = cidrs
    manifest.aws.serverless_egress_published_at = published_at
    save_manifest(manifest)
    reset_deployed_aws_posture_cache()
    print(
        f"RESEAL deployed app ingress {previous} -> {len(cidrs)} published "
        f"{manifest.aws.region} prefix(es), feed published "
        f"{datetime.fromtimestamp(published_at, UTC).date().isoformat()}",
        flush=True,
    )


def reconcile_infrastructure(manifest: DemoManifest) -> DemoManifest:
    """Apply the checked-in Terraform to the exact manifest-owned AWS environment."""
    # Reconciliation is how an install is repaired, so an expired timestamp must
    # not be the thing that prevents repair. Identity, account, and ownership are
    # verified below and are the checks that actually protect this apply.
    _warn_if_expired(manifest)
    actual_user = _verify_databricks_identity(manifest.databricks.profile)
    if actual_user != manifest.databricks.user:
        raise RuntimeError(
            f"Databricks profile resolved to {actual_user}, expected {manifest.databricks.user}"
        )
    _verify_aws_identity(
        manifest.aws.profile,
        manifest.aws.region,
        manifest.aws.account_id,
        manifest.aws.auth_mode,
    )
    _refresh_operator_cidr(manifest)
    _refresh_serverless_egress_cidrs(manifest)
    print("RECONCILE manifest-owned AWS infrastructure", flush=True)
    _terraform_init(manifest)
    managed_before = _terraform_managed_addresses(manifest)
    _reconcile_legacy_round5_partial_state(manifest, managed_before)
    static_round5_addresses = {
        "aws_iam_role.round5_proxy_service",
        "aws_iam_role_policy.round5_proxy_secrets",
        "aws_secretsmanager_secret.round5_aurora_proxy_credentials",
        "aws_secretsmanager_secret.round5_rds_proxy_credentials",
    }
    migrating_round5 = bool(managed_before & ROUND5_LEGACY_DYNAMIC_ADDRESSES) or not (
        static_round5_addresses <= managed_before
    )
    if isinstance(manifest.round5, Round5Resources) and migrating_round5:
        _require_round5_clean_baseline(manifest)
    _run(_terraform_base() + ["validate"], env=_terraform_environment(manifest))
    plan = _terraform_plan(manifest)
    _terraform_apply(manifest, plan)
    managed_after = _terraform_managed_addresses(manifest)
    expected_addresses = _expected_aws_state_addresses(manifest)
    if not _aws_state_is_complete(manifest, managed_after):
        missing = expected_addresses - managed_after
        remaining = managed_after - expected_addresses
        raise RuntimeError(
            "Round 5 baseline reconciliation did not converge; missing="
            + ",".join(sorted(missing))
            + "; remaining="
            + ",".join(sorted(remaining))
        )
    outputs = _terraform_outputs(manifest)
    # Both halves are refreshed from the same outputs before either is persisted.
    # Saving the flat mirror on its own leaves the manifest briefly inconsistent,
    # and `save_manifest` validates, so the write would be refused rather than
    # producing a bad file.
    _hydrate_aws_resources(manifest, outputs, persist=False)
    _reseal_v7_aws_round_environments(manifest, outputs)
    save_manifest(manifest)
    if manifest.manifest_version >= 4:
        _require_round5_clean_baseline(manifest)
    return manifest


RENEW_JOURNAL_NAME = "renew.json"

# Hours from now that a fresh provision and a renew default to.
#
# 72, not 24, because the TTL is measured from `created_at` and is never re-based:
# the usable life of a 24-hour install is 24 hours minus however long provisioning
# took, so it is already partway expired the first time anyone can use it. Nothing
# reaps on the tag, so its only job is ownership attribution in a shared sandbox,
# and a longer window costs nothing there. 72 hours covers a working session plus a
# weekend while still expiring well before the schedule-driven account sweep, so the
# tag keeps meaning something rather than becoming permanently stale.
DEFAULT_TTL_HOURS = 72.0

# The largest TTL either provision or renew will accept, unchanged.
MAX_TTL_HOURS = 720.0

# The only Terraform attributes a renew may change. Renewing rewrites one tag
# value and the IAM policy documents whose conditions pin it. Anything else in the
# plan means the checked-in Terraform has drifted from the running environment,
# which is `reconcile_infrastructure`'s job to resolve deliberately -- not
# something a timestamp bump should apply as a side effect.
#
# `instance_class` is absent on purpose, as are `allocated_storage`, `engine`, and
# the serverless capacity settings: those describe deliberately chosen database
# shapes, and a renew that silently resized one would be far worse than a stale
# tag. `root_block_device` and `volume_tags` are present because the runner
# instance carries the tag on its root volume as well as on itself, and `policy`
# is present because four IAM policy documents depend on the Round 5 master secret
# ARNs and therefore re-read as "known after apply" on any plan.
_RENEW_ALLOWED_PLAN_ATTRIBUTES = frozenset(
    {
        "tags",
        "tags_all",
        "volume_tags",
        "root_block_device",
        "policy",
        "assume_role_policy",
    }
)


def _renew_journal_path() -> Path:
    return manifest_path().parent / RENEW_JOURNAL_NAME


def _terraform_plan_json(manifest: DemoManifest, plan_path: Path) -> dict[str, Any]:
    return _run_json(
        _terraform_base() + ["show", "-json", str(plan_path)],
        env=_terraform_environment(manifest),
    )


def _renew_plan_violations(
    manifest: DemoManifest,
    plan: Mapping[str, Any],
) -> list[str]:
    """Reject any plan that does more than move the expires-at tag.

    Read as JSON rather than eyeballed, because the diff for a tag change and the
    diff for a database replacement look similar at a glance and differ by an
    outage. Creation, deletion, and replacement are all refused outright: a renew
    has no business making or destroying a resource.
    """
    allowed_addresses = _expected_aws_state_addresses(manifest)
    violations: list[str] = []
    for entry in plan.get("resource_changes") or []:
        address = str(entry.get("address") or "?")
        change = entry.get("change") or {}
        actions = [str(action) for action in (change.get("actions") or [])]
        if not actions or actions == ["no-op"] or actions == ["read"]:
            continue
        if address not in allowed_addresses:
            violations.append(f"{address}: not a manifest-owned address")
            continue
        if actions != ["update"]:
            violations.append(f"{address}: plans {'+'.join(actions)}, not a tag update")
            continue
        before = change.get("before") or {}
        after = change.get("after") or {}
        unknown = change.get("after_unknown") or {}
        touched = {key for key in {*before, *after} if before.get(key) != after.get(key)}
        touched |= {str(key) for key, flag in unknown.items() if flag}
        forbidden = sorted(touched - _RENEW_ALLOWED_PLAN_ATTRIBUTES)
        if forbidden:
            violations.append(f"{address}: changes {', '.join(forbidden)}")
    return violations


def deployed_renew_followup(manifest: DemoManifest) -> list[str]:
    """Say what a renew cannot reach: the deployed app's own copy of the manifest.

    app.yaml binds ANTI_DEMO_MANIFEST_JSON to the `anti-demo-manifest-json`
    secret, so a deployed app reads the secret and never this file. Renewing
    locally therefore leaves the deployed copy stale. This reports the remaining
    work rather than performing it -- rewriting a workspace secret and restarting
    someone's running app are not side effects a timestamp command should take.
    """
    return [
        "NEXT  a deployed app still holds the previous expires-at:",
        "NEXT    1. rewrite the 'anti-demo-manifest-json' Databricks secret with "
        f"{manifest_path()}",
        "NEXT    2. restart the app so it re-reads the secret "
        "(app.yaml binds ANTI_DEMO_MANIFEST_JSON, not the file)",
        "NEXT  a local run needs neither step; it reads the manifest file directly.",
    ]


def _renew_inconsistency_report(
    previous_tag: str,
    target_tag: str,
    stage: str,
    reason: str,
) -> str:
    """Say exactly what a half-finished renew left behind, and how to finish it."""
    if stage == "apply":
        return (
            f"Renew did not change anything: the AWS apply failed before it ran. {reason}\n"
            f"  CONSISTENT: the manifest, the Round 5 ownership tag set, and the live "
            f"AWS tags all still say {previous_tag}. Nothing is stranded and cleanup "
            f"is unaffected.\n"
            f"  TO RETRY: re-run 'antidemo renew'. The journal at {_renew_journal_path()} "
            f"holds {target_tag}, so the retry resumes to the same target."
        )
    if stage == "manifest":
        return (
            f"Renew applied the new expiry to AWS but could not write the manifest. "
            f"{reason}\n"
            f"  INCONSISTENT: live AWS tags say {target_tag} while the manifest still "
            f"says {previous_tag}.\n"
            f"  CONSEQUENCE: 'antidemo cleanup' will refuse -- including under --dry-run -- "
            f"because it compares manifest tags against live AWS tags. No resource is "
            f"lost, but the cleanup path is closed until the two agree.\n"
            f"  TO FINISH: re-run 'antidemo renew'. It resumes to {target_tag} from the "
            f"journal at {_renew_journal_path()} and converges the manifest onto the "
            f"tags already applied. Do this before anything else."
        )
    return (
        f"Renew applied the new expiry to AWS and to the manifest, but the Round 5 "
        f"re-seal did not complete. {reason}\n"
        f"  PARTIAL: live AWS tags and the manifest both say {target_tag}; the frozen "
        f"Round 5 ownership tag set still says {previous_tag}.\n"
        f"  CONSEQUENCE: cleanup is safe and unaffected, because the manifest matches "
        f"the live tags. Round 5 bouts will refuse to start -- IAM now requires "
        f"{target_tag} while the seal would tag new resources {previous_tag} -- so a "
        f"bout is denied at creation rather than creating anything un-cleanable.\n"
        f"  TO FINISH: re-run 'antidemo renew' (it resumes from the journal at "
        f"{_renew_journal_path()}), or run 'antidemo setup', which re-seals Round 5 "
        f"through the same path."
    )


def renew(*, ttl_hours: float = DEFAULT_TTL_HOURS, timeout_seconds: float = 900) -> DemoManifest:
    """Move this installation's expires-at forward without re-provisioning it.

    The timestamp lives in four places that have to end up agreeing: the manifest,
    the frozen Round 5 per-bout ownership tag set, the Terraform-applied AWS tags
    together with the two Round 5 IAM `RequestTag` conditions that require them,
    and -- for a deployed app only -- the `anti-demo-manifest-json` secret. The
    first three move here; the fourth is reported by `deployed_renew_followup`
    rather than performed, because rewriting a workspace secret and restarting
    someone's running app are not side effects a timestamp command should take.

    Two constraints fix the order, and they point in opposite directions:

    *   `cleanup` compares the manifest's expiry against live AWS tags and refuses
        on any mismatch, even under `--dry-run`. So the manifest must never be
        written ahead of the retag: the likely failure would close the cleanup path.
    *   The only correct way to rebuild `round5.ownership_tags` is
        `_prepare_and_reseal_round5`, which validates the rebuilt tags against the
        live Terraform output `round5_bout_base_tags` and recomputes
        `baseline_sha256` and `config_sha256` over them. So the re-seal can only
        run *after* the apply. Hand-building that frozen model instead would
        desynchronise the seal from reality -- and would in fact be rejected at
        save time, since those hashes cover it.

    The resolution is: apply first with an explicit variable override, then write
    the manifest, then re-seal Round 5 through the sanctioned path. That ordering
    puts the fully-consistent no-op outcome on the most likely failure (the apply),
    and each later failure is both narrower than the one before it and recoverable
    by re-running this command, which resumes to the journaled target instead of
    choosing a new timestamp.

    It also refuses to start while a bout holds the ring or while any Round 5
    per-bout resource still exists: those resources were tagged with the current
    value and are authorized for cleanup by exact tag-set equality against it, so
    rotating the tag underneath them is what would make them un-cleanable.
    """
    if ttl_hours <= 0 or ttl_hours > MAX_TTL_HOURS:
        raise RuntimeError(
            f"ttl-hours must be greater than zero and no more than {MAX_TTL_HOURS:.0f}"
        )
    manifest = load_manifest()
    if manifest.status == "cleanup_failed":
        raise RuntimeError("Cleanup previously failed; inspect it before changing resources")
    if manifest.status != "ready":
        raise RuntimeError(
            f"Renew requires a ready installation; this one is {manifest.status.upper()}. "
            "Run 'antidemo setup' to finish it first."
        )
    _verify_aws_identity(
        manifest.aws.profile,
        manifest.aws.region,
        manifest.aws.account_id,
        manifest.aws.auth_mode,
    )
    previous_tag = _utc_tag(manifest.expires_at)
    resumed = _resume_renew_target()
    if resumed is not None:
        target = resumed
        print(
            f"RESUME an earlier renew was interrupted; converging every copy on "
            f"{_utc_tag(target)} rather than choosing a new timestamp",
            flush=True,
        )
    else:
        # Whole seconds only: the AWS tag and the IAM condition are second-
        # granularity, so a microsecond component would leave the manifest and the
        # tag differing in a way that reads as drift from then on.
        target = (datetime.now(UTC) + timedelta(hours=ttl_hours)).replace(microsecond=0)
        if target <= manifest.expires_at:
            raise RuntimeError(
                f"Renew would move expires-at backwards, from {previous_tag} to "
                f"{_utc_tag(target)}; choose a larger --ttl-hours"
            )
    return asyncio.run(
        _renew_under_ring_lease(manifest, target, previous_tag, timeout_seconds)
    )


def _resume_renew_target() -> datetime | None:
    """Recover an interrupted renew's target so a re-run converges rather than drifts.

    Without this, a second `antidemo renew` would pick a fresh timestamp and move only
    the copies it manages to reach, leaving the earlier partial move permanently
    unreconciled. Resuming to the journaled value is what makes every failure below
    recoverable by running the same command again.
    """
    journal = _renew_journal_path()
    if not journal.exists():
        return None
    try:
        record = json.loads(journal.read_text(encoding="utf-8"))
        target_tag = record["to"]
        return datetime.strptime(target_tag, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except (OSError, json.JSONDecodeError, TypeError, KeyError, ValueError) as exc:
        raise RuntimeError(
            f"An unreadable renew journal sits beside the manifest at {journal}. It "
            "records an interrupted renewal, so read it before removing it: the "
            "timestamp it names may already be applied to AWS."
        ) from exc


def _write_renew_journal(previous_tag: str, target: datetime) -> None:
    journal = _renew_journal_path()
    journal.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = journal.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(
            {
                "from": previous_tag,
                "to": _utc_tag(target),
                "recorded_at": _utc_tag(datetime.now(UTC)),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    temporary.chmod(0o600)
    os.replace(temporary, journal)


def renew_fence_ring_keys(manifest: DemoManifest) -> list[tuple[str, str]]:
    """Every ring a bout of this installation can hold, with a label per ring.

    The ring `antidemo renew` used to claim was the module default, `main`. On a v7
    installation with an `installation_id` that is the one ring no bout ever
    holds: `RunManager._lease_store_for_round` returns `self._lease_store` --
    the `main` store -- only when round isolation is off, and isolation is on for
    exactly this shape of manifest. Every bout claims `round_ring_key(...)`
    instead, and Round 5's backstage cleanup claims the `:cleanup` sibling. So a
    fence on `main` refused nothing, and a renew during a live Round 3 would have
    rewritten the AWS and IAM expiry tags underneath it.

    The label travels with the key because the refusal has to name *which* round
    is busy. An operator told only that "a bout is live" has to go and find it.
    """
    from .coordination import RING_KEY, round_ring_key

    if manifest.manifest_version != 7 or not manifest.installation_id:
        # Pre-isolation installations really do run every bout on `main`.
        return [(RING_KEY, "a bout")]
    from .bout_cost import ROUND_ORDER

    installation_id = manifest.installation_id
    keys = [
        (
            round_ring_key(installation_id, round_id.value),
            f"Round {number} ({title})",
        )
        for round_id, number, title in ROUND_ORDER
    ]
    keys.append(
        (
            round_ring_key(
                installation_id,
                RoundId.SURVIVE_CONNECTION_SPIKE.value,
                cleanup=True,
            ),
            "Round 5 backstage cleanup",
        )
    )
    return keys


def _renew_fence_refusal(label: str, ring_key: str, lease: Any) -> str:
    """Name the round, not just the fact of a bout.

    The label comes from the ring this renew was refused on rather than from the
    row it read, because the ring is what renew chose and is therefore the answer
    to "which round". The ring key follows it so the operator can query the row
    directly instead of guessing at its spelling.
    """
    owner = lease.operator.email or lease.operator.display_name
    return (
        f"Renew refused: {label} is live -- {owner} owns it ({lease.phase}) on "
        f"ring {ring_key}. The expiry tag is rewritten across AWS and IAM, so it "
        "cannot move while a bout is live."
    )


async def _claim_renew_fence(
    rings: list[tuple[Any, str]],
    *,
    session_id: str,
    operator: Any,
    ttl: timedelta,
) -> list[tuple[Any, Any]]:
    """Hold every ring, or hold none and say which round refused.

    NOT ATOMIC, AND IT CANNOT BE. There is one row per ring and no transaction
    spans them, so a sweep that finds the fifth ring held has already claimed
    four. A check-then-claim would be atomic-looking and *worse*: reading
    `current()` on all seven and then claiming proves only that no bout was live
    at the moment of the read, while the thing being fenced is a Terraform apply
    that runs for minutes. A bout starting inside that window is precisely the
    failure -- it would be tagged with the old expiry and authorized for cleanup
    by exact equality against a value that no longer exists.

    So this claims, and takes the non-atomicity on with two properties that make
    a partial sweep harmless. A partial hold is released here, before the refusal
    reaches the caller, so no ring is left fenced by a renew that never ran. And
    every claim carries the same short TTL, so even a process killed mid-sweep
    leaves rings that free themselves rather than a fence nobody can clear.

    Deadlock is not reachable: `claim` never waits, the order is the fixed
    `ROUND_ORDER` one, and `antidemo renew` already holds the generation lock. Two
    renews cannot interleave, and if they somehow did, the loser refuses.
    """
    from .coordination import LeaseHeldError
    from .models import SessionState

    held: list[tuple[Any, Any]] = []
    try:
        for store, label in rings:
            try:
                lease = await store.claim(
                    session_id=session_id,
                    operator=operator,
                    phase="maintenance_renew",
                    session_state=SessionState.RUNNING,
                    round_id="maintenance_renew",
                    round_title="Backstage expiry renewal",
                    competitor_id="all",
                    competitor_name="Lakebase + AWS competitors",
                    ttl=ttl,
                )
            except LeaseHeldError as exc:
                raise RuntimeError(
                    _renew_fence_refusal(
                        label,
                        getattr(store, "ring_key", "unknown"),
                        exc.lease,
                    )
                ) from exc
            held.append((store, lease))
    except BaseException:
        await _release_renew_fence(held)
        raise
    return held


async def _release_renew_fence(held: list[tuple[Any, Any]]) -> None:
    """Give every ring back, newest first, without letting one failure keep the rest."""
    for store, lease in reversed(held):
        try:
            await store.release(lease)
        except Exception as exc:
            # A ring this fails to release expires on its own TTL. Raising here
            # would hide the reason the fence is being torn down.
            print(
                f"WARN  the ring {getattr(store, 'ring_key', 'unknown')} could not be "
                f"released ({type(exc).__name__}); it expires on its own TTL",
                flush=True,
            )


async def _renew_under_ring_lease(
    manifest: DemoManifest,
    target: datetime,
    previous_tag: str,
    timeout_seconds: float,
) -> DemoManifest:
    from .coordination import build_lease_store
    from .models import BoutOperator

    # `build_lease_store` reads the coordination endpoint out of the environment,
    # so without this the ring it claims is whatever this process happens to be
    # pointed at -- which for `antidemo renew` was nothing. The refusal below could
    # therefore never see a live bout, and the retag would proceed underneath one.
    apply_manifest_environment(manifest)
    ring_keys = renew_fence_ring_keys(manifest)
    primary = build_lease_store(ring_key=ring_keys[0][0])
    await primary.initialize()
    # Derived from the primary rather than built again: `for_ring_key` carries the
    # resolved coordination host and user across, so the sweep costs one endpoint
    # lookup instead of one per ring.
    rings: list[tuple[Any, str]] = [(primary, ring_keys[0][1])]
    for ring_key, label in ring_keys[1:]:
        sibling = primary.for_ring_key(ring_key)
        await sibling.initialize()
        rings.append((sibling, label))
    display_name = manifest.owner.split("@", 1)[0].replace(".", " ").title()
    operator = BoutOperator(
        display_name=display_name or "Demo operator",
        email=manifest.owner if "@" in manifest.owner else None,
        subject=f"maintenance:{manifest.owner.casefold()}",
    )
    ttl = timedelta(seconds=90)
    try:
        held = await _claim_renew_fence(
            rings,
            session_id=f"maintenance-renew-{manifest.run_id}",
            operator=operator,
            ttl=ttl,
        )

        async def heartbeat() -> None:
            while True:
                await asyncio.sleep(15)
                for index, (store, lease) in enumerate(held):
                    held[index] = (store, await store.renew(lease, ttl=ttl))

        async def operation() -> DemoManifest:
            return await asyncio.to_thread(
                _renew_locked,
                manifest,
                target,
                previous_tag,
                timeout_seconds,
            )

        heartbeat_task = asyncio.create_task(heartbeat(), name="renew-lease-heartbeat")
        operation_task = asyncio.create_task(operation(), name="owned-expiry-renew")
        try:
            done, _ = await asyncio.wait(
                {heartbeat_task, operation_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if heartbeat_task in done:
                failure = heartbeat_task.exception()
                operation_task.cancel()
                await asyncio.gather(operation_task, return_exceptions=True)
                raise RuntimeError(
                    "Renew stopped because the shared ring lease was lost"
                ) from failure
            return operation_task.result()
        finally:
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)
            await _release_renew_fence(held)
    finally:
        for store, _label in rings:
            try:
                await store.close()
            except Exception as exc:
                print(
                    f"WARN  a renew coordinator could not be closed ({type(exc).__name__})",
                    flush=True,
                )


def _renew_locked(
    manifest: DemoManifest,
    target: datetime,
    previous_tag: str,
    timeout_seconds: float,
) -> DemoManifest:
    """Move every local copy of the timestamp while holding the ring.

    See `renew` for why the order is apply, then manifest, then Round 5 re-seal.
    Each stage's failure is reported by `_renew_inconsistency_report` with the
    exact state it leaves behind.
    """
    target_tag = _utc_tag(target)
    if manifest.round5_ready:
        # Per-bout resources were tagged with the current value and are authorized
        # for cleanup by exact equality against the manifest's copy of it. Rotating
        # that copy underneath them is precisely what would strand them, so refuse
        # instead of stranding them.
        _require_round5_clean_baseline(manifest)
    _write_renew_journal(previous_tag, target)

    print(
        f"RENEW  applying expires-at {previous_tag} -> {target_tag} to AWS tags and "
        "the Round 5 IAM conditions",
        flush=True,
    )
    # Read before the plan so the operator is told what is being repaired rather
    # than inferring it from an `assume_role_policy` line in Terraform's diff.
    # This is the fortnightly case: the sweep deleted the app's IAM user, the
    # installer recreated it with the same name, and the trust policy still holds
    # the old unique ID.
    trust_before = _anti_demo_runtime_trust_check(manifest)
    if not trust_before.ok:
        print(
            f"RENEW  repairing the sealed runtime role's trust: {trust_before.detail}",
            flush=True,
        )

    try:
        _terraform_init(manifest)
        plan = _terraform_plan(manifest, expires_at_override=target)
        violations = _renew_plan_violations(manifest, _terraform_plan_json(manifest, plan))
        if violations:
            raise RuntimeError(
                "the plan does more than move the expires-at tag: " + "; ".join(violations)
            )
        _terraform_apply(manifest, plan)
    except Exception as exc:
        raise RuntimeError(
            _renew_inconsistency_report(previous_tag, target_tag, "apply", str(exc))
        ) from exc

    if not trust_before.ok:
        # Proving the repair, not assuming it. A renew whose apply succeeded but
        # left the trust unresolved would be the exact shape of failure this
        # project keeps rediscovering: a surface reporting health it never
        # checked. The seal is untouched either way -- the trusted ARN strings
        # never changed, only the unique IDs IAM stores behind them.
        trust_after = _anti_demo_runtime_trust_check(manifest)
        if not trust_after.ok:
            raise RuntimeError(
                "Renew applied the new expiry, but the sealed runtime role's trust is still "
                f"broken: {trust_after.detail}\n"
                "  CONSISTENT: the expiry moved everywhere it was going to move; nothing is "
                "stranded and cleanup is unaffected.\n"
                "  CONSEQUENCE: any process authenticating through the runtime role will be "
                "denied at AssumeRole, and Round 5 will fail after the bell rather than "
                "refusing to arm.\n"
                "  TO FINISH: confirm every principal in "
                f"{', '.join(manifest.aws.runtime_role_trusted_principal_arns or ())} exists in "
                "IAM, recreate any the sweep removed, then run 'antidemo renew' again."
            )
        print("RENEW  the sealed runtime role trusts its sealed principals again", flush=True)

    try:
        manifest.expires_at = target
        save_manifest(manifest)
    except Exception as exc:
        raise RuntimeError(
            _renew_inconsistency_report(previous_tag, target_tag, "manifest", str(exc))
        ) from exc
    print(f"RENEW  manifest now agrees with the live AWS tags at {target_tag}", flush=True)

    try:
        if manifest.round5_ready:
            # Only this path may rebuild the frozen ownership tag set: it validates
            # the result against the live Terraform output and recomputes the
            # baseline and config digests that cover it. It also drops status to
            # seeding when Round 6 is sealed, so the Round 6 re-seal restores
            # readiness exactly as reset does.
            had_round6_seal = manifest.round6 is not None
            print("RENEW  re-sealing Round 5 ownership tags from live Terraform", flush=True)
            manifest = _prepare_and_reseal_round5(manifest, timeout=timeout_seconds)
            if had_round6_seal:
                manifest = _prepare_and_reseal_round6(manifest, timeout=timeout_seconds)
            manifest.status = "ready"
            save_manifest(manifest)
    except Exception as exc:
        raise RuntimeError(
            _renew_inconsistency_report(previous_tag, target_tag, "reseal", str(exc))
        ) from exc

    _renew_journal_path().unlink(missing_ok=True)
    print(f"RENEW  every owned copy of expires-at now says {target_tag}", flush=True)
    return manifest


def _required_tags(manifest: DemoManifest) -> dict[str, str]:
    return {
        "anti-demo-run-id": manifest.run_id,
        "Owner": manifest.owner,
        "owner": manifest.owner,
        "expires-at": _utc_tag(manifest.expires_at),
        "managed-by": "terraform",
    }


def _round_installation_slug(manifest: DemoManifest, round_key: str) -> str:
    installation_id = manifest.installation_id
    if installation_id is None:
        raise RuntimeError("The per-round AWS installation ID is missing")
    digest = hashlib.sha256(f"{installation_id.strip()}:{round_key}".encode()).hexdigest()
    return f"i{digest[:20]}-{round_key}"


def _required_round_tags(manifest: DemoManifest, round_key: str) -> dict[str, str]:
    tags = _required_tags(manifest)
    # Provisioning carries the immutable installation ID from manifest v1;
    # the version only advances to v7 after all per-round seals are complete.
    if manifest.installation_id is not None:
        tags.update(
            {
                "anti-demo-installation-slug": _round_installation_slug(manifest, round_key),
                "anti-demo-round": round_key,
            }
        )
    return tags


def _required_tags_for_address(manifest: DemoManifest, address: str) -> dict[str, str]:
    """The ownership tags this address must carry to be recognised as ours.

    IAM treats tag keys case-insensitively and rejects ``Owner`` and ``owner``
    as a duplicate pair, so `infra/aws/locals.tf` tags every IAM resource from
    ``iam_role_required_tags``, which carries only the lowercase key. Demanding
    the capital one back from an IAM address asks for a tag AWS would not store.

    Only that key is excused, and only for IAM. Ownership is still proven by the
    lowercase ``owner``, alongside the run ID, the expiry and ``managed-by`` --
    so a resource belonging to somebody else is refused here exactly as before.
    """

    tags = _required_tags(manifest)
    if address.startswith(("aws_iam_role.", "aws_iam_instance_profile.", "aws_iam_policy.")):
        tags.pop("Owner")
    return tags


def _ownership_environments(manifest: DemoManifest) -> list[tuple[str, Any, Any]]:
    """The databases the seal names, per round.

    Shared by the ownership tag check and the absence probe below so the two
    cannot end up disagreeing about which resources this installation owns.
    """
    if manifest.manifest_version == 7:
        return [
            (
                f"r{number}",
                manifest.round_environment(number).aurora,
                manifest.round_environment(number).rds,
            )
            for number in (1, 2, 3, 5)
        ]
    resources = manifest.aws.resources
    return [
        (
            "r1",
            AuroraEnvironmentSeal.model_construct(
                cluster_id=resources.aurora_cluster_id,
                writer_instance_id=resources.aurora_writer_instance_id,
            ),
            RdsEnvironmentSeal.model_construct(
                instance_id=resources.rds_instance_id,
            ),
        )
    ]


#: The error codes with which RDS states, positively, that an identifier names
#: nothing. Only these count as an absence; every other failure is a failure to
#: look. Kept narrow on purpose -- see `_sealed_databases_absent`.
_ABSENT_DATABASE_ERROR_CODES = frozenset(
    {"DBClusterNotFoundFault", "DBInstanceNotFoundFault"}
)


def _is_absent_database_error(exc: BaseException) -> bool:
    """Whether AWS said this identifier does not exist, as opposed to anything else."""
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        error = response.get("Error")
        if isinstance(error, dict) and str(error.get("Code") or "") in (
            _ABSENT_DATABASE_ERROR_CODES
        ):
            return True
    return type(exc).__name__ in _ABSENT_DATABASE_ERROR_CODES


def _sealed_databases_absent(manifest: DemoManifest) -> bool:
    """Whether every sealed database is positively reported as not existing.

    This exists to separate the two reasons `_aws_ownership` fails, because they
    call for opposite answers. Ownership returning False means either "these are
    not ours" or "we could not tell", and the second covers the case where the
    resources are simply gone -- which is what the sandbox reaper leaves behind
    every fourteen days.

    Fails closed in every direction that is not an explicit not-found:

    *   A describe that *succeeds* means something is still there, so the answer
        is no even if its tags disagree with the seal. That keeps a foreign or
        mistagged resource on the refusing side.
    *   Expired credentials, throttling, a denial, an unbuildable session -- all
        failures to look, and none of them may read as a confirmed absence.
    *   A seal naming no databases answers no, because nothing was established.

    Every identifier must be absent, not merely one. A partial deletion is a
    genuinely ambiguous state that deserves an operator's eyes rather than an
    automatic rebind; a reap is unambiguous and is the case this unblocks.
    """
    try:
        rds = _aws_session(manifest).client("rds")
        environments = _ownership_environments(manifest)
    except Exception:
        return False
    sealed: list[tuple[str, str]] = []
    for _round_key, aurora, postgres in environments:
        if aurora is not None:
            sealed.append(("cluster", aurora.cluster_id))
            sealed.append(("instance", aurora.writer_instance_id))
        if postgres is not None:
            sealed.append(("instance", postgres.instance_id))
    if not sealed:
        return False
    for kind, identifier in sealed:
        try:
            if kind == "cluster":
                rds.describe_db_clusters(DBClusterIdentifier=identifier)
            else:
                rds.describe_db_instances(DBInstanceIdentifier=identifier)
        except Exception as exc:
            if _is_absent_database_error(exc):
                continue
            return False
        return False
    return True


def _aws_ownership(manifest: DemoManifest) -> Check:
    try:
        session = _aws_session(manifest)
        rds = session.client("rds")
        environments = _ownership_environments(manifest)
        checked = 0
        for round_key, aurora, postgres in environments:
            assert aurora is not None
            cluster = rds.describe_db_clusters(DBClusterIdentifier=aurora.cluster_id)["DBClusters"][
                0
            ]
            instances = [
                rds.describe_db_instances(DBInstanceIdentifier=aurora.writer_instance_id)[
                    "DBInstances"
                ][0],
            ]
            # Round 1 stands up no RDS instance, so there is no instance there to
            # tag-check. Its Aurora cluster and writer are still checked, because
            # Aurora is the lane that actually competes in Round 1 -- dropping the
            # whole round from the ownership check would stop verifying a resource
            # that does exist.
            # A blank identifier is not a narrower describe, it is an unfiltered
            # one: RDS returns every instance in the account and [0] is then some
            # stranger's database, which this check reads as a tag mismatch. Round
            # 1 seals exactly that blank, so the account only has to contain one
            # unrelated RDS instance for the refusal above to fire.
            if postgres is not None and postgres.instance_id:
                instances.append(
                    rds.describe_db_instances(DBInstanceIdentifier=postgres.instance_id)[
                        "DBInstances"
                    ][0]
                )
            expected = _required_round_tags(manifest, round_key)
            for arn in [
                cluster["DBClusterArn"],
                *(item["DBInstanceArn"] for item in instances),
            ]:
                actual = {
                    tag["Key"]: tag["Value"]
                    for tag in rds.list_tags_for_resource(ResourceName=arn).get("TagList", [])
                }
                if any(actual.get(key) != value for key, value in expected.items()):
                    return Check(
                        "aws_ownership",
                        False,
                        f"tag mismatch on {arn.rsplit(':', 1)[-1]}",
                    )
                checked += 1
        return Check("aws_ownership", True, f"{checked} database resource tags match manifest")
    except Exception:
        return Check("aws_ownership", False, "could not validate owned AWS resource tags")


def _sealed_ingress_summary(manifest: DemoManifest) -> str:
    """What the database groups admit, said without publishing an address.

    The operator CIDR was already printed here and stays: it is this machine's
    own address, an operator reading `antidemo doctor` needs to recognise it, and
    it is never written to a file this repository tracks. The app's prefixes are
    counted instead. They are globally routable and belong to Databricks, and a
    doctor transcript is exactly the kind of thing that gets pasted into an issue.
    """

    sealed = manifest.aws.serverless_egress_cidrs or ()
    if not sealed:
        return manifest.aws.operator_cidr
    return (
        f"{manifest.aws.operator_cidr} plus {len(sealed)} published "
        f"{manifest.aws.region} app egress prefix(es)"
    )


def _aws_ingress(manifest: DemoManifest) -> Check:
    try:
        session = _aws_session(manifest)
        runner_group = (
            manifest.require_round5_resources().runner_security_group_id
            if manifest.round5_ready
            else None
        )
        if manifest.manifest_version == 7:
            groups = [
                (
                    f"r{number}",
                    manifest.round_environment(number).aurora.security_group_id,
                    runner_group if number == 5 else None,
                )
                for number in (1, 2, 3, 5)
            ]
        else:
            groups = [
                (
                    "r1",
                    manifest.aws.resources.security_group_id,
                    runner_group if manifest.round5_ready else None,
                )
            ]
        for round_key, group_id, expected_runner in groups:
            group = session.client("ec2").describe_security_groups(GroupIds=[group_id])[
                "SecurityGroups"
            ][0]
            if not _postgres_ingress_is_exact(
                group.get("IpPermissions") or [],
                operator_cidr=manifest.aws.operator_cidr,
                runner_group=expected_runner,
                serverless_egress_cidrs=manifest.aws.serverless_egress_cidrs or (),
            ):
                return Check(
                    "aurora_ingress",
                    False,
                    f"{round_key} ingress differs from the sealed operator /32, the "
                    f"sealed app egress prefixes and the sealed runner",
                )
        return Check(
            "aurora_ingress",
            True,
            _sealed_ingress_summary(manifest),
        )
    except Exception:
        return Check("aurora_ingress", False, "could not validate Aurora security group")


def _postgres_ingress_is_exact(
    permissions: list[dict[str, Any]],
    *,
    operator_cidr: str,
    runner_group: str | None,
    serverless_egress_cidrs: Sequence[str] = (),
) -> bool:
    """Is this security group admitting precisely what the installation sealed?

    The invariant here was never "one address" -- it was "exactly the set this
    installation sealed, and nothing else", and only the second half was ever
    doing the work. Admitting the deployed app changes the size of the sealed
    set and nothing about the strictness of the comparison: a hand-added rule,
    a widened prefix, an IPv6 range or a prefix list is refused exactly as
    before, and Terraform revokes it on the next apply because these are inline
    `ingress` blocks and therefore authoritative over the whole rule set.

    `len(permissions)` is unchanged on purpose. Terraform renders one `ingress`
    block carrying several `cidr_blocks` as a single `IpPermission` with several
    `IpRanges`, because AWS groups permissions by protocol and port range -- so
    widening the CIDR set adds `IpRanges` to the permission that already exists
    rather than adding a permission. `tests/test_operator_ingress.py` pins that
    shape, since the whole `{1}` / `{1, 2}` arithmetic rests on it.
    """

    cidrs = [
        item["CidrIp"] for permission in permissions for item in permission.get("IpRanges", [])
    ]
    referenced_groups = [
        item.get("GroupId")
        for permission in permissions
        for item in permission.get("UserIdGroupPairs", [])
    ]
    return (
        len(permissions) in ({1, 2} if runner_group else {1})
        and sorted(cidrs) == sorted({operator_cidr, *serverless_egress_cidrs})
        and referenced_groups == ([runner_group] if runner_group else [])
        and all(permission.get("IpProtocol") == "tcp" for permission in permissions)
        and all(permission.get("FromPort") == 5432 for permission in permissions)
        and all(permission.get("ToPort") == 5432 for permission in permissions)
        and all(
            (permission.get("IpRanges") or permission.get("UserIdGroupPairs"))
            and not permission.get("Ipv6Ranges")
            and not permission.get("PrefixListIds")
            for permission in permissions
        )
    )


def _rds_ingress(manifest: DemoManifest) -> Check:
    try:
        session = _aws_session(manifest)
        runner_group = (
            manifest.require_round5_resources().runner_security_group_id
            if manifest.round5_ready
            else None
        )
        if manifest.manifest_version == 7:
            databases = [
                (
                    f"r{number}",
                    manifest.round_environment(number).rds,
                    runner_group if number == 5 else None,
                )
                for number in (1, 2, 3, 5)
                # Round 1 seals no RDS instance: its lane refuses to enter on
                # engine semantics and is never timed, so there is no instance and
                # no security group to validate ingress on. Reading the seal
                # unconditionally validated a lane that does not compete.
                if rds_lane_is_scored(_ROUND_NUMBER_IDS[number])
            ]
        else:
            resources = manifest.aws.resources
            databases = [
                (
                    "r1",
                    RdsEnvironmentSeal.model_construct(
                        instance_id=resources.rds_instance_id,
                        security_group_id=resources.rds_security_group_id,
                    ),
                    runner_group if manifest.round5_ready else None,
                )
            ]
        for round_key, sealed, expected_runner in databases:
            assert sealed is not None
            instance = session.client("rds").describe_db_instances(
                DBInstanceIdentifier=sealed.instance_id
            )["DBInstances"][0]
            attached_groups = [
                item.get("VpcSecurityGroupId")
                for item in instance.get("VpcSecurityGroups", [])
                if item.get("Status") == "active"
            ]
            group = session.client("ec2").describe_security_groups(
                GroupIds=[sealed.security_group_id]
            )["SecurityGroups"][0]
            correct = (
                instance.get("PubliclyAccessible") is True
                and attached_groups == [sealed.security_group_id]
                and _postgres_ingress_is_exact(
                    group.get("IpPermissions") or [],
                    operator_cidr=manifest.aws.operator_cidr,
                    runner_group=expected_runner,
                    serverless_egress_cidrs=manifest.aws.serverless_egress_cidrs or (),
                )
            )
            if not correct:
                return Check(
                    "rds_ingress",
                    False,
                    f"{round_key} RDS ingress differs from the sealed operator /32, the "
                    f"sealed app egress prefixes and the sealed runner",
                )
        return Check(
            "rds_ingress",
            True,
            _sealed_ingress_summary(manifest),
        )
    except Exception:
        return Check("rds_ingress", False, "could not validate RDS security group")


async def _live_target_checks(manifest: DemoManifest, competitor: str) -> list[Check]:
    apply_manifest_environment(manifest)
    if competitor == "rds":
        checks: list[tuple[str, Any]] = [("rds_capability", RdsCredentialProvider())]
    else:
        checks = [("aurora_scale_zero", AuroraCredentialProvider())]
    results: list[Check] = []
    for name, provider in checks:
        try:
            evidence = await provider.assert_armed()
            state = str(evidence.get("state") or "verified")
            results.append(Check(name, True, state))
        except Exception as exc:
            results.append(Check(name, False, str(exc)))
    return results


async def _lakebase_scale_zero_check(manifest: DemoManifest, timeout_seconds: float) -> Check:
    apply_manifest_environment(manifest)
    provider = LakebaseCredentialProvider()
    deadline = time.monotonic() + timeout_seconds
    final_observation = "no endpoint sample was returned"
    while True:
        try:
            evidence = await provider.assert_armed()
            state = str(evidence.get("state") or "").upper()
            disabled = evidence.get("disabled")
            final_observation = f"state={state or 'UNKNOWN'}, disabled={disabled!r}"
            if state == "IDLE" and disabled is False:
                return Check("lakebase_scale_zero", True, final_observation)
        except Exception as exc:
            final_observation = str(exc) or type(exc).__name__
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return Check(
                "lakebase_scale_zero",
                False,
                f"timed out after {timeout_seconds:.0f}s; final observation: {final_observation}",
            )
        await asyncio.sleep(min(5.0, remaining))


async def _coordination_check(manifest: DemoManifest) -> Check:
    apply_manifest_environment(manifest)
    try:
        from .coordination import build_lease_store

        store = build_lease_store()
        if store.mode != "lakebase":
            return Check("coordination_lease", False, "not using Lakebase")
        active = await store.current()
        await store.close()
        detail = (
            f"active · {active.operator.email or active.operator.display_name} · {active.phase}"
            if active is not None
            else "ready · no active owner"
        )
        return Check("coordination_lease", True, detail)
    except Exception as exc:
        return Check("coordination_lease", False, str(exc))


def _round4_check(manifest: DemoManifest, *, timeout_seconds: float = 120) -> Check:
    sealed = manifest.round4
    if manifest.manifest_version < 2 or sealed is None:
        return Check("round4_managed_sync", False, "manifest has no sealed Round 4 resources")
    # A deliberately stopped pipeline is answered before anything below asks the
    # account about sync health, because every one of those checks requires a
    # RUNNING pipeline and would report a chosen, money-saving state as a
    # failure. An operator who reads red here stops trusting the check, or
    # restarts a pipeline they had just paid attention to switching off.
    #
    # Gated on the local marker so the ordinary path costs nothing: no marker
    # means no stop was ever requested, and asking the account would be a network
    # call to confirm something already known locally.
    from .pipeline_power import PipelinePower, power_state, read_stop_marker

    # Carried past the branch below because an owed stop is the one power state
    # that does *not* leave the pipeline down. `PipelinePower.stop_owed` requires
    # `running`, and the advisory return below requires `not running`, so the two
    # are mutually exclusive: an overdue stop fell straight through to the full
    # check, which rebuilt a bare `PipelinePower` and printed a plain, healthy
    # RUNNING line. `doctor` was structurally unable to say the one thing that
    # costs $14.57/day -- the check was green and correct about sync health, and
    # silent about the money.
    owed_since = ""
    if read_stop_marker(sealed.pipeline_id) is not None:
        power = power_state(
            manifest,
            lambda identifier: _round4_get_pipeline(manifest, identifier),
        )
        if not power.running:
            return Check(
                "round4_managed_sync",
                True,
                f"{sealed.synced_table_resource_name} · {power.summary()}",
                advisory=True,
            )
        owed_since = power.stop_owed_since
    try:
        names = _round4_names(manifest)
        expected_fields = {
            "source_table_full_name": names["source_table"],
            "storage_catalog": names["catalog"],
            "storage_schema": names["storage_schema"],
            "synced_table_id": names["synced_table_id"],
            "synced_table_resource_name": names["resource_name"],
            "physical_database": ROUND4_DATABASE,
            "physical_schema": names["online_schema"],
            "physical_table": ROUND4_SYNCED_TABLE,
            "branch": names["branch"],
            "endpoint_name": names["endpoint_name"],
        }
        if any(getattr(sealed, field) != value for field, value in expected_fields.items()):
            raise RuntimeError("sealed Round 4 namespaces do not match the owned run")
        project = _get_lakebase_project_or_none(
            manifest,
            project_id=names["project"].removeprefix("projects/"),
        )
        branch = _round4_get_branch(manifest, names)
        if project is None or branch is None:
            raise RuntimeError("sealed Round 4 project or branch does not exist")
        project_uid, branch_uid = _validate_round4_project_and_branch(
            manifest, names, project, branch, sealed=sealed
        )
        _ensure_round4_app_roles(
            manifest,
            sealed.app_service_principal_client_id,
            timeout=120,
            create=False,
        )
        payload = _round4_get_synced_table(manifest, names)
        if payload is None:
            raise RuntimeError("sealed Round 4 synced table does not exist")
        _validate_round4_synced_table(manifest, payload, names, sealed=sealed)
        database_payload = _round4_get_database_synced_table(manifest, names)
        if database_payload is None:
            raise RuntimeError("sealed Round 4 /database synced table does not exist")
        _validate_round4_database_synced_table(
            database_payload,
            names,
            project_uid=project_uid,
            branch_uid=branch_uid,
            pipeline_id=sealed.pipeline_id,
            require_sync_position=True,
        )
        pipeline_payload = _round4_get_pipeline(manifest, sealed.pipeline_id)
        _validate_round4_pipeline(
            pipeline_payload,
            pipeline_id=sealed.pipeline_id,
            synced_table_uid=sealed.synced_table_uid,
            setup_principal=sealed.setup_principal,
            names=names,
        )
        _validate_round4_uc_contract(
            manifest,
            names,
            setup_principal=sealed.setup_principal,
            pipeline_id=sealed.pipeline_id,
            require_storage_schema=True,
        )
        state = str(((payload.get("status") or {}).get("detailed_state")) or "")
        database_state = str(
            ((database_payload.get("data_synchronization_status") or {}).get("detailed_state"))
            or ""
        )
        # A pipeline that is down with no stop recorded is still a fault -- that
        # is `PipelinePower.summary`'s own verdict, "a failure rather than a
        # choice", and the advisory return above has already let every *recorded*
        # stop past. What it must not do is name a pipeline failure that never
        # happened: the deliberate stop reached here whenever the record was
        # written somewhere this process cannot read, which on a laptop is any
        # stop the deployed app made.
        stopped_not_broken = synced_table_failure_is_a_stopped_pipeline(
            (state, database_state),
            str(pipeline_payload.get("state") or ""),
            latest_pipeline_update_state(pipeline_payload),
        )
        for label, observed in (
            ("synced table", state),
            ("/database synced table", database_state),
        ):
            if observed in SYNCED_TABLE_HEALTHY_STATES:
                continue
            if stopped_not_broken:
                raise RuntimeError(
                    f"Round 4 {label} reports {observed} because pipeline "
                    f"{sealed.pipeline_id} is switched off, not broken -- its newest "
                    f"update was cancelled rather than failed. No deliberate stop is "
                    f"recorded where this check can read one, so it cannot call this "
                    f"healthy; start it with '{ROUND4_PIPELINE_START_COMMAND}', or "
                    f"stop it through '{ROUND4_PIPELINE_STOP_COMMAND}' so the next "
                    f"check knows the stop was yours."
                )
            raise RuntimeError(f"Round 4 {label} is not healthy: {observed or 'UNKNOWN'}")
        detail = _sql_statement(
            manifest.databricks.profile,
            sealed.warehouse_id,
            "DESCRIBE DETAIL "
            f"`{names['catalog']}`.`{names['source_schema']}`.`{ROUND4_SOURCE_TABLE}`",
        )
        rows = _sql_rows(detail)
        properties: Any = rows[0].get("properties") if rows else None
        if isinstance(properties, str):
            properties = json.loads(properties)
        if (
            not isinstance(properties, dict)
            or str(properties.get("delta.enableChangeDataFeed", "")).lower() != "true"
        ):
            raise RuntimeError("Round 4 source table is not CDF-enabled")
        restored = _restore_round4_baseline_if_owned(
            manifest,
            names,
            sealed.warehouse_id,
            pipeline_id=sealed.pipeline_id,
            timeout=timeout_seconds,
        )
        expected_contract = ModelScoreContract(
            pipeline_id=sealed.pipeline_id,
            source_table=sealed.source_table_full_name,
            synced_table=(
                f"{sealed.physical_database}.{sealed.physical_schema}.{sealed.physical_table}"
            ),
        )
        if sealed.contract_sha256 != expected_contract.sha256:
            raise RuntimeError("Round 4 contract hash differs from the sealed resources")
        # The price rides along with the healthy answer too. A number in front of
        # the operator every time they run `doctor` is what makes a forgotten
        # stop cost a bout rather than a day. Read off the pipeline payload this
        # check already fetched above, so saying what it costs adds no call.
        baseline_note = " · baseline restored after a completed run" if restored else ""
        running = PipelinePower(
            pipeline_id=sealed.pipeline_id,
            cloud_state=str(pipeline_payload.get("state") or ""),
            stopped_deliberately=False,
            stop_owed_since=owed_since,
        )
        return Check(
            "round4_managed_sync",
            True,
            f"{sealed.synced_table_resource_name}{baseline_note} · {running.summary()}",
        )
    except Exception as exc:
        return Check("round4_managed_sync", False, str(exc))


def _round6_check(manifest: DemoManifest) -> Check:
    if manifest.round6 is None:
        return Check(
            "round6_native_cdf",
            True,
            "planned until a complete manifest v6 seal is live-validated",
        )
    from .round6_lifecycle import check_round6

    ok, detail = check_round6(manifest)
    return Check("round6_native_cdf", ok, detail)


def _prepare_and_reseal_round6(manifest: DemoManifest, *, timeout: float) -> DemoManifest:
    """Create the native CDF proof and publish the final version after live gates."""

    from .round6_lifecycle import prepare_round6

    manifest.status = "seeding"
    save_manifest(manifest)
    apply_manifest_environment(manifest)
    if manifest.round_environments is not None:
        round6_endpoint = _round_lakebase_binding(manifest, 6).endpoint_name
        _run(
            [
                "databricks",
                "postgres",
                "update-endpoint",
                round6_endpoint,
                "spec.suspension",
                "--json",
                json.dumps(
                    {"spec": {"suspend_timeout_duration": f"{LAKEBASE_SUSPEND_SECONDS}s"}}
                ),
                "--timeout",
                "10m",
                "-p",
                manifest.databricks.profile,
                "-o",
                "json",
            ],
            capture=True,
            timeout=700,
        )
    sealed = prepare_round6(
        manifest,
        timeout=timeout,
        scale_zero_validator=lambda candidate, seconds: asyncio.run(
            wait_for_scale_zero(candidate, seconds)
        ),
    )
    manifest.round6 = sealed
    manifest.manifest_version = (
        7
        if manifest.installation_id is not None
        and manifest.round_environments is not None
        and manifest.coordination_environment is not None
        else 6
    )
    manifest.status = "ready"
    save_manifest(manifest)
    return manifest


def _installation_presence_checks(manifest: DemoManifest) -> list[Check]:
    """Doctor's two answers from one sweep: is it still there, and is anything extra.

    A Round 2 or 3 clone whose bout lost its process keeps running with nobody
    tracking it, and until this check existed the only thing that noticed was a
    bill. Reported as findings rather than exceptions so that a drifted
    installation stays inspectable.

    Two lines rather than one because `resource_reconciliation` is False for any
    drift at all -- an orphan clone, a public-address count that moved -- and
    "somebody left a clone running" is a different problem from "the installation
    no longer exists". A single verdict over both reads as neither.

    Probes directly rather than through the cache, for the same reason
    `_operator_cidr_check` does: a doctor run is an operator asking now, so a
    five-minute-old answer would be the wrong one.
    """
    report = reconcile_live(manifest, _aws_session)
    presence = presence_from_report(report)
    detail = report.summary()
    for line in report.report_lines():
        detail = f"{detail}\n  {line}"
    return [
        Check(
            "installation_presence",
            presence.state != PRESENCE_MISSING,
            presence.detail,
            advisory=presence.state == PRESENCE_UNVERIFIED,
        ),
        Check("resource_reconciliation", report.ok, detail),
    ]


def doctor(competitor: str = "aurora", *, timeout_seconds: float = 90) -> list[Check]:
    checks = [
        Check(
            name,
            shutil.which(name) is not None,
            "available" if shutil.which(name) else "missing",
        )
        for name in ("uv", "node", "npm", "databricks", "aws", "terraform", "psql")
    ]
    try:
        manifest = load_manifest()
    except Exception as exc:
        checks.append(Check("owned_manifest", False, str(exc)))
        return checks
    checks.append(Check("owned_manifest", True, f"run {manifest.run_id} ({manifest.status})"))
    checks.append(
        Check(
            "manifest_status",
            manifest.status == "ready",
            manifest.status,
        )
    )
    checks.append(
        Check(
            "schema_source",
            manifest.schema_sha256 == _schema_sha256(),
            "matches last reset" if manifest.schema_sha256 == _schema_sha256() else "changed",
        )
    )
    checks.append(_expiry_check(manifest))
    try:
        _verify_aws_identity(
            manifest.aws.profile,
            manifest.aws.region,
            manifest.aws.account_id,
            manifest.aws.auth_mode,
        )
        checks.append(Check("aws_identity", True, f"account {manifest.aws.account_id}"))
    except Exception as exc:
        checks.append(Check("aws_identity", False, str(exc)))
    checks.append(_operator_cidr_check(manifest))
    # Immediately after aws_identity, and deliberately before anything that reads
    # a round. `aws_identity` proves the credentials resolve and name the right
    # account; this proves the principal they resolve to can still be assumed by
    # what the trust policy actually holds. Those are different questions, and
    # only the second one goes wrong silently.
    checks.append(_anti_demo_runtime_trust_check(manifest))
    try:
        actual_user = _verify_databricks_identity(manifest.databricks.profile)
        checks.append(
            Check(
                "databricks_identity",
                actual_user == manifest.databricks.user,
                actual_user,
            )
        )
    except Exception as exc:
        checks.append(Check("databricks_identity", False, str(exc)))
    checks.append(_region_parity(manifest))
    checks.append(_capacity_parity(manifest))
    checks.append(_round4_check(manifest, timeout_seconds=timeout_seconds))
    if manifest.round5_ready:
        checks.append(_round5_topology_check(manifest))
    else:
        checks.append(
            Check(
                "round5_secret_free_topology",
                True,
                "planned until a complete factory-ready manifest v5 is sealed",
            )
        )
    checks.append(_round6_check(manifest))
    checks.append(asyncio.run(_coordination_check(manifest)))
    checks.extend([_aws_ownership(manifest), _aws_ingress(manifest), _rds_ingress(manifest)])
    checks.extend(_installation_presence_checks(manifest))
    checks.extend(asyncio.run(_live_target_checks(manifest, competitor)))
    # This must remain the final check: earlier doctor work can activate Lakebase.
    checks.append(asyncio.run(_lakebase_scale_zero_check(manifest, timeout_seconds)))
    return checks


def setup(
    *,
    databricks_profile: str,
    aws_profile: str,
    aws_region: str,
    expected_account: str,
    owner: str,
    operator_cidr: str | None,
    ttl_hours: float | None,
    timeout_seconds: float,
) -> DemoManifest:
    round4_prepared = False
    round5_prepared = False
    round6_prepared = False
    if manifest_path().exists():
        # `--ttl-hours` only ever reached `provision`, so on an existing install it
        # was accepted, ignored, and the run proceeded -- a flag that looks like the
        # answer to an expiry problem and silently does nothing. Renewing an existing
        # installation is a different operation with its own ordering and safety
        # requirements, so point at the command that implements it rather than
        # pretending this one did.
        if ttl_hours is not None:
            raise RuntimeError(
                "--ttl-hours applies only to a first provision; this manifest already "
                f"exists at {manifest_path()}. Use 'antidemo renew --ttl-hours "
                f"{ttl_hours:g}' to move its expiry, which also retags AWS and "
                "re-seals the Round 5 ownership tags."
            )
        existing = load_manifest()
        if existing.status == "ready":
            if existing.round5_ready:
                _require_round5_clean_baseline(existing)
            reconcile_infrastructure(existing)
            manifest = reset(timeout_seconds)
            round4_prepared = True
            round6_prepared = manifest.round6_ready
        else:
            manifest = resume_provision(timeout_seconds)
            round4_prepared = True
            round5_prepared = True
            round6_prepared = manifest.round6_ready
    else:
        manifest = provision(
            databricks_profile=databricks_profile,
            aws_profile=aws_profile,
            aws_region=aws_region,
            expected_account=expected_account,
            owner=owner,
            operator_cidr=operator_cidr,
            ttl_hours=DEFAULT_TTL_HOURS if ttl_hours is None else ttl_hours,
            zero_timeout_seconds=timeout_seconds,
        )

    if not round4_prepared:
        manifest = _prepare_and_reseal_round4(manifest, timeout=timeout_seconds)
    if not round5_prepared:
        manifest = _prepare_and_reseal_round5(manifest, timeout=timeout_seconds)
    if not round6_prepared:
        manifest = _prepare_and_reseal_round6(manifest, timeout=timeout_seconds)
    failures: list[str] = []
    for competitor in ("aurora", "rds"):
        # Advisory checks are skipped here for the same reason they are skipped in
        # the CLI: they have already printed themselves, and they describe
        # something an operator should know rather than something setup must not
        # proceed past.
        failures.extend(
            f"{competitor}:{check.name}"
            for check in doctor(competitor, timeout_seconds=timeout_seconds)
            if not check.ok and not check.advisory
        )
    if failures:
        raise RuntimeError("Setup checks failed: " + ", ".join(failures))
    return manifest


def _inspect_round4_for_cleanup(
    manifest: DemoManifest,
) -> tuple[
    dict[str, str],
    dict[str, Any] | None,
    dict[str, dict[str, Any]],
]:
    sealed = manifest.round4
    names = _round4_names(manifest)
    setup_identity = manifest.databricks.user
    if sealed is not None:
        expected = {
            "source_table_full_name": names["source_table"],
            "storage_catalog": names["catalog"],
            "storage_schema": names["storage_schema"],
            "synced_table_id": names["synced_table_id"],
            "synced_table_resource_name": names["resource_name"],
            "physical_database": ROUND4_DATABASE,
            "physical_schema": names["online_schema"],
            "physical_table": ROUND4_SYNCED_TABLE,
            "branch": names["branch"],
            "endpoint_name": names["endpoint_name"],
        }
        for field, value in expected.items():
            if getattr(sealed, field) != value:
                raise RuntimeError(f"Cleanup refused: Round 4 {field} is not owned by this run")
        if sealed.setup_principal != setup_identity:
            raise RuntimeError("Cleanup refused: Round 4 setup identity differs from the manifest")
        sealed_contract = ModelScoreContract(
            pipeline_id=sealed.pipeline_id,
            source_table=sealed.source_table_full_name,
            synced_table=(
                f"{sealed.physical_database}.{sealed.physical_schema}.{sealed.physical_table}"
            ),
        )
        if sealed.contract_sha256 != sealed_contract.sha256:
            raise RuntimeError(
                "Cleanup refused: Round 4 contract hash does not match its resources"
            )
    elif manifest.manifest_version != 1:
        raise RuntimeError("Cleanup refused: Round 4 manifest seal is incomplete")

    project = (
        _get_lakebase_project_or_none(
            manifest,
            project_id=names["project"].removeprefix("projects/"),
        )
        if manifest.round_environments is not None
        else _get_lakebase_project_or_none(manifest)
    )
    branch = _round4_get_branch(manifest, names) if project is not None else None
    project_uid = branch_uid = ""
    if project is not None:
        if branch is None:
            raise RuntimeError("Cleanup refused: Round 4 production branch is missing")
        project_uid, branch_uid = _validate_round4_project_and_branch(
            manifest, names, project, branch, sealed=sealed
        )
    payload = _round4_get_synced_table(manifest, names)
    database_payload = _round4_get_database_synced_table(manifest, names)
    if (payload is None) != (database_payload is None):
        raise RuntimeError("Cleanup refused: Round 4 synced table is inconsistent across APIs")
    if payload is not None and database_payload is not None:
        if project is None or branch is None:
            raise RuntimeError("Cleanup refused: Round 4 project or branch is missing")
        uid, pipeline_id = _validate_round4_synced_table(manifest, payload, names, sealed=sealed)
        _validate_round4_database_synced_table(
            database_payload,
            names,
            project_uid=project_uid,
            branch_uid=branch_uid,
            pipeline_id=pipeline_id,
        )
        _validate_round4_pipeline(
            _round4_get_pipeline(manifest, pipeline_id),
            pipeline_id=pipeline_id,
            synced_table_uid=uid,
            setup_principal=setup_identity,
            names=names,
        )

    uc_objects: dict[str, dict[str, Any]] = {}
    expected_objects = {
        "source_schema": (
            "schemas",
            f"{names['catalog']}.{names['source_schema']}",
        ),
        "storage_schema": (
            "schemas",
            f"{names['catalog']}.{names['storage_schema']}",
        ),
        "online_schema": (
            "schemas",
            f"{names['catalog']}.{names['online_schema']}",
        ),
        "source_table": ("tables", names["source_table"]),
        "synced_table": ("tables", names["synced_table_id"]),
    }
    for key, (kind, full_name) in expected_objects.items():
        item = _round4_get_uc_object(manifest, kind, full_name)
        if item is None:
            continue
        if item.get("full_name") != full_name:
            raise RuntimeError(f"Cleanup refused: Round 4 {key} name is not exact")
        if item.get("owner") != setup_identity:
            raise RuntimeError(f"Cleanup refused: Round 4 {key} has a different owner")
        if item.get("created_by") != setup_identity:
            raise RuntimeError(f"Cleanup refused: Round 4 {key} has a different creator")
        uc_objects[key] = item
    if "source_table" in uc_objects and "source_schema" not in uc_objects:
        raise RuntimeError("Cleanup refused: Round 4 source table has no owned schema")
    if "synced_table" in uc_objects and "online_schema" not in uc_objects:
        raise RuntimeError("Cleanup refused: Round 4 synced table has no owned schema")
    for key, schema_name, expected_tables in (
        ("source_schema", names["source_schema"], [names["source_table"]]),
        ("storage_schema", names["storage_schema"], []),
        ("online_schema", names["online_schema"], [names["synced_table_id"]]),
    ):
        if key not in uc_objects:
            continue
        actual_tables = [
            str(item.get("full_name") or "")
            for item in _round4_list_uc_tables(manifest, schema_name, catalog=names["catalog"])
        ]
        expected_present = [
            full_name
            for full_name in expected_tables
            if full_name in {item.get("full_name") for item in uc_objects.values()}
        ]
        if actual_tables != expected_present:
            raise RuntimeError(f"Cleanup refused: Round 4 {key} contains unexpected tables")
    return names, payload, uc_objects


def _delete_round4_resources(
    manifest: DemoManifest,
    inventory: tuple[
        dict[str, str],
        dict[str, Any] | None,
        dict[str, dict[str, Any]],
    ],
) -> None:
    names, synced_table, uc_objects = inventory
    if synced_table is not None:
        operation = _databricks_api(
            manifest.databricks.profile,
            "delete",
            f"/api/2.0/postgres/{quote(names['resource_name'], safe='/')}",
            timeout=120,
        )
        _wait_round4_operation(manifest.databricks.profile, operation, timeout=700)
    _delete_round4_pipeline(manifest)
    for key, schema_name in (
        ("online_schema", names["online_schema"]),
        ("storage_schema", names["storage_schema"]),
        ("source_schema", names["source_schema"]),
    ):
        if key not in uc_objects:
            continue
        full_name = f"{names['catalog']}.{schema_name}"
        _databricks_api_delete_no_response(
            manifest.databricks.profile,
            f"/api/2.1/unity-catalog/schemas/{quote(full_name, safe='')}?force=true",
        )


def _delete_round4_pipeline(manifest: DemoManifest) -> None:
    """Confirm the Managed Sync pipeline is gone, and remove it when it is not.

    Deleting the synced table is *supposed* to take its pipeline with it -- the
    pipeline is created by, and owned by, the synced table's
    ``new_pipeline_spec``. :func:`_round4_survivor_lines` states that to the
    operator on the strength of the API contract alone, and a statement is not a
    check. Two ways it fails, both of which leave the single largest standing
    line in this installation running after a teardown reports success:

    *   The synced table was already absent when cleanup inventoried it, so the
        delete in :func:`_delete_round4_resources` never runs. A half-finished
        earlier teardown, or a table dropped by hand, lands exactly here -- and a
        pipeline whose table is gone is *more* likely to be orphaned, not less.
    *   The delete returned before the control plane finished reaping the
        pipeline, or did not reap it.

    So this asks. A pipeline the workspace no longer knows is the expected case
    and costs one call; a pipeline that is still there is deleted by ID.

    Raises on a delete that fails, deliberately. Cleanup's caller turns that into
    ``cleanup_failed`` and keeps the manifest, which is the only local record of
    the pipeline's ID -- and reporting a completed teardown over a pipeline that
    is still billing is the failure this whole function exists to prevent.
    """

    sealed = getattr(manifest, "round4", None)
    pipeline_id = str(getattr(sealed, "pipeline_id", "") or "") if sealed is not None else ""
    if not pipeline_id:
        return
    path = f"/api/2.0/pipelines/{quote(pipeline_id, safe='')}"
    if _databricks_api_optional(manifest.databricks.profile, path) is None:
        return
    print(
        f"DELETE Round 4 Managed Sync pipeline {pipeline_id}: it outlived its synced table",
        flush=True,
    )
    _databricks_api_delete_no_response(manifest.databricks.profile, path)
    if _databricks_api_optional(manifest.databricks.profile, path) is not None:
        raise RuntimeError(
            f"Cleanup could not remove the Round 4 Managed Sync pipeline {pipeline_id}; "
            f"it is still in the workspace and still billing. Remove it with: "
            f"databricks pipelines delete {pipeline_id} -p {manifest.databricks.profile}"
        )


def _delete_databricks_app(manifest: DemoManifest) -> None:
    """Delete the app this installation deployed, when it can be shown to be ours.

    The app is created by the Databricks CLI and has never been a Terraform
    resource, so a state-derived inventory has never seen it -- and until now
    cleanup only ever *reported* it, printing "SURVIVES THIS CLEANUP" over
    compute that stays ACTIVE and billing indefinitely after the operator
    believes the installation is gone. An app bills for its compute whether or
    not anything is deployed to it.

    Ownership is decided in :func:`_owned_app` by the recorded service-principal
    client ID, never by the name: `DEFAULT_APP_NAME` is a convention every
    installation of this repo shares. An app that cannot be shown to be ours is
    left alone and reported by :func:`_round4_survivor_lines`, which is the same
    answer this command gives for a neighbour's AWS residue.

    A workspace that cannot be read raises rather than passing quietly. "Could
    not ask" is not "already gone", and this is the last point at which anything
    still knows the app's name.
    """

    app = _owned_app(manifest)
    if app.unreadable:
        raise RuntimeError(
            f"Cleanup could not ask the workspace about the Databricks app {app.name} "
            f"({app.unreadable}), so it cannot report whether it is still billing. "
            f"Check by hand: databricks apps get {app.name} -p {manifest.databricks.profile}"
        )
    if not app.present or not app.owned:
        return
    print(f"DELETE Databricks app {app.name} ({app.compute_state or 'UNKNOWN'})", flush=True)
    _databricks_api_delete_no_response(
        manifest.databricks.profile,
        f"/api/2.0/apps/{quote(app.name, safe='')}",
    )


def _round4_app_record(manifest: DemoManifest) -> dict[str, Any] | None:
    """The deploy record for the app this installation put in the workspace.

    Written by `bootstrap.sh` beside the manifest. Read from disk rather than
    from Terraform state because the app is created by the Databricks CLI and
    has never been a Terraform resource -- which is exactly why a state-derived
    inventory has never been able to see it.

    Absent far more often than it looks. `bootstrap.sh` writes this file only
    when a deploy *succeeds*, so an installation whose app was created but never
    deployed to -- or whose every deploy failed -- has none, and the app is
    billing the whole time.
    """

    del manifest
    return _read_json_object(manifest_path().parent / "app-deploy.json")


#: The app name `bootstrap.sh` uses when `DATABRICKS_APP_NAME` is unset. Mirrored
#: rather than imported because it lives in shell; the resolution below reads the
#: recorded value first and only falls back to this.
DEFAULT_APP_NAME = "lakebase-anti-demo"

#: The record `bootstrap.sh` writes beside the manifest on every run that holds
#: the generation lock, deploy or provision, success or failure. Unlike
#: `app-deploy.json` it exists whenever bootstrap has ever touched this
#: generation, which is what makes it a usable source for the app's name.
BOOTSTRAP_RECORD_NAME = "bootstrap.json"


def _read_json_object(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def _deployed_app_name(manifest: DemoManifest) -> tuple[str, str, str]:
    """The app this installation owns, where the name came from, and its client ID.

    Three sources in falling order of directness, and never no answer at all:

    1. ``app-deploy.json``, written by a *successful* deploy. Most direct, and
       the one that is missing whenever it matters most.
    2. ``bootstrap.json``, written by every locked bootstrap run including the
       failed ones, under ``databricks_app_name``.
    3. The documented default. A name is not existence -- the caller confirms it
       against the workspace before saying anything about compute -- so guessing
       here costs nothing and buys the one thing that was missing: a name to
       confirm.

    This replaces reading (1) alone. With (1) absent, cleanup printed "no deploy
    record beside this manifest" and stopped, which reads as an all-clear over an
    app whose compute is ACTIVE and billing.

    The third element is the service-principal client ID this installation
    recorded for that app, or ``""`` when nothing recorded one. Both (1) and (2)
    carry it -- ``app_client_id`` and ``databricks_app_client_id`` respectively --
    which is what makes deletion decidable from source (2) as well as (1): the
    name alone is a convention and several installations share it, but the client
    ID is minted per app. Source (3) carries none, and an app matched only by the
    documented default name is therefore reported and never deleted.
    """

    record = _round4_app_record(manifest)
    recorded = str((record or {}).get("app_name") or "").strip()
    if recorded:
        return recorded, "app-deploy.json", str((record or {}).get("app_client_id") or "").strip()
    bootstrap = _read_json_object(manifest_path().parent / BOOTSTRAP_RECORD_NAME)
    configured = str((bootstrap or {}).get("databricks_app_name") or "").strip()
    if configured:
        return (
            configured,
            BOOTSTRAP_RECORD_NAME,
            str((bootstrap or {}).get("databricks_app_client_id") or "").strip(),
        )
    return DEFAULT_APP_NAME, "the documented default", ""


@dataclass(frozen=True, slots=True)
class _OwnedApp:
    """The Databricks app this installation deployed, as far as it can be known.

    Deliberately holds "could not ask" apart from "not there". They look
    identical from a distance and mean opposite things when the question is
    whether something is billing, and collapsing them is the defect this whole
    inventory exists to stop.
    """

    name: str
    #: Which record named it: `app-deploy.json`, `bootstrap.json` or the default.
    source: str
    #: What the workspace returned, or None when the app is not there.
    payload: dict[str, Any] | None = None
    #: Why the workspace could not be asked. Empty when it was asked.
    unreadable: str = ""
    #: Whether the live app's service principal matches the ID this
    #: installation recorded. Only ever True on a positive match.
    owned: bool = False

    @property
    def present(self) -> bool:
        return self.payload is not None

    @property
    def compute_state(self) -> str:
        return str(((self.payload or {}).get("compute_status") or {}).get("state") or "")


def _owned_app(manifest: DemoManifest) -> _OwnedApp:
    """Resolve the app's name, ask the workspace about it, and decide ownership.

    Never raises: this feeds an inventory whose entire value is being readable
    when the installation is broken.

    Ownership is proven by the service-principal client ID rather than by the
    name. `DEFAULT_APP_NAME` is a convention shared by every installation of
    this repo, so a name match alone would let one installation's teardown
    delete another's running app -- the mirror image of the bug being fixed, and
    a worse one. A recorded ID that does *not* match is likewise not ownership:
    it means the name now points at a different app, and that app is somebody
    else's.
    """

    name, source, recorded_client_id = _deployed_app_name(manifest)
    try:
        payload = _databricks_api_optional(
            manifest.databricks.profile,
            f"/api/2.0/apps/{quote(name, safe='')}",
        )
    except Exception as error:
        return _OwnedApp(name, source, unreadable=type(error).__name__)
    if payload is None:
        return _OwnedApp(name, source)
    live_client_id = str(payload.get("service_principal_client_id") or "").strip()
    owned = bool(recorded_client_id) and recorded_client_id == live_client_id
    return _OwnedApp(name, source, payload=payload, owned=owned)


def _round4_survivor_lines(manifest: DemoManifest) -> list[str]:
    """Name the two Round 4 resources a Terraform-derived inventory cannot see.

    The Managed Sync pipeline and the deployed app are both Databricks-side and
    neither is in Terraform state, so `_terraform_managed_addresses` has never
    listed either one. Between them the pipeline alone is the largest standing
    line in this installation, and until now a teardown could print a clean
    inventory and a completed destroy while it kept billing.

    **The dollars, and not a share.** This docstring claimed "63% of this
    installation's standing cost" and `OPEN-FINDINGS.md` D15a claimed "56% of the
    Databricks side"; neither was right, and they were wrong against two different
    denominators. A share is meaningless without naming the total it is taken
    against, and the only thing that knows which total that is is
    `standing_cost._continuous`, which derives the share the panel states at
    render time from the payload it is stating. So the figure this function puts
    in front of an operator is the rate, in dollars, and it arrives from
    `pipeline_power.PIPELINE_USD_PER_DAY` by way of `power.summary()` below rather
    than from anything written here.

    Reported, never deleted here. The pipeline goes when its synced table goes,
    and the app is not this command's to remove -- but an operator who is told it
    survives can remove it in one call, whereas an operator who is told nothing
    finds out from a bill.

    Never raises. This runs inside an inventory whose entire value is being
    readable when the installation is broken.
    """

    lines: list[str] = []
    sealed = getattr(manifest, "round4", None)
    pipeline_id = str(getattr(sealed, "pipeline_id", "") or "") if sealed is not None else ""
    if pipeline_id:
        from .pipeline_power import power_state

        power = power_state(
            manifest,
            lambda identifier: _round4_get_pipeline(manifest, identifier),
        )
        lines.append(f"OWNED Round 4 Managed Sync pipeline: {pipeline_id} · {power.summary()}")
        if power.running:
            lines.append(
                "      Deleted with its synced table below. Until then it is the "
                "largest standing line in this installation."
            )
    else:
        lines.append("OWNED Round 4 Managed Sync pipeline: not sealed by this manifest")
    app = _owned_app(manifest)
    if app.unreadable:
        # This function must stay readable when the workspace is not. An
        # unreachable workspace is reported as unread, never as absent: "not
        # there" and "could not ask" look identical from a distance and mean
        # opposite things when the question is whether something is billing.
        lines.append(
            f"OWNED Databricks app: {app.name} (from {app.source}; the workspace could not "
            f"be asked: {app.unreadable}). It is neither confirmed present nor "
            f"confirmed gone, and an app that exists bills whether or not anything is "
            f"deployed to it. Check by hand: databricks apps get {app.name} "
            f"-p {manifest.databricks.profile}"
        )
        return lines
    if not app.present:
        lines.append(f"OWNED Databricks app: {app.name} (not in the workspace; from {app.source})")
        return lines
    verdict = "DELETED BY THIS CLEANUP" if app.owned else "SURVIVES THIS CLEANUP"
    lines.append(
        f"OWNED Databricks app: {app.name} · {app.compute_state or 'UNKNOWN'} · {verdict}"
    )
    if app.source != "app-deploy.json":
        lines.append(
            f"      Named from {app.source}: no successful deploy is recorded beside this "
            f"manifest, so the workspace was asked directly. Compute above is what the "
            f"workspace reports and is billing now."
        )
    if not app.owned:
        # Reported and left alone, because the only thing tying this manifest to
        # that app is a name every installation of this repo shares. Deleting on
        # a name match would let one operator's teardown remove another's running
        # app, which is worse than the miss it would be fixing.
        lines.append(
            f"      Not deleted here: no recorded service principal for this "
            f"installation matches the app now holding that name, so it cannot be "
            f"shown to be ours. Remove it by hand if it is: databricks apps delete "
            f"{app.name} -p {manifest.databricks.profile}"
        )
    return lines


def _refuse_or_report(finding: str, *, dry_run: bool) -> None:
    """One rule, both of cleanup's cost gates: `--yes` refuses, a dry run reports.

    An inventory that aborts on its first surprise takes away the inspection the
    operator came for, and a drifted installation is exactly the one being
    inspected -- so a dry run says the thing and keeps going. Only `--yes`
    raises. Both callers sit above cleanup's ``try:``, so a refusal deletes
    nothing, writes no receipt, and leaves the manifest on disk. That last part
    is the point rather than a side effect: the manifest is the only local
    record of this run ID, and both ``doctor`` and ``cleanup`` load it, so
    unlinking it while resources still bill removes the operator's handle for
    ever finding them again.
    """

    if dry_run:
        print(f"DRIFT {finding}", flush=True)
        return
    raise RuntimeError(f"Cleanup refused: {finding}")


def cleanup(*, dry_run: bool, force_round6: str = "") -> DemoManifest:
    """Inventory or delete only manifest-owned resources.

    ``force_round6`` is empty on every ordinary invocation and carries an
    operator-typed confirmation token when ``--force-round6`` was passed. It
    reaches exactly one gate -- Round 6's seal verification -- and every other
    identity, ownership and topology refusal below is untouched by it.
    """

    manifest = load_manifest()
    expected_project = f"projects/{manifest.run_id}"
    databricks_bindings: list[DatabricksManifest] = []
    if manifest.round_environments is not None:
        actual_user = _verify_databricks_identity(manifest.databricks.profile)
        if actual_user != manifest.databricks.user:
            raise RuntimeError(
                f"Cleanup refused: Databricks profile {manifest.databricks.profile} "
                f"resolved to {actual_user}, expected {manifest.databricks.user}"
            )
    else:
        seen_bindings: set[tuple[str, str]] = set()
        for binding in [manifest.databricks, *manifest.prior_databricks]:
            binding_key = (binding.profile, binding.project_id)
            if binding_key in seen_bindings:
                continue
            seen_bindings.add(binding_key)
            if (
                binding.project_id != manifest.run_id
                or binding.endpoint_name.split("/branches/", 1)[0] != expected_project
            ):
                raise RuntimeError("Manifest Lakebase project does not match its run ID")
            actual_user = _verify_databricks_identity(binding.profile)
            if actual_user != binding.user:
                raise RuntimeError(
                    f"Cleanup refused: Databricks profile {binding.profile} resolved to "
                    f"{actual_user}, expected {binding.user}"
                )
            databricks_bindings.append(binding)
    _verify_aws_identity(
        manifest.aws.profile,
        manifest.aws.region,
        manifest.aws.account_id,
        manifest.aws.auth_mode,
    )
    _terraform_init(manifest)
    managed_addresses = _terraform_managed_addresses(manifest)
    aws_resources_exist = bool(managed_addresses)
    # Terraform only knows what Terraform made, and only for as long as its
    # state file says so. A state file that was lost, moved or never written
    # lists nothing, which is indistinguishable from a finished teardown -- so
    # the account is asked by tag here, before a word is printed about AWS.
    # This read used to happen below, after the all-clear, where its verdict
    # gated nothing and could contradict the line printed just above it.
    reconciliation = reconcile_live(manifest, _aws_session)
    complete_baseline = _aws_state_is_complete(manifest, managed_addresses)
    destroy_plan: Path | None = None
    if aws_resources_exist:
        if complete_baseline:
            # An inventory must not rewrite the file it inventories. Only the
            # in-memory hydration is needed to check ownership; every later
            # cleanup rederives these values from Terraform outputs anyway.
            _hydrate_aws_resources(manifest, persist=not dry_run)
            ownership = _aws_ownership(manifest)
            if not ownership.ok:
                raise RuntimeError("Cleanup refused: AWS ownership tags do not match the manifest")
        else:
            _validate_partial_aws_destroy_retry(manifest, managed_addresses)
    lakebase_projects: list[tuple[str, str, dict[str, Any] | None]] = []
    if manifest.round_environments is not None:
        sealed_projects = [
            environment.lakebase for environment in manifest.round_environments.values()
        ]
        if manifest.coordination_lakebase is None:
            raise RuntimeError("Cleanup refused: coordination project seal is missing")
        sealed_projects.append(manifest.coordination_lakebase)
        for sealed in sealed_projects:
            project = _get_lakebase_project_or_none(manifest, project_id=sealed.project_id)
            if project is not None:
                _validate_lakebase_project(manifest, project, project_id=sealed.project_id)
                if project.get("uid") != sealed.project_uid:
                    raise RuntimeError(
                        "Cleanup refused: Lakebase project UID differs from its seal"
                    )
            lakebase_projects.append((manifest.databricks.profile, sealed.project_id, project))
            state = "exists" if project is not None else "already removed"
            print(
                f"OWNED Lakebase project: projects/{sealed.project_id} via "
                f"{manifest.databricks.profile} ({state})"
            )
    else:
        for binding in databricks_bindings:
            candidate = manifest.model_copy(update={"databricks": binding})
            project = _get_lakebase_project_or_none(candidate)
            if project is not None:
                _validate_lakebase_project(candidate, project)
            lakebase_projects.append((binding.profile, binding.project_id, project))
            state = "exists" if project is not None else "already removed"
            print(f"OWNED Lakebase project: {expected_project} via {binding.profile} ({state})")
    round4_inventory = _inspect_round4_for_cleanup(manifest)
    round4_state = "exists" if round4_inventory[1] is not None else "already removed"
    print(f"OWNED Round 4 synced table: {round4_inventory[0]['resource_name']} ({round4_state})")
    for line in _round4_survivor_lines(manifest):
        print(line, flush=True)
    if aws_resources_exist:
        print(f"OWNED Aurora cluster: {manifest.aws.resources.aurora_cluster_id}")
        print(f"OWNED RDS instance: {manifest.aws.resources.rds_instance_id}")
        print("PLAN  Terraform destroy after Round 5 clean-baseline authorization")
    else:
        # An empty Terraform state is not evidence of an empty account, and
        # Terraform cannot destroy what its state does not list. Three answers,
        # and they must never collapse back into the single "already removed"
        # that used to stand here: read and clear, read and still running, and
        # not read at all. The middle one was the defect -- it printed the
        # all-clear, skipped the destroy plan, wrote a receipt and unlinked the
        # manifest while the fleet billed on. Residue tagged for somebody
        # else's run is not this manifest's to refuse over; the orphan lines
        # below report it, which is the right response to a shared account.
        stranded = [
            resource
            for resource in reconciliation.observed
            if resource.run_id == manifest.run_id and not resource.retiring
        ]
        if reconciliation.unavailable:
            finding = (
                f"Terraform state lists no AWS resources for {manifest.run_id}, and the "
                f"account could not be read ({reconciliation.unavailable}), so the empty "
                f"state is not evidence that anything was removed. Whatever stopped the "
                f"read is the thing to fix. To inventory by hand: aws ec2 "
                f"describe-instances --filters Name=tag:{TAG_RUN_ID},Values={manifest.run_id} "
                f"--profile {manifest.aws.profile} --region {manifest.aws.region}, and "
                f"aws rds describe-db-instances --profile {manifest.aws.profile} "
                f"--region {manifest.aws.region}, whose TagList carries the same key"
            )
        elif stranded:
            inventory = ", ".join(
                f"{resource.kind}={resource.identifier} (status {resource.status})"
                for resource in stranded
            )
            finding = (
                f"Terraform state lists no AWS resources for {manifest.run_id}, but the "
                f"account is still running {len(stranded)} tagged {TAG_RUN_ID}="
                f"{manifest.run_id}: {inventory}. Terraform cannot destroy what its state "
                f"does not list, so this cleanup would report a teardown it did not "
                f"perform. Restore the state this manifest seals ({manifest.aws.terraform_state}) "
                f"or delete these by hand, then run cleanup again"
            )
        else:
            finding = ""
        if finding:
            _refuse_or_report(finding, dry_run=dry_run)
        else:
            print(
                f"OWNED AWS resources: nothing in Terraform state and nothing tagged "
                f"{TAG_RUN_ID}={manifest.run_id} in the account"
            )
    # A Round 2 or 3 clone that outlived its bout was never in Terraform state,
    # so the destroy plan above cannot see it either.
    print(f"CHECK Resource reconciliation: {reconciliation.summary()}", flush=True)
    for line in reconciliation.report_lines():
        print(line, flush=True)
    # The same rule as the empty-state answer above, reached down the other
    # path, and printed after the lines that evidence it. Terraform destroys what
    # its state lists; a per-bout clone was never in that state, so this teardown
    # would remove the sealed fleet, write a receipt saying every owned resource
    # was removed, and unlink the manifest while the clone billed on.
    #
    # `ORPHAN_EPHEMERAL` only, which is exactly the set `reap.py` is permitted to
    # delete, and the narrowness is load-bearing in both directions:
    #
    # * never `ORPHAN_FOREIGN_RUN`. That is a neighbour's residue in a shared
    #   sandbox -- this operator did not create it and cannot remove it, so
    #   refusing on it would wedge their teardown permanently behind somebody
    #   else's resource. Worse than the defect being fixed. It stays reported.
    # * never `ORPHAN_UNEXPECTED`. `expected_resources` reads the round seals
    #   rather than `aws.resources`, so on any manifest sealing no
    #   `round_environments` a *healthy* installation reports its own live Aurora
    #   and RDS under that code -- verified, not assumed. Refusing on it would
    #   block every ordinary teardown of such an installation.
    #
    # No `--force` escape hatch, unlike `--force-round6`. A Round 6 seal
    # mismatch has no mechanical remedy and needs an operator's judgement,
    # which is what an override is for. This names RDS resources the operator
    # created under their own run ID, and the message below carries the exact
    # delete calls, so the remedy is mechanical. An override here would put the
    # receipt that says "removed" over resources still billing back by a third
    # path -- so the route forward is to remove them, not to assert past them.
    leaked = [finding for finding in reconciliation.findings if finding.code == ORPHAN_EPHEMERAL]
    if leaked:
        _refuse_or_report(
            f"the account is still running {len(leaked)} per-bout clone(s) that Terraform "
            f"never created and this teardown therefore cannot remove: "
            + "; ".join(orphan.line() for orphan in leaked)
            + f". Destroying the sealed fleet now would report every owned resource "
            f"removed and unlink the manifest, leaving these billing with nothing on this "
            f"machine that still knows run {manifest.run_id}. The manifest is being kept "
            f"for that reason. Delete them first -- aws rds delete-db-instance "
            f"--db-instance-identifier ID --skip-final-snapshot, or delete-db-cluster "
            f"--db-cluster-identifier ID --skip-final-snapshot for a cluster, with "
            f"--profile {manifest.aws.profile} --region {manifest.aws.region} -- then run "
            f"cleanup again",
            dry_run=dry_run,
        )
    try:
        from .round6_lifecycle import cleanup_round6

        # Validate the exact v6 seal. During cleanup this must precede Round
        # 4/schema/project deletion because the CDF config still refers to its
        # source and destination schemas. A dry run reports a seal mismatch here
        # instead of raising, so a drifted environment stays inspectable.
        for finding in cleanup_round6(manifest, dry_run=dry_run, force_token=force_round6):
            print(finding, flush=True)
        if complete_baseline:
            _require_round5_clean_baseline(manifest)
        if dry_run:
            return manifest
        if manifest.round5_ready and complete_baseline:
            round5 = _round5_topology_check(manifest)
            if not round5.ok:
                raise RuntimeError(
                    "Cleanup refused: Round 5 ownership topology differs from the manifest"
                )
        if complete_baseline:
            _require_round5_runner_idle(manifest)
        if complete_baseline:
            clean_receipt = _write_round5_clean_receipt(manifest)
            print(f"CLEAN {clean_receipt}", flush=True)
            _run(
                _terraform_base() + ["state", "rm", "terraform_data.round5_destroy_guard"],
                env=_terraform_environment(manifest),
            )
        if aws_resources_exist:
            destroy_plan = _terraform_plan(manifest, destroy=True)
            print(f"PLAN  {destroy_plan}", flush=True)
        # Before the destroy, not after. The app assumes the runtime IAM role
        # Terraform is about to remove, so destroying first leaves an app that
        # is broken *and* still billing -- and an operator watching a successful
        # destroy scroll past has no reason to look for it.
        _delete_databricks_app(manifest)
        _delete_round4_resources(manifest, round4_inventory)
        if destroy_plan is not None:
            if complete_baseline:
                asyncio.run(reset_safe_change_artifacts(manifest))
            else:
                asyncio.run(reset_safe_change_only_artifacts(manifest))
            _terraform_apply(manifest, destroy_plan)
        for profile, project_id, project in lakebase_projects:
            if project is None:
                continue
            _run(
                [
                    "databricks",
                    "postgres",
                    "delete-project",
                    f"projects/{project_id}",
                    "--purge",
                    "--timeout",
                    "10m",
                    "-p",
                    profile,
                    "-o",
                    "json",
                ],
                capture=True,
                timeout=700,
            )
    except Exception:
        # Not on a dry run. `cleanup_failed` means "a teardown ran partway and a
        # human must adjudicate what survived" -- `require_ready_manifest`
        # refuses all six rounds on it, and no automatic recovery accepts it. A
        # dry run deletes nothing: every deletion in this block sits below the
        # `return` above, and `cleanup_round6` returns before its first delete in
        # that mode too. So the state this records is one an inventory cannot
        # produce, and recording it wedged installations whose only sin was
        # following the `--dry-run` instruction in `README.md`.
        #
        # The guard stays *below* the two reads rather than above them, because
        # their findings are what the dry run is for. They can still raise --
        # `_require_round5_clean_baseline` reaches
        # `secretsmanager:ListSecrets`, which the app-runtime principal is not
        # granted -- and that exception still propagates, which is correct: an
        # inventory that could not complete must say so instead of printing a
        # clean sheet. Only the write is withheld.
        if not dry_run:
            manifest.status = "cleanup_failed"
            save_manifest(manifest)
        raise

    receipt = manifest_path().parent / "cleanup-receipt.json"
    receipt.write_text(
        json.dumps(
            {
                "run_id": manifest.run_id,
                "owner": manifest.owner,
                "cleaned_at": datetime.now(UTC).isoformat(),
                "aws_account_id": manifest.aws.account_id,
                "aws_region": manifest.aws.region,
                "lakebase_project": expected_project,
                "lakebase_profiles": [binding.profile for binding in databricks_bindings],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    receipt.chmod(0o600)
    manifest_path().unlink()
    return manifest
