from __future__ import annotations

import hashlib
import ipaddress
import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .aws_auth import AwsAuthMode
from .capacity import rds_lane_is_scored
from .models import RoundId
from .round6_contract import round6_contract_sha256

PROJECT_ROOT = Path(__file__).resolve().parents[1]

#: The deployed app's manifest source, read from `app.yaml`'s binding of the
#: `anti-demo-manifest-json` secret. `load_manifest` consults it *before*
#: `ANTI_DEMO_MANIFEST`, which makes it the higher-precedence of the two and the
#: one anything wanting to neutralise an ambient manifest has to name first. A
#: constant rather than a literal at the read site for exactly that reason:
#: `tests/conftest.py` scrubs it by importing this name, and a literal there
#: could drift away from a literal here without either one looking wrong.
MANIFEST_JSON_ENV = "ANTI_DEMO_MANIFEST_JSON"

#: The operator identity a local run serves from. `apply_manifest_environment`
#: writes these into the *real* `os.environ` and `server/api.py` reads them back,
#: which makes them the same shape of hazard as the endpoint variables in
#: `server/process_registry.py`: production code that a test calls leaves them
#: behind, and every later test in the session inherits an identity it never set.
#: Named here rather than spelled at the two sites so that the writer, the reader
#: and the containment in `tests/conftest.py` cannot drift apart.
LOCAL_OPERATOR_ENV = "ANTI_DEMO_LOCAL_OPERATOR"
LOCAL_OPERATOR_EMAIL_ENV = "ANTI_DEMO_LOCAL_OPERATOR_EMAIL"
LOCAL_OPERATOR_ID_ENV = "ANTI_DEMO_LOCAL_OPERATOR_ID"
LOCAL_OPERATOR_ENV_NAMES = (
    LOCAL_OPERATOR_ENV,
    LOCAL_OPERATOR_EMAIL_ENV,
    LOCAL_OPERATOR_ID_ENV,
)

#: The narrowest published prefix this installation will admit to a database
#: security group. Named here rather than at the two sites that enforce it --
#: `AwsManifest.require_narrow_serverless_egress` and the Terraform variable's
#: own validation -- because a floor that disagrees with itself across the
#: Python/HCL boundary would be enforced by whichever side ran first.
SERVERLESS_EGRESS_MIN_PREFIXLEN = 24

# There is deliberately no default manifest path. A default pointed at
# .anti-demo/manifest.json, which outlived the generation that wrote it: a bare
# `./antidemo` command silently operated on a dead environment while the live one ran
# beside it under ANTI_DEMO_MANIFEST. Refusing to guess is what makes a second
# generation visible instead of invisible.
_NO_MANIFEST_SELECTED = (
    "No owned demo manifest is selected. Set ANTI_DEMO_MANIFEST to the manifest.json "
    "of the generation you mean to operate on, or start the run through the launcher, "
    "which sets it for you. There is no default: silently resolving one would act on a "
    "previous generation's state. "
    'Run \'eval "$(./bootstrap.sh --print-env)"\' to set it for the current shell; '
    "plain ./bootstrap.sh reports the generation it resolves but cannot export into "
    "the shell that called it. That command re-runs the whole preflight and prints "
    "nothing at all when any check fails, so an eval of it can quietly set nothing and "
    "land you back here. When it comes back empty, select the newest generation on disk "
    "directly with: "
    'export ANTI_DEMO_MANIFEST="$PWD/$(ls -d .anti-demo-v*/ | sort -V | tail -1)manifest.json"'
)


class AwsResources(BaseModel):
    aurora_cluster_id: str = ""
    aurora_writer_instance_id: str = ""
    aurora_secret_arn: str = ""
    rds_instance_id: str = ""
    rds_secret_arn: str = ""
    security_group_id: str = ""
    rds_security_group_id: str = ""
    db_subnet_group_name: str = ""


class AwsManifest(BaseModel):
    # Defaults preserve compatibility with manifests created before auth_mode existed.
    auth_mode: AwsAuthMode = "profile"
    profile: str = ""
    account_id: str
    region: str
    operator_cidr: str
    terraform_state: str
    resources: AwsResources = Field(default_factory=AwsResources)
    #: The single principal every copy of this installation authenticates as --
    #: the operator's laptop through `role_arn` in `~/.aws/config`, the deployed
    #: app through its IAM user's keys. Both `None` on every installation sealed
    #: before this existed, which is why they are `None` rather than `""`/`()`:
    #: `save_manifest` writes with `exclude_none=True`, so an older manifest
    #: round-trips byte-identical instead of silently gaining two empty keys.
    runtime_role_arn: str | None = Field(
        default=None, pattern=r"^arn:[^:]+:iam::\d{12}:role/.+$"
    )
    #: Exactly the principals the runtime role's trust policy names. Sealed as a
    #: set, because the trust document is what `antidemo doctor` holds AWS against
    #: after the fortnightly sweep deletes and the installer recreates the IAM
    #: user -- and a recreated user keeps its ARN while losing the unique
    #: principal ID the trust actually stored.
    runtime_role_trusted_principal_arns: tuple[str, ...] | None = None
    #: The published Databricks serverless egress prefixes this installation
    #: admits to its database security groups, alongside -- never instead of --
    #: `operator_cidr`. Sealed because the deployed app leaves from addresses
    #: this account neither owns nor controls, and an allowlist with one entry in
    #: it is the only thing that ever stood between the app and seven databases
    #: that are all already `publicly_accessible = true`.
    #:
    #: Never written by hand and never carried in this repository: `tests/
    #: test_no_live_identifiers_committed.py` refuses any globally routable IPv4
    #: literal in a tracked *or* untracked file, and every published prefix is
    #: routable. `server.lifecycle._refresh_serverless_egress_cidrs` fetches them
    #: at reconcile time, which is the only way they can reach a manifest.
    serverless_egress_cidrs: tuple[str, ...] | None = None
    #: The feed's own `timestampSeconds` for the snapshot the list above came
    #: from. Sealing it is what makes staleness *detectable* rather than guessed:
    #: Databricks publishes as often as every 30 days and new addresses go live
    #: 60 days after publication, so the age of this value is the whole warning.
    serverless_egress_published_at: int | None = None

    @field_validator("serverless_egress_cidrs")
    @classmethod
    def require_narrow_serverless_egress(
        cls, value: tuple[str, ...] | None
    ) -> tuple[str, ...] | None:
        """A separate rule from the operator CIDR's, because it is a separate claim.

        `_validate_operator_cidr` says "one laptop, one address" and is left
        exactly as it was. This says something weaker and different: every entry
        is a published provider egress prefix, and none of them is wide enough to
        be a boundary in front of a live database. The floor is a `/24` because
        that is the widest prefix the feed actually publishes for this region,
        and because the premise this whole change removes was that the only
        option was a `/16` -- 65,536 addresses against the 401 the four published
        prefixes total.

        Deliberately **not** a "must be globally routable" rule, which is what
        the design called for and what this tree cannot have. Every real prefix
        is routable, and the identifier guard refuses routable literals in any
        file it can see, so a global-only rule would make the sealed set
        impossible to test without committing the very values the guard exists to
        keep out. What is refused instead is every address kind that could never
        be a provider's egress source, which leaves RFC5737 documentation space
        -- the range the tests are obliged to use -- available and says nothing
        untrue.
        """

        if value is None:
            return None
        if not value:
            raise ValueError("serverless_egress_cidrs cannot be empty")
        if len(set(value)) != len(value):
            raise ValueError("serverless_egress_cidrs must be distinct")
        for entry in value:
            try:
                network = ipaddress.ip_network(entry, strict=True)
            except ValueError as exc:
                raise ValueError(
                    f"serverless_egress_cidrs entries must be exact IPv4 CIDRs; got {entry!r}"
                ) from exc
            if network.version != 4:
                raise ValueError(
                    f"serverless_egress_cidrs must be IPv4; got {entry!r}"
                )
            if network.prefixlen < SERVERLESS_EGRESS_MIN_PREFIXLEN:
                raise ValueError(
                    f"serverless_egress_cidrs refuses anything wider than a "
                    f"/{SERVERLESS_EGRESS_MIN_PREFIXLEN} in front of a live database; "
                    f"got {entry!r}"
                )
            if (
                network.is_loopback
                or network.is_multicast
                or network.is_link_local
                or network.is_unspecified
            ):
                raise ValueError(
                    f"serverless_egress_cidrs entries must be addresses something "
                    f"can egress from; got {entry!r}"
                )
        return value

    @field_validator("runtime_role_trusted_principal_arns")
    @classmethod
    def require_distinct_runtime_principals(
        cls, value: tuple[str, ...] | None
    ) -> tuple[str, ...] | None:
        if value is None:
            return None
        if not value:
            raise ValueError("runtime_role_trusted_principal_arns cannot be empty")
        if len(set(value)) != len(value):
            raise ValueError("runtime_role_trusted_principal_arns must be distinct")
        pattern = re.compile(r"^arn:[^:]+:iam::\d{12}:(?:role|user)/.+$")
        if any(pattern.fullmatch(arn) is None for arn in value):
            raise ValueError(
                "runtime_role_trusted_principal_arns must be exact IAM role or user ARNs"
            )
        return value

    @model_validator(mode="after")
    def require_runtime_role_seal_is_whole(self) -> AwsManifest:
        if (self.runtime_role_arn is None) != (self.runtime_role_trusted_principal_arns is None):
            raise ValueError(
                "runtime_role_arn and runtime_role_trusted_principal_arns are sealed together "
                "or not at all"
            )
        return self

    @model_validator(mode="after")
    def require_serverless_egress_seal_is_whole(self) -> AwsManifest:
        """The prefixes and the snapshot they came from are one seal, or neither.

        Same shape as `require_runtime_role_seal_is_whole` above, and for a
        sharper reason. A list with no timestamp cannot be aged, so the drift
        observer has nothing to compare and the re-poll obligation silently stops
        existing -- which is the exact failure mode the observer was added to
        prevent. A timestamp with no list claims a seal that admits nobody.
        """

        if (self.serverless_egress_cidrs is None) != (
            self.serverless_egress_published_at is None
        ):
            raise ValueError(
                "serverless_egress_cidrs and serverless_egress_published_at are sealed "
                "together or not at all"
            )
        return self


class DatabricksManifest(BaseModel):
    profile: str
    project_id: str
    endpoint_name: str
    coordination_endpoint_name: str = ""
    database: str = "anti_demo"
    user: str = ""
    workspace_ownership: Literal["adopted"] = "adopted"
    project_ownership: Literal["owned"] = "owned"


_INSTALLATION_ID_PATTERN = (
    r"^(?:[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-"
    r"[0-9a-f]{12}|[0-9A-HJKMNP-TV-Z]{26})$"
)
_ROUND_IDS = tuple(RoundId)
_ROUND_NUMBER_IDS = dict(enumerate(_ROUND_IDS, start=1))


class LakebaseEnvironmentSeal(BaseModel):
    """Immutable identity of one owned production Lakebase endpoint."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    project_id: str = Field(pattern=r"^[a-z][a-z0-9-]{2,62}$")
    project_uid: str = Field(min_length=1)
    branch_name: str = Field(min_length=1)
    branch_uid: str = Field(min_length=1)
    endpoint_name: str = Field(min_length=1)
    endpoint_uid: str = Field(min_length=1)
    direct_host: str = Field(min_length=1, max_length=253)
    pooled_host: str = Field(min_length=1, max_length=253)

    @field_validator("direct_host", "pooled_host")
    @classmethod
    def require_dns_host(cls, value: str) -> str:
        labels = value.split(".")
        if len(labels) < 2 or any(
            re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?", label) is None
            for label in labels
        ):
            raise ValueError("environment host must be an exact returned DNS name")
        return value

    @model_validator(mode="after")
    def require_production_primary_endpoint(self) -> LakebaseEnvironmentSeal:
        expected_branch = f"projects/{self.project_id}/branches/production"
        if self.branch_name != expected_branch:
            raise ValueError("Lakebase environment must seal its production branch")
        if self.endpoint_name != f"{expected_branch}/endpoints/primary":
            raise ValueError("Lakebase environment must seal its primary production endpoint")
        return self


class AuroraEnvironmentSeal(BaseModel):
    """Immutable identity of one dedicated Aurora environment."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    cluster_id: str = Field(min_length=1)
    cluster_resource_id: str = Field(min_length=1)
    writer_instance_id: str = Field(min_length=1)
    direct_host: str = Field(min_length=1, max_length=253)
    secret_arn: str = Field(pattern=r"^arn:[^:]+:secretsmanager:[^:]+:\d{12}:secret:[^:]+$")
    security_group_id: str = Field(pattern=r"^sg-[0-9a-f]{8,17}$")
    db_subnet_group_name: str = Field(min_length=1)

    _require_dns_host = field_validator("direct_host")(
        LakebaseEnvironmentSeal.require_dns_host.__func__
    )


class RdsEnvironmentSeal(BaseModel):
    """Immutable identity of one dedicated RDS PostgreSQL environment."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    instance_id: str = Field(min_length=1)
    resource_id: str = Field(min_length=1)
    direct_host: str = Field(min_length=1, max_length=253)
    secret_arn: str = Field(pattern=r"^arn:[^:]+:secretsmanager:[^:]+:\d{12}:secret:[^:]+$")
    security_group_id: str = Field(pattern=r"^sg-[0-9a-f]{8,17}$")
    db_subnet_group_name: str = Field(min_length=1)

    _require_dns_host = field_validator("direct_host")(
        LakebaseEnvironmentSeal.require_dns_host.__func__
    )


class RoundEnvironmentSeal(BaseModel):
    """All exact provider identities measured by one round."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    lakebase: LakebaseEnvironmentSeal
    aurora: AuroraEnvironmentSeal | None = None
    rds: RdsEnvironmentSeal | None = None


class Round3AnchorLane(BaseModel):
    provider: Literal["lakebase", "aurora", "rds"]
    source_id: str
    recovery_at: datetime


class Round3Anchor(BaseModel):
    run_id: str
    owner: str
    aws_account_id: str
    aws_region: str
    contract_sha256: str
    schema_sha256: str
    last_reset_at: datetime
    lakebase: Round3AnchorLane
    aurora: Round3AnchorLane
    rds: Round3AnchorLane


class Round4Resources(BaseModel):
    """Sealed identifiers for the owned Round 4 continuous-sync proof."""

    warehouse_id: str = Field(min_length=1)
    setup_principal: str = Field(min_length=1)
    app_service_principal_client_id: str = Field(min_length=1)
    source_table_full_name: str = Field(min_length=1)
    storage_catalog: str = Field(min_length=1)
    storage_schema: str = Field(min_length=1)
    synced_table_id: str = Field(min_length=1)
    synced_table_resource_name: str = Field(min_length=1)
    synced_table_uid: str = Field(min_length=1)
    pipeline_id: str = Field(min_length=1)
    physical_database: str = Field(min_length=1)
    physical_schema: str = Field(min_length=1)
    physical_table: str = Field(min_length=1)
    project_uid: str = Field(min_length=1)
    branch_uid: str = Field(min_length=1)
    branch: str = Field(min_length=1)
    endpoint_name: str = Field(min_length=1)
    contract_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class Round6Resources(BaseModel):
    """Sealed identifiers for the native Lakebase CDF live-order proof."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    warehouse_id: str = Field(min_length=1)
    setup_principal: str = Field(min_length=1)
    app_service_principal_client_id: str = Field(min_length=1)
    branch_name: str = Field(min_length=1)
    branch_id: str = Field(pattern=r"^[a-z][a-z0-9-]{0,62}$")
    branch_uid: str = Field(min_length=1)
    branch_create_time: datetime
    endpoint_name: str = Field(min_length=1)
    endpoint_id: Literal["primary"] = "primary"
    endpoint_uid: str = Field(min_length=1)
    endpoint_create_time: datetime
    database_resource_name: str = Field(min_length=1)
    database_resource_id: Literal["databricks-postgres"] = "databricks-postgres"
    postgres_database: Literal["databricks_postgres"] = "databricks_postgres"
    source_schema: str = Field(pattern=r"^[a-z][a-z0-9_]{0,62}$")
    source_table: Literal["live_orders"] = "live_orders"
    source_table_oid: int = Field(gt=0)
    cdf_config_name: str = Field(min_length=1)
    cdf_config_id: str = Field(pattern=r"^[a-z][a-z0-9_]{0,62}$")
    cdf_config_create_time: datetime
    cdf_status_name: str = Field(min_length=1)
    cdf_status_id: Literal["live_orders"] = "live_orders"
    cdf_status_create_time: datetime
    destination_catalog: str = Field(min_length=1)
    destination_schema: str = Field(pattern=r"^[a-z][a-z0-9_]{0,62}$")
    destination_schema_id: str = Field(min_length=1)
    destination_table_full_name: str = Field(min_length=1)
    destination_table_id: str = Field(min_length=1)
    baseline_order_id: Literal["00000000-0000-4000-8000-000000000006"] = (
        "00000000-0000-4000-8000-000000000006"
    )
    baseline_proof_nonce: Literal["round6-baseline"] = "round6-baseline"
    baseline_sku: Literal["RED-GLOVE"] = "RED-GLOVE"
    baseline_store: Literal["CHICAGO"] = "CHICAGO"
    baseline_quantity: Literal[1] = 1
    baseline_total_cents: Literal[8450] = 8450
    baseline_status: Literal["baseline"] = "baseline"
    contract_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator(
        "branch_create_time",
        "endpoint_create_time",
        "cdf_config_create_time",
        "cdf_status_create_time",
    )
    @classmethod
    def require_aware_create_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Round 6 create times must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def require_exact_resource_contract(self) -> Round6Resources:
        if self.branch_name != self.endpoint_name.rsplit("/endpoints/", 1)[0]:
            raise ValueError("Round 6 endpoint is outside its sealed branch")
        if self.branch_name.rsplit("/branches/", 1)[-1] != self.branch_id:
            raise ValueError("Round 6 branch name does not match its sealed ID")
        if self.endpoint_name.rsplit("/endpoints/", 1)[-1] != self.endpoint_id:
            raise ValueError("Round 6 endpoint name does not match its sealed ID")
        expected_database = self.branch_name
        expected_database = f"{expected_database}/databases/{self.database_resource_id}"
        if self.database_resource_name != expected_database:
            raise ValueError("Round 6 database resource name is not the default SQL database")
        expected_config = f"{self.database_resource_name}/cdf-configs/{self.cdf_config_id}"
        if self.cdf_config_name != expected_config:
            raise ValueError("Round 6 CDF config name does not match its sealed ID")
        expected_status = f"{self.cdf_config_name}/cdf-statuses/{self.cdf_status_id}"
        if self.cdf_status_name != expected_status:
            raise ValueError("Round 6 CDF status name does not match its sealed ID")
        expected_table = (
            f"{self.destination_catalog}.{self.destination_schema}."
            f"{self.destination_table_full_name.rsplit('.', 1)[-1]}"
        )
        if self.destination_table_full_name != expected_table:
            raise ValueError("Round 6 history table is outside its sealed schema")
        expected_hash = round6_contract_sha256(
            branch_name=self.branch_name,
            branch_id=self.branch_id,
            branch_uid=self.branch_uid,
            branch_create_time=self.branch_create_time.isoformat(),
            endpoint_name=self.endpoint_name,
            endpoint_id=self.endpoint_id,
            endpoint_uid=self.endpoint_uid,
            endpoint_create_time=self.endpoint_create_time.isoformat(),
            database_resource_name=self.database_resource_name,
            database_resource_id=self.database_resource_id,
            postgres_database=self.postgres_database,
            source_schema=self.source_schema,
            source_table=self.source_table,
            source_table_oid=self.source_table_oid,
            cdf_config_name=self.cdf_config_name,
            cdf_config_id=self.cdf_config_id,
            cdf_config_create_time=self.cdf_config_create_time.isoformat(),
            cdf_status_name=self.cdf_status_name,
            cdf_status_id=self.cdf_status_id,
            cdf_status_create_time=self.cdf_status_create_time.isoformat(),
            destination_catalog=self.destination_catalog,
            destination_schema=self.destination_schema,
            destination_schema_id=self.destination_schema_id,
            destination_table_full_name=self.destination_table_full_name,
            destination_table_id=self.destination_table_id,
            baseline_order_id=self.baseline_order_id,
            baseline_proof_nonce=self.baseline_proof_nonce,
            baseline_sku=self.baseline_sku,
            baseline_store=self.baseline_store,
            baseline_quantity=self.baseline_quantity,
            baseline_total_cents=self.baseline_total_cents,
            baseline_status=self.baseline_status,
        )
        if self.contract_sha256 != expected_hash:
            raise ValueError("Round 6 contract hash does not match the sealed resources")
        return self


class Round5FrozenConstants(BaseModel):
    """Reviewed constants that define the fair Round 5 burst."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    runner_instance_type: Literal["m6i.large"] = "m6i.large"
    warmup_attempts_per_lane: Literal[4] = 4
    scored_attempts_per_lane: Literal[128] = 128
    max_concurrent_attempts_per_lane: Literal[64] = 64
    witness_clients_per_lane: Literal[64] = 64
    witness_transaction_concurrency_per_lane: Literal[8] = 8
    python_version: Literal["3.12"] = "3.12"
    psycopg_version: Literal["3.3.4"] = "3.3.4"
    connect_timeout_seconds: Literal[10] = 10
    attempt_timeout_seconds: Literal[20] = 20
    ssm_timeout_seconds: Literal[120] = 120
    settlement_reserve_seconds: Literal[10] = 10
    max_launch_skew_ms: Literal[10] = 10
    tls_mode: Literal["verify-full"] = "verify-full"
    psycopg_preparation: Literal["disabled"] = "disabled"
    rds_proxy_max_connections_percent: Literal[90] = 90
    rds_proxy_borrow_timeout_seconds: Literal[120] = 120


class LegacyRound5Resources(BaseModel):
    """The retired v3 pre-provisioned topology, retained only for safe loading."""

    model_config = ConfigDict(extra="forbid")

    lakebase_direct_host: str = Field(min_length=1, max_length=253)
    lakebase_pooled_host: str = Field(
        min_length=1,
        max_length=253,
        description="Exact status.hosts.read_write_pooled_host returned by Lakebase",
    )
    rds_direct_host: str = Field(
        min_length=1,
        max_length=253,
        description="Exact RDS Endpoint.Address used only for witness and cleanup",
    )
    rds_proxy_endpoint: str = Field(min_length=1, max_length=253)
    rds_proxy_name: str = Field(min_length=1)
    rds_proxy_arn: str = Field(pattern=r"^arn:[^:]+:rds:[^:]+:\d{12}:db-proxy:[^:]+$")
    rds_proxy_target: str = Field(min_length=1)
    lakebase_secret_arn: str = Field(
        pattern=r"^arn:[^:]+:secretsmanager:[^:]+:\d{12}:secret:[^:]+$"
    )
    rds_proxy_secret_arn: str = Field(
        pattern=r"^arn:[^:]+:secretsmanager:[^:]+:\d{12}:secret:[^:]+$"
    )
    rds_proxy_role_arn: str = Field(pattern=r"^arn:[^:]+:iam::\d{12}:role/.+$")
    execution_role_arn: str = Field(pattern=r"^arn:[^:]+:iam::\d{12}:role/.+$")
    runner_instance_id: str = Field(pattern=r"^i-[0-9a-f]{8,17}$")
    runner_instance_profile_arn: str = Field(pattern=r"^arn:[^:]+:iam::\d{12}:instance-profile/.+$")
    runner_subnet_id: str = Field(pattern=r"^subnet-[0-9a-f]{8,17}$")
    runner_security_group_id: str = Field(pattern=r"^sg-[0-9a-f]{8,17}$")
    native_role: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
    probe_identity: Literal["public.anti_demo_probe"]
    ssm_document_name: Literal["AWS-RunShellScript"] = "AWS-RunShellScript"
    runner_path: Literal["/opt/lakebase-anti-demo/round5/run_connection_spike.sh"] = (
        "/opt/lakebase-anti-demo/round5/run_connection_spike.sh"
    )
    trust_bundle_path: Literal["/opt/lakebase-anti-demo/round5/round5-ca.pem"] = (
        "/opt/lakebase-anti-demo/round5/round5-ca.pem"
    )
    trust_bundle_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    harness_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    config_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    frozen_constants: Round5FrozenConstants
    contract_sha256: Literal["f0e9a6960fb22cc052486b62cf01e32dcaabacf70508a2bc087ddc25deafa81c"]

    @field_validator(
        "lakebase_direct_host",
        "lakebase_pooled_host",
        "rds_direct_host",
        "rds_proxy_endpoint",
    )
    @classmethod
    def require_returned_dns_host(cls, value: str) -> str:
        labels = value.split(".")
        if len(labels) < 2 or any(
            not label
            or len(label) > 63
            or not label[0].isalnum()
            or not label[-1].isalnum()
            or any(not character.isalnum() and character != "-" for character in label)
            for label in labels
        ):
            raise ValueError("must be an exact DNS host without a scheme, port, or path")
        return value

    @property
    def runner_harness_sha256(self) -> str:
        """Compatibility spelling used by the lazy runner factory."""
        return self.harness_sha256


class Round5OwnershipTags(BaseModel):
    """Canonical immutable tags applied to every API-created bout resource."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    anti_demo_run_id: str = Field(min_length=1)
    owner: str = Field(min_length=1)
    expires_at: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
    # V7 installations add a collision-resistant installation-and-round scope
    # to every static and per-bout AWS resource.  Both fields stay optional so
    # already-sealed V4-V6 manifests retain their canonical hashes.
    anti_demo_installation_slug: str | None = Field(
        default=None,
        pattern=r"^i[0-9a-f]{20}-r5$",
        exclude_if=lambda value: value is None,
    )
    anti_demo_round: Literal["r5"] | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )

    @model_validator(mode="after")
    def require_complete_installation_scope(self) -> Round5OwnershipTags:
        if (self.anti_demo_installation_slug is None) != (self.anti_demo_round is None):
            raise ValueError("Round 5 installation ownership scope is incomplete")
        return self

    def as_aws_tags(self, *, bout_id: str | None = None) -> dict[str, str]:
        tags = {
            "anti-demo-run-id": self.anti_demo_run_id,
            "Owner": self.owner,
            "owner": self.owner,
            "expires-at": self.expires_at,
            "managed-by": "round5-lifecycle",
        }
        if self.anti_demo_installation_slug is not None:
            tags["anti-demo-installation-slug"] = self.anti_demo_installation_slug
            tags["anti-demo-round"] = self.anti_demo_round
        if bout_id:
            tags["anti-demo-bout-id"] = bout_id
        return tags


class Round5Resources(BaseModel):
    """Secret-free Round 5 seal for the clean, Terraform-owned baseline."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    lakebase_direct_host: str = Field(min_length=1, max_length=253)
    lakebase_pooled_host: str = Field(min_length=1, max_length=253)
    # These nullable defaults are a one-way migration bridge for v4 manifests
    # sealed before Aurora was a selectable Round 5 baseline.  A complete seal
    # requires every Aurora field; the all-None legacy shape can only be loaded
    # with its original canonical hash and is not factory-ready.
    aurora_direct_host: str | None = Field(default=None, min_length=1, max_length=253)
    aurora_cluster_id: str | None = Field(default=None, min_length=1)
    aurora_cluster_resource_id: str | None = Field(default=None, min_length=1)
    aurora_writer_instance_id: str | None = Field(default=None, min_length=1)
    aurora_master_secret_arn: str | None = Field(
        default=None,
        pattern=r"^arn:[^:]+:secretsmanager:[^:]+:\d{12}:secret:[^:]+$",
    )
    rds_direct_host: str = Field(min_length=1, max_length=253)
    rds_master_secret_arn: str = Field(
        pattern=r"^arn:[^:]+:secretsmanager:[^:]+:\d{12}:secret:[^:]+$"
    )
    rds_resource_id: str = Field(min_length=1)
    vpc_id: str = Field(pattern=r"^vpc-[0-9a-f]{8,17}$")
    proxy_subnet_ids: tuple[str, ...] = Field(min_length=2)
    control_role_arn: str = Field(pattern=r"^arn:[^:]+:iam::\d{12}:role/.+$")
    control_role_trusted_principal_arn: str = Field(
        pattern=r"^arn:[^:]+:iam::\d{12}:(?:role|user)/.+$"
    )
    proxy_service_role_arn: str | None = Field(
        default=None, pattern=r"^arn:[^:]+:iam::\d{12}:role/.+$"
    )
    proxy_service_policy_name: str | None = Field(default=None, min_length=1, max_length=128)
    aurora_proxy_secret_arn: str | None = Field(
        default=None,
        pattern=r"^arn:[^:]+:secretsmanager:[^:]+:\d{12}:secret:[^:]+$",
    )
    rds_proxy_secret_arn: str | None = Field(
        default=None,
        pattern=r"^arn:[^:]+:secretsmanager:[^:]+:\d{12}:secret:[^:]+$",
    )
    # Retained only so an already-sealed v4 manifest can be loaded and safely
    # migrated. The factory-ready static topology does not use this boundary.
    per_bout_role_boundary_arn: str | None = Field(
        default=None, pattern=r"^arn:[^:]+:iam::\d{12}:policy/.+$"
    )
    runner_permissions_boundary_arn: str = Field(pattern=r"^arn:[^:]+:iam::\d{12}:policy/.+$")
    runner_instance_id: str = Field(pattern=r"^i-[0-9a-f]{8,17}$")
    runner_instance_profile_arn: str = Field(pattern=r"^arn:[^:]+:iam::\d{12}:instance-profile/.+$")
    runner_role_arn: str = Field(pattern=r"^arn:[^:]+:iam::\d{12}:role/.+$")
    runner_subnet_id: str = Field(pattern=r"^subnet-[0-9a-f]{8,17}$")
    runner_security_group_id: str = Field(pattern=r"^sg-[0-9a-f]{8,17}$")
    runner_egress_rule_id: str = Field(pattern=r"^sgr-[0-9a-f]{8,17}$")
    runner_public_key_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    lakebase_credential_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    aurora_credential_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    rds_credential_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    bout_name_prefix: str = Field(min_length=1, max_length=48)
    # Legacy per-bout secret discovery prefix, load-compatible for migration.
    secret_name_prefix: str | None = Field(default=None, min_length=1, max_length=128)
    ownership_tags: Round5OwnershipTags
    credential_root: Literal["/var/lib/lakebase-anti-demo/credentials"] = (
        "/var/lib/lakebase-anti-demo/credentials"
    )
    journal_table: Literal["anti_demo_coordination.round5_creation_journal"] = (
        "anti_demo_coordination.round5_creation_journal"
    )
    native_role: Literal["anti_demo_burst"] = "anti_demo_burst"
    probe_identity: Literal["public.anti_demo_probe"] = "public.anti_demo_probe"
    ssm_document_name: Literal["AWS-RunShellScript"] = "AWS-RunShellScript"
    runner_path: Literal["/opt/lakebase-anti-demo/round5/run_connection_spike.sh"] = (
        "/opt/lakebase-anti-demo/round5/run_connection_spike.sh"
    )
    trust_bundle_path: Literal["/opt/lakebase-anti-demo/round5/round5-ca.pem"] = (
        "/opt/lakebase-anti-demo/round5/round5-ca.pem"
    )
    trust_bundle_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    harness_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    config_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    baseline_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    frozen_constants: Round5FrozenConstants
    contract_sha256: Literal["f0e9a6960fb22cc052486b62cf01e32dcaabacf70508a2bc087ddc25deafa81c"]

    @field_validator(
        "lakebase_direct_host",
        "lakebase_pooled_host",
        "aurora_direct_host",
        "rds_direct_host",
    )
    @classmethod
    def require_returned_dns_host(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return LegacyRound5Resources.require_returned_dns_host(value)

    @property
    def aurora_baseline_ready(self) -> bool:
        return all(
            getattr(self, field, None) is not None
            for field in (
                "aurora_direct_host",
                "aurora_cluster_id",
                "aurora_cluster_resource_id",
                "aurora_writer_instance_id",
                "aurora_master_secret_arn",
                "aurora_credential_sha256",
            )
        )

    @property
    def factory_ready(self) -> bool:
        return self.aurora_baseline_ready and all(
            getattr(self, field, None) is not None
            for field in (
                "proxy_service_role_arn",
                "proxy_service_policy_name",
                "aurora_proxy_secret_arn",
                "rds_proxy_secret_arn",
            )
        )

    @field_validator("proxy_subnet_ids")
    @classmethod
    def require_exact_proxy_subnets(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(value)) != len(value) or any(
            re.fullmatch(r"subnet-[0-9a-f]{8,17}", subnet) is None for subnet in value
        ):
            raise ValueError("proxy_subnet_ids must contain distinct exact subnet IDs")
        return value

    @property
    def runner_harness_sha256(self) -> str:
        return self.harness_sha256

    @property
    def runner_trust_policy(self) -> dict[str, object]:
        """The complete, fixed trust document for the neutral EC2 runner."""
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "ec2.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }

    @property
    def proxy_service_trust_policy(self) -> dict[str, object]:
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "rds.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }

    @property
    def control_trust_policy(self) -> dict[str, object]:
        """The complete trust document sealed by the app principal identity."""
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": self.control_role_trusted_principal_arn,
                    },
                    "Action": "sts:AssumeRole",
                }
            ],
        }

    @model_validator(mode="after")
    def require_canonical_hashes(self) -> Round5Resources:
        aurora_fields = {
            "aurora_direct_host",
            "aurora_cluster_id",
            "aurora_cluster_resource_id",
            "aurora_writer_instance_id",
            "aurora_master_secret_arn",
            "aurora_credential_sha256",
        }
        aurora_values = [getattr(self, field) for field in aurora_fields]
        if any(value is not None for value in aurora_values) and not self.aurora_baseline_ready:
            raise ValueError("Aurora clean baseline seal is incomplete")
        payload = self.model_dump(
            mode="json",
            exclude={"baseline_sha256", "config_sha256"},
            exclude_none=True,
        )
        expected_baseline = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        if self.baseline_sha256 != expected_baseline:
            raise ValueError("baseline_sha256 does not match the clean baseline")
        expected_config = hashlib.sha256(
            json.dumps(
                {
                    "baseline_sha256": expected_baseline,
                    "contract_sha256": self.contract_sha256,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        if self.config_sha256 != expected_config:
            raise ValueError("config_sha256 does not match the clean baseline")
        return self


class DemoManifest(BaseModel):
    manifest_version: Literal[1, 2, 3, 4, 5, 6, 7] = 1
    installation_id: str | None = Field(
        default=None,
        frozen=True,
        pattern=_INSTALLATION_ID_PATTERN,
    )
    run_id: str
    owner: str
    created_at: datetime
    expires_at: datetime
    status: Literal["provisioning", "seeding", "waiting_for_zero", "ready", "cleanup_failed"]
    aws: AwsManifest
    databricks: DatabricksManifest
    prior_databricks: list[DatabricksManifest] = Field(default_factory=list)
    schema_sha256: str = ""
    last_reset_at: datetime | None = None
    round3_anchor: Round3Anchor | None = None
    round4: Round4Resources | None = None
    round5: Round5Resources | LegacyRound5Resources | None = None
    round6: Round6Resources | None = None
    round_environments: dict[RoundId, RoundEnvironmentSeal] | None = None
    coordination_environment: LakebaseEnvironmentSeal | None = None

    @model_validator(mode="after")
    def require_versioned_resources(self) -> DemoManifest:
        if self.manifest_version in (2, 3, 4, 5, 6, 7) and self.round4 is None:
            raise ValueError(
                f"manifest_version {self.manifest_version} requires sealed Round 4 resources"
            )
        if self.manifest_version == 3 and self.round5 is None:
            raise ValueError("manifest_version 3 requires sealed Round 5 resources")
        if self.manifest_version == 3 and not isinstance(self.round5, LegacyRound5Resources):
            raise ValueError("manifest_version 3 requires the legacy Round 5 seal")
        if self.manifest_version == 4 and not isinstance(self.round5, Round5Resources):
            raise ValueError("manifest_version 4 requires the clean Round 5 baseline seal")
        if self.manifest_version in (5, 6, 7) and not isinstance(self.round5, Round5Resources):
            raise ValueError(
                f"manifest_version {self.manifest_version} requires the static Round 5 "
                "baseline seal"
            )
        if (
            self.manifest_version in (5, 6, 7)
            and isinstance(self.round5, Round5Resources)
            and not self.round5.factory_ready
        ):
            raise ValueError(
                f"manifest_version {self.manifest_version} requires the factory-ready static seal"
            )
        if self.manifest_version in (6, 7) and self.round6 is None:
            raise ValueError(
                f"manifest_version {self.manifest_version} requires sealed Round 6 resources"
            )
        if self.manifest_version < 6 and self.round6 is not None:
            raise ValueError("sealed Round 6 resources require manifest_version 6")
        self._validate_environment_seals()
        return self

    def _validate_environment_seals(self) -> None:
        environments = self.round_environments
        if environments is None:
            if self.manifest_version == 7:
                raise ValueError("manifest_version 7 requires exactly six round environments")
            if (
                self.coordination_environment is not None
                and self.coordination_environment.endpoint_name
                != self.databricks.coordination_endpoint_name
            ):
                raise ValueError("coordination_environment must match coordination_endpoint_name")
            return

        expected = set(_ROUND_IDS)
        actual = set(environments)
        if self.manifest_version == 7 and actual != expected:
            missing = sorted(round_id.value for round_id in expected - actual)
            extra = sorted(str(round_id) for round_id in actual - expected)
            raise ValueError(
                "round_environments must contain exactly the six canonical RoundId keys; "
                f"missing={missing}, extra={extra}"
            )

        if self.manifest_version == 7:
            if self.installation_id is None:
                raise ValueError("manifest_version 7 requires a full installation_id")
            if self.coordination_environment is None:
                raise ValueError("manifest_version 7 requires a coordination environment")

        if self.manifest_version == 7:
            aws_rounds = {
                RoundId.WAKE_IDLE_APP,
                RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
                RoundId.RECOVER_DELETED_ORDER,
                RoundId.SURVIVE_CONNECTION_SPIKE,
            }
            lakebase_only_rounds = {
                RoundId.PUT_MODEL_SCORE_IN_APP,
                RoundId.ANALYZE_LIVE_ORDERS,
            }
            for round_id in aws_rounds:
                sealed = environments[round_id]
                if sealed.aurora is None:
                    raise ValueError(f"{round_id.value} requires a dedicated Aurora seal")
                # Round 1 stands up no RDS instance: its RDS lane refuses to enter
                # on engine semantics and is never timed, so there is nothing for
                # it to seal. Only the rounds whose RDS lane is actually raced owe
                # a seal. An r1 seal that is still present stays loadable, which is
                # what lets a pre-deletion manifest be read in order to reseal it.
                if sealed.rds is None and rds_lane_is_scored(round_id):
                    raise ValueError(f"{round_id.value} requires a dedicated RDS seal")
            for round_id in lakebase_only_rounds:
                sealed = environments[round_id]
                if sealed.aurora is not None or sealed.rds is not None:
                    raise ValueError(f"{round_id.value} must not seal unused AWS databases")

        self._require_unique_round_identity(
            "Lakebase project", [item.lakebase.project_id for item in environments.values()]
        )
        self._require_unique_round_identity(
            "Lakebase project UID",
            [item.lakebase.project_uid for item in environments.values()],
        )
        self._require_unique_round_identity(
            "Lakebase endpoint",
            [item.lakebase.endpoint_name for item in environments.values()],
        )
        self._require_unique_round_identity(
            "Lakebase endpoint UID",
            [item.lakebase.endpoint_uid for item in environments.values()],
        )
        self._require_unique_round_identity(
            "Lakebase direct host",
            [item.lakebase.direct_host for item in environments.values()],
        )
        self._require_unique_round_identity(
            "Lakebase pooled host",
            [item.lakebase.pooled_host for item in environments.values()],
        )
        aurora = [item.aurora for item in environments.values() if item.aurora is not None]
        rds = [item.rds for item in environments.values() if item.rds is not None]
        for label, values in (
            ("Aurora cluster", [item.cluster_id for item in aurora]),
            ("Aurora cluster resource", [item.cluster_resource_id for item in aurora]),
            ("Aurora writer", [item.writer_instance_id for item in aurora]),
            ("Aurora host", [item.direct_host for item in aurora]),
            ("RDS instance", [item.instance_id for item in rds]),
            ("RDS resource", [item.resource_id for item in rds]),
            ("RDS host", [item.direct_host for item in rds]),
        ):
            self._require_unique_round_identity(label, values)

        coordination = self.coordination_environment
        if coordination is not None:
            if coordination.endpoint_name != self.databricks.coordination_endpoint_name:
                raise ValueError("coordination_environment must match coordination_endpoint_name")
            if coordination.project_id in {
                item.lakebase.project_id for item in environments.values()
            }:
                raise ValueError("coordination project aliases a measured round project")
            if coordination.endpoint_name in {
                item.lakebase.endpoint_name for item in environments.values()
            }:
                raise ValueError("coordination endpoint aliases a measured round endpoint")

        if self.manifest_version == 7:
            self._require_round1_legacy_mirror(environments[RoundId.WAKE_IDLE_APP])
            self._require_adapter_seal_matches(environments)

    @staticmethod
    def _require_unique_round_identity(label: str, values: list[str]) -> None:
        if len(set(values)) != len(values):
            raise ValueError(f"cross-round alias of {label} is forbidden")

    def _require_round1_legacy_mirror(self, sealed: RoundEnvironmentSeal) -> None:
        if self.databricks.project_id != sealed.lakebase.project_id:
            raise ValueError("legacy Databricks project_id is not the Round 1 mirror")
        if self.databricks.endpoint_name != sealed.lakebase.endpoint_name:
            raise ValueError("legacy Databricks endpoint_name is not the Round 1 mirror")
        assert sealed.aurora is not None
        resources = self.aws.resources
        expected = {
            "aurora_cluster_id": sealed.aurora.cluster_id,
            "aurora_writer_instance_id": sealed.aurora.writer_instance_id,
            "aurora_secret_arn": sealed.aurora.secret_arn,
            "security_group_id": sealed.aurora.security_group_id,
            "db_subnet_group_name": sealed.aurora.db_subnet_group_name,
        }
        if sealed.rds is not None:
            expected.update(
                {
                    "rds_instance_id": sealed.rds.instance_id,
                    "rds_secret_arn": sealed.rds.secret_arn,
                    "rds_security_group_id": sealed.rds.security_group_id,
                }
            )
        for field, value in expected.items():
            if getattr(resources, field) != value:
                raise ValueError(f"legacy AWS {field} is not the Round 1 mirror")
        if sealed.rds is None:
            # Round 1 stands no RDS instance up, so the legacy mirror fields must
            # be empty rather than borrowed from another round. Aliasing r2's
            # instance into a field named for r1 would make the seal describe a
            # resource Round 1 does not have, and the arming path would then try
            # to describe it.
            for field in ("rds_instance_id", "rds_secret_arn", "rds_security_group_id"):
                if getattr(resources, field, ""):
                    raise ValueError(f"legacy AWS {field} is set but Round 1 seals no RDS instance")
            return
        if sealed.rds.db_subnet_group_name != sealed.aurora.db_subnet_group_name:
            raise ValueError("Round 1 AWS mirrors disagree on the DB subnet group")

    def _require_adapter_seal_matches(
        self, environments: dict[RoundId, RoundEnvironmentSeal]
    ) -> None:
        assert self.round4 is not None and isinstance(self.round5, Round5Resources)
        assert self.round6 is not None
        round4 = environments[RoundId.PUT_MODEL_SCORE_IN_APP].lakebase
        if (
            self.round4.project_uid != round4.project_uid
            or self.round4.branch_uid != round4.branch_uid
            or self.round4.branch != round4.branch_name
            or self.round4.endpoint_name != round4.endpoint_name
        ):
            raise ValueError("Round 4 adapter identities do not match its environment seal")

        round5 = environments[RoundId.SURVIVE_CONNECTION_SPIKE]
        assert round5.aurora is not None and round5.rds is not None
        if (
            self.round5.lakebase_direct_host != round5.lakebase.direct_host
            or self.round5.lakebase_pooled_host != round5.lakebase.pooled_host
            or self.round5.aurora_direct_host != round5.aurora.direct_host
            or self.round5.aurora_cluster_id != round5.aurora.cluster_id
            or self.round5.aurora_cluster_resource_id != round5.aurora.cluster_resource_id
            or self.round5.aurora_writer_instance_id != round5.aurora.writer_instance_id
            or self.round5.aurora_master_secret_arn != round5.aurora.secret_arn
            or self.round5.rds_direct_host != round5.rds.direct_host
            or self.round5.rds_resource_id != round5.rds.resource_id
            or self.round5.rds_master_secret_arn != round5.rds.secret_arn
        ):
            raise ValueError("Round 5 adapter identities do not match its environment seal")

        round6 = environments[RoundId.ANALYZE_LIVE_ORDERS].lakebase
        if (
            self.round6.branch_name != round6.branch_name
            or self.round6.branch_uid != round6.branch_uid
            or self.round6.endpoint_name != round6.endpoint_name
            or self.round6.endpoint_uid != round6.endpoint_uid
        ):
            raise ValueError("Round 6 adapter identities do not match its environment seal")

    @staticmethod
    def _coerce_round_id(round_id: RoundId | str | int) -> RoundId:
        if isinstance(round_id, int):
            try:
                return _ROUND_NUMBER_IDS[round_id]
            except KeyError as error:
                raise KeyError(f"unknown round number: {round_id}") from error
        return RoundId(round_id)

    def round_environment(self, round_id: RoundId | str | int) -> RoundEnvironmentSeal:
        if self.round_environments is None:
            raise RuntimeError("manifest has no per-round environment seals")
        return self.round_environments[self._coerce_round_id(round_id)]

    def round_lakebase(self, round_id: RoundId | str | int) -> LakebaseEnvironmentSeal:
        return self.round_environment(round_id).lakebase

    @property
    def coordination_lakebase(self) -> LakebaseEnvironmentSeal | None:
        return self.coordination_environment

    @property
    def round5_ready(self) -> bool:
        return (
            self.manifest_version in (5, 6, 7)
            and self.round4 is not None
            and isinstance(self.round5, Round5Resources)
            and self.round5.factory_ready
        )

    def require_round5_resources(self) -> Round5Resources:
        if not self.round5_ready or not isinstance(self.round5, Round5Resources):
            raise RuntimeError("Round 5 requires a complete factory-ready manifest v5")
        return self.round5

    @property
    def round6_ready(self) -> bool:
        return self.manifest_version in (6, 7) and self.round5_ready and self.round6 is not None

    # There is deliberately no `assert_not_expired` here, and re-adding one would
    # re-arm a trap rather than restore a safeguard.
    #
    # It used to exist and raise `RuntimeError` on a passed `expires_at`. Every
    # control path that consulted it did so inside `except (RuntimeError,
    # ValueError): return None`, so a timestamp written once at provision time
    # silently removed capability from a running installation -- Round 5 stopped
    # being able to arm while Rounds 1-4 and 6 carried on, with no log line
    # naming the cause. By the time it was removed it had no production callers
    # at all, and the only thing keeping the method alive was tests asserting it
    # was *not* being called.
    #
    # `expires_at` is an ownership label, not a lease: nothing reaps on it, and it
    # is never re-based, so it cannot distinguish "abandoned" from "in daily use".
    # `expiry_warning` below is the whole supported treatment -- it reports, and
    # the caller carries on. `antidemo renew` moves the timestamp and the AWS tags
    # that mirror it; `antidemo cleanup --yes` is what actually ends the spend.
    def expiry_warning(self) -> str | None:
        """Describe a passed TTL without deciding that anything must stop.

        An expired timestamp carries no information about whether the resources
        are healthy: it is a wall-clock comparison against a value written once
        at provision time.  Callers that need liveness ask the resources, not the
        clock, so control paths warn with this text and continue.  `antidemo renew`
        moves the timestamp forward; `antidemo cleanup --yes` is what ends the spend.
        """
        if self.expires_at > datetime.now(UTC):
            return None
        return (
            f"Demo resources passed their declared expiry at {self.expires_at.isoformat()}. "
            "Nothing reaps that tag, so this is an ownership signal only: run "
            "'antidemo renew --ttl-hours N' to move it forward, or 'antidemo cleanup --yes' "
            "to stop the spend."
        )


def manifest_path() -> Path:
    configured = os.environ.get("ANTI_DEMO_MANIFEST", "")
    if not configured:
        raise RuntimeError(_NO_MANIFEST_SELECTED)
    return Path(configured).expanduser().resolve()


def load_manifest(path: Path | None = None) -> DemoManifest:
    if path is None:
        encoded = os.environ.get(MANIFEST_JSON_ENV)
        if encoded is not None:
            return DemoManifest.model_validate_json(encoded)
    target = path if path is not None else manifest_path()
    if not target.exists():
        raise RuntimeError(f"No owned demo manifest exists at {target}")
    return DemoManifest.model_validate_json(target.read_text(encoding="utf-8"))


def save_manifest(manifest: DemoManifest, path: Path | None = None) -> Path:
    # Assignment is intentionally supported by lifecycle orchestration. Revalidate
    # its final snapshot so an incomplete version upgrade can never reach disk.
    manifest = DemoManifest.model_validate(manifest.model_dump())
    target = path or manifest_path()
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = target.with_suffix(".tmp")
    temporary.write_text(
        manifest.model_dump_json(indent=2, exclude_none=True) + "\n",
        encoding="utf-8",
    )
    temporary.chmod(0o600)
    os.replace(temporary, target)
    return target


def apply_manifest_environment(manifest: DemoManifest) -> None:
    coordination_endpoint = manifest.databricks.coordination_endpoint_name or (
        f"projects/{manifest.databricks.project_id}/branches/coordination/endpoints/primary"
    )
    values = {
        "AWS_AUTH_MODE": manifest.aws.auth_mode,
        "AWS_REGION": manifest.aws.region,
        "AWS_EXPECTED_ACCOUNT_ID": manifest.aws.account_id,
        "DATABRICKS_PROFILE": manifest.databricks.profile,
        "LAKEBASE_ENDPOINT_NAME": manifest.databricks.endpoint_name,
        "LAKEBASE_DATABASE": manifest.databricks.database,
        "LAKEBASE_USER": manifest.databricks.user,
        "LAKEBASE_EXPECTED_REGION": manifest.aws.region,
        "ANTI_DEMO_COORDINATION_ENDPOINT_NAME": coordination_endpoint,
        "ANTI_DEMO_COORDINATION_DATABASE": manifest.databricks.database,
        "ANTI_DEMO_COORDINATION_USER": manifest.databricks.user,
        "AURORA_CLUSTER_ID": manifest.aws.resources.aurora_cluster_id,
        "AURORA_SECRET_ARN": manifest.aws.resources.aurora_secret_arn,
        "AURORA_DATABASE": manifest.databricks.database,
        "RDS_INSTANCE_ID": manifest.aws.resources.rds_instance_id,
        "RDS_SECRET_ARN": manifest.aws.resources.rds_secret_arn,
        "RDS_DATABASE": manifest.databricks.database,
        "EXPECTED_POSTGRES_MAJOR": "17",
    }
    if manifest.aws.auth_mode == "profile":
        values["AWS_PROFILE"] = manifest.aws.profile
    else:
        os.environ.pop("AWS_PROFILE", None)
        os.environ.pop("AWS_DEFAULT_PROFILE", None)
    if "@" in manifest.owner:
        values.update(
            {
                LOCAL_OPERATOR_ENV: manifest.owner.split("@", 1)[0]
                .replace(".", " ")
                .title(),
                LOCAL_OPERATOR_EMAIL_ENV: manifest.owner,
                LOCAL_OPERATOR_ID_ENV: f"local:{manifest.owner.casefold()}",
            }
        )
    for name, value in values.items():
        if value:
            os.environ[name] = value
