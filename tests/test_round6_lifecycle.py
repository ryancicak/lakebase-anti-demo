from __future__ import annotations

import ast
import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from databricks.sdk.errors import NotFound, PermissionDenied, TooManyRequests
from pydantic import ValidationError

from server.manifest import Round6Resources
from server.round6_contract import round6_contract_sha256
from server.round6_lifecycle import (
    ROUND6_BASELINE_NONCE,
    ROUND6_BASELINE_ORDER_ID,
    ROUND6_BASELINE_QUANTITY,
    ROUND6_BASELINE_SKU,
    ROUND6_BASELINE_STATUS,
    ROUND6_BASELINE_STORE,
    ROUND6_BASELINE_TOTAL_CENTS,
    ROUND6_OWNER_PROPERTY,
    ROUND6_SUSPEND_WINDOW,
    _endpoint_contract_findings,
    _ensure_round6_app_role,
    _validate_branch,
    _validate_catalog,
    _validate_database,
    _validate_endpoint,
    _wait_for_cdf_baseline,
    authorize_force_round6,
    check_round6,
    cleanup_round6,
    force_round6_tokens,
    round6_names,
)

CREATED = datetime(2026, 8, 19, 18, 0, tzinfo=UTC)
#: A timestamp a restored or recreated resource would carry: same name, same
#: server-assigned UID, different creation time.
RESTORED = datetime(2026, 8, 20, 9, 30, tzinfo=UTC)


def test_v7_round6_uses_its_dedicated_production_endpoint() -> None:
    endpoint = "projects/install-r6/branches/production/endpoints/primary"
    manifest = SimpleNamespace(
        run_id="ad-test-v7",
        round_environments={"sealed": True},
        round_lakebase=lambda number: SimpleNamespace(endpoint_name=endpoint),
        databricks=SimpleNamespace(
            endpoint_name="projects/legacy/branches/production/endpoints/primary"
        ),
    )

    names = round6_names(manifest)

    assert names["branch_name"] == "projects/install-r6/branches/production"
    assert names["branch_id"] == "production"
    assert names["endpoint_name"] == endpoint
    assert names["uses_production_endpoint"] == "true"


def test_round6_app_role_targets_only_its_source_branch(monkeypatch) -> None:
    calls: list[tuple[object, str, tuple[str, ...], float]] = []
    manifest = SimpleNamespace(databricks=SimpleNamespace(profile="fe-vm-test"))
    names = {"branch_name": "projects/install-r6/branches/production"}

    monkeypatch.setattr(
        "server.lifecycle._ensure_lakebase_app_roles",
        lambda candidate, principal, branches, *, timeout: calls.append(
            (candidate, principal, branches, timeout)
        ),
    )

    _ensure_round6_app_role(
        manifest,
        names,
        "11111111-2222-3333-4444-555555555555",
        timeout=90,
    )

    assert calls == [
        (
            manifest,
            "11111111-2222-3333-4444-555555555555",
            ("projects/install-r6/branches/production",),
            90,
        )
    ]


def round6_values() -> dict[str, object]:
    values: dict[str, object] = {
        "warehouse_id": "warehouse-id",
        "setup_principal": "operator@databricks.com",
        "app_service_principal_client_id": "app-client-id",
        "branch_name": "projects/ad-test-v3/branches/round6-ad-test-v3",
        "branch_id": "round6-ad-test-v3",
        "branch_uid": "branch-uid",
        "branch_create_time": CREATED,
        "endpoint_name": ("projects/ad-test-v3/branches/round6-ad-test-v3/endpoints/primary"),
        "endpoint_id": "primary",
        "endpoint_uid": "endpoint-uid",
        "endpoint_create_time": CREATED,
        "database_resource_name": (
            "projects/ad-test-v3/branches/round6-ad-test-v3/databases/databricks-postgres"
        ),
        "database_resource_id": "databricks-postgres",
        "postgres_database": "databricks_postgres",
        "source_schema": "anti_demo_r6_ad_test_v3",
        "source_table": "live_orders",
        "source_table_oid": 16432,
        "cdf_config_name": (
            "projects/ad-test-v3/branches/round6-ad-test-v3/databases/databricks-postgres/"
            "cdf-configs/anti_demo_r6_ad_test_v3"
        ),
        "cdf_config_id": "anti_demo_r6_ad_test_v3",
        "cdf_config_create_time": CREATED,
        "cdf_status_name": (
            "projects/ad-test-v3/branches/round6-ad-test-v3/databases/databricks-postgres/"
            "cdf-configs/anti_demo_r6_ad_test_v3/cdf-statuses/live_orders"
        ),
        "cdf_status_id": "live_orders",
        "cdf_status_create_time": CREATED,
        "destination_catalog": "main",
        "destination_schema": "anti_demo_r6_ad_test_v3",
        "destination_schema_id": "schema-id",
        "destination_table_full_name": "main.anti_demo_r6_ad_test_v3.live_orders_history",
        "destination_table_id": "table-id",
        "baseline_order_id": "00000000-0000-4000-8000-000000000006",
        "baseline_proof_nonce": "round6-baseline",
        "baseline_sku": "RED-GLOVE",
        "baseline_store": "CHICAGO",
        "baseline_quantity": 1,
        "baseline_total_cents": 8450,
        "baseline_status": "baseline",
    }
    values["contract_sha256"] = round6_contract_sha256(
        branch_name=str(values["branch_name"]),
        branch_id=str(values["branch_id"]),
        branch_uid=str(values["branch_uid"]),
        branch_create_time=CREATED.isoformat(),
        endpoint_name=str(values["endpoint_name"]),
        endpoint_id=str(values["endpoint_id"]),
        endpoint_uid=str(values["endpoint_uid"]),
        endpoint_create_time=CREATED.isoformat(),
        database_resource_name=str(values["database_resource_name"]),
        database_resource_id=str(values["database_resource_id"]),
        postgres_database=str(values["postgres_database"]),
        source_schema=str(values["source_schema"]),
        source_table=str(values["source_table"]),
        source_table_oid=int(values["source_table_oid"]),
        cdf_config_name=str(values["cdf_config_name"]),
        cdf_config_id=str(values["cdf_config_id"]),
        cdf_config_create_time=CREATED.isoformat(),
        cdf_status_name=str(values["cdf_status_name"]),
        cdf_status_id=str(values["cdf_status_id"]),
        cdf_status_create_time=CREATED.isoformat(),
        destination_catalog=str(values["destination_catalog"]),
        destination_schema=str(values["destination_schema"]),
        destination_schema_id=str(values["destination_schema_id"]),
        destination_table_full_name=str(values["destination_table_full_name"]),
        destination_table_id=str(values["destination_table_id"]),
        baseline_order_id=str(values["baseline_order_id"]),
        baseline_proof_nonce=str(values["baseline_proof_nonce"]),
        baseline_sku=str(values["baseline_sku"]),
        baseline_store=str(values["baseline_store"]),
        baseline_quantity=int(values["baseline_quantity"]),
        baseline_total_cents=int(values["baseline_total_cents"]),
        baseline_status=str(values["baseline_status"]),
    )
    return values


def test_round6_seal_captures_every_returned_identity_and_rejects_tampering() -> None:
    sealed = Round6Resources.model_validate(round6_values())

    assert sealed.database_resource_id == "databricks-postgres"
    assert sealed.postgres_database == "databricks_postgres"
    assert sealed.source_table_oid == 16432
    assert sealed.destination_schema_id == "schema-id"
    assert sealed.destination_table_id == "table-id"

    changed = round6_values()
    changed["destination_table_id"] = "different-table"
    with pytest.raises(ValidationError, match="contract hash"):
        Round6Resources.model_validate(changed)


def test_manifest_v6_requires_round6_and_preserves_round5_readiness(tmp_path) -> None:
    from test_manifest import _round5_manifest

    sealed = Round6Resources.model_validate(round6_values())
    manifest = _round5_manifest(tmp_path, manifest_version=6, round6=sealed)

    assert manifest.round5_ready is True
    assert manifest.round6_ready is True
    with pytest.raises(ValidationError, match="requires sealed Round 6"):
        _round5_manifest(tmp_path, manifest_version=6, round6=None)


def test_round6_names_bind_default_database_and_dedicated_owned_schemas() -> None:
    names = round6_names(
        SimpleNamespace(
            run_id="ad-20260819-1800-abcd",
            databricks=SimpleNamespace(
                endpoint_name=(
                    "projects/ad-20260819-1800-abcd/branches/production/endpoints/primary"
                )
            ),
        )
    )

    assert "/branches/round6-" in names["database_resource_name"]
    assert names["database_resource_name"].endswith("/databases/databricks-postgres")
    assert names["production_branch_name"].endswith("/branches/production")
    assert names["source_schema"] == names["destination_schema"]
    assert names["source_schema"].startswith("anti_demo_r6_")
    assert names["cdf_config_id"] == "anti_demo_r6_ad_20260819_1800_abcd"


def test_live_status_only_branch_and_endpoint_responses_validate() -> None:
    names = round6_names(
        SimpleNamespace(
            run_id="ad-20260819-1800-abcd",
            databricks=SimpleNamespace(
                endpoint_name=(
                    "projects/ad-20260819-1800-abcd/branches/production/endpoints/primary"
                )
            ),
        )
    )
    branch = {
        "name": names["branch_name"],
        "branch_id": names["branch_id"],
        "parent": names["project_name"],
        "uid": "branch-uid",
        "create_time": CREATED.isoformat(),
        "status": {
            "branch_id": names["branch_id"],
            "default": False,
            "source_branch": names["production_branch_name"],
        },
    }
    endpoint = {
        "name": names["endpoint_name"],
        "endpoint_id": names["endpoint_id"],
        "parent": names["branch_name"],
        "uid": "endpoint-uid",
        "create_time": CREATED.isoformat(),
        "status": {
            "endpoint_id": names["endpoint_id"],
            "endpoint_type": "ENDPOINT_TYPE_READ_WRITE",
            "disabled": False,
            "suspend_timeout_duration": "60s",
        },
    }

    assert _validate_branch(branch, names)[0]["uid"] == "branch-uid"
    assert _validate_endpoint(endpoint, names)[0]["uid"] == "endpoint-uid"

    endpoint["status"]["suspend_timeout_duration"] = "86400s"
    with pytest.raises(RuntimeError, match="scale-to-zero contract changed"):
        _validate_endpoint(endpoint, names)

    endpoint["status"]["suspend_timeout_duration"] = "60s"
    endpoint["spec"] = {"no_suspension": True}
    with pytest.raises(RuntimeError, match="scale-to-zero contract changed"):
        _validate_endpoint(endpoint, names)

    del endpoint["spec"]
    del endpoint["status"]["suspend_timeout_duration"]
    with pytest.raises(RuntimeError, match="scale-to-zero contract changed"):
        _validate_endpoint(endpoint, names)


def test_database_and_catalog_validation_fail_closed() -> None:
    names = {"database_resource_name": "projects/p/branches/b/databases/databricks-postgres"}
    database = {
        "name": names["database_resource_name"],
        "database_id": "databricks-postgres",
        "status": {
            "database_id": "databricks-postgres",
            "postgres_database": "databricks_postgres",
        },
    }
    postgres = SimpleNamespace(
        get_database=lambda name: database,
        list_databases=lambda **kwargs: iter([database]),
    )
    workspace = SimpleNamespace(postgres=postgres)
    assert _validate_database(workspace, names)["database_id"] == "databricks-postgres"

    workspace.catalogs = SimpleNamespace(
        get=lambda name: {"name": name, "full_name": name, "storage_root": "s3://bucket"}
    )
    with pytest.raises(RuntimeError, match="catalog-level managed storage"):
        _validate_catalog(workspace, "main")


def round6_manifest(tmp_path):
    from test_manifest import _round5_manifest

    return _round5_manifest(
        tmp_path,
        manifest_version=6,
        round6=Round6Resources.model_validate(round6_values()),
    )


def round6_branch_payload(sealed, names: dict[str, str]) -> dict[str, object]:
    return {
        "name": sealed.branch_name,
        "branch_id": sealed.branch_id,
        "parent": names["project_name"],
        "uid": sealed.branch_uid,
        "create_time": sealed.branch_create_time.isoformat(),
        "status": {
            "branch_id": sealed.branch_id,
            "default": False,
            "source_branch": names["production_branch_name"],
        },
    }


def aug19_drifted_endpoint(sealed) -> dict[str, object]:
    """An endpoint carrying a suspend window that is not the vendor minimum.

    Every identity field matches the manifest; only the idle policy differs,
    which is what the disclosure would otherwise misreport.
    """

    return {
        "name": sealed.endpoint_name,
        "endpoint_id": sealed.endpoint_id,
        "parent": sealed.branch_name,
        "uid": sealed.endpoint_uid,
        "create_time": sealed.endpoint_create_time.isoformat(),
        "status": {
            "endpoint_id": sealed.endpoint_id,
            "endpoint_type": "ENDPOINT_TYPE_READ_WRITE",
            "disabled": False,
            "suspend_timeout_duration": "86400s",
        },
    }


def round6_workspace(sealed, names, endpoint, deletions: list[str]):
    """A workspace that answers only the branch and endpoint reads.

    Every destructive call records itself instead of acting, so a test can
    assert that neither mode reached a deletion.
    """

    def refuse_to_delete(label):
        def record(*args, **kwargs):
            deletions.append(label)
            raise AssertionError(f"cleanup must not call {label} here")

        return record

    return SimpleNamespace(
        postgres=SimpleNamespace(
            list_branches=lambda parent: iter([round6_branch_payload(sealed, names)]),
            list_endpoints=lambda parent: iter([endpoint]),
            get_branch=lambda name: round6_branch_payload(sealed, names),
            get_endpoint=lambda name: endpoint,
            delete_cdf_config=refuse_to_delete("delete_cdf_config"),
            delete_branch=refuse_to_delete("delete_branch"),
        ),
        schemas=SimpleNamespace(delete=refuse_to_delete("schemas.delete")),
        tables=SimpleNamespace(list=refuse_to_delete("tables.list")),
    )


def sealed_round6_reads(sealed, names, run_id: str) -> dict[str, dict[str, object]]:
    """Every read the seal check makes, answered exactly as the seal expects.

    ``round6_workspace`` above answers the branch and the endpoint and nothing
    else, so the seal check has only ever been run against an environment that
    fails at its second gate. Everything behind that gate -- the destination
    schema, the CDF config, the CDF status, the history table -- was therefore
    never compared against anything, in either direction.

    Keyed by read so a test can drift exactly one field and leave the rest
    intact, which is what makes a refusal attributable to one predicate.
    """

    destination_schema = f"{sealed.destination_catalog}.{sealed.destination_schema}"
    return {
        "branch": round6_branch_payload(sealed, names),
        "endpoint": {
            "name": sealed.endpoint_name,
            "endpoint_id": sealed.endpoint_id,
            "parent": sealed.branch_name,
            "uid": sealed.endpoint_uid,
            "create_time": sealed.endpoint_create_time.isoformat(),
            "spec": {
                "endpoint_type": "ENDPOINT_TYPE_READ_WRITE",
                "suspend_timeout_duration": ROUND6_SUSPEND_WINDOW,
            },
            "status": {"endpoint_id": sealed.endpoint_id, "disabled": False},
        },
        "database": {
            "name": sealed.database_resource_name,
            "database_id": sealed.database_resource_id,
            "status": {
                "database_id": sealed.database_resource_id,
                "postgres_database": sealed.postgres_database,
            },
        },
        "catalog": {
            "name": sealed.destination_catalog,
            "full_name": sealed.destination_catalog,
        },
        "schema": {
            "full_name": destination_schema,
            "catalog_name": sealed.destination_catalog,
            "name": sealed.destination_schema,
            "owner": sealed.setup_principal,
            "created_by": sealed.setup_principal,
            "schema_id": sealed.destination_schema_id,
            "properties": {ROUND6_OWNER_PROPERTY: run_id, "managed_by": "round6-lifecycle"},
        },
        "cdf_config": {
            "name": sealed.cdf_config_name,
            "cdf_config_id": sealed.cdf_config_id,
            "catalog": sealed.destination_catalog,
            "schema": sealed.destination_schema,
            "postgres_schema": sealed.source_schema,
            "create_time": sealed.cdf_config_create_time.isoformat(),
        },
        "cdf_status": {
            "name": sealed.cdf_status_name,
            "postgres_table": sealed.source_table,
            "uc_table": sealed.destination_table_full_name,
            "state": "CDF_STATE_STREAMING",
            "committed_lsn": "0/1A2B3C4",
            "create_time": sealed.cdf_status_create_time.isoformat(),
        },
        "table": {
            "full_name": sealed.destination_table_full_name,
            "table_id": sealed.destination_table_id,
        },
    }


def verifying_round6_workspace(reads, deletions: list[str]):
    """A workspace the seal check can run to a verdict on, and never delete through.

    Same contract as ``round6_workspace``: every destructive call records itself
    and raises instead of acting, so a test can assert that a refusal arrived
    before anything was destroyed.
    """

    def refuse_to_delete(label):
        def record(*args, **kwargs):
            deletions.append(label)
            raise AssertionError(f"cleanup must not call {label} here")

        return record

    return SimpleNamespace(
        postgres=SimpleNamespace(
            list_branches=lambda parent: iter([reads["branch"]]),
            list_endpoints=lambda parent: iter([reads["endpoint"]]),
            get_branch=lambda name: reads["branch"],
            get_endpoint=lambda name: reads["endpoint"],
            get_database=lambda name: reads["database"],
            list_databases=lambda parent: iter([reads["database"]]),
            get_cdf_config=lambda name: reads["cdf_config"],
            list_cdf_configs=lambda parent: iter([reads["cdf_config"]]),
            get_cdf_status=lambda name: reads["cdf_status"],
            list_cdf_statuses=lambda parent: iter([reads["cdf_status"]]),
            delete_cdf_config=refuse_to_delete("delete_cdf_config"),
            delete_branch=refuse_to_delete("delete_branch"),
        ),
        catalogs=SimpleNamespace(get=lambda name: reads["catalog"]),
        schemas=SimpleNamespace(
            get=lambda name: reads["schema"], delete=refuse_to_delete("schemas.delete")
        ),
        tables=SimpleNamespace(
            get=lambda name: reads["table"], list=refuse_to_delete("tables.list")
        ),
    )


def uc_baseline_row(**drift: object) -> dict[str, object]:
    """The one row the Round 6 canary writes, as a warehouse hands it back."""

    return {
        "order_id": ROUND6_BASELINE_ORDER_ID,
        "sku": ROUND6_BASELINE_SKU,
        "store": ROUND6_BASELINE_STORE,
        "quantity": ROUND6_BASELINE_QUANTITY,
        "total_cents": ROUND6_BASELINE_TOTAL_CENTS,
        "status": ROUND6_BASELINE_STATUS,
        "proof_nonce": ROUND6_BASELINE_NONCE,
        "_pg_change_type": "insert",
        **drift,
    }


@pytest.fixture
def sealed_source_and_history(monkeypatch):
    """The two seal reads that need live infrastructure, answered intact.

    ``_check_source`` opens a psycopg connection to the branch endpoint and
    ``_read_uc_baseline`` runs a statement on a warehouse, so both are doubled
    here for the same reason ``_source_connection`` and ``_delete_source_schema``
    already are elsewhere in this file. Everything between them -- the branch,
    endpoint, database, catalog, schema, CDF config, CDF status and history
    table comparisons -- is production's.
    """

    async def source_is_intact(sealed, workspace):
        return True, str(sealed.source_table_oid)

    async def history_holds_the_canary(workspace, warehouse_id, table):
        return [uc_baseline_row()]

    monkeypatch.setattr("server.round6_lifecycle._check_source", source_is_intact)
    monkeypatch.setattr("server.round6_lifecycle._read_uc_baseline", history_holds_the_canary)


def test_dry_run_reports_the_drifted_always_on_field_instead_of_dying(tmp_path) -> None:
    manifest = round6_manifest(tmp_path)
    sealed = manifest.round6
    names = round6_names(manifest)
    deletions: list[str] = []
    workspace = round6_workspace(sealed, names, aug19_drifted_endpoint(sealed), deletions)

    report = cleanup_round6(manifest, dry_run=True, workspace=workspace)

    assert report[0] == (
        "DRIFT Round 6 cleanup would refuse: "
        "Round 6 endpoint identity or scale-to-zero contract changed"
    )
    mismatched = [line for line in report if line.startswith("DRIFT   field=")]
    assert len(mismatched) == 1
    assert mismatched[0] == (
        "DRIFT   field=spec.no_suspension/status.suspend_timeout_duration "
        "expected=suspension enabled with a 60s window on every returned copy "
        "found=spec.no_suspension=None, spec.suspend_timeout_duration=None, "
        "status.suspend_timeout_duration='86400s'"
    )
    assert deletions == []


def test_real_cleanup_still_refuses_the_same_drifted_endpoint(tmp_path) -> None:
    manifest = round6_manifest(tmp_path)
    sealed = manifest.round6
    names = round6_names(manifest)
    deletions: list[str] = []
    workspace = round6_workspace(sealed, names, aug19_drifted_endpoint(sealed), deletions)

    with pytest.raises(RuntimeError) as refusal:
        cleanup_round6(manifest, dry_run=False, workspace=workspace)

    assert str(refusal.value) == (
        "Cleanup refused: Round 6 endpoint identity or scale-to-zero contract changed"
    )
    assert deletions == []


def test_both_modes_refuse_a_sealed_resource_the_manifest_does_not_own(tmp_path) -> None:
    """One drifted field at a time, each of them the only thing wrong.

    Every case here is convention-clean: the resource satisfies its own
    provisioning contract and would pass ``_validate_endpoint`` on its own. What
    it fails is the comparison against the manifest, which is the only thing
    that distinguishes the environment this install sealed from a physically
    different one standing at the same name.

    Drifting one field at a time is the point rather than tidiness. The single
    case this replaces drifted the endpoint UID, and the UID comparison sits
    first in an ``or`` chain with the creation-time comparison, so the latter
    could be inverted without any of the fifty-three tests here noticing.

    Nothing here doubles the source or the history table, deliberately: every
    refusal below is supposed to arrive before either is read, so a refusal
    that starts arriving later shows up here as a different message rather than
    as a test that still passes.
    """

    manifest = round6_manifest(tmp_path)
    sealed = manifest.round6
    names = round6_names(manifest)
    drifts = (
        # A different physical endpoint, standing at the sealed name.
        ("endpoint", {"uid": "endpoint-uid-belonging-to-another-run"}, "branch or endpoint seal"),
        # Same name, same UID, recreated: only the creation time gives it away.
        ("endpoint", {"create_time": RESTORED.isoformat()}, "branch or endpoint seal"),
        # A CDF config torn down and rebuilt under the same identity. The
        # creation time is the only half of that comparison a drift can reach:
        # `_validate_cdf_config` has already checked the config ID against the
        # derived name, and derived and sealed are the same string for every
        # manifest, so the ID half cannot refuse anything.
        ("cdf_config", {"create_time": RESTORED.isoformat()}, "CDF config seal"),
        # The service answered with a status that is not the one asked for.
        ("cdf_status", {"name": f"{sealed.cdf_status_name}-restored"}, "CDF status seal"),
    )

    for read, drift, refused in drifts:
        deletions: list[str] = []
        reads = sealed_round6_reads(sealed, names, manifest.run_id)
        reads[read] = {**reads[read], **drift}
        workspace = verifying_round6_workspace(reads, deletions)

        report = cleanup_round6(manifest, dry_run=True, workspace=workspace)
        with pytest.raises(RuntimeError) as refusal:
            cleanup_round6(manifest, dry_run=False, workspace=workspace)

        assert report[0].startswith(f"DRIFT Round 6 cleanup would refuse: Round 6 {refused}")
        assert str(refusal.value).startswith(f"Cleanup refused: Round 6 {refused}")
        assert deletions == []


def test_endpoint_identity_gates_name_the_field_they_refuse() -> None:
    names = round6_names(
        SimpleNamespace(
            run_id="ad-20260819-1800-abcd",
            databricks=SimpleNamespace(
                endpoint_name=(
                    "projects/ad-20260819-1800-abcd/branches/production/endpoints/primary"
                )
            ),
        )
    )

    def endpoint(**overrides) -> dict[str, object]:
        payload: dict[str, object] = {
            "name": names["endpoint_name"],
            "endpoint_id": names["endpoint_id"],
            "parent": names["branch_name"],
            "uid": "endpoint-uid",
            "create_time": CREATED.isoformat(),
            "spec": {
            "endpoint_type": "ENDPOINT_TYPE_READ_WRITE",
            "suspend_timeout_duration": "60s",
        },
        }
        payload.update(overrides)
        return payload

    assert _endpoint_contract_findings(endpoint(), names) == ()
    for field, tampered in (
        ("endpoint_id", endpoint(endpoint_id="someone-elses-endpoint")),
        ("uid", endpoint(uid="")),
        ("name", endpoint(name="projects/other/branches/production/endpoints/primary")),
        ("parent", endpoint(parent="projects/other/branches/production")),
        ("status.endpoint_id", endpoint(status={"endpoint_id": "someone-elses-endpoint"})),
        ("status.disabled", endpoint(status={"disabled": True})),
    ):
        findings = _endpoint_contract_findings(tampered, names)
        assert [name for name, _expected, _found in findings] == [field]
        with pytest.raises(RuntimeError, match="identity or scale-to-zero contract changed"):
            _validate_endpoint(tampered, names)


def test_dry_run_over_an_intact_round6_reports_nothing_and_deletes_nothing(
    tmp_path, sealed_source_and_history
) -> None:
    """The verifying path, run for real rather than stubbed out.

    This used to monkeypatch ``_check_round6`` away, so the seal check had no
    test anywhere in this file that ran it to a verdict of ``True``: the others
    all hand it a drifted endpoint and it refuses at the second gate. That left
    every sealed-resource comparison behind that gate inert -- the CDF config
    ID, the CDF status name, the endpoint creation time -- because a predicate
    that is never reached cannot fail, and one whose result never varies cannot
    either. Inverting any of them changed nothing. Running the real check over
    an intact environment is what gives each of them something to say: inverted,
    they now refuse an environment that has not drifted, and this fails.
    """

    manifest = round6_manifest(tmp_path)
    deletions: list[str] = []
    reads = sealed_round6_reads(manifest.round6, round6_names(manifest), manifest.run_id)
    workspace = verifying_round6_workspace(reads, deletions)

    # Asserted on the tuple, not on the flag alone: the detail carries the
    # refusal text, so a broken comparison names itself here.
    assert check_round6(manifest, workspace=workspace) == (
        True,
        manifest.round6.destination_table_full_name,
    )
    assert cleanup_round6(manifest, dry_run=True, workspace=workspace) == ()
    assert deletions == []


def test_the_baseline_canary_only_accepts_the_exact_row_it_wrote(tmp_path, monkeypatch) -> None:
    """A streaming status is not proof the feed works; the arriving row is.

    ``_wait_for_cdf_baseline`` is the gate that authorises sealing Round 6 at
    all, and it is the only place the change feed is ever proved end to end: it
    waits until the exact row Postgres was seeded with has arrived in Unity
    Catalog. Nothing in this suite ran it, so every field comparison in that
    row could be inverted and the suite stayed green -- including the SKU and
    the total, which are the two the audience reads off the screen.

    Both directions are asserted because a canary has two ways to be useless.
    One is refusing the row it wrote, which would strand setup on a healthy
    feed. The other is accepting a row it did not write, which arms Round 6 on
    a feed that is not carrying its data.
    """

    manifest = round6_manifest(tmp_path)
    sealed = manifest.round6
    names = round6_names(manifest)
    workspace = verifying_round6_workspace(sealed_round6_reads(sealed, names, manifest.run_id), [])
    history = [uc_baseline_row()]

    async def read_history(candidate_workspace, warehouse_id, table):
        assert table == sealed.destination_table_full_name
        return history

    monkeypatch.setattr("server.round6_lifecycle._read_uc_baseline", read_history)

    def canary():
        return _wait_for_cdf_baseline(
            workspace,
            names=names,
            catalog=sealed.destination_catalog,
            warehouse_id=sealed.warehouse_id,
            timeout=0.0,
        )

    status, created, table = canary()

    assert status["name"] == sealed.cdf_status_name
    assert created == sealed.cdf_status_create_time
    assert table["table_id"] == sealed.destination_table_id

    for drift in ({"sku": "BLUE-GLOVE"}, {"total_cents": ROUND6_BASELINE_TOTAL_CENTS + 1}):
        history = [uc_baseline_row(**drift)]

        with pytest.raises(RuntimeError, match="baseline canary timed out") as refusal:
            canary()

        # The status it gave up on was streaming, so the row is what refused --
        # not a feed that never started, which would fail this the same way.
        assert "state=CDF_STATE_STREAMING" in str(refusal.value)


def test_cleanup_force_deletes_exact_cdf_before_owned_schemas(monkeypatch) -> None:
    sealed = Round6Resources.model_validate(round6_values())
    calls: list[str] = []

    class Operation:
        def wait(self) -> None:
            calls.append("cdf-wait")

    workspace = SimpleNamespace(
        postgres=SimpleNamespace(
            delete_cdf_config=lambda name, force: (
                calls.append(f"cdf-delete:{name}:{force}") or Operation()
            ),
            list_cdf_configs=lambda parent: iter([]),
            delete_branch=lambda name, allow_missing, purge: (
                calls.append(f"branch:{name}:{allow_missing}:{purge}") or Operation()
            ),
        ),
        tables=SimpleNamespace(
            list=lambda **kwargs: iter(
                [
                    {
                        "full_name": sealed.destination_table_full_name,
                        "table_id": sealed.destination_table_id,
                    }
                ]
            )
        ),
        schemas=SimpleNamespace(
            delete=lambda name, force: calls.append(f"uc-schema:{name}:{force}")
        ),
    )
    manifest = SimpleNamespace(round6=sealed)
    monkeypatch.setattr(
        "server.round6_lifecycle._check_round6", lambda *args, **kwargs: (True, "ok", ())
    )

    async def delete_source(candidate, candidate_workspace) -> None:
        assert candidate_workspace is workspace
        calls.append(f"pg-schema:{candidate.source_schema}")

    monkeypatch.setattr("server.round6_lifecycle._delete_source_schema", delete_source)

    cleanup_round6(manifest, dry_run=False, workspace=workspace)

    assert calls == [
        f"cdf-delete:{sealed.cdf_config_name}:True",
        "cdf-wait",
        f"uc-schema:{sealed.destination_catalog}.{sealed.destination_schema}:True",
        f"pg-schema:{sealed.source_schema}",
        f"branch:{sealed.branch_name}:False:True",
        "cdf-wait",
    ]


def test_cleanup_refuses_unexpected_tables_before_destroying_anything(monkeypatch) -> None:
    """A refusal that arrives after the first delete strands the environment.

    The unexpected-table check used to run after the CDF config had already
    been force-deleted. Refusing at that point left a branch and an endpoint
    that no longer matched their seal, so every later cleanup refused at the
    gate and the endpoint billed with no way left to reap it. The refusal has
    to come first, while turning back is still free.
    """

    sealed = Round6Resources.model_validate(round6_values())
    calls: list[str] = []

    class Operation:
        def wait(self) -> None:
            calls.append("cdf-wait")

    workspace = SimpleNamespace(
        postgres=SimpleNamespace(
            delete_cdf_config=lambda name, force: (
                calls.append(f"cdf-delete:{name}:{force}") or Operation()
            ),
            list_cdf_configs=lambda parent: iter([]),
            delete_branch=lambda name, allow_missing, purge: (
                calls.append(f"branch:{name}:{allow_missing}:{purge}") or Operation()
            ),
        ),
        tables=SimpleNamespace(
            list=lambda **kwargs: iter(
                [
                    {
                        "full_name": sealed.destination_table_full_name,
                        "table_id": sealed.destination_table_id,
                    },
                    {"full_name": "main.round6.someone_elses_table", "table_id": "t-stranger"},
                ]
            )
        ),
        schemas=SimpleNamespace(
            delete=lambda name, force: calls.append(f"uc-schema:{name}:{force}")
        ),
    )
    manifest = SimpleNamespace(round6=sealed)
    monkeypatch.setattr(
        "server.round6_lifecycle._check_round6", lambda *args, **kwargs: (True, "ok", ())
    )

    with pytest.raises(RuntimeError, match="unexpected tables"):
        cleanup_round6(manifest, dry_run=False, workspace=workspace)

    assert calls == []


# ---------------------------------------------------------------------------
# --force-round6: the sanctioned way out of an unrepairably drifted seal.
#
# The gate it weakens is a destructive-operation gate, so the tests below are
# mostly about what it still refuses to do.
# ---------------------------------------------------------------------------


def forceable_workspace(
    sealed, names, endpoint, calls: list[str], *, tables=None, branch=None, fails=None
):
    """Answers the seal reads, and records every destructive call rather than acting.

    ``fails`` maps the name of one teardown call to the exception it raises
    instead of acting, which is how a test puts a single step into the state a
    half-finished earlier teardown leaves it in. The two listings raise from
    inside their iterator rather than from the call, because that is where the
    real SDK raises: both return a paginated ``Iterator`` and touch the network
    only once it is consumed, so a tolerance wrapped around the call alone would
    still let the failure past.
    """

    failures: dict[str, BaseException] = dict(fails or {})

    class Operation:
        def wait(self) -> None:
            calls.append("wait")

    branch_payload = round6_branch_payload(sealed, names) if branch is None else branch
    listed = (
        [{"full_name": sealed.destination_table_full_name, "table_id": sealed.destination_table_id}]
        if tables is None
        else tables
    )

    def refuse(step: str) -> None:
        failure = failures.get(step)
        if failure is not None:
            raise failure

    def listing(step: str, items):
        def pages():
            refuse(step)
            yield from items

        return pages()

    def delete_cdf_config(name, force):
        refuse("delete_cdf_config")
        calls.append(f"cdf-delete:{name}:{force}")
        return Operation()

    def delete_branch(name, allow_missing, purge):
        refuse("delete_branch")
        calls.append(f"branch:{name}:{allow_missing}:{purge}")
        return Operation()

    def delete_schema(name, force):
        refuse("schemas.delete")
        calls.append(f"uc-schema:{name}:{force}")

    return SimpleNamespace(
        postgres=SimpleNamespace(
            list_branches=lambda parent: iter([round6_branch_payload(sealed, names)]),
            list_endpoints=lambda parent: iter([endpoint]),
            get_branch=lambda name: branch_payload,
            get_endpoint=lambda name: endpoint,
            delete_cdf_config=delete_cdf_config,
            list_cdf_configs=lambda parent: listing("list_cdf_configs", []),
            delete_branch=delete_branch,
        ),
        tables=SimpleNamespace(list=lambda **kwargs: listing("tables.list", listed)),
        schemas=SimpleNamespace(delete=delete_schema),
    )


def already_torn_down(sealed) -> dict[str, BaseException]:
    """What each teardown step raises once an earlier teardown already did it.

    The CDF config and destination schema wordings are the ones two real failed
    teardowns produced, down to the punctuation, because the phrasing is what
    the tolerance actually matches on.
    """

    schema = f"{sealed.destination_catalog}.{sealed.destination_schema}"
    return {
        "tables.list": NotFound(f"Schema '{schema}' does not exist."),
        "delete_cdf_config": NotFound(f"CdfConfig not found: {sealed.cdf_config_name}"),
        "list_cdf_configs": NotFound(f"Database not found: {sealed.database_resource_name}"),
        "schemas.delete": NotFound(f"Schema '{schema}' does not exist."),
        "delete_branch": NotFound(f"Branch not found: {sealed.branch_name}"),
    }


@pytest.fixture
def no_source_schema_delete(monkeypatch):
    recorded: list[str] = []

    async def delete_source(candidate, candidate_workspace) -> None:
        recorded.append(f"pg-schema:{candidate.source_schema}")

    monkeypatch.setattr("server.round6_lifecycle._delete_source_schema", delete_source)
    return recorded


def test_the_drift_refusal_now_teaches_the_operator_the_escape_hatch(tmp_path) -> None:
    """A stranded environment stays stranded if the refusal never names the way out."""

    manifest = round6_manifest(tmp_path)
    workspace = round6_workspace(
        manifest.round6, round6_names(manifest), aug19_drifted_endpoint(manifest.round6), []
    )

    report = cleanup_round6(manifest, dry_run=True, workspace=workspace)

    hatch = [line for line in report if "--force-round6" in line]
    assert len(hatch) == 1
    assert f"uv run antidemo cleanup --yes --force-round6 {manifest.run_id}" in hatch[0]


def test_an_absent_flag_leaves_the_seal_check_exactly_as_it_was(tmp_path) -> None:
    """The default path is the one that matters most; forcing must not leak into it."""

    manifest = round6_manifest(tmp_path)
    deletions: list[str] = []
    workspace = round6_workspace(
        manifest.round6, round6_names(manifest), aug19_drifted_endpoint(manifest.round6), deletions
    )

    for empty in ("", "   ", "\t"):
        with pytest.raises(RuntimeError) as refusal:
            cleanup_round6(manifest, dry_run=False, workspace=workspace, force_token=empty)
        assert str(refusal.value) == (
            "Cleanup refused: Round 6 endpoint identity or scale-to-zero contract changed"
        )
    assert deletions == []


def test_the_token_must_name_this_environment(tmp_path) -> None:
    """A --force-round6 recalled from history carries the previous run's token."""

    manifest = round6_manifest(tmp_path)
    deletions: list[str] = []
    workspace = round6_workspace(
        manifest.round6, round6_names(manifest), aug19_drifted_endpoint(manifest.round6), deletions
    )

    with pytest.raises(RuntimeError) as refusal:
        cleanup_round6(
            manifest,
            dry_run=False,
            workspace=workspace,
            force_token="ad-20260818-1200-abcd",
            state_dir=tmp_path,
        )

    assert "does not name this environment" in str(refusal.value)
    assert deletions == []
    assert not (tmp_path / "round6-force.jsonl").exists()


def test_both_the_run_id_and_the_sealed_uids_are_accepted_tokens(tmp_path) -> None:
    manifest = round6_manifest(tmp_path)

    tokens = force_round6_tokens(manifest)

    assert tokens[0] == manifest.run_id
    assert manifest.round6.branch_uid in tokens
    assert manifest.round6.endpoint_uid in tokens
    for token in tokens:
        assert authorize_force_round6(manifest, token) == token
    with pytest.raises(RuntimeError, match="requires a confirmation token"):
        authorize_force_round6(manifest, "  ")


def test_a_forced_dry_run_prints_the_whole_manifest_of_destruction_and_deletes_nothing(
    tmp_path, capsys
) -> None:
    manifest = round6_manifest(tmp_path)
    sealed = manifest.round6
    calls: list[str] = []
    workspace = forceable_workspace(
        sealed, round6_names(manifest), aug19_drifted_endpoint(sealed), calls
    )

    report = cleanup_round6(
        manifest,
        dry_run=True,
        workspace=workspace,
        force_token=manifest.run_id,
        state_dir=tmp_path,
    )
    printed = capsys.readouterr().out

    assert calls == []
    assert report[-1].startswith("FORCE Dry run: nothing was deleted")
    body = "\n".join(report)
    assert "the Round 6 seal check is being OVERRIDDEN" in body
    assert "Round 6 endpoint identity or scale-to-zero contract changed" in body
    assert "mismatch field=spec.no_suspension/status.suspend_timeout_duration" in body
    assert sealed.cdf_config_name in body
    assert f"{sealed.destination_catalog}.{sealed.destination_schema}" in body
    assert sealed.source_schema in body
    assert "STILL ENFORCED" in body
    assert "unexpected-tables check" in body
    # Printed as it happens, not only returned, so an operator watching a real
    # cleanup sees it before the first delete rather than after the last.
    assert "OVERRIDDEN" in printed


def test_forcing_gets_past_the_drifted_seal_and_deletes_in_the_safe_order(
    tmp_path, no_source_schema_delete
) -> None:
    manifest = round6_manifest(tmp_path)
    sealed = manifest.round6
    calls: list[str] = []
    workspace = forceable_workspace(
        sealed, round6_names(manifest), aug19_drifted_endpoint(sealed), calls
    )

    cleanup_round6(
        manifest,
        dry_run=False,
        workspace=workspace,
        force_token=sealed.branch_uid,
        state_dir=tmp_path,
    )

    assert calls[0] == f"cdf-delete:{sealed.cdf_config_name}:True"
    assert calls[1] == "wait"
    assert calls[2] == f"uc-schema:{sealed.destination_catalog}.{sealed.destination_schema}:True"
    assert no_source_schema_delete == [f"pg-schema:{sealed.source_schema}"]


def test_forcing_does_not_bypass_the_unexpected_tables_check(
    tmp_path, no_source_schema_delete
) -> None:
    """The one check whose ordering was just fixed. Force must not undo that."""

    manifest = round6_manifest(tmp_path)
    sealed = manifest.round6
    calls: list[str] = []
    workspace = forceable_workspace(
        sealed,
        round6_names(manifest),
        aug19_drifted_endpoint(sealed),
        calls,
        tables=[
            {
                "full_name": sealed.destination_table_full_name,
                "table_id": sealed.destination_table_id,
            },
            {"full_name": "main.round6.someone_elses_table", "table_id": "t-stranger"},
        ],
    )

    with pytest.raises(RuntimeError, match="unexpected tables"):
        cleanup_round6(
            manifest,
            dry_run=False,
            workspace=workspace,
            force_token=manifest.run_id,
            state_dir=tmp_path,
        )

    assert calls == []
    assert no_source_schema_delete == []


@pytest.mark.parametrize(
    ("step", "still_performed"),
    [
        ("tables.list", ("cdf-delete", "wait", "uc-schema", "branch", "wait")),
        ("delete_cdf_config", ("uc-schema", "branch", "wait")),
        ("list_cdf_configs", ("cdf-delete", "wait", "uc-schema", "branch", "wait")),
        ("schemas.delete", ("cdf-delete", "wait", "branch", "wait")),
        ("delete_branch", ("cdf-delete", "wait", "uc-schema")),
    ],
)
def test_a_forced_teardown_finishes_over_any_step_an_earlier_one_already_did(
    tmp_path, no_source_schema_delete, step, still_performed
) -> None:
    """Every step here is reachable already done, and none of them may stop the rest.

    A teardown that deletes some of Round 6 and then dies further along leaves a
    seal that no longer verifies, and ``--force-round6`` is the sanctioned way
    back in. But force only downgrades the seal check, so the resumed teardown
    walks the same sequence into resources it removed itself last time. None of
    these APIs takes an ``allow_missing``, so each absence arrived as a failure
    and stranded the environment one step further along than the attempt before
    -- a branch and an endpoint billing on, with no way left to reap them.

    Each row is one step found already done, and the steps that must still run
    after it. The rows for the two listings keep the whole sequence: a listing
    is not a delete, so nothing is skipped when its subject is gone.
    """

    manifest = round6_manifest(tmp_path)
    sealed = manifest.round6
    calls: list[str] = []
    workspace = forceable_workspace(
        sealed,
        round6_names(manifest),
        aug19_drifted_endpoint(sealed),
        calls,
        fails={step: already_torn_down(sealed)[step]},
    )

    cleanup_round6(
        manifest,
        dry_run=False,
        workspace=workspace,
        force_token=manifest.run_id,
        state_dir=tmp_path,
    )

    assert tuple(call.split(":", 1)[0] for call in calls) == still_performed
    assert no_source_schema_delete == [f"pg-schema:{sealed.source_schema}"]


@pytest.mark.parametrize(
    ("step", "performed_before_it"),
    [
        ("tables.list", ()),
        ("delete_cdf_config", ()),
        ("list_cdf_configs", ("cdf-delete", "wait")),
        ("schemas.delete", ("cdf-delete", "wait")),
        ("delete_branch", ("cdf-delete", "wait", "uc-schema")),
    ],
)
@pytest.mark.parametrize(
    "failure",
    [
        PermissionDenied("User does not have USE SCHEMA on Schema 'main.anti_demo_r6_ad_test_v3'"),
        TooManyRequests("Request limit exceeded, please retry"),
        ConnectionError("Connection aborted, RemoteDisconnected"),
        TimeoutError("Read timed out"),
    ],
    ids=["denied", "throttled", "disconnected", "timed-out"],
)
def test_a_step_that_failed_for_any_reason_but_absence_still_refuses(
    tmp_path, no_source_schema_delete, step, performed_before_it, failure
) -> None:
    """Tolerating only the absence is what keeps these steps from being decoration.

    None of these failures say the resource is gone. They say we could not find
    out, which is the opposite thing, and reading one as an absence would let a
    forced teardown walk straight past a check it never managed to perform --
    including the unexpected-tables check, which is the only thing standing
    between it and a schema somebody else has put tables in.
    """

    manifest = round6_manifest(tmp_path)
    sealed = manifest.round6
    calls: list[str] = []
    workspace = forceable_workspace(
        sealed,
        round6_names(manifest),
        aug19_drifted_endpoint(sealed),
        calls,
        fails={step: failure},
    )

    with pytest.raises(type(failure)):
        cleanup_round6(
            manifest,
            dry_run=False,
            workspace=workspace,
            force_token=manifest.run_id,
            state_dir=tmp_path,
        )

    assert tuple(call.split(":", 1)[0] for call in calls) == performed_before_it
    assert not [call for call in calls if call.startswith("branch:")]


def test_a_source_schema_whose_endpoint_is_already_gone_does_not_stop_the_teardown(
    tmp_path, monkeypatch
) -> None:
    """No endpoint means no Postgres behind it, so the schema went with it."""

    manifest = round6_manifest(tmp_path)
    sealed = manifest.round6
    calls: list[str] = []
    workspace = forceable_workspace(
        sealed, round6_names(manifest), aug19_drifted_endpoint(sealed), calls
    )

    async def endpoint_is_gone(candidate_workspace, endpoint_name, user):
        raise NotFound(f"Endpoint not found: {endpoint_name}")

    monkeypatch.setattr("server.round6_lifecycle._source_connection", endpoint_is_gone)

    cleanup_round6(
        manifest,
        dry_run=False,
        workspace=workspace,
        force_token=manifest.run_id,
        state_dir=tmp_path,
    )

    assert f"branch:{sealed.branch_name}:False:True" in calls


def test_a_source_endpoint_that_merely_could_not_be_reached_still_refuses(
    tmp_path, monkeypatch
) -> None:
    """The tolerance sits on the connection alone, and only for a stated absence.

    It has to: the refusals immediately behind it are the source schema's
    ownership and contents checks, and a match wide enough to reach those could
    swallow the one thing that stops a teardown dropping somebody else's table.
    """

    manifest = round6_manifest(tmp_path)
    sealed = manifest.round6
    calls: list[str] = []
    workspace = forceable_workspace(
        sealed, round6_names(manifest), aug19_drifted_endpoint(sealed), calls
    )

    async def unreachable(candidate_workspace, endpoint_name, user):
        raise PermissionDenied("User is not authorised to generate a database credential")

    monkeypatch.setattr("server.round6_lifecycle._source_connection", unreachable)

    with pytest.raises(PermissionDenied):
        cleanup_round6(
            manifest,
            dry_run=False,
            workspace=workspace,
            force_token=manifest.run_id,
            state_dir=tmp_path,
        )

    assert not [call for call in calls if call.startswith("branch:")]


def test_forcing_does_not_bypass_the_source_schema_contents_check(tmp_path, monkeypatch) -> None:
    manifest = round6_manifest(tmp_path)
    sealed = manifest.round6
    calls: list[str] = []
    workspace = forceable_workspace(
        sealed, round6_names(manifest), aug19_drifted_endpoint(sealed), calls
    )

    async def refuse(candidate, candidate_workspace) -> None:
        raise RuntimeError("Cleanup refused: Round 6 source schema has unexpected objects")

    monkeypatch.setattr("server.round6_lifecycle._delete_source_schema", refuse)

    with pytest.raises(RuntimeError, match="source schema has unexpected objects"):
        cleanup_round6(
            manifest,
            dry_run=False,
            workspace=workspace,
            force_token=manifest.run_id,
            state_dir=tmp_path,
        )

    assert f"branch:{sealed.branch_name}:False:True" not in calls


def test_forcing_refuses_to_purge_a_branch_whose_uid_has_drifted(
    tmp_path, no_source_schema_delete
) -> None:
    """A drifted UID means the branch at that name belongs to somebody else now."""

    manifest = round6_manifest(tmp_path)
    sealed = manifest.round6
    names = round6_names(manifest)
    calls: list[str] = []
    stranger = {
        **round6_branch_payload(sealed, names),
        "uid": "branch-uid-belonging-to-someone-else",
    }
    workspace = forceable_workspace(
        sealed, names, aug19_drifted_endpoint(sealed), calls, branch=stranger
    )

    report = cleanup_round6(
        manifest,
        dry_run=True,
        workspace=workspace,
        force_token=manifest.run_id,
        state_dir=tmp_path,
    )

    assert "WILL NOT be deleted" in "\n".join(report)
    assert calls == []

    cleanup_round6(
        manifest,
        dry_run=False,
        workspace=workspace,
        force_token=manifest.run_id,
        state_dir=tmp_path,
    )

    assert not [call for call in calls if call.startswith("branch:")]


def test_forcing_writes_a_loud_audit_record_before_the_first_delete(
    tmp_path, no_source_schema_delete
) -> None:
    manifest = round6_manifest(tmp_path)
    sealed = manifest.round6
    calls: list[str] = []
    workspace = forceable_workspace(
        sealed, round6_names(manifest), aug19_drifted_endpoint(sealed), calls
    )

    cleanup_round6(
        manifest,
        dry_run=False,
        workspace=workspace,
        force_token=manifest.run_id,
        state_dir=tmp_path,
    )

    entry = json.loads((tmp_path / "round6-force.jsonl").read_text().strip())
    assert entry["event"] == "force_round6"
    assert entry["run_id"] == manifest.run_id
    assert entry["confirmation_token"] == manifest.run_id
    assert entry["dry_run"] is False
    assert entry["seal_failure"] == "Round 6 endpoint identity or scale-to-zero contract changed"
    assert entry["mismatches"][0]["field"] == (
        "spec.no_suspension/status.suspend_timeout_duration"
    )
    assert entry["forced_by"]["os_user"]
    assert entry["forced_by"]["pid"] == os.getpid()
    assert entry["targets"]["cdf_config_name"] == sealed.cdf_config_name


def test_a_wrong_token_is_refused_even_when_the_seal_verifies(tmp_path, monkeypatch) -> None:
    """Being wrong about which environment you are in is worth stopping on its own."""

    manifest = round6_manifest(tmp_path)
    calls: list[str] = []
    workspace = forceable_workspace(
        manifest.round6, round6_names(manifest), aug19_drifted_endpoint(manifest.round6), calls
    )
    monkeypatch.setattr(
        "server.round6_lifecycle._check_round6", lambda *args, **kwargs: (True, "ok", ())
    )

    with pytest.raises(RuntimeError, match="does not name this environment"):
        cleanup_round6(
            manifest,
            dry_run=False,
            workspace=workspace,
            force_token="not-this-place",
            state_dir=tmp_path,
        )

    assert calls == []


def test_the_flag_is_argv_only_and_needs_a_value(monkeypatch) -> None:
    """No default, no env-var shortcut, and no bare switch form."""

    from server.cli import _parser

    monkeypatch.setenv("ANTI_DEMO_FORCE_ROUND6", "yes")
    monkeypatch.setenv("FORCE_ROUND6", "yes")

    assert _parser().parse_args(["cleanup", "--yes"]).force_round6 == ""
    assert _parser().parse_args(["cleanup", "--dry-run"]).force_round6 == ""
    parsed = _parser().parse_args(["cleanup", "--yes", "--force-round6", "ad-1"])
    assert parsed.force_round6 == "ad-1"
    with pytest.raises(SystemExit):
        _parser().parse_args(["cleanup", "--yes", "--force-round6"])


_SQL_VERBS = frozenset({"SELECT", "INSERT", "UPDATE", "DELETE"})


def _source_table_verbs() -> set[str]:
    """Every SQL verb the runtime adapter aims at the Round 6 Postgres source.

    Read out of `server/live_orders.py` itself rather than listed here, for the
    same reason the coordination grant plan is checked against the `CREATE
    TABLE` statements: a list of verbs kept next to another list of verbs agrees
    with itself. Only methods that resolve their table through
    `_postgres_identifier` count, so `read_history` -- which reads the Unity
    Catalog side through `_uc_identifier` -- is correctly left out.
    """

    tree = ast.parse(
        (Path(__file__).resolve().parents[1] / "server" / "live_orders.py").read_text(
            encoding="utf-8"
        )
    )
    verbs: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        if not any(
            isinstance(inner, ast.Call) and getattr(inner.func, "id", "") == "_postgres_identifier"
            for inner in ast.walk(node)
        ):
            continue
        for inner in ast.walk(node):
            if not isinstance(inner, ast.JoinedStr) or not inner.values:
                continue
            head = inner.values[0]
            if not isinstance(head, ast.Constant) or not isinstance(head.value, str):
                continue
            verb = head.value.strip().split(" ", 1)[0].upper()
            if verb in _SQL_VERBS:
                verbs.add(verb)
    return verbs


def _granted_source_table_privileges() -> set[str]:
    """The privileges `ensure_round6_source` hands the app on its source table."""

    source = (
        Path(__file__).resolve().parents[1] / "server" / "round6_lifecycle.py"
    ).read_text(encoding="utf-8")
    matches = re.findall(r'"GRANT ([A-Z, ]+) ON TABLE \{\} TO \{\}"', source)
    assert len(matches) == 1, matches
    return {part.strip() for part in matches[0].split(",")}


def test_the_app_can_settle_the_proof_row_it_commits() -> None:
    """Round 6 verified from the deployed app and then could not clean up after itself.

    Setup granted `SELECT, INSERT` on the source table while `cleanup_checkout`
    issues a `DELETE ... RETURNING`, so every bout left one proof order behind
    in a table whose whole contract is "exactly the baseline row plus whatever
    this bout is proving". The symptom is quiet: the bout reaches `verified`,
    and only the app log carries `Round 6 settlement attempt n/4 failed ...
    InsufficientPrivilege: permission denied for table live_orders`.
    """

    assert _source_table_verbs() == _granted_source_table_privileges()
    # Named explicitly as well, so that a future adapter change which drops a
    # verb cannot quietly relax the grant by making both sides agree on less.
    assert _granted_source_table_privileges() == {"SELECT", "INSERT", "DELETE"}
    # UPDATE is absent on purpose: a proof row is written once and withdrawn,
    # never amended, and the baseline row is not the app's to touch.
    assert "UPDATE" not in _granted_source_table_privileges()
