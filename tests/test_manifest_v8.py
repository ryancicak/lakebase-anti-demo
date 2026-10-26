"""Manifest v8: an Azure installation (design section 9), and the AWS edition's manifest unchanged.

An installation lives in one cloud for life. An AWS manifest is v1-v7, with `aws` and no `azure`,
and its file is written exactly as before; an Azure one is v8, with `azure` and no AWS field at all.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from pydantic import ValidationError
from test_azure_manifest import manifest as azure_section
from test_manifest import (
    _round5_round4_resources,
    _v7_aurora,
    _v7_lakebase,
    _v7_manifest,
    _v7_round6,
)

from server.manifest import (
    DatabricksManifest,
    DemoManifest,
    LakebaseEnvironmentSeal,
    RoundEnvironmentSeal,
    apply_manifest_environment,
    load_manifest,
    save_manifest,
)
from server.models import RoundId
from server.targets import lakebase_region_from_host

INSTALLATION = "018f6f50-7d3a-7cc1-9d5d-4d9ac8d107a1"
NOW = datetime(2026, 10, 19, 20, 0, tzinfo=UTC)


def _azure_lakebase(project_id: str, number: int) -> LakebaseEnvironmentSeal:
    """Lakebase on Azure: the AWS shape under Azure's domain."""

    return _v7_lakebase(project_id, number).model_copy(
        update={
            "direct_host": f"ep-r{number}.database.westus2.azuredatabricks.net",
            "pooled_host": f"ep-r{number}-pooler.database.westus2.azuredatabricks.net",
        }
    )


def azure_manifest(**updates: Any) -> DemoManifest:
    lakebase = {
        round_id: _azure_lakebase(f"install-r{number}", number)
        for number, round_id in enumerate(RoundId, start=1)
    }
    coordination = _azure_lakebase("install-coordination", 7)
    round1 = lakebase[RoundId.WAKE_IDLE_APP]
    values: dict[str, Any] = {
        "manifest_version": 8,
        "cloud": "azure",
        "installation_id": INSTALLATION,
        "run_id": "az-20261019-test",
        "owner": "operator@example.com",
        "created_at": NOW,
        "expires_at": NOW + timedelta(hours=72),
        "status": "ready",
        "azure": azure_section(),
        "databricks": DatabricksManifest(
            profile="anti-demo-azure-test",
            project_id=round1.project_id,
            endpoint_name=round1.endpoint_name,
            coordination_endpoint_name=coordination.endpoint_name,
        ),
        "round_environments": {
            round_id: RoundEnvironmentSeal(lakebase=sealed) for round_id, sealed in lakebase.items()
        },
        "coordination_environment": coordination,
    }
    values.update(updates)
    return DemoManifest(**values)


def test_an_azure_installation_round_trips_as_v8_and_its_file_names_no_aws_field(
    tmp_path,
) -> None:
    manifest = azure_manifest()
    path = save_manifest(manifest, tmp_path / "manifest.json")
    written = json.loads(path.read_text(encoding="utf-8"))

    assert written["cloud"] == "azure" and written["manifest_version"] == 8
    assert "aws" not in written
    assert not {name for name in written if name.startswith("round") and "aws" in name}
    loaded = load_manifest(path)
    assert loaded == manifest
    assert loaded.azure is not None and loaded.azure.region == "westus2"
    assert loaded.round_environment(2).aurora is None and loaded.round_environment(2).rds is None


def test_an_aws_manifest_is_written_exactly_as_before(tmp_path) -> None:
    """No `cloud` key and no `azure` key: the file a v1.2.2 app wrote, byte for byte."""

    manifest = _v7_manifest(tmp_path)
    assert manifest.cloud == "aws"
    path = save_manifest(manifest, tmp_path / "manifest-v7.json")
    written = json.loads(path.read_text(encoding="utf-8"))

    assert "cloud" not in written and "azure" not in written
    assert "cloud" not in manifest.model_dump() and "azure" not in manifest.model_dump()
    assert load_manifest(path).cloud == "aws"


def test_the_cloud_is_sealed_for_life() -> None:
    manifest = azure_manifest()
    with pytest.raises(ValidationError, match="frozen"):
        manifest.cloud = "aws"


@pytest.mark.parametrize(
    "updates, refusal",
    [
        ({"manifest_version": 7}, "requires manifest_version 8"),
        ({"azure": None}, "requires its azure section"),
        ({"round4_aws_source_location": "s3://bucket-name/round4/source"}, "got round4_aws"),
        ({"round6_aws_unsupported": "no"}, "got round6_aws_unsupported"),
        ({"installation_id": None}, "requires a full installation_id"),
        ({"coordination_environment": None}, "requires a coordination environment"),
    ],
)
def test_what_an_azure_manifest_refuses(updates: dict[str, Any], refusal: str) -> None:
    with pytest.raises(ValidationError, match=refusal):
        azure_manifest(**updates)


def test_an_azure_manifest_with_an_aws_section_names_it(tmp_path) -> None:
    aws = _v7_manifest(tmp_path).aws
    with pytest.raises(ValidationError, match="carries no AWS field; got aws"):
        azure_manifest(aws=aws)


def test_an_azure_round_cannot_seal_an_aws_database() -> None:
    manifest = azure_manifest()
    environments = dict(manifest.round_environments or {})
    environments[RoundId.MAKE_SCHEMA_CHANGE_SAFELY] = RoundEnvironmentSeal(
        lakebase=environments[RoundId.MAKE_SCHEMA_CHANGE_SAFELY].lakebase, aurora=_v7_aurora(2)
    )
    with pytest.raises(ValidationError, match="seals an AWS database on an Azure installation"):
        azure_manifest(round_environments=environments)


def test_an_azure_manifest_still_seals_six_rounds_or_none() -> None:
    manifest = azure_manifest()
    environments = dict(manifest.round_environments or {})
    environments.pop(RoundId.ANALYZE_LIVE_ORDERS)
    with pytest.raises(ValidationError, match="exactly the six canonical RoundId keys"):
        azure_manifest(round_environments=environments)
    # A provision that hasn't created the projects yet has none, and that is a whole state.
    provisioning = azure_manifest(status="provisioning", round_environments=None)
    assert provisioning.round_environments is None


def test_an_aws_manifest_refuses_the_azure_editions_fields(tmp_path) -> None:
    aws = _v7_manifest(tmp_path)
    payload = aws.model_dump(mode="json")
    with pytest.raises(ValidationError, match="carries no azure section"):
        DemoManifest.model_validate({**payload, "azure": azure_section()})
    with pytest.raises(ValidationError, match="manifest_version 8 is the Azure edition's"):
        DemoManifest.model_validate({**payload, "manifest_version": 8})
    with pytest.raises(ValidationError, match="requires its aws section"):
        DemoManifest.model_validate({key: value for key, value in payload.items() if key != "aws"})


def test_an_azure_installations_lakebase_seals_are_checked_once_sealed() -> None:
    manifest = azure_manifest()
    assert manifest.round5_ready is False and manifest.round6_ready is False

    round4_lakebase = manifest.round_lakebase(4)
    round4 = _round5_round4_resources().model_copy(
        update={
            "project_uid": round4_lakebase.project_uid,
            "branch_uid": round4_lakebase.branch_uid,
            "branch": round4_lakebase.branch_name,
            "endpoint_name": round4_lakebase.endpoint_name,
        }
    )
    sealed = azure_manifest(round4=round4, round6=_v7_round6(manifest.round_lakebase(6)))
    # Azure's Round 6 is its own server and data flow: it needs no Round 5 seal.
    assert sealed.round6_ready is True and sealed.round5_ready is False

    with pytest.raises(ValidationError, match="Round 4 adapter identities do not match"):
        azure_manifest(round4=round4.model_copy(update={"project_uid": "someone-elses"}))
    with pytest.raises(ValidationError, match="Round 6 adapter identities do not match"):
        azure_manifest(round6=_v7_round6(manifest.round_lakebase(2)))


def test_an_azure_manifest_exports_lakebase_and_never_aws(monkeypatch) -> None:
    for name in ("AWS_PROFILE", "AWS_REGION", "AURORA_CLUSTER_ID", "RDS_SECRET_ARN"):
        monkeypatch.setenv(name, "left-over-from-another-shell")

    apply_manifest_environment(azure_manifest())

    assert not {name for name in os.environ if name.startswith(("AWS_", "AURORA_", "RDS_"))}
    assert os.environ["LAKEBASE_EXPECTED_REGION"] == "westus2"
    assert os.environ["LAKEBASE_ENDPOINT_NAME"].startswith("projects/install-r1/")
    assert os.environ["AZURE_SUBSCRIPTION_ID"] == azure_section()["subscription_id"]


def test_a_lakebase_host_names_its_region_on_either_cloud() -> None:
    assert lakebase_region_from_host("ep-r1.database.westus2.azuredatabricks.net") == "westus2"
    assert lakebase_region_from_host("ep-r1.database.us-west-2.cloud.databricks.com") == (
        "us-west-2"
    )
