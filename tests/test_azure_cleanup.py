"""server/azure_cleanup.py: only this run's copies are deleted, in-flight flows are cancelled,
and PgBouncer is reset, with a fake ARM client."""

from __future__ import annotations

from typing import Any

from server.azure_cleanup import copies_to_delete, park_pipelines, settle
from server.azure_inventory import Standing

RUN = "ad-test-001"
SERVER = "Microsoft.DBforPostgreSQL/flexibleServers"


class FakeArm:
    def __init__(self) -> None:
        self.deleted: list[str] = []
        self.cancelled: list[str] = []
        self.resource_group_path = "/subscriptions/s/resourceGroups/rg"

    def pipeline_runs(self, factory: str, pipeline: str, **_: Any) -> list[dict[str, str]]:
        return [{"runId": f"{pipeline}-run"}] if pipeline == "r4_pipeline" else []

    def cancel_pipeline_run(self, factory: str, run_id: str) -> None:
        self.cancelled.append(run_id)

    def delete_server(self, name: str) -> str:
        self.deleted.append(name)
        return "deleted"


def test_only_this_runs_copy_names_are_deleted() -> None:
    standing = [Standing("adsc-ad-test-001-azure", SERVER, "copy name"),
                Standing("adrc-ad-test-001-azure", SERVER, "run tag"),
                Standing("server-lakebase-anti-demo-r2", SERVER, "run tag"),  # the r2 source
                Standing("vnet", "Microsoft.Network/virtualNetworks", "run tag")]
    assert copies_to_delete(standing, RUN) == ["adrc-ad-test-001-azure", "adsc-ad-test-001-azure"]


def test_private_endpoints_are_removed_and_their_connections_waited_out() -> None:
    from server.azure_arm import Response
    from server.azure_cleanup import release_private_endpoints
    from tests.test_azure_arm import TOKEN, client

    arm, transport = client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("DELETE", "/managedPrivateEndpoints/mpe-r4-postgres?", Response(200, None, {})),
        ("GET", "/flexibleServers/r4/privateEndpointConnections?", Response(200, {"value": [
            {"id": "pec-1"}]}, {})),
        ("GET", "/flexibleServers/r4/privateEndpointConnections?", Response(200, {"value": []},
                                                                            {})),
    ])
    release_private_endpoints(arm, "adf", ["mpe-r4-postgres"], ["r4"], sleep=lambda _: None)
    assert [method for method, url, _ in transport.calls if "oauth2" not in url] == [
        "DELETE", "GET", "GET"]


def test_in_flight_runs_are_cancelled_per_pipeline() -> None:
    arm = FakeArm()
    assert park_pipelines(arm, "adf", ["r4_pipeline", "r6_pipeline"]) == ["r4_pipeline-run"]  # type: ignore[arg-type]


async def test_settle_parks_deletes_and_resets(monkeypatch) -> None:
    arm = FakeArm()
    monkeypatch.setattr("server.azure_cleanup.inventory", lambda _arm, _run: [
        Standing("adsc-ad-test-001-azure", SERVER, "copy name")])
    resets: list[bool] = []

    async def reset() -> None:
        resets.append(True)

    report = await settle(arm, run_id=RUN, factory="adf",  # type: ignore[arg-type]
                          pipelines=["r4_pipeline", "r6_pipeline"], reset_pgbouncer=reset)
    assert report.cancelled_runs == ["r4_pipeline-run"]
    assert report.deleted_copies == ["adsc-ad-test-001-azure"] == arm.deleted
    assert report.pgbouncer_reset and resets == [True]
