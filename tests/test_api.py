import asyncio
import inspect
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from databricks.sdk.errors.platform import PermissionDenied
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from starlette.requests import Request

import app as app_module
from app import app
from server import api as api_module
from server import generation_lock, lifecycle, round_availability
from server.api import operator_from_request, router
from server.aws_auth import APP_AWS_BINDINGS, AwsAuthConfigurationError
from server.aws_credential_probe import CredentialVerdict
from server.catalog import catalog as sealed_catalog
from server.coordination import (
    ALLOW_INMEMORY_COORDINATION_ENV,
    INMEMORY_COORDINATION_LOSSES,
    ROUND5_RING_KEY,
    InMemoryBoutLeaseStore,
    round_ring_key,
)
from server.manager import InvalidStateError, RunManager, SessionNotFoundError
from server.models import (
    Availability,
    BoutOperator,
    BoutStatus,
    CompetitorId,
    Corner,
    RoundId,
    SessionCreate,
    SessionState,
)
from server.readiness import RecoveryState
from server.reconcile import (
    PRESENCE_MISSING,
    PRESENCE_NEVER_CHECKED,
    PRESENCE_PRESENT,
    PRESENCE_UNVERIFIED,
    InstallationPresence,
)
from server.server_launch import RestartHistory, RestartJournal, restart_record_path

APP_CLIENT_ID = "11111111-2222-3333-4444-555555555555"


@dataclass
class FakeSlowPreparedTarget:
    """A lane whose application transaction never finishes on its own."""

    id: str
    name: str

    async def attempt(self, nonce: str, expected_value: str, timeout_seconds: float) -> None:
        await asyncio.sleep(3600)


@dataclass
class FakeSlowLiveTarget:
    """An eligible lane that stays in flight so a duplicate start races a live run."""

    id: str
    name: str

    async def assert_armed(self, *, not_before=None) -> dict[str, object]:
        return {"state": "ZERO"}

    async def prepare(self) -> FakeSlowPreparedTarget:
        return FakeSlowPreparedTarget(self.id, self.name)


def test_v7_runtime_enables_installation_scoped_round_isolation() -> None:
    installation_id = "018f6f50-7d3a-7cc1-9d5d-4d9ac8d107a1"

    assert app_module._round_isolation_config(None) == (False, "")
    assert app_module._round_isolation_config(
        SimpleNamespace(manifest_version=6, installation_id=None)
    ) == (False, "")
    assert app_module._round_isolation_config(
        SimpleNamespace(manifest_version=7, installation_id=installation_id)
    ) == (True, installation_id)
    assert app_module._round5_lease_ring_key(None) == ROUND5_RING_KEY
    assert app_module._round5_lease_ring_key(
        SimpleNamespace(manifest_version=6, installation_id=None)
    ) == ROUND5_RING_KEY
    assert app_module._round5_lease_ring_key(
        SimpleNamespace(manifest_version=7, installation_id=installation_id)
    ) == round_ring_key(
        installation_id,
        RoundId.SURVIVE_CONNECTION_SPIKE.value,
        cleanup=True,
    )


def deployed_manifest():
    run_id = "ad-app-001"
    production = f"projects/{run_id}/branches/production/endpoints/primary"
    coordination = f"projects/{run_id}/branches/coordination/endpoints/primary"
    return SimpleNamespace(
        manifest_version=2,
        round5_ready=False,
        status="ready",
        # Startup reports a passed TTL instead of refusing to serve; None means
        # this fake is inside its window.
        expiry_warning=lambda: None,
        aws=SimpleNamespace(
            region="us-west-2",
            account_id="123456789012",
            resources=SimpleNamespace(
                aurora_cluster_id="owned-aurora",
                aurora_secret_arn=(
                    "arn:aws:secretsmanager:us-west-2:123456789012:secret:aurora"
                ),
                rds_instance_id="owned-rds",
                rds_secret_arn=(
                    "arn:aws:secretsmanager:us-west-2:123456789012:secret:rds"
                ),
            ),
        ),
        databricks=SimpleNamespace(
            profile="operator-profile-must-not-be-used",
            user="operator@databricks.com",
            project_id=run_id,
            endpoint_name=production,
            coordination_endpoint_name=coordination,
            database="anti_demo",
        ),
        round4=SimpleNamespace(
            app_service_principal_client_id=APP_CLIENT_ID,
            endpoint_name=production,
            physical_database="anti_demo",
        ),
    )


async def test_v7_lifespan_wires_one_scoped_round5_artifact_fence(monkeypatch) -> None:
    manifest = deployed_manifest()
    manifest.manifest_version = 7
    manifest.installation_id = "018f6f50-7d3a-7cc1-9d5d-4d9ac8d107a1"
    manifest.round5_ready = True
    expected_key = round_ring_key(
        manifest.installation_id,
        RoundId.SURVIVE_CONNECTION_SPIKE.value,
        cleanup=True,
    )
    built: list[str] = []
    captured: dict[str, object] = {}

    monkeypatch.setenv("ANTI_DEMO_ENV", "databricks-app")
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", APP_CLIENT_ID)
    monkeypatch.setattr(app_module, "load_manifest", lambda: manifest)
    monkeypatch.setattr(
        app_module,
        "WorkspaceClient",
        lambda: SimpleNamespace(
            current_user=SimpleNamespace(
                me=lambda: SimpleNamespace(user_name=APP_CLIENT_ID)
            )
        ),
    )
    monkeypatch.setattr(app_module, "validate_app_aws_environment", lambda _env: None)

    class FakeLeaseStore:
        mode = "lakebase"

        def __init__(self, ring_key: str) -> None:
            self.ring_key = ring_key

        async def initialize(self) -> None:
            return None

        async def close(self) -> None:
            return None

    def fake_build_lease_store(*, ring_key: str = "main") -> FakeLeaseStore:
        built.append(ring_key)
        return FakeLeaseStore(ring_key)

    class FakeCostStore:
        mode = "lakebase"

        async def initialize(self) -> None:
            return None

        async def close(self) -> None:
            return None

    class FakeReadinessGate:
        status = SimpleNamespace(
            ring_ready=True,
            maintenance_state="ready",
            maintenance_detail=None,
        )
        round5_status = status

        def __init__(self, *_args, **kwargs) -> None:
            captured["readiness"] = kwargs["round5_lease_store"]

        def require_ready(self) -> None:
            return None

        def require_round5_ready(self) -> None:
            return None

        async def round5_prearm_guard(
            self,
            _session_id: str,
            _fencing_token: int,
        ) -> None:
            return None

        async def run(self) -> None:
            return None

    def fake_connection_spike_factory(_manifest, *, lease_store=None):
        captured["factory"] = lease_store
        return None

    monkeypatch.setattr(app_module, "build_lease_store", fake_build_lease_store)
    monkeypatch.setattr(app_module, "build_cost_ledger_store", FakeCostStore)
    monkeypatch.setattr(app_module, "ShowtimeReadinessGate", FakeReadinessGate)
    monkeypatch.setattr(
        app_module,
        "connection_spike_factory_from_manifest",
        fake_connection_spike_factory,
    )

    async with app.router.lifespan_context(app):
        scoped_store = app.state.round5_lease_store
        assert scoped_store.ring_key == expected_key
        assert app.state.run_manager._round5_cleanup_store() is scoped_store
        assert captured == {"readiness": scoped_store, "factory": scoped_store}

    assert built == ["main", expected_key]


@pytest.mark.parametrize("lever", ["RunManager", "_restart_history"])
async def test_a_startup_failure_after_the_readiness_gate_leaves_nothing_behind(
    monkeypatch, lever
) -> None:
    """A partial runtime must not leak a durable fenced lease with no owner.

    The retry loop's cleanup covers only the code up to its `break`. Everything
    after it was unprotected, and by then the readiness gate is already running as
    a task holding a *durable* fenced lease on its ring, with three coordination
    stores open. A failure there left all four behind and nothing able to cancel
    the gate.

    It matters most on the transitional-wait path, which re-enters `_open_runtime`
    every few seconds: a manifest stuck transitional plus any post-loop failure
    would accumulate orphaned gates contending for one fence indefinitely.

    The two levers are the real shapes of that failure -- the `RunManager`
    constructor's own validation raising, and anything at all going wrong after
    the background tasks are started.
    """
    manifest = deployed_manifest()
    manifest.manifest_version = 7
    manifest.installation_id = "018f6f50-7d3a-7cc1-9d5d-4d9ac8d107a1"
    manifest.round5_ready = True
    round5_key = round_ring_key(
        manifest.installation_id,
        RoundId.SURVIVE_CONNECTION_SPIKE.value,
        cleanup=True,
    )
    monkeypatch.setenv("ANTI_DEMO_ENV", "databricks-app")
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", APP_CLIENT_ID)
    monkeypatch.setattr(app_module, "load_manifest", lambda: manifest)
    monkeypatch.setattr(
        app_module,
        "WorkspaceClient",
        lambda: SimpleNamespace(
            current_user=SimpleNamespace(
                me=lambda: SimpleNamespace(user_name=APP_CLIENT_ID)
            )
        ),
    )
    monkeypatch.setattr(app_module, "validate_app_aws_environment", lambda _env: None)

    closed: list[str] = []
    started: dict[str, asyncio.Task[None]] = {}

    class FakeLeaseStore:
        mode = "lakebase"

        def __init__(self, ring_key: str) -> None:
            self.ring_key = ring_key

        async def initialize(self) -> None:
            return None

        async def close(self) -> None:
            closed.append(self.ring_key)

    monkeypatch.setattr(
        app_module,
        "build_lease_store",
        lambda *, ring_key="main": FakeLeaseStore(ring_key),
    )

    class FakeCostStore:
        mode = "lakebase"

        async def initialize(self) -> None:
            return None

        async def close(self) -> None:
            closed.append("cost-ledger")

    monkeypatch.setattr(app_module, "build_cost_ledger_store", FakeCostStore)

    class BlockingReadinessGate:
        """Holds its fence until somebody cancels it, which is the whole point."""

        status = SimpleNamespace(
            ring_ready=True, maintenance_state="ready", maintenance_detail=None
        )
        round5_status = status

        def __init__(self, *_args, **_kwargs) -> None:
            return None

        def require_ready(self) -> None:
            return None

        def require_round5_ready(self) -> None:
            return None

        async def round5_prearm_guard(self, _session_id: str, _fencing_token: int) -> None:
            return None

        async def run(self) -> None:
            task = asyncio.current_task()
            assert task is not None
            started["readiness"] = task
            await asyncio.Event().wait()

    class BlockingSentry:
        """Stands in for the AWS probe so no test reaches STS."""

        def __init__(self, *_args, **_kwargs) -> None:
            return None

        def verdict(self):
            return None

        async def run(self) -> None:
            await asyncio.Event().wait()

    monkeypatch.setattr(app_module, "ShowtimeReadinessGate", BlockingReadinessGate)
    monkeypatch.setattr(app_module, "CredentialSentry", BlockingSentry)

    async def reap(*_args, **_kwargs):
        # Awaited, so the readiness task reaches its first suspension point and is
        # genuinely running by the time the third lever raises.
        await asyncio.sleep(0)
        return None

    monkeypatch.setattr(app_module, "_reap_startup_orphans", reap)

    def explode(*_args, **_kwargs):
        raise InvalidStateError("startup failed after the gate was already running")

    monkeypatch.setattr(app_module, lever, explode)

    with pytest.raises(InvalidStateError):
        await app_module._open_runtime(app, deployed=True)

    # Nothing this startup created is still running. `all_tasks` reports only
    # unfinished ones, so an orphaned readiness gate -- the leak that mattered,
    # because it holds a durable fenced lease and nothing else can cancel it --
    # shows up here by name.
    assert [
        task.get_name()
        for task in asyncio.all_tasks()
        if task.get_name() in {"showtime-startup-readiness", "aws-credential-probe"}
    ] == []
    readiness = started.get("readiness")
    if readiness is not None:
        # Cancelled *and* finished: a merely-requested cancellation would let the
        # gate resume against a store that has since been closed.
        assert readiness.done()
        assert readiness.cancelled()
    assert sorted(closed) == sorted(["cost-ledger", "main", round5_key])


async def test_catalog_and_session_creation_do_not_touch_databases(monkeypatch) -> None:
    monkeypatch.delenv("DATABRICKS_APP_NAME", raising=False)
    monkeypatch.delenv("ANTI_DEMO_ENV", raising=False)
    monkeypatch.delenv("ANTI_DEMO_COORDINATION_ENDPOINT_NAME", raising=False)
    # This test wants the process-local ring: it asserts that startup opens no
    # database connection at all. Since the fallback now refuses by default, the
    # want has to be stated instead of inherited.
    monkeypatch.setenv(ALLOW_INMEMORY_COORDINATION_ENV, "1")
    monkeypatch.setattr(app_module, "require_ready_manifest", lambda: None)
    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            health = await client.get("/api/health")
            bout = await client.get("/api/bout")
            catalog = await client.get("/api/catalog")
            created = await client.post(
                "/api/sessions",
                json={
                    "competitor": "aurora_serverless_v2",
                    "primary_persona": "sre",
                    "secondary_personas": ["executive"],
                    "corners": ["cost", "simplicity", "performance"],
                },
            )

    assert health.status_code == 200
    assert health.json()["database_connections"] == "sealed"
    assert bout.status_code == 200
    assert bout.json() == {
        "scope": "global",
        "round_id": None,
        "ring_ready": True,
        "can_start": True,
        "maintenance_state": "ready",
        "maintenance_detail": None,
        "active": False,
        "operator": None,
        "started_at": None,
        "updated_at": None,
        "expires_at": None,
        "phase": None,
        "state": None,
        "round_title": None,
        "competitor": None,
    }
    assert catalog.status_code == 200
    assert created.status_code == 201
    assert created.json()["state"] == "draft"
    assert created.json()["corners"] == ["cost", "simplicity", "performance"]


async def test_catalog_reports_round_four_ready_without_calling_factory(monkeypatch) -> None:
    """Ready off its factory, and never green over a ring that cannot arm.

    Two halves, deliberately one test. The first is the original assertion:
    Round 4's availability comes from its adapter factory, and reading the
    catalog must not instantiate the live adapter.

    The second is what that assertion was blind to. Availability was computed
    entirely from the seal -- Rounds 1-3 were literal `READY` constants and
    Rounds 4-6 came from factory presence fixed at construction -- so the catalog
    kept offering six green rounds while `/readyz` reported that none of them
    could start. Every round arms through `require_ready`, so an unready gate
    means nothing can start, including the two rounds that need no AWS at all.

    The tail of the list is the other half of the rule: this overlay may only
    ever take readiness away. A `planned` or `preview` round keeps its sealed
    answer and gains no reason, because those are facts about the build rather
    than about this minute.

    A third thing factory presence cannot see -- a round that is built, sealed
    and refused by Databricks the instant it tries to arm -- is the subject of
    the test below. It needs the factory to actually be called, which is the one
    thing this test forbids, so it could not be another half of this one.
    """

    monkeypatch.delenv("DATABRICKS_APP_NAME", raising=False)
    monkeypatch.delenv("ANTI_DEMO_ENV", raising=False)
    factory_calls = 0

    def factory():
        nonlocal factory_calls
        factory_calls += 1
        raise AssertionError("catalog must not instantiate the live adapter")

    api_app = FastAPI()
    api_app.include_router(router)
    api_app.state.run_manager = RunManager(model_score_factory=factory)
    async with AsyncClient(
        transport=ASGITransport(app=api_app),
        base_url="http://anti-demo.test",
    ) as client:
        response = await client.get("/api/catalog")
        api_app.state.readiness_gate = SimpleNamespace(
            status=SimpleNamespace(
                ring_ready=False,
                maintenance_state="blocked",
                maintenance_detail=(
                    "BACKSTAGE CLEANUP BLOCKED · NOT RETRYING · "
                    "OPERATOR ACTION REQUIRED"
                ),
            ),
            round5_status=SimpleNamespace(ring_ready=True, maintenance_detail=None),
        )
        unready = await client.get("/api/catalog")

    assert response.status_code == 200
    round_four = next(
        item
        for item in response.json()["rounds"]
        if item["id"] == "put_model_score_in_app"
    )
    assert round_four["availability"] == "ready"
    # A ready round carries no reason: there is nothing to explain.
    assert "availability_reason" not in round_four

    assert unready.status_code == 200
    rounds = unready.json()["rounds"]
    assert [item["availability"] for item in rounds] == [
        "unavailable",
        "unavailable",
        "unavailable",
        "unavailable",
        "planned",
        "preview",
    ]
    refused = [item for item in rounds if item["availability"] == "unavailable"]
    # "Not ready" with no reason is barely better than a wrong "ready".
    assert all(
        "OPERATOR ACTION REQUIRED" in item["availability_reason"] for item in refused
    )
    assert all(
        "availability_reason" not in item
        for item in rounds
        if item["availability"] in {"planned", "preview"}
    )
    assert factory_calls == 0


async def test_the_catalog_stops_offering_a_round_databricks_has_already_refused(
    monkeypatch,
) -> None:
    """A round that died at the bell may not be advertised green afterwards.

    The incident, 2026-08-23. The deployed app was driven for the first time.
    Rounds 4 and 6 were both refused by Databricks on their first Lakebase call
    -- one missing `SELECT` on the synced table, one missing `Can Use` on the
    database project -- and `/api/catalog` went on reporting both of them
    `ready`, with no reason, to the screen a room full of customers reads.

    Both refusals are verbatim below, and both surfaces are read in one process
    on purpose. The defect had two faces that look independent and were not: the
    catalog was green *because* the arm path discarded the diagnosis, so there
    was nothing for a catalog to consult. Asserting them apart is what let each
    one look like somebody else's problem.

    Rounds 1-3 are the control. They ran nothing, nothing refused them, and they
    stay `ready` -- one round's refusal is not evidence about another's, and a
    wall of grey rounds would be the overcorrection this module already has a
    rule against.

    The two refusals below are the real sentences with the identifiers swapped
    for the placeholder convention this tree already uses. That is not a
    weakening: what has to survive is a message naming a table, a principal, a
    permission and a database project, and the placeholders carry all four in
    the right shape. Keeping the live values would have committed a real catalog,
    a real service principal and a real database project -- which is exactly what
    `test_no_live_identifiers_committed` caught on the first run of this test.
    """

    monkeypatch.delenv("DATABRICKS_APP_NAME", raising=False)
    monkeypatch.delenv("ANTI_DEMO_ENV", raising=False)

    round_four_refusal = (
        "User does not have SELECT on Table "
        "'example_catalog.anti_demo_online_ad_20260101_0000_0000.model_scores'."
    )
    round_six_refusal = (
        "The user is not authorized to make the request, please contact the "
        "workspace admin to assign the user 11111111-2222-3333-4444-555555555555 "
        "'Can Use' or 'Can Manage' for Database project "
        "66666666-7777-8888-9999-000000000000."
    )

    class RefusingEngine:
        def __init__(self, message: str) -> None:
            self._message = message

        async def arm(self, on_progress=None):
            raise PermissionDenied(self._message)

    run_manager = RunManager(
        model_score_factory=lambda: RefusingEngine(round_four_refusal),
        live_orders_factory=lambda: RefusingEngine(round_six_refusal),
    )
    api_app = FastAPI()
    api_app.include_router(router)
    api_app.state.run_manager = run_manager

    async with AsyncClient(
        transport=ASGITransport(app=api_app),
        base_url="http://anti-demo.test",
    ) as client:
        before = await client.get("/api/catalog")

        failures: dict[RoundId, str] = {}
        for round_id, persona in (
            (RoundId.PUT_MODEL_SCORE_IN_APP, "data_scientist_ml"),
            (RoundId.ANALYZE_LIVE_ORDERS, "data_scientist_ml"),
        ):
            created = await client.post(
                "/api/sessions",
                json={
                    "competitor": "rds_postgres",
                    "primary_persona": persona,
                    "corners": ["performance"],
                    "round_id": round_id.value,
                },
            )
            assert created.status_code == 201
            session_id = created.json()["id"]
            armed = await client.post(f"/api/sessions/{session_id}/arm")
            assert armed.status_code == 200
            for _ in range(400):
                snapshot = await client.get(f"/api/sessions/{session_id}")
                if snapshot.json()["state"] == "failed":
                    break
                await asyncio.sleep(0.01)
            body = snapshot.json()
            assert body["state"] == "failed", round_id
            failures[round_id] = body["failure"]

        after = await client.get("/api/catalog")

    # Before anything was armed the catalog is green, and that is not the defect:
    # nothing had asked Databricks anything yet, and manufacturing a refusal out
    # of not having asked is the overcorrection this module forbids.
    ready_before = {
        item["id"] for item in before.json()["rounds"] if item["availability"] == "ready"
    }
    assert "put_model_score_in_app" in ready_before
    assert "analyze_live_orders_without_slowing_checkout" in ready_before

    # What an operator now reads when the arm is refused. Databricks' own words,
    # naming the table, the principal and the permission, on the API rather than
    # in a container log behind a WebSocket.
    assert round_four_refusal in failures[RoundId.PUT_MODEL_SCORE_IN_APP]
    assert round_six_refusal in failures[RoundId.ANALYZE_LIVE_ORDERS]
    for failure in failures.values():
        assert round_availability.GRANT_REFUSAL_HEADLINE in failure
        # Readable, not a traceback. This text reaches a screen an audience sees.
        assert "\n" not in failure and "Traceback" not in failure

    # And what `/api/catalog` now says about the two rounds that cannot arm.
    after_by_id = {item["id"]: item for item in after.json()["rounds"]}
    for round_key, refusal in (
        ("put_model_score_in_app", round_four_refusal),
        ("analyze_live_orders_without_slowing_checkout", round_six_refusal),
    ):
        item = after_by_id[round_key]
        assert item["availability"] == "unavailable", round_key
        reason = item["availability_reason"]
        # Not a bare refusal: the reason has to carry the remedy, and the remedy
        # is the sentence Databricks wrote.
        assert refusal in reason
        assert round_availability.GRANT_REFUSAL_HEADLINE in reason

    # The control. Nothing refused Rounds 1-3, so nothing takes them away.
    for round_key in ("wake_idle_app", "make_schema_change_safely", "recover_deleted_order"):
        assert after_by_id[round_key]["availability"] == "ready", round_key
        assert "availability_reason" not in after_by_id[round_key]


@pytest.mark.parametrize(
    ("credentials", "detail", "presence", "expected", "expected_in_reason"),
    [
        # A verdict that has answered and says the credentials are unusable. Every
        # AWS lane arms through `rds:DescribeDBInstances`, so this is the whole set.
        (
            "rejected",
            "AWS REJECTED THE CREDENTIALS IN THIS PROCESS (ExpiredToken)",
            PRESENCE_PRESENT,
            "unavailable",
            "AWS REJECTED",
        ),
        (
            "unpermitted",
            "THE AWS CREDENTIALS IN THIS PROCESS ARE VALID BUT NOT PERMITTED",
            PRESENCE_PRESENT,
            "unavailable",
            "NOT PERMITTED",
        ),
        # Read and gone. The one presence state that is evidence of a loss, and
        # the reason comes from `InstallationPresence` so the catalog and
        # `/readyz` quote the same sentence.
        ("ok", None, PRESENCE_MISSING, "unavailable", "IS GONE"),
        # The three states that are *not* evidence of a loss, and must not
        # manufacture a refusal. `/readyz` claims no lost capabilities for any of
        # them, so inventing one here would re-create the same disagreement
        # pointed the other way -- and on a reap the sweep lands on `unverified`
        # rather than `missing` precisely because the credentials died with it.
        ("ok", None, PRESENCE_UNVERIFIED, "ready", None),
        ("ok", None, PRESENCE_NEVER_CHECKED, "ready", None),
        # "The probe stopped reporting" says nothing about whether the credentials
        # work. `capabilities_lost` claims nothing for it either.
        (
            "stale",
            "THE AWS CREDENTIAL PROBE HAS STOPPED REPORTING",
            PRESENCE_PRESENT,
            "ready",
            None,
        ),
        # The first seconds of a process's life, before the first probe answers.
        # Refusing here would make every startup flap.
        ("unknown", None, PRESENCE_NEVER_CHECKED, "ready", None),
    ],
)
def test_an_unread_signal_is_never_read_as_a_refusal_or_as_a_confirmation(
    credentials,
    detail,
    presence,
    expected,
    expected_in_reason,
) -> None:
    """Four presence states and ten credential states, kept from collapsing.

    The defect being guarded against has two faces and this test pins both. One
    is the original: a surface reporting health it never checked. The other is
    the overcorrection, which is just as wrong and easier to ship by accident --
    treating "nobody has looked yet" as a fault, so an installation that is
    perfectly fine shows a wall of refused rounds because a cache is cold.

    The rule is that only a signal which has *answered adversely* refuses.
    `never_checked` and `unverified` are absences of knowledge, `stale` means the
    checker stopped rather than that the thing broke, and `unknown` is the first
    second of a process's life. None of them is a green light either: they are
    passed through as themselves and never counted as a confirmation, which is
    why `InstallationPresence` has four states instead of a boolean.
    """

    signals = round_availability.AvailabilitySignals(
        credentials=CredentialVerdict(state=credentials, detail=detail),
        presence=InstallationPresence(presence, sealed=12, absent=3),
    )
    # Round 1 races a live Aurora or RDS opponent, so every AWS signal reaches it.
    availability, reason = round_availability.resolve(
        RoundId.WAKE_IDLE_APP, Availability.READY, signals
    )
    assert availability.value == expected
    if expected_in_reason is None:
        assert reason is None
    else:
        assert reason is not None and expected_in_reason in reason.detail
        # A refusal is never only an operator's paragraph. Whatever fired, the
        # room gets a sentence it can read, and that sentence carries none of
        # the vocabulary the detail is written in.
        assert reason.headline.startswith(round_availability.NOT_ON_THE_CARD)


async def test_bout_query_reports_only_the_requested_v7_round_and_refuses_none() -> None:
    run_manager = RunManager(round_isolation=True, installation_id="install-a")
    created = await run_manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.WAKE_IDLE_APP,
        )
    )
    record = run_manager._records[created.id]
    await run_manager._claim_bout(
        record,
        BoutOperator(display_name="Owner", subject="owner"),
    )
    api_app = FastAPI()
    api_app.include_router(router)
    api_app.state.run_manager = run_manager

    async with AsyncClient(
        transport=ASGITransport(app=api_app),
        base_url="http://anti-demo.test",
    ) as client:
        active = await client.get("/api/bout?round_id=wake_idle_app")
        idle = await client.get("/api/bout?round_id=put_model_score_in_app")
        unscoped = await client.get("/api/bout")

    assert active.json()["scope"] == "round"
    assert active.json()["round_id"] == "wake_idle_app"
    assert active.json()["active"] is True
    assert idle.json()["active"] is False
    # The unscoped call used to read the unused installation-wide ring and
    # report idle -- a green light while round one was mid-bout. It now refuses.
    assert unscoped.status_code == 400
    detail = unscoped.json()["detail"]
    assert "ROUND REQUIRED" in detail
    assert "round_id" in detail
    assert "wake_idle_app" in detail
    await run_manager._release_bout(record)


async def test_all_bout_statuses_cover_six_rounds_with_one_bounded_read(
    monkeypatch,
) -> None:
    """One browser request reads the bounded ring set and sanitizes every tile."""

    run_manager = RunManager(
        round_isolation=True,
        installation_id="install-board",
        model_score_factory=lambda: object(),
        connection_spike_factory=lambda: object(),
        live_orders_factory=lambda: object(),
    )
    monkeypatch.setattr(
        api_module,
        "_availability_signals",
        lambda _request: round_availability.AvailabilitySignals(
            round5_ring_ready=False,
            round5_detail="Cleanup state has not propagated yet",
        ),
    )
    owner = BoutOperator(
        display_name="Owner Name",
        email="owner@example.com",
        subject="owner-subject",
    )
    phases = {
        RoundId.WAKE_IDLE_APP: ("run_committed", SessionState.RUNNING),
        RoundId.MAKE_SCHEMA_CHANGE_SAFELY: ("cooldown", SessionState.VERIFIED),
        RoundId.RECOVER_DELETED_ORDER: ("cooldown", SessionState.VERIFIED),
    }
    for round_id, (phase, session_state) in phases.items():
        await run_manager._lease_store_for_round(round_id).claim(
            session_id=f"secret-{round_id.value}",
            operator=owner,
            phase=phase,
            session_state=session_state,
            round_id=round_id.value,
            round_title=f"Round {round_id.value}",
            competitor_id=CompetitorId.AURORA_SERVERLESS_V2.value,
            competitor_name="Aurora Serverless v2",
            ttl=timedelta(minutes=10),
        )
    await run_manager._round5_cleanup_store().claim(
        session_id="secret-round-five",
        operator=owner,
        phase="round5_cleanup",
        session_state=SessionState.VERIFIED,
        round_id=RoundId.SURVIVE_CONNECTION_SPIKE.value,
        round_title="Round 5",
        competitor_id=CompetitorId.AURORA_SERVERLESS_V2.value,
        competitor_name="Aurora Serverless v2",
        ttl=timedelta(minutes=10),
    )

    reads = 0
    for store in run_manager._every_ring_store():
        current = store.current

        async def counted_current(current=current):
            nonlocal reads
            reads += 1
            return await current()

        store.current = counted_current  # type: ignore[method-assign]

    api_app = FastAPI()
    api_app.include_router(router)
    api_app.state.run_manager = run_manager
    async with AsyncClient(
        transport=ASGITransport(app=api_app),
        base_url="http://anti-demo.test",
    ) as client:
        response = await client.get("/api/bout/all")

    assert response.status_code == 200
    payload = response.json()
    assert set(payload["rounds"]) == {round_id.value for round_id in RoundId}
    assert len(payload["rounds"]) == 6
    assert reads == len(RoundId) + 1
    assert payload["rounds"][RoundId.WAKE_IDLE_APP.value]["state"] == "bout_in_progress"
    assert (
        payload["rounds"][RoundId.MAKE_SCHEMA_CHANGE_SAFELY.value]["state"]
        == "cleanup_in_progress"
    )
    assert (
        payload["rounds"][RoundId.RECOVER_DELETED_ORDER.value]["state"]
        == "cleanup_in_progress"
    )
    assert (
        payload["rounds"][RoundId.SURVIVE_CONNECTION_SPIKE.value]["state"]
        == "cleanup_in_progress"
    )
    assert payload["rounds"][RoundId.PUT_MODEL_SCORE_IN_APP.value]["can_start"] is True
    assert payload["rounds"][RoundId.ANALYZE_LIVE_ORDERS.value]["can_start"] is True
    serialized = response.text
    for secret in ("owner@example.com", "Owner Name", "owner-subject", "secret-round-five"):
        assert secret not in serialized


@pytest.mark.parametrize(
    ("round_id", "phase"),
    [
        (RoundId.SURVIVE_CONNECTION_SPIKE, "round5_cleanup"),
        (RoundId.MAKE_SCHEMA_CHANGE_SAFELY, "cooldown"),
    ],
)
def test_cleanup_phase_never_flashes_unavailable_during_catalog_gap(
    round_id: RoundId,
    phase: str,
) -> None:
    """A terminal cleanup fence outranks one stale generic catalog refusal.

    This reproduces the live Round 5 ordering gap: the terminal result is
    visible and its durable lease is already in cleanup, while a replica-local
    readiness/catalog sample has not learned the cleanup reason yet. Round 2
    proves the precedence is all-round behavior, not a Round 5 display patch.
    """

    sealed = api_module.catalog(
        model_score_available=True,
        connection_spike_available=True,
        live_orders_available=True,
    )
    round_definition = next(item for item in sealed.rounds if item.id == round_id)
    stale_catalog_sample = round_definition.model_copy(
        update={
            "availability": Availability.UNAVAILABLE,
            "availability_reason_code": None,
            "availability_headline": "Temporarily unavailable",
            "availability_reason": "Cleanup detail has not propagated yet",
        }
    )
    terminal_cleanup = BoutStatus(
        scope="round",
        round_id=round_id,
        active=True,
        can_start=False,
        phase=phase,
        state=SessionState.VERIFIED,
    )

    status = api_module._fight_card_round_status(
        round_id,
        stale_catalog_sample,
        terminal_cleanup,
    )

    assert status.state == "cleanup_in_progress"
    assert status.can_start is False
    assert status.state != "unavailable"


async def test_all_bout_statuses_keep_health_failures_unavailable(monkeypatch) -> None:
    run_manager = RunManager(
        round_isolation=True,
        installation_id="install-blocked",
        model_score_factory=lambda: object(),
        connection_spike_factory=lambda: object(),
        live_orders_factory=lambda: object(),
    )
    monkeypatch.setattr(
        api_module,
        "_availability_signals",
        lambda _request: round_availability.AvailabilitySignals(
            ring_ready=False,
            ring_detail="Durable coordination health check failed",
        ),
    )
    api_app = FastAPI()
    api_app.include_router(router)
    api_app.state.run_manager = run_manager

    async with AsyncClient(
        transport=ASGITransport(app=api_app),
        base_url="http://anti-demo.test",
    ) as client:
        response = await client.get("/api/bout/all")

    assert response.status_code == 200
    assert {
        item["state"] for item in response.json()["rounds"].values()
    } == {"unavailable"}
    assert all(not item["can_start"] for item in response.json()["rounds"].values())


async def test_double_posted_run_returns_one_bout_over_http() -> None:
    """Two POST /run calls for one session must not open two runs.

    This exercises the real HTTP surface a double-clicked bell hits. It uses the
    in-process ASGI transport rather than TestClient on purpose: TestClient drives
    the app from its own event loop, which would not share the RunManager's loop or
    its per-session lock, so the in-flight idempotency branch could not be reached.
    """
    lakebase = FakeSlowLiveTarget("lakebase", "Lakebase")
    competitor = FakeSlowLiveTarget("competitor", "Aurora Serverless v2")
    run_manager = RunManager(
        resolver=SimpleNamespace(resolve=lambda _competitor: (lakebase, competitor))
    )
    created = await run_manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="software_engineer",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.WAKE_IDLE_APP,
        )
    )
    # Claim the ring as the identity operator_from_request derives locally, so the
    # bell arrives from the same owner that armed the bout.
    await run_manager.start_arm(
        created.id,
        BoutOperator(display_name="Local operator", subject="local-operator"),
    )
    for _ in range(200):
        if (await run_manager.get(created.id)).state.value == "armed":
            break
        await asyncio.sleep(0.005)
    assert (await run_manager.get(created.id)).state.value == "armed"

    api_app = FastAPI()
    api_app.include_router(router)
    api_app.state.run_manager = run_manager

    async with AsyncClient(
        transport=ASGITransport(app=api_app),
        base_url="http://anti-demo.test",
    ) as client:
        first = await client.post(f"/api/sessions/{created.id}/run")
        second = await client.post(f"/api/sessions/{created.id}/run")

    # The duplicate must be answered, not rejected and not run again.
    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["id"] == second.json()["id"] == created.id
    assert first.json()["run_started_at"] == second.json()["run_started_at"]

    # The run task publishes run_started after the POST returns, so let it land.
    events = run_manager._records[created.id].event_log.events
    for _ in range(400):
        if (await run_manager.get(created.id)).state.value == "running":
            break
        await asyncio.sleep(0.005)
    assert (await run_manager.get(created.id)).state.value == "running"
    assert [event.event for event in events].count("run_started") == 1

    await run_manager.start_towel(created.id)
    for _ in range(400):
        snapshot = await run_manager.get(created.id)
        if snapshot.towel is not None and snapshot.towel.state == "ready":
            break
        await asyncio.sleep(0.005)
    assert await run_manager._lease_store.current() is None


async def test_catalog_reports_round_five_ready_without_instantiating_factory(
    monkeypatch,
) -> None:
    """Ready locally, and unavailable in the deployed app for the stated reason.

    Every round that touches an AWS lane is decided by where the process runs,
    and for two independent reasons. Round 5's runner assumes a control role
    whose trust policy seals exactly one principal, and the deployed Databricks
    App does not authenticate as that principal. Rounds 1-3 have a plainer
    problem one layer down: they open TCP 5432 to Aurora and RDS, whose security
    groups admit only the operator laptop that provisioned the install, and the
    deployed app egresses from somewhere else.

    Neither is visible to the seal, so the catalog offered all four as `ready` on
    the deployed app -- and they arm clean and fail *after* the bell, which is the
    worst possible place for it to be discovered.

    They have to report unavailable there with the context as the reason, and not
    as one of the transient faults: a permanent structural refusal dressed up as a
    blip invites an operator to wait for something that will never change.
    """

    monkeypatch.delenv("DATABRICKS_APP_NAME", raising=False)
    monkeypatch.delenv("ANTI_DEMO_ENV", raising=False)
    factory_calls = 0

    def factory(_competitor: CompetitorId):
        nonlocal factory_calls
        factory_calls += 1
        raise AssertionError("catalog must not instantiate the Round 5 adapter")

    api_app = FastAPI()
    api_app.include_router(router)
    api_app.state.run_manager = RunManager(connection_spike_factory=factory)
    async with AsyncClient(
        transport=ASGITransport(app=api_app),
        base_url="http://anti-demo.test",
    ) as client:
        response = await client.get("/api/catalog")
        monkeypatch.setenv("ANTI_DEMO_ENV", "databricks-app")
        deployed = await client.get("/api/catalog")

    assert response.status_code == 200
    round_five = next(
        item
        for item in response.json()["rounds"]
        if item["id"] == "survive_connection_spike"
    )
    assert round_five["availability"] == "ready"
    assert round_five["competitors"] == [
        "rds_postgres",
        "aurora_serverless_v2",
    ]

    assert deployed.status_code == 200
    deployed_rounds = deployed.json()["rounds"]
    deployed_five = next(
        item for item in deployed_rounds if item["id"] == "survive_connection_spike"
    )
    assert deployed_five["availability"] == "unavailable"
    assert "CANNOT RUN IN THE DEPLOYED APP" in deployed_five["availability_reason"]
    assert "NOT A FAULT TO WAIT OUT" in deployed_five["availability_reason"]

    # Rounds 1-3 are refused in the deployed app too, for a different reason, and
    # this half of the test is the one that was wrong first. Round 5's principal
    # mismatch really is scoped to Round 5 -- but every round that races an AWS
    # lane has to open TCP 5432 to a database security group, and those groups
    # admit exactly the sealed operator /32. Measured 2026-08-22: the deployed app
    # reached AWS from two different AWS-managed egress addresses inside one
    # 70-minute window (198.51.100.x, then 203.0.113.x -- documentation stand-ins,
    # because the real pair are live values and the finding is that they *change*,
    # not what they were). So there is no /32 to admit, and the narrowest
    # published range around them is a /16 of general-purpose EC2. Offering these
    # three as `ready` is the same defect as offering Round 5: they arm clean and
    # die at connect, after the bell.
    #
    # Nothing below asserts on an address, deliberately. Pinning one would pin the
    # very thing that was observed to vary; what has to hold is that the round is
    # refused and that the reason sends the operator to the security group rather
    # than to the trust policy.
    deployed_by_id = {item["id"]: item for item in deployed_rounds}
    for round_id in (
        "wake_idle_app",
        "make_schema_change_safely",
        "recover_deleted_order",
    ):
        item = deployed_by_id[round_id]
        assert item["availability"] == "unavailable", round_id
        assert "CANNOT RUN IN THE DEPLOYED APP" in item["availability_reason"]
        # The reason has to name the network, not the principal. An operator sent
        # to the trust policy for a security-group problem loses the evening.
        assert "security group" in item["availability_reason"]
        assert "principal" not in item["availability_reason"]

    # Rounds 4 and 6 reach no AWS database, so the deployed context takes nothing
    # from them. Refusing them here would be the overcorrection.
    assert deployed_by_id["put_model_score_in_app"]["availability"] == "planned"
    assert deployed_by_id["analyze_live_orders_without_slowing_checkout"][
        "availability"
    ] == "preview"
    assert factory_calls == 0

    # Every refused round also carries the sentence the room reads. The fight
    # card is further into the demo than the installation banner and therefore
    # more likely to be on a projector, and what was on it was the paragraph
    # above: TCP 5432, security groups, operator CIDRs, a /16 of EC2. The
    # headline is the same refusal with none of that in it, and it has to hold
    # for every refused round rather than for the one that was noticed.
    operator_vocabulary = (
        "5432",
        "security group",
        "CIDR",
        "/16",
        "trust policy",
        "terraform",
        "./antidemo",
    )
    for round_id in (
        "wake_idle_app",
        "make_schema_change_safely",
        "recover_deleted_order",
        "survive_connection_spike",
    ):
        headline = deployed_by_id[round_id]["availability_headline"]
        assert headline.startswith(round_availability.NOT_ON_THE_CARD), round_id
        # Short enough for the one line the card gives it. The strip under the
        # buttons overstruck itself on a 900-character reason.
        assert len(headline) < 220, round_id
        for word in operator_vocabulary:
            assert word.casefold() not in headline.casefold(), (round_id, word)
        # And the detail is untouched: this is a reclassification of who reads
        # what, not a deletion of the diagnosis.
        assert "CANNOT RUN IN THE DEPLOYED APP" in deployed_by_id[round_id][
            "availability_reason"
        ], round_id


def test_every_branch_that_refuses_a_round_also_writes_one_for_the_room() -> None:
    """No way to reach a refusal that has only the operator's paragraph in it.

    The defect this closes is the audience-visible one this module's own
    docstring names, arriving on the fight card instead of the round list: a
    refusal written for the person who provisioned the install, rendered in
    front of a room. Fixing the branch that was screenshotted would leave six
    others able to do the same thing the next time one of them fires, and half
    of those carry text written by somebody else -- the credential probe, the
    account sweep, Databricks itself -- so "it will read fine" is not something
    this module can know. What it can guarantee is that a headline exists,
    that it is its own sentence rather than a slice of the detail, and that the
    detail survives underneath.
    """

    class _Verdict:
        def __init__(self, state: str) -> None:
            self.state = state
            self.detail = f"OPERATOR PROSE ABOUT {state.upper()}"

    round_one = RoundId.WAKE_IDLE_APP
    round_five = RoundId.SURVIVE_CONNECTION_SPIKE
    signals = round_availability.AvailabilitySignals
    branches = {
        "unready ring": (round_one, signals(ring_ready=False, ring_detail="RING PROSE")),
        "credential fault": (round_one, signals(credentials=_Verdict("rejected"))),
        "deployed aws lane": (round_one, signals(deployed=True)),
        "deployed round 5": (round_five, signals(deployed=True)),
        "databricks grant": (
            RoundId.PUT_MODEL_SCORE_IN_APP,
            signals(
                grant_refusals={
                    RoundId.PUT_MODEL_SCORE_IN_APP: round_availability.grant_refusal(
                        "PERMISSION_DENIED on a synced table"
                    )
                }
            ),
        ),
        "round 5 principal": (
            round_five,
            signals(credentials=_Verdict("principal_mismatch")),
        ),
        "round 5 cleanup": (
            round_five,
            signals(round5_ring_ready=False, round5_detail="ROUND 5 PROSE"),
        ),
        "swept installation": (
            round_one,
            signals(presence=InstallationPresence(PRESENCE_MISSING, sealed=12, absent=12)),
        ),
    }

    for name, (round_id, signal) in branches.items():
        refusal = round_availability.refusal(round_id, signal)
        assert refusal is not None, name
        assert refusal.headline.startswith(round_availability.NOT_ON_THE_CARD), name
        assert refusal.headline != refusal.detail, name
        assert refusal.detail, name


def test_round_five_cleanup_is_machine_readable_without_reclassifying_failures() -> None:
    """Only normal retrying cleanup gets the temporary fight-card state."""

    round_five = next(
        item
        for item in sealed_catalog(connection_spike_available=True).rounds
        if item.id == RoundId.SURVIVE_CONNECTION_SPIKE
    )
    retrying = round_availability.apply(
        [round_five],
        round_availability.AvailabilitySignals(
            round5_ring_ready=False,
            round5_reason_code="cleanup_in_progress",
            round5_detail="ROUND 5 BACKSTAGE CLEANUP",
        ),
    )[0].model_dump(mode="json")
    assert retrying["availability"] == "unavailable"
    assert retrying["availability_reason_code"] == "cleanup_in_progress"

    blocked = round_availability.apply(
        [round_five],
        round_availability.AvailabilitySignals(
            round5_ring_ready=False,
            round5_detail="ROUND 5 CLEANUP FAILED",
        ),
    )[0].model_dump(mode="json")
    assert blocked["availability"] == "unavailable"
    assert "availability_reason_code" not in blocked

    permanent = round_availability.apply(
        [round_five],
        round_availability.AvailabilitySignals(
            deployed=True,
            round5_ring_ready=False,
            round5_reason_code="cleanup_in_progress",
        ),
    )[0].model_dump(mode="json")
    assert permanent["availability"] == "unavailable"
    assert "availability_reason_code" not in permanent

    reopened = round_availability.apply(
        [round_five],
        round_availability.AvailabilitySignals(),
    )[0].model_dump(mode="json")
    assert reopened["availability"] == "ready"
    assert "availability_reason_code" not in reopened


def test_the_deployed_refusals_lift_once_the_installation_admits_the_app() -> None:
    """The correction, and it is the mirror image of the original incident.

    Both deployed refusals were unconditional on `deployed`, because the network
    path was believed to be unopenable: "there is no stable address to admit, the
    narrowest published range covering them is a /16 of general-purpose EC2".
    That premise was false. Databricks publishes the egress prefixes its apps
    leave from, and an installation that seals them admits the app -- so on such
    an installation the refusal is a round-select screen telling a room a round
    cannot run while it demonstrably does. A round shown green that dies at the
    bell and a round shown refused that runs are the same defect pointed in
    opposite directions, and the second one throws away the argument this project
    exists to make.
    """

    signals = round_availability.AvailabilitySignals
    racing = sorted(round_availability.AWS_BACKED_ROUNDS, key=lambda item: item.value)

    # Before the seal: unchanged, all four refused, and Round 5 still gets its
    # own sentence about the trust policy rather than the network one.
    for round_id in racing:
        refusal = round_availability.refusal(round_id, signals(deployed=True))
        assert refusal is not None, round_id

    # After it: Rounds 1, 2 and 3 are offered from the deployed app.
    admitted = signals(deployed=True, deployed_aws_path_sealed=True)
    for round_id in racing:
        if round_id == RoundId.SURVIVE_CONNECTION_SPIKE:
            continue
        assert round_availability.refusal(round_id, admitted) is None, round_id

    # Round 5 is three problems, not one. Sealing the network path is necessary
    # and not sufficient: its control role still has to trust something both the
    # app and the operator can assume, and that can only be sealed at first
    # provision. Reporting it as fixed here would send an operator to a reseal
    # that will not help.
    round_five = round_availability.refusal(
        RoundId.SURVIVE_CONNECTION_SPIKE, admitted
    )
    assert round_five is not None
    assert round_five.detail == round_availability.ROUND5_DEPLOYED_REFUSAL

    both = signals(
        deployed=True, deployed_aws_path_sealed=True, round5_runtime_role_sealed=True
    )
    assert round_availability.refusal(RoundId.SURVIVE_CONNECTION_SPIKE, both) is None

    # And a runtime role on its own does not open a security group.
    role_only = signals(deployed=True, round5_runtime_role_sealed=True)
    refusal = round_availability.refusal(RoundId.SURVIVE_CONNECTION_SPIKE, role_only)
    assert refusal is not None
    assert refusal.detail == round_availability.AWS_LANE_DEPLOYED_REFUSAL


def test_the_deployed_refusal_no_longer_states_the_false_premise() -> None:
    """The `/16` sentence is the reason this was believed unsolvable.

    It was true of *AWS's* published ranges and false of Databricks', and
    Databricks publishes its own -- 401 addresses against 65,536. Leaving the
    claim standing would keep telling every reader that there is nothing to be
    done, which is the one thing the text must no longer say.
    """

    text = round_availability.AWS_LANE_DEPLOYED_REFUSAL

    assert "There is no ingress rule to add" not in text
    assert "there is no stable address to admit" not in text
    assert "no room for a second" not in text
    # It may still name the /16 -- as the belief that was wrong, which is worth
    # recording -- but it must now point at the rule that closes this.
    assert "there is one to add" in text
    assert "./antidemo setup" in text
    # And it must not promise the viewer something only a fresh install can do.
    assert "no account admin" in text


def test_the_signals_that_lift_a_refusal_default_to_refusing() -> None:
    """The one place this module defaults the strict way round, and why.

    Every other field here defaults permissively because "no gate to ask" is a
    real state with no disagreement to have. These two are different: an
    installation that sealed nothing admits nobody, that is a definite fact, and
    guessing otherwise would put a green round on the card that dies at the bell
    -- which is the incident this module was written for.
    """

    fresh = round_availability.AvailabilitySignals()

    assert fresh.deployed_aws_path_sealed is False
    assert fresh.round5_runtime_role_sealed is False
    # Harmless locally, which is what keeps the strict default from costing
    # anything: the refusals they gate are reached only when `deployed` is true.
    assert fresh.deployed is False
    assert round_availability.refusal(RoundId.WAKE_IDLE_APP, fresh) is None


async def test_lifespan_wires_manifest_model_score_factory(monkeypatch) -> None:
    monkeypatch.delenv("DATABRICKS_APP_NAME", raising=False)
    monkeypatch.delenv("ANTI_DEMO_ENV", raising=False)
    monkeypatch.delenv("ANTI_DEMO_COORDINATION_ENDPOINT_NAME", raising=False)
    monkeypatch.setenv(ALLOW_INMEMORY_COORDINATION_ENV, "1")
    monkeypatch.setattr(
        app_module,
        "load_manifest",
        lambda: SimpleNamespace(
            manifest_version=2,
            round4=object(),
            round5_ready=False,
        ),
    )

    async with app.router.lifespan_context(app):
        manager = app.state.run_manager
        assert manager.model_score_available is True

    monkeypatch.setattr(
        app_module,
        "load_manifest",
        lambda: SimpleNamespace(
            manifest_version=1,
            round4=None,
            round5_ready=False,
        ),
    )
    assert app_module.model_score_factory_from_manifest() is None
    local_v4 = SimpleNamespace(round5_ready=True)
    in_memory_store = SimpleNamespace(
        mode="memory",
        _run=lambda operation: operation,
        current=lambda: None,
    )
    assert (
        app_module.connection_spike_factory_from_manifest(
            local_v4, lease_store=in_memory_store
        )
        is None
    )
    assert app_module.connection_spike_factory_from_manifest(local_v4) is None

    app_config = (Path(__file__).parents[1] / "app.yaml").read_text(encoding="utf-8")
    assert "name: ANTI_DEMO_MANIFEST_JSON" in app_config
    assert "valueFrom: anti-demo-manifest-json" in app_config
    assert "valueFrom: aws-session-token" in app_config
    assert "replace-with-owned" not in app_config
    assert "000000000000" not in app_config


async def test_redo_api_returns_200_404_and_409(monkeypatch) -> None:
    monkeypatch.delenv("DATABRICKS_APP_NAME", raising=False)
    backing = RunManager()
    snapshot = await backing.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.WAKE_IDLE_APP,
        )
    )

    async def start_redo(session_id, _operator):
        if session_id == "missing":
            raise SessionNotFoundError(session_id)
        if session_id == "conflict":
            raise InvalidStateError("redo conflict")
        if session_id == "unavailable":
            raise RuntimeError("provider detail must stay private")
        return snapshot

    async def retry_connection_spike_cleanup(session_id, _operator):
        if session_id == "missing":
            raise SessionNotFoundError(session_id)
        if session_id == "conflict":
            raise InvalidStateError("cleanup conflict")
        return snapshot

    event_calls: list[tuple[str, int]] = []

    async def events(session_id, after):
        event_calls.append((session_id, after))
        if False:
            yield None

    backing.start_redo = start_redo
    backing.retry_connection_spike_cleanup = retry_connection_spike_cleanup
    backing.events = events
    api_app = FastAPI()
    api_app.include_router(router)
    api_app.state.run_manager = backing
    async with AsyncClient(
        transport=ASGITransport(app=api_app),
        base_url="http://anti-demo.test",
    ) as client:
        ok = await client.post("/api/sessions/ok/redo")
        missing = await client.post("/api/sessions/missing/redo")
        conflict = await client.post("/api/sessions/conflict/redo")
        unavailable = await client.post("/api/sessions/unavailable/redo")
        cleanup_ok = await client.post("/api/sessions/ok/retry-cleanup")
        cleanup_missing = await client.post("/api/sessions/missing/retry-cleanup")
        cleanup_conflict = await client.post("/api/sessions/conflict/retry-cleanup")
        header_resume = await client.get(
            f"/api/sessions/{snapshot.id}/events?after=4",
            headers={"Last-Event-ID": "9"},
        )
        query_resume = await client.get(
            f"/api/sessions/{snapshot.id}/events?after=11",
            headers={"Last-Event-ID": "5"},
        )
        invalid_resume = await client.get(
            f"/api/sessions/{snapshot.id}/events",
            headers={"Last-Event-ID": "not-an-event-id"},
        )

    assert ok.status_code == 200
    assert missing.status_code == 404
    assert conflict.status_code == 409
    assert conflict.json()["detail"] == "redo conflict"
    assert unavailable.status_code == 503
    assert unavailable.json()["detail"] == (
        "Ring state is temporarily unavailable. Refresh before retrying this action."
    )
    assert "provider detail" not in unavailable.text
    assert cleanup_ok.status_code == 200
    assert cleanup_missing.status_code == 404
    assert cleanup_conflict.status_code == 409
    assert cleanup_conflict.json()["detail"] == "cleanup conflict"
    assert header_resume.status_code == 200
    assert query_resume.status_code == 200
    assert event_calls == [(snapshot.id, 9), (snapshot.id, 11)]
    assert invalid_resume.status_code == 400
    assert invalid_resume.json()["detail"] == "Last-Event-ID must be a nonnegative integer"


async def test_a_control_action_on_a_fight_card_this_process_lost_explains_the_ring(
    monkeypatch,
) -> None:
    """A ring the process cannot see into must not answer `Session not found`.

    The incident, 2026-08-23. The deployed app was driven end to end for the
    first time. An arm succeeded, the run that followed reached a server process
    that had no record of the session, and the operator got a bare 404 while the
    durable ring went on holding the armed lease for the rest of its window. On
    stage that reads as the round dying at the bell with no reason given, and
    then refusing to start again for no visible reason either -- which is the
    exact failure mode this project is built to avoid.

    The bare 404 is the part that must never come back. The armed fight card
    itself is process-local by construction, so this cannot promise the run will
    survive; it can promise the operator is told what happened, that nothing ran,
    and how long the ring is theirs to wait out.
    """

    monkeypatch.delenv("DATABRICKS_APP_NAME", raising=False)
    installation = "aa11bb22cc33dd44"
    store = InMemoryBoutLeaseStore()
    # Round isolation on, because that is the deployed shape: the installation-wide
    # ring goes unused and the armed row sits on the round's own key, so a search
    # that only read the main ring would report a free ring and explain nothing.
    manager = RunManager(
        lease_store=store,
        round_isolation=True,
        installation_id=installation,
    )
    orphan = "0f6b3d9e4c1a4f7b9d2e5a8c1b4d7e00"
    round_ring = store.for_ring_key(
        round_ring_key(installation, RoundId.ANALYZE_LIVE_ORDERS.value)
    )
    claimed = await round_ring.claim(
        session_id=orphan,
        operator=BoutOperator(display_name="Local operator", subject="local-operator"),
        phase="checking",
        session_state=SessionState.CHECKING,
        round_id="analyze_live_orders_without_slowing_checkout",
        round_title="Analyze live orders without slowing checkout",
        competitor_id="aurora_serverless_v2",
        competitor_name="Aurora Serverless v2",
        ttl=timedelta(seconds=90),
    )
    await round_ring.transition(
        claimed,
        operator=BoutOperator(display_name="Local operator", subject="local-operator"),
        expected_phase="checking",
        phase="armed",
        session_state=SessionState.ARMED,
        ttl=timedelta(seconds=180),
    )

    api_app = FastAPI()
    api_app.include_router(router)
    api_app.state.run_manager = manager
    async with AsyncClient(
        transport=ASGITransport(app=api_app),
        base_url="http://anti-demo.test",
    ) as client:
        orphaned_run = await client.post(f"/api/sessions/{orphan}/run")
        unknown_run = await client.post("/api/sessions/never-existed/run")

    # Still a 404: the browser clears a stale live snapshot on exactly this
    # status, and turning it into a 409 would strand the screen it is presenting.
    assert orphaned_run.status_code == 404
    orphaned_detail = orphaned_run.json()["detail"]
    assert orphaned_detail != "Session not found"
    assert "ANALYZE LIVE ORDERS WITHOUT SLOWING CHECKOUT" in orphaned_detail
    assert "ARMED" in orphaned_detail
    assert "RING UNLOCKS IN" in orphaned_detail
    assert "nothing ran" in orphaned_detail
    assert "prepare the fight card again" in orphaned_detail

    # A session id the ring has never heard of is a different fact and says so,
    # rather than blaming a ring that is free.
    assert unknown_run.status_code == 404
    unknown_detail = unknown_run.json()["detail"]
    assert unknown_detail != "Session not found"
    assert "RING UNLOCKS IN" not in unknown_detail
    assert "no ring lease names it" in unknown_detail


async def test_session_creation_returns_conflict_while_setup_is_not_ready(
    monkeypatch,
) -> None:
    monkeypatch.delenv("DATABRICKS_APP_NAME", raising=False)
    monkeypatch.delenv("ANTI_DEMO_ENV", raising=False)
    monkeypatch.delenv("ANTI_DEMO_COORDINATION_ENDPOINT_NAME", raising=False)
    monkeypatch.setenv(ALLOW_INMEMORY_COORDINATION_ENV, "1")

    def not_ready() -> None:
        raise InvalidStateError("Demo setup is currently SEEDING, not READY")

    monkeypatch.setattr(app_module, "require_ready_manifest", not_ready)
    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://anti-demo.test",
        ) as client:
            created = await client.post(
                "/api/sessions",
                json={
                    "competitor": "aurora_serverless_v2",
                    "primary_persona": "sre",
                    "secondary_personas": [],
                    "corners": ["performance"],
                },
            )

    assert created.status_code == 409
    assert created.json()["detail"] == "Demo setup is currently SEEDING, not READY"


def test_operator_identity_uses_only_trusted_databricks_app_headers(monkeypatch) -> None:
    headers = [
        (b"x-forwarded-preferred-username", b"operator@example.com"),
        (b"x-forwarded-email", b"operator@example.com"),
    ]
    request = Request({"type": "http", "headers": headers})

    monkeypatch.delenv("DATABRICKS_APP_NAME", raising=False)
    monkeypatch.delenv("ANTI_DEMO_LOCAL_OPERATOR", raising=False)
    assert operator_from_request(request).display_name == "Local operator"

    monkeypatch.setenv("DATABRICKS_APP_NAME", "lakebase-anti-demo")
    operator = operator_from_request(request)
    assert operator.display_name == "Operator"
    assert operator.email == "operator@example.com"
    assert operator.subject == "operator@example.com"


def test_databricks_app_requires_trusted_sso_identity_headers(monkeypatch) -> None:
    request = Request({"type": "http", "headers": []})
    monkeypatch.setenv("DATABRICKS_APP_NAME", "lakebase-anti-demo")

    try:
        operator_from_request(request)
    except Exception as exc:
        assert getattr(exc, "status_code", None) == 401
    else:
        raise AssertionError("Missing Databricks SSO headers must be rejected")


async def test_databricks_app_aws_preflight_runs_before_manager(monkeypatch) -> None:
    monkeypatch.setenv("ANTI_DEMO_ENV", "databricks-app")
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", APP_CLIENT_ID)
    monkeypatch.setattr(app_module, "load_manifest", deployed_manifest)
    monkeypatch.setattr(
        app_module,
        "WorkspaceClient",
        lambda: SimpleNamespace(
            current_user=SimpleNamespace(
                me=lambda: SimpleNamespace(user_name=APP_CLIENT_ID)
            )
        ),
    )

    def fail_preflight(_environment) -> None:
        raise RuntimeError("wrong AWS account")

    monkeypatch.setattr(app_module, "validate_app_aws_environment", fail_preflight)
    monkeypatch.setattr(
        app_module,
        "build_lease_store",
        lambda: pytest.fail("lease store must not be built before AWS preflight"),
    )

    with pytest.raises(RuntimeError, match="wrong AWS account"):
        async with app.router.lifespan_context(app):
            pass


@pytest.mark.parametrize("mirrors_an_rds_instance", [True, False])
async def test_databricks_app_startup_derives_sealed_runtime_bindings(
    monkeypatch, mirrors_an_rds_instance
) -> None:
    """Both shapes of the Round 1 legacy mirror must start the deployed app.

    ``aws.resources`` is Round 1's flat mirror, and under v7 its RDS fields are
    empty because Round 1 stands no RDS instance up -- its lane refuses to enter
    on engine semantics, so Terraform provisions none. That emptiness is not
    drift to be repaired by a reseal: ``manifest._require_round1_legacy_mirror``
    *forbids* those fields from carrying another round's box, because a seal that
    named one would send the arming path off to describe a resource Round 1 does
    not have. So the deployed binding step cannot require them, and this covers
    the empty shape alongside the pre-v7 populated one.
    """

    manifest = deployed_manifest()
    if not mirrors_an_rds_instance:
        manifest.aws.resources.rds_instance_id = ""
        manifest.aws.resources.rds_secret_arn = ""
    for name in ("RDS_INSTANCE_ID", "RDS_SECRET_ARN"):
        monkeypatch.delenv(name, raising=False)
    events: list[str] = []
    monkeypatch.setenv("ANTI_DEMO_ENV", "databricks-app")
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", APP_CLIENT_ID)
    monkeypatch.setenv("AWS_PROFILE", "must-be-removed")
    monkeypatch.setenv("AWS_DEFAULT_PROFILE", "must-be-removed")
    monkeypatch.setenv("DATABRICKS_PROFILE", "must-be-removed")
    monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", "must-be-removed")
    monkeypatch.setattr(app_module, "load_manifest", lambda: manifest)
    monkeypatch.setattr(
        app_module,
        "WorkspaceClient",
        lambda: events.append("workspace_identity")
        or SimpleNamespace(
            current_user=SimpleNamespace(
                me=lambda: SimpleNamespace(user_name=APP_CLIENT_ID)
            )
        ),
    )

    def validate_aws(environment) -> None:
        events.append("aws_preflight")
        assert environment["AWS_AUTH_MODE"] == "environment"
        assert environment["LAKEBASE_USER"] == APP_CLIENT_ID
        assert environment["ANTI_DEMO_COORDINATION_USER"] == APP_CLIENT_ID
        assert environment["AURORA_CLUSTER_ID"] == "owned-aurora"
        # The real gate rather than a hand-picked subset of it. The startup path
        # runs `validate_app_aws_environment` three lines after the binding step,
        # and that function rejects an incomplete `APP_AWS_BINDINGS` set on its
        # own -- so a binding the seal cannot supply has to stop being demanded in
        # *both* places or the app still refuses to start, one gate later.
        assert [name for name in APP_AWS_BINDINGS if not environment.get(name, "")] == []
        if mirrors_an_rds_instance:
            assert environment["RDS_INSTANCE_ID"] == "owned-rds"
        else:
            assert "RDS_INSTANCE_ID" not in environment
            assert "RDS_SECRET_ARN" not in environment
        assert "AWS_PROFILE" not in environment
        assert "AWS_DEFAULT_PROFILE" not in environment
        assert "DATABRICKS_PROFILE" not in environment
        assert "DATABRICKS_CONFIG_PROFILE" not in environment

    class FakeLeaseStore:
        mode = "lakebase"

        def __init__(self, ring_key: str) -> None:
            self.ring_key = ring_key

        async def initialize(self) -> None:
            events.append(f"coordination_initialize:{self.ring_key}")

        async def close(self) -> None:
            events.append(f"coordination_close:{self.ring_key}")

    monkeypatch.setattr(app_module, "validate_app_aws_environment", validate_aws)
    class FakeReadinessGate:
        status = SimpleNamespace(
            ring_ready=False,
            maintenance_state="maintenance",
            maintenance_detail="BACKSTAGE CLEANUP IN PROGRESS",
        )
        round5_status = SimpleNamespace(
            ring_ready=True,
            maintenance_state="ready",
            maintenance_detail=None,
        )

        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def require_ready(self) -> None:
            raise InvalidStateError("BACKSTAGE CLEANUP IN PROGRESS")

        def require_round5_ready(self) -> None:
            return None

        async def round5_prearm_guard(
            self,
            _session_id: str,
            _fencing_token: int,
        ) -> None:
            return None

        async def run(self) -> None:
            return None

    monkeypatch.setattr(app_module, "ShowtimeReadinessGate", FakeReadinessGate)
    def fake_build_lease_store(*, ring_key: str = "main") -> FakeLeaseStore:
        events.append(f"build_lease_store:{ring_key}")
        return FakeLeaseStore(ring_key)

    monkeypatch.setattr(app_module, "build_lease_store", fake_build_lease_store)

    async with app.router.lifespan_context(app):
        assert app.state.coordination_mode == "lakebase"
        assert app.state.run_manager._lease_store.ring_key == "main"
        assert app.state.run_manager._round5_lease_store.ring_key == "round5"
        assert app.state.run_manager._lease_store is not app.state.run_manager._round5_lease_store

    assert events == [
        "workspace_identity",
        "aws_preflight",
        "build_lease_store:main",
        "build_lease_store:round5",
        "coordination_initialize:main",
        "coordination_initialize:round5",
        "coordination_close:round5",
        "coordination_close:main",
    ]


@pytest.mark.parametrize(
    ("injected_client_id", "ambient_user", "message"),
    [
        ("different-client", APP_CLIENT_ID, "client ID"),
        (APP_CLIENT_ID, "different-client", "Ambient Databricks identity"),
    ],
)
async def test_databricks_app_startup_rejects_identity_mismatch_before_external_access(
    monkeypatch, injected_client_id, ambient_user, message
) -> None:
    monkeypatch.setenv("ANTI_DEMO_ENV", "databricks-app")
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", injected_client_id)
    monkeypatch.setattr(app_module, "load_manifest", deployed_manifest)
    monkeypatch.setattr(
        app_module,
        "WorkspaceClient",
        lambda: SimpleNamespace(
            current_user=SimpleNamespace(
                me=lambda: SimpleNamespace(user_name=ambient_user)
            )
        ),
    )
    monkeypatch.setattr(
        app_module,
        "validate_app_aws_environment",
        lambda environment: pytest.fail("STS validation must follow app identity checks"),
    )
    monkeypatch.setattr(
        app_module,
        "build_lease_store",
        lambda: pytest.fail("coordination must follow app identity checks"),
    )

    with pytest.raises(InvalidStateError, match=message):
        async with app.router.lifespan_context(app):
            pass


async def test_readyz_names_the_process_local_ring_instead_of_reporting_ready(
    monkeypatch,
) -> None:
    """The degraded process must not answer /readyz the way a healthy one does.

    Its readiness gate is a stub whose status is a constant, so `ring_ready`
    alone said "ready" while nothing had checked anything. A monitor comparing
    status to "ready" now fails, and the payload says why.
    """
    monkeypatch.delenv("DATABRICKS_APP_NAME", raising=False)
    monkeypatch.delenv("ANTI_DEMO_ENV", raising=False)
    monkeypatch.delenv("ANTI_DEMO_COORDINATION_ENDPOINT_NAME", raising=False)
    monkeypatch.setenv(ALLOW_INMEMORY_COORDINATION_ENV, "1")
    monkeypatch.setattr(app_module, "require_ready_manifest", lambda: None)
    # This test is about the coordination half of the degraded vocabulary. The
    # credential probe writes into the same fields, so it is pinned rather than
    # left to race the request.
    monkeypatch.setattr(app_module, "CredentialSentry", _HealthyCredentialSentry)

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            response = await client.get("/readyz")
            alias = await client.get("/api/ready")

    payload = response.json()
    assert response.status_code == 200
    assert payload["status"] == "degraded"
    assert payload["degraded"] is True
    assert payload["coordination_mode"] == "memory"
    assert payload["coordination_durable"] is False
    assert payload["readiness_verified"] is False
    assert "ring_ready is a constant here" in payload["degraded_detail"]
    assert payload["degraded_capabilities"] == list(INMEMORY_COORDINATION_LOSSES)
    # The stub still reports its constant; the point is that it no longer stands
    # alone as the whole answer.
    assert payload["ring_ready"] is True
    assert alias.json() == payload


async def test_readyz_reports_a_durable_ring_as_ready_and_not_degraded(
    monkeypatch,
) -> None:
    manifest = deployed_manifest()
    monkeypatch.setenv("ANTI_DEMO_ENV", "databricks-app")
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", APP_CLIENT_ID)
    monkeypatch.setattr(app_module, "load_manifest", lambda: manifest)
    monkeypatch.setattr(
        app_module,
        "WorkspaceClient",
        lambda: SimpleNamespace(
            current_user=SimpleNamespace(
                me=lambda: SimpleNamespace(user_name=APP_CLIENT_ID)
            )
        ),
    )
    monkeypatch.setattr(app_module, "validate_app_aws_environment", lambda _env: None)

    class FakeLeaseStore:
        mode = "lakebase"

        def __init__(self, ring_key: str) -> None:
            self.ring_key = ring_key

        async def initialize(self) -> None:
            return None

        async def close(self) -> None:
            return None

    class FakeReadinessGate:
        status = SimpleNamespace(
            ring_ready=True,
            maintenance_state="ready",
            maintenance_detail=None,
        )
        round5_status = status

        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def require_ready(self) -> None:
            return None

        def require_round5_ready(self) -> None:
            return None

        async def round5_prearm_guard(
            self,
            _session_id: str,
            _fencing_token: int,
        ) -> None:
            return None

        async def run(self) -> None:
            return None

    monkeypatch.setattr(
        app_module,
        "build_lease_store",
        lambda *, ring_key="main": FakeLeaseStore(ring_key),
    )
    monkeypatch.setattr(app_module, "ShowtimeReadinessGate", FakeReadinessGate)
    monkeypatch.setattr(app_module, "CredentialSentry", _HealthyCredentialSentry)

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            response = await client.get("/readyz")
            # The manifest goes bad *underneath a process that is already up*,
            # which is the only way this state is reachable: the deployed path
            # loads a ready manifest at startup and refuses to boot otherwise.
            # On the installation that found this, the process bound a `ready`
            # manifest at 22:58:09Z, `cleanup_failed` was written at 23:28:56Z,
            # and /readyz was still answering `ready` at 23:55:18Z.
            manifest.status = "cleanup_failed"
            refused = await client.get("/readyz")

    payload = response.json()
    assert response.status_code == 200
    assert payload["status"] == "ready"
    assert payload["degraded"] is False
    assert payload["credentials_state"] == "ok"
    assert payload["coordination_mode"] == "lakebase"
    assert payload["coordination_durable"] is True
    assert payload["readiness_verified"] is True
    assert payload["degraded_detail"] is None
    assert payload["degraded_capabilities"] == []
    assert payload["recovery_state"] == "settled"
    assert payload["recovering"] is False
    # Present and quiet, never absent: a reader must not have to tell a healthy
    # manifest apart from a key this endpoint forgot to publish.
    assert payload["manifest_status"] == "ready"
    assert payload["manifest_lifecycle_state"] == "ready"
    assert payload["manifest_lifecycle_detail"] is None

    # Same durable ring, same verified gate, same healthy credentials -- only the
    # manifest changed. `ShowtimeReadinessGate.require_ready` calls the manifest
    # check *before* it looks at `ring_ready`, so every round now refuses to arm.
    # This endpoint reported `ready`/`degraded: false` straight through that, and
    # it is the surface an operator and a monitor reach without typing anything.
    stopped = refused.json()
    assert stopped["ring_ready"] is True
    assert stopped["readiness_verified"] is True
    assert stopped["credentials_state"] == "ok"
    assert stopped["manifest_status"] == "cleanup_failed"
    assert stopped["manifest_lifecycle_state"] == "refused"
    assert "cleanup_failed" in stopped["manifest_lifecycle_detail"]
    assert "not resumable" in stopped["manifest_lifecycle_detail"]
    assert stopped["status"] == "degraded"
    assert stopped["degraded"] is True
    # Degraded, never a 503: the process can still serve the page that explains
    # why nothing arms, and taking it out of rotation would hide that.
    assert refused.status_code == 200
    # It ranks last for the one detail sentence, so it may only take a slot that
    # nothing else wanted. Here nothing else did.
    assert stopped["degraded_detail"] == stopped["manifest_lifecycle_detail"]


class _DurableFakeLeaseStore:
    mode = "lakebase"

    def __init__(self, ring_key: str) -> None:
        self.ring_key = ring_key

    async def initialize(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _ReadyGate:
    status = SimpleNamespace(ring_ready=True, maintenance_state="ready", maintenance_detail=None)
    round5_status = status

    def __init__(self, *_args, **_kwargs) -> None:
        pass

    def require_ready(self) -> None:
        return None

    def require_round5_ready(self) -> None:
        return None

    async def round5_prearm_guard(self, _session_id: str, _fencing_token: int) -> None:
        return None

    def notify_credentials_recovered(self) -> None:
        """Part of the real gate's surface, so the stub has to carry it.

        The lifespan hands this to the credential sentry. A stub without it
        would let the wiring silently go missing while every test still passed.
        """
        return None

    async def run(self) -> None:
        return None


class _HealthyCredentialSentry:
    """A credential probe that has already answered, without asking AWS.

    The real one is started by every lifespan. Left unstubbed it correctly finds
    no credentials -- `tests/conftest.py` strips them from the environment on
    purpose -- and correctly reports the process as degraded, which is right in
    production and drowns out whatever a test about something else was asserting.
    Tests about the credential surface build their own verdicts instead.
    """

    def __init__(self, *_args, **_kwargs) -> None:
        self._verdict = CredentialVerdict(
            state="ok", account="123456789012", checked_at_monotonic=1.0
        )

    def verdict(self) -> CredentialVerdict:
        return self._verdict

    async def run(self) -> None:
        return None


def _fixed_credential_verdict(monkeypatch, verdict: CredentialVerdict) -> None:
    """Serve one verdict, from a probe that makes no call and never re-asks."""

    class Fixed(_HealthyCredentialSentry):
        def __init__(self, *args, **kwargs) -> None:
            self._verdict = verdict

    monkeypatch.setattr(app_module, "CredentialSentry", Fixed)


def _deployed_app_runtime(monkeypatch, manifest) -> None:
    """Everything a deployed lifespan needs, with no cloud call behind any of it."""

    monkeypatch.setattr(app_module, "CredentialSentry", _HealthyCredentialSentry)
    monkeypatch.setenv("ANTI_DEMO_ENV", "databricks-app")
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", APP_CLIENT_ID)
    monkeypatch.setattr(app_module, "load_manifest", lambda: manifest)
    monkeypatch.setattr(
        app_module,
        "WorkspaceClient",
        lambda: SimpleNamespace(
            current_user=SimpleNamespace(me=lambda: SimpleNamespace(user_name=APP_CLIENT_ID))
        ),
    )
    monkeypatch.setattr(app_module, "validate_app_aws_environment", lambda _env: None)
    monkeypatch.setattr(
        app_module,
        "build_lease_store",
        lambda *, ring_key="main": _DurableFakeLeaseStore(ring_key),
    )
    monkeypatch.setattr(app_module, "ShowtimeReadinessGate", _ReadyGate)


_SESSION_BODY = {
    "competitor": "aurora_serverless_v2",
    "primary_persona": "sre",
    "secondary_personas": [],
    "corners": ["performance"],
}


async def test_a_mid_mutation_manifest_is_a_wait_state_not_a_dead_server(
    monkeypatch,
    tmp_path,
) -> None:
    """A transitional status used to stop the server from starting at all.

    A reseal that died inside `wait_for_scale_zero` left `waiting_for_zero`
    behind, `_load_ready_manifest` raised, the retry loop called it
    non-transient, and the lifespan died -- so an interrupted mutation became an
    outage that outlived it. The server now serves the truth and waits.
    """

    manifest_file = tmp_path / "manifest.json"
    manifest_file.write_text('{"status": "waiting_for_zero"}\n', encoding="utf-8")
    original_bytes = manifest_file.read_bytes()
    original_mtime = manifest_file.stat().st_mtime_ns
    monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(manifest_file))
    monkeypatch.setattr(app_module, "MUTATION_WAIT_POLL_SECONDS", 0.01)
    manifest = deployed_manifest()
    manifest.status = "waiting_for_zero"
    _deployed_app_runtime(monkeypatch, manifest)

    assert generation_lock._HELD_BY_THIS_PROCESS == {}

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            waiting = await client.get("/readyz")
            payload = waiting.json()
            # Live, and honest about being unusable: a monitor comparing status
            # to "ready" still fails, which is the whole point.
            assert waiting.status_code == 503
            assert payload["status"] == "not_ready"
            assert payload["coordination_mode"] == "pending"
            assert payload["coordination_durable"] is False
            assert payload["degraded"] is True
            # Nothing holds the lock, so this status was abandoned rather than
            # in flight, and it will not clear itself.
            assert payload["recovery_state"] == "given_up"
            assert payload["recovering"] is False
            assert "INTERRUPTED WHILE WAITING_FOR_ZERO" in payload["maintenance_detail"]

            refused = await client.post("/api/sessions", json=_SESSION_BODY)
            assert refused.status_code == 409
            assert "WAITING_FOR_ZERO, not READY" in refused.json()["detail"]

            bout = await client.get("/api/bout")
            assert bout.status_code == 200
            assert bout.json()["can_start"] is False

            # A mutator claims the generation: the same wait is now one that
            # will clear itself, and the copy says so.
            with generation_lock.hold_generation(
                manifest_file,
                "antidemo reset",
                environ={},
                argv=("antidemo", "reset"),
            ):
                for _ in range(500):
                    if app.state.readiness_gate.recovery.state == "retrying":
                        break
                    await asyncio.sleep(0.01)
                in_flight = (await client.get("/readyz")).json()
                assert in_flight["recovery_state"] == "retrying"
                assert in_flight["recovering"] is True
                assert "A MUTATION IS IN PROGRESS" in in_flight["maintenance_detail"]

            # The mutation finishes. Nothing here wrote that status.
            manifest.status = "ready"
            for _ in range(500):
                if getattr(app.state, "coordination_mode", "") == "lakebase":
                    break
                await asyncio.sleep(0.01)

            settled = await client.get("/readyz")
            assert settled.status_code == 200
            assert settled.json()["status"] == "ready"
            assert settled.json()["recovery_state"] == "settled"
            assert app.state.run_manager is not None

    # No mutation from the serving process, in either state: the manifest is
    # byte-for-byte what it was, and nothing here ever became a lock holder.
    assert manifest_file.read_bytes() == original_bytes
    assert manifest_file.stat().st_mtime_ns == original_mtime
    assert generation_lock._HELD_BY_THIS_PROCESS == {}


async def test_a_corrupt_or_missing_manifest_still_fails_loudly(monkeypatch) -> None:
    """Only a mid-mutation status is survivable. Everything else must still die."""

    monkeypatch.setattr(app_module, "MUTATION_WAIT_POLL_SECONDS", 0.01)
    manifest = deployed_manifest()
    manifest.status = "cleanup_failed"
    _deployed_app_runtime(monkeypatch, manifest)

    with pytest.raises(InvalidStateError, match="CLEANUP_FAILED, not READY"):
        async with app.router.lifespan_context(app):
            pass

    def unloadable():
        raise RuntimeError("No owned demo manifest exists at /nowhere/manifest.json")

    monkeypatch.setattr(app_module, "load_manifest", unloadable)
    with pytest.raises(InvalidStateError, match="owned manifest is unavailable"):
        async with app.router.lifespan_context(app):
            pass

    wrong_generation = deployed_manifest()
    wrong_generation.manifest_version = 1
    monkeypatch.setattr(app_module, "load_manifest", lambda: wrong_generation)
    with pytest.raises(InvalidStateError, match="manifest v2 or newer"):
        async with app.router.lifespan_context(app):
            pass


async def test_the_serving_process_is_never_a_mutation_lock_holder(
    monkeypatch,
    tmp_path,
) -> None:
    """`antidemo serve` releases the lock before serving; this proves it stays released.

    A long-lived holder would make `antidemo setup` impossible without stopping the
    demo, which is why the lock exists in the shape it does. The proof that
    matters is behavioural: a mutator can take the generation while the server
    is answering requests.
    """

    manifest_file = tmp_path / "manifest.json"
    manifest_file.write_text('{"status": "ready"}\n', encoding="utf-8")
    original_bytes = manifest_file.read_bytes()
    original_mtime = manifest_file.stat().st_mtime_ns
    monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(manifest_file))
    _deployed_app_runtime(monkeypatch, deployed_manifest())

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            assert (await client.get("/readyz")).status_code == 200
            assert generation_lock._HELD_BY_THIS_PROCESS == {}
            with generation_lock.hold_generation(
                manifest_file,
                "antidemo setup",
                environ={},
                argv=("antidemo", "setup"),
            ) as lock:
                # Granted, and granted outright rather than inherited: the
                # serving process was not holding it.
                assert lock.inherited is False
                assert (await client.get("/readyz")).status_code == 200

    assert manifest_file.read_bytes() == original_bytes
    assert manifest_file.stat().st_mtime_ns == original_mtime
    assert generation_lock._HELD_BY_THIS_PROCESS == {}
    # And statically: the serving module has no way to write a manifest or hold
    # a generation, so no future edit can start doing it by accident.
    source = inspect.getsource(app_module)
    assert "save_manifest(" not in source
    assert "hold_generation(" not in source
    assert not hasattr(app_module, "save_manifest")
    assert not hasattr(app_module, "hold_generation")


@pytest.mark.parametrize(
    ("state", "recovering", "expected_detail"),
    [
        ("retrying", True, "RECOVERY IS IN PROGRESS"),
        ("escalated", True, "RECOVERY HAS OUTLASTED ITS BUDGET"),
        ("given_up", False, "RECOVERY HAS STOPPED"),
    ],
)
async def test_readyz_separates_a_retrying_gate_from_one_that_has_given_up(
    monkeypatch,
    state,
    recovering,
    expected_detail,
) -> None:
    """"Not ready" is the same word for a blip and for a dead end.

    An operator and a monitor both have to be able to tell which one they are
    looking at, so the recovery state is reported beside it.
    """

    manifest = deployed_manifest()
    _deployed_app_runtime(monkeypatch, manifest)

    class RecoveringGate(_ReadyGate):
        status = SimpleNamespace(
            ring_ready=False,
            maintenance_state="blocked" if state != "retrying" else "maintenance",
            maintenance_detail="BACKSTAGE CLEANUP RETRYING · ATTEMPT 4 FAILED",
        )
        round5_status = status
        recovery = RecoveryState(
            state,
            attempts=4,
            detail="BACKSTAGE CLEANUP RETRYING · ATTEMPT 4 FAILED",
            next_attempt_seconds=16.0,
            error="OperationalError",
        )

    monkeypatch.setattr(app_module, "ShowtimeReadinessGate", RecoveringGate)

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            response = await client.get("/readyz")

    payload = response.json()
    assert response.status_code == 503
    assert payload["status"] == "not_ready"
    assert payload["recovery_state"] == state
    assert payload["recovering"] is recovering
    assert payload["recovery_attempts"] == 4
    assert payload["recovery_error"] == "OperationalError"
    assert payload["recovery_next_attempt_seconds"] == 16.0
    assert payload["degraded"] is True
    assert expected_detail in payload["degraded_detail"]
    # The durable ring is still the durable ring; being mid-recovery does not
    # relabel the coordination mode.
    assert payload["coordination_mode"] == "lakebase"


async def test_readyz_says_it_is_waiting_on_credentials_rather_than_broken(
    monkeypatch,
) -> None:
    """An expired SSO session and a thrashing server both read as "retrying".

    They need completely different responses -- one clears itself, and the other
    is the only one a restart could conceivably help -- so the health surface has
    to name *what* is being waited for, not just that something is.
    """

    manifest = deployed_manifest()
    _deployed_app_runtime(monkeypatch, manifest)

    class WaitingOnCredentialsGate(_ReadyGate):
        status = SimpleNamespace(
            ring_ready=False,
            maintenance_state="maintenance",
            maintenance_detail=(
                "BACKSTAGE CLEANUP RETRYING · ATTEMPT 7 FAILED "
                "(SAFECHANGERESETERROR) · NEXT ATTEMPT IN 60S · "
                "WAITING ON AWS CREDENTIALS"
            ),
        )
        round5_status = status
        recovery = RecoveryState(
            "retrying",
            attempts=7,
            detail=status.maintenance_detail,
            next_attempt_seconds=60.0,
            error="SafeChangeResetError",
            waiting_on="AWS credentials",
        )

    monkeypatch.setattr(app_module, "ShowtimeReadinessGate", WaitingOnCredentialsGate)

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            response = await client.get("/readyz")

    payload = response.json()
    assert response.status_code == 503
    assert payload["recovery_state"] == "retrying"
    # Still recovering, indefinitely, and no attempt cap is implied anywhere.
    assert payload["recovering"] is True
    assert payload["recovery_waiting_on"] == "AWS credentials"
    detail = payload["degraded_detail"]
    assert "WAITING ON AWS CREDENTIALS, NOT BROKEN" in detail
    assert "no restart is needed" in detail


async def test_readyz_reports_a_round5_reconciler_that_has_stopped(monkeypatch) -> None:
    """A ready ring can still be hiding a reconciler that quietly died.

    Round 5's loop lives for the whole process, so it fails long after startup.
    It does not make the ring unready -- the other rounds are fine -- but leaving
    it out of the health surface is exactly how drift goes unnoticed on a box
    nobody is watching.
    """

    manifest = deployed_manifest()
    _deployed_app_runtime(monkeypatch, manifest)

    class StalledRound5Gate(_ReadyGate):
        round5_recovery = RecoveryState(
            "given_up",
            attempts=1,
            detail="ROUND 5 SETTLEMENT IS BLOCKED · NOT RETRYING",
            error="RuntimeError",
        )

    monkeypatch.setattr(app_module, "ShowtimeReadinessGate", StalledRound5Gate)

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            response = await client.get("/readyz")

    payload = response.json()
    # The ring really is ready, so this stays a 200 and the other rounds keep
    # running. It is not "ready" though, which is what a monitor needs to see.
    assert response.status_code == 200
    assert payload["ring_ready"] is True
    # A monitor comparing status against "ready" has to fail here rather than be
    # reassured, which is the whole point of the three-value status.
    assert payload["status"] == "degraded"
    assert payload["recovery_state"] == "settled"
    assert payload["round5_recovery_state"] == "given_up"
    assert payload["round5_recovery_detail"] == "ROUND 5 SETTLEMENT IS BLOCKED · NOT RETRYING"
    assert payload["degraded"] is True
    assert "ROUND 5 RECONCILER HAS STOPPED" in payload["degraded_detail"]


async def test_readyz_refuses_to_say_ready_when_aws_has_rejected_the_credentials(
    monkeypatch,
) -> None:
    """The failure this probe exists for: green health, dead lanes.

    Everything local is fine here -- durable ring, verified gate, settled
    recovery -- and no round can run, because AWS is refusing the keys the
    process is holding. A status of "ready" would be a lie told through the one
    field a monitor is watching.
    """

    manifest = deployed_manifest()
    _deployed_app_runtime(monkeypatch, manifest)
    _fixed_credential_verdict(
        monkeypatch,
        CredentialVerdict(
            state="rejected",
            detail="AWS REJECTED THE CREDENTIALS IN THIS PROCESS (InvalidClientTokenId)",
            recovery=RecoveryState("escalated", attempts=2, error="rejected"),
            checked_at_monotonic=1.0,
        ),
    )

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            response = await client.get("/readyz")

    payload = response.json()
    # Still a 200 and still serving: the ring is genuinely ready, and a probe
    # observation is not allowed to present itself as the server having failed.
    assert response.status_code == 200
    assert payload["ring_ready"] is True
    assert payload["status"] == "degraded"
    assert payload["degraded"] is True
    assert payload["credentials_state"] == "rejected"
    assert payload["credentials_recovery_state"] == "escalated"
    assert payload["credentials_recovery_attempts"] == 2
    assert "InvalidClientTokenId" in payload["degraded_detail"]
    assert any("every AWS lane" in loss for loss in payload["degraded_capabilities"])
    # The ring's own vocabulary is untouched: this is not a coordination fault
    # and must not be reported as one.
    assert payload["coordination_durable"] is True
    assert payload["recovery_state"] == "settled"


async def test_a_credential_fault_that_stops_every_lane_outranks_the_ring_for_the_detail(
    monkeypatch,
) -> None:
    """There is one `degraded_detail` slot, so the precedence has to be decided.

    A ring being retried does not matter to anybody if no bout can reach AWS at
    all, so the more fundamental fault wins the sentence -- and the state that
    lost the slot is still readable in a field of its own.
    """

    manifest = deployed_manifest()
    _deployed_app_runtime(monkeypatch, manifest)

    class RetryingGate(_ReadyGate):
        recovery = RecoveryState("retrying", attempts=1, error="OperationalError")

    monkeypatch.setattr(app_module, "ShowtimeReadinessGate", RetryingGate)
    _fixed_credential_verdict(
        monkeypatch,
        CredentialVerdict(
            state="unpermitted",
            detail="THE AWS CREDENTIALS IN THIS PROCESS ARE VALID BUT NOT PERMITTED",
            recovery=RecoveryState("escalated", attempts=2, error="unpermitted"),
            checked_at_monotonic=1.0,
        ),
    )

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            payload = (await client.get("/readyz")).json()

    assert payload["degraded_detail"] == (
        "THE AWS CREDENTIALS IN THIS PROCESS ARE VALID BUT NOT PERMITTED"
    )
    assert payload["recovery_state"] == "retrying"
    assert payload["credentials_state"] == "unpermitted"


async def test_a_principal_round5_does_not_trust_does_not_take_the_ring_detail(
    monkeypatch,
) -> None:
    """A narrow fault reported narrowly.

    Round 5 alone is broken, so it degrades the status and names its own loss,
    but it must not overwrite a sentence about something wider -- and it must
    not claim the other five rounds are down when they are not.
    """

    manifest = deployed_manifest()
    _deployed_app_runtime(monkeypatch, manifest)

    class RetryingGate(_ReadyGate):
        recovery = RecoveryState("retrying", attempts=1, error="OperationalError")

    monkeypatch.setattr(app_module, "ShowtimeReadinessGate", RetryingGate)
    _fixed_credential_verdict(
        monkeypatch,
        CredentialVerdict(
            state="principal_mismatch",
            detail="THIS PROCESS IS NOT THE PRINCIPAL ROUND 5 TRUSTS",
            recovery=RecoveryState("escalated", attempts=2, error="principal_mismatch"),
            checked_at_monotonic=1.0,
        ),
    )

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            payload = (await client.get("/readyz")).json()

    assert payload["credentials_state"] == "principal_mismatch"
    assert payload["degraded"] is True
    assert "RECOVERY IS IN PROGRESS" in payload["degraded_detail"]
    losses = payload["degraded_capabilities"]
    assert any("Round 5" in loss for loss in losses)
    assert not any("every AWS lane" in loss for loss in losses)


async def test_readyz_does_not_probe_aws_no_matter_how_often_it_is_polled(
    monkeypatch,
) -> None:
    """A health check must not be a way to generate AWS traffic.

    A monitor polling once a second would otherwise become one STS call and one
    RDS call per second, and a slow or hanging probe would make the endpoint
    slow with it. The endpoint reads a cached verdict and nothing else.
    """

    manifest = deployed_manifest()
    _deployed_app_runtime(monkeypatch, manifest)
    reads = 0

    class CountingSentry(_HealthyCredentialSentry):
        def verdict(self) -> CredentialVerdict:
            nonlocal reads
            reads += 1
            return self._verdict

        def check_once(self) -> CredentialVerdict:
            raise AssertionError("a request may never trigger a probe")

    monkeypatch.setattr(app_module, "CredentialSentry", CountingSentry)

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            for _ in range(25):
                assert (await client.get("/readyz")).json()["credentials_state"] == "ok"

    assert reads == 25


async def test_a_probe_that_cannot_be_read_does_not_break_the_health_surface(
    monkeypatch,
) -> None:
    """Fail soft, including here. A broken observer is not a broken server."""

    manifest = deployed_manifest()
    _deployed_app_runtime(monkeypatch, manifest)

    class BrokenSentry(_HealthyCredentialSentry):
        def verdict(self) -> CredentialVerdict:
            raise RuntimeError("boom")

    monkeypatch.setattr(app_module, "CredentialSentry", BrokenSentry)

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            response = await client.get("/readyz")

    payload = response.json()
    assert response.status_code == 200
    assert payload["status"] == "ready"
    assert payload["credentials_state"] == "unprobed"
    assert payload["credentials_checked"] is False


def test_a_start_with_no_manifest_has_nothing_to_probe_and_says_so_quietly(
    caplog,
) -> None:
    """An in-memory dev start is not a probe failure.

    Nothing is sealed on that path, so there are no expectations to compare
    against -- but the parameter was typed non-optional and the `None` reached
    `expectations_from_manifest`, which raised `AttributeError` on `manifest.aws`.
    The fail-soft handler swallowed it into a warning traceback, so every local
    start logged what looked like a broken AWS probe and reported a credential
    state indistinguishable from a real one that could not be read.
    """

    holder = FastAPI()
    with caplog.at_level(logging.WARNING, logger=app_module.LOGGER.name):
        task = app_module._start_credential_sentry(holder, None)

    assert task is None
    assert holder.state.credential_sentry is None
    assert caplog.records == []


async def test_a_credential_probe_that_will_not_start_does_not_stop_the_server(
    monkeypatch,
) -> None:
    manifest = deployed_manifest()
    _deployed_app_runtime(monkeypatch, manifest)

    def refuse(*_args, **_kwargs):
        raise RuntimeError("no probe for you")

    monkeypatch.setattr(app_module, "CredentialSentry", refuse)

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            payload = (await client.get("/readyz")).json()

    assert payload["status"] == "ready"
    assert payload["credentials_state"] == "unprobed"


async def test_the_real_probe_is_wired_in_and_reports_absent_credentials_honestly(
    monkeypatch,
) -> None:
    """No stub in this one: the lifespan must really start a real probe.

    The suite strips AWS credentials from the environment, so the honest verdict
    here is `absent` -- and a process with no credential source cannot run one
    bout, which is why it degrades. This is also the proof that the probe makes
    no network call when there is nothing to authenticate with.
    """

    manifest = deployed_manifest()
    _deployed_app_runtime(monkeypatch, manifest)
    wiring: list[str] = []
    monkeypatch.setattr(
        app_module, "CredentialSentry", _real_credential_sentry(wiring)
    )

    async with app.router.lifespan_context(app):
        for _ in range(500):
            sentry = getattr(app.state, "credential_sentry", None)
            if sentry is not None and sentry.verdict().state != "unknown":
                break
            await asyncio.sleep(0.01)
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            payload = (await client.get("/readyz")).json()

    assert payload["credentials_state"] == "absent"
    assert payload["credentials_recovery_state"] == "given_up"
    assert payload["status"] == "degraded"
    assert "NO AWS CREDENTIALS ARE CONFIGURED" in payload["degraded_detail"]
    # And the probe is gone with the runtime that started it.
    assert app.state.credential_sentry is None
    # The recovery signal really is coupled to the gate. Without this the widened
    # retry classification still works but the gate can only ever learn that
    # credentials came back by blind-polling, and the sentry -- which already
    # knows -- has nowhere to say so.
    assert wiring == ["notify_credentials_recovered"]


def _refused_startup_credentials(monkeypatch, message: str) -> None:
    """Play the deployed startup credential check being refused by AWS.

    `AwsAuthConfigurationError` and not some other exception: that is the only
    class `validate_app_aws_environment` raises for a credential it cannot use,
    and the narrowness is deliberate -- anything else out of that function is a
    fault in the check rather than a verdict about the credentials.
    """

    def refuse(_environment) -> None:
        raise AwsAuthConfigurationError(message)

    monkeypatch.setattr(app_module, "validate_app_aws_environment", refuse)


@pytest.mark.parametrize(
    ("exports_a_key_pair", "expected_state", "expected_recovery"),
    [
        # The sweep deletes the IAM users along with the databases, so the keys
        # are still in the container's environment and the principal behind them
        # is gone. AWS is the thing refusing, and it is worth re-asking.
        (True, "rejected", "retrying"),
        # Nothing exported at all. Re-asking cannot invent a key pair, which is
        # why the probe treats this state as terminal and so does this.
        (False, "absent", "given_up"),
    ],
)
async def test_a_deployed_app_with_a_broken_aws_credential_boots_and_serves(
    monkeypatch,
    exports_a_key_pair,
    expected_state,
    expected_recovery,
) -> None:
    """The container has to come up, because two rounds do not need AWS at all.

    `validate_app_aws_environment` used to raise out of the lifespan, and an
    `AwsAuthConfigurationError` is not a transient coordination error, so the
    first attempt re-raised and the container never started. That is fatal on a
    schedule: the sweep that deletes this account's databases deletes its IAM
    users with them, and the next restart afterwards would have brought up
    nothing -- including Rounds 4 and 6, which reach Lakebase and no AWS.

    The end state asserted here is the one this process already reaches when
    credentials die *under* a serving replica: 200, `ring_ready`, a named
    credential fault, every lost lane enumerated, and Rounds 4 and 6 still on
    the card. The probe has not answered yet in this window, which is the
    hardest part of it -- reporting `unknown` over a refusal AWS has already
    given is the false green this repository keeps having to close.
    """

    manifest = deployed_manifest()
    manifest.round6_ready = True
    _deployed_app_runtime(monkeypatch, manifest)
    if exports_a_key_pair:
        # Obvious placeholders, and never used: the STS call they would drive is
        # stubbed out below. They are here only so the environment matches the
        # story each row is telling.
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "not-a-real-key-id")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "not-a-real-secret")
    _refused_startup_credentials(
        monkeypatch, "Databricks App AWS credentials failed STS validation"
    )
    _fixed_credential_verdict(monkeypatch, CredentialVerdict())

    async with app.router.lifespan_context(app):
        # The three preconditions `RunManager.arm` checks before it does anything
        # else, asserted against the objects the arm path itself reads: the
        # readiness gate, and the two adapters. The fourth is the round's own
        # `availability`, which is what the catalog below reports.
        app.state.readiness_gate.require_ready()
        assert app.state.run_manager.model_score_available is True
        assert app.state.run_manager.live_orders_available is True
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            readyz = await client.get("/readyz")
            catalog_response = await client.get("/api/catalog")

    payload = readyz.json()
    # In rotation, which is the whole point: a 503 here would take the app out
    # of service over a fault that breaks four of six rounds.
    assert readyz.status_code == 200
    assert payload["ring_ready"] is True
    assert payload["maintenance_state"] == "ready"
    # And not pretending to be healthy either.
    assert payload["status"] == "degraded"
    assert payload["degraded"] is True
    assert payload["credentials_state"] == expected_state
    assert payload["credentials_recovery_state"] == expected_recovery
    assert payload["credentials_checked"] is True
    assert "failed STS validation" in payload["credentials_detail"]
    assert payload["degraded_detail"] == payload["credentials_detail"]
    assert any("every AWS lane" in loss for loss in payload["degraded_capabilities"])
    # A credential fault is not a coordination fault and must not be reported as
    # one; the ring genuinely opened.
    assert payload["coordination_durable"] is True
    assert payload["recovery_state"] == "settled"

    rounds = {item["id"]: item for item in catalog_response.json()["rounds"]}
    for round_id in (RoundId.PUT_MODEL_SCORE_IN_APP, RoundId.ANALYZE_LIVE_ORDERS):
        offered = rounds[round_id.value]
        assert offered["availability"] == "ready", round_id
        # The refusal fields are excluded when unset, so their absence is the
        # assertion: a ready round carries no reason and no headline.
        assert "availability_reason" not in offered, round_id
        assert "availability_headline" not in offered, round_id
    # And not one of the four that races a live Aurora or RDS opponent is
    # offered. Round 5 is sealed `planned` on this manifest rather than refused,
    # because live signals may take readiness away and never grant it.
    for round_id in round_availability.AWS_BACKED_ROUNDS:
        assert rounds[round_id.value]["availability"] != "ready", round_id
    # Refused with something an operator can act on, never bare. This
    # installation has sealed no serverless egress prefixes, so there are two
    # true reasons available -- the credential and the network path a hosted app
    # cannot take -- and the credential is reported because it is the wider of
    # the two. On an installation that has sealed them the network reason is
    # gone entirely, which is the case
    # `test_a_sealed_network_path_does_not_offer_a_round_with_no_credential`
    # covers.
    for round_id in (
        RoundId.WAKE_IDLE_APP,
        RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
        RoundId.RECOVER_DELETED_ORDER,
    ):
        refused = rounds[round_id.value]
        assert refused["availability"] == "unavailable", round_id
        assert refused["availability_reason"], round_id
        assert refused["availability_headline"], round_id


async def test_a_credential_that_becomes_valid_clears_a_degraded_boot(
    monkeypatch,
) -> None:
    """The startup refusal is evidence, not a latch.

    A verdict pinned at boot would defeat the thing that makes degrading
    acceptable in the first place: the probe re-asks AWS on its interval, so a
    key an operator publishes after the container started has to be able to
    turn this process green without a restart. So the refusal only speaks while
    the probe has not answered, and the probe outranks it the moment it does.
    """

    manifest = deployed_manifest()
    _deployed_app_runtime(monkeypatch, manifest)
    _refused_startup_credentials(
        monkeypatch, "Databricks App AWS credentials failed STS validation"
    )

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            payload = (await client.get("/readyz")).json()

    # `_deployed_app_runtime` leaves the healthy stub in place, so this is a
    # probe that has answered `ok` over a startup that was refused.
    assert payload["credentials_state"] == "ok"
    assert payload["status"] == "ready"
    assert payload["degraded"] is False
    assert payload["degraded_capabilities"] == []


async def test_a_credential_refusal_at_startup_is_loud_in_the_log(
    monkeypatch,
    caplog,
) -> None:
    """Degrading quietly would be worse than refusing to boot.

    The reason fail-closed was chosen is that a misconfigured app is worse than
    no app, so the trade only holds if the operator is told. Error level, the
    provider's own words, and the traceback -- the same treatment a broken
    orphan sweep gets, and for the same reason.
    """

    manifest = deployed_manifest()
    _deployed_app_runtime(monkeypatch, manifest)
    _refused_startup_credentials(monkeypatch, "no key pair in the environment")

    with caplog.at_level(logging.ERROR, logger=app_module.LOGGER.name):
        async with app.router.lifespan_context(app):
            pass

    refusals = [
        record
        for record in caplog.records
        if "no key pair in the environment" in record.getMessage()
    ]
    assert len(refusals) == 1
    assert refusals[0].levelno == logging.ERROR
    assert refusals[0].exc_info is not None


async def test_a_refused_startup_credential_is_not_reported_as_unprobed(
    monkeypatch,
) -> None:
    """The one window where a startup refusal would otherwise be permanent.

    A probe that cannot be constructed leaves `credential_sentry` as None, and
    `unprobed` deliberately does not degrade -- so before this, a deployed app
    that had been refused by AWS at boot and could not start a probe afterwards
    would have answered `status: ready` for the rest of its life. Nothing would
    ever have re-asked and nothing would ever have corrected it.
    """

    manifest = deployed_manifest()
    _deployed_app_runtime(monkeypatch, manifest)
    _refused_startup_credentials(monkeypatch, "no key pair in the environment")

    def refuse(*_args, **_kwargs):
        raise RuntimeError("no probe for you")

    monkeypatch.setattr(app_module, "CredentialSentry", refuse)

    async with app.router.lifespan_context(app):
        assert app.state.credential_sentry is None
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            payload = (await client.get("/readyz")).json()

    assert payload["credentials_state"] == "absent"
    assert payload["status"] == "degraded"
    assert "no key pair in the environment" in payload["credentials_detail"]


async def test_a_sealed_network_path_does_not_offer_a_round_with_no_credential(
    monkeypatch,
) -> None:
    """The catalog and `/readyz` have to name the same fault in the same window.

    The startup refusal was only ever read by `/readyz`, and that was defensible
    while the deployed network refusal covered all of `AWS_BACKED_ROUNDS`
    unconditionally: both refusals refused the identical rounds, so the catalog
    reading only the probe cost nothing but a choice between two true reasons.

    Sealing the published serverless egress prefixes removes that cover. An
    installation that has sealed them admits the deployed app -- see the sealed
    half of `test_the_deployed_refusals_lift_once_the_installation_admits_the_app`,
    where Rounds 1, 2 and 3 carry no refusal at all -- so in the window before
    the probe's first answer the only thing left saying anything about AWS is
    the startup refusal. A catalog that cannot see it offers three rounds that
    cannot arm, at the one moment somebody is most likely to be looking: the
    first render after a restart.
    """

    manifest = deployed_manifest()
    _deployed_app_runtime(monkeypatch, manifest)
    # An installation whose database groups admit this app. The count is what
    # `egress_sealed` reads; the addresses themselves are not this test's
    # business and are deliberately not written down.
    monkeypatch.setattr(
        api_module,
        "deployed_aws_posture",
        lambda: lifecycle.DeployedAwsPosture(egress_prefix_count=4),
    )
    _refused_startup_credentials(
        monkeypatch, "Databricks App AWS credentials failed STS validation"
    )
    # The pre-first-probe window, held open: `unknown` is what a probe reports
    # before its first check returns, and it is the state this defect lived in.
    _fixed_credential_verdict(monkeypatch, CredentialVerdict())

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            catalog_response = await client.get("/api/catalog")
            readyz = (await client.get("/readyz")).json()

    rounds = {item["id"]: item for item in catalog_response.json()["rounds"]}
    for round_id in (
        RoundId.WAKE_IDLE_APP,
        RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
        RoundId.RECOVER_DELETED_ORDER,
    ):
        offered = rounds[round_id.value]
        assert offered["availability"] == "unavailable", round_id
        # The credential, named, and not the network path: this installation
        # sealed the prefixes, so saying the app cannot reach the database would
        # be a refusal an operator would go and fail to fix.
        assert "failed STS validation" in offered["availability_reason"], round_id
        assert (
            round_availability.AWS_LANE_DEPLOYED_REFUSAL
            not in offered["availability_reason"]
        ), round_id
        # And the room gets a sentence too, in its own register.
        assert round_availability.NOT_ON_THE_CARD in offered["availability_headline"]

    # Same fault, same window, same words as the surface next door. This is the
    # agreement the catalog exists to keep, and the reason both surfaces now
    # read one helper rather than a copy each.
    assert readyz["credentials_detail"] == rounds[RoundId.WAKE_IDLE_APP.value][
        "availability_reason"
    ]
    # Rounds 4 and 6 reach Lakebase and no AWS, and are untouched by any of it.
    assert rounds[RoundId.PUT_MODEL_SCORE_IN_APP.value]["availability"] == "ready"


async def test_what_a_restart_history_does_to_readyz(monkeypatch) -> None:
    """The condition on the restarting supervisor, across the range that matters.

    Bringing a crashed server back is only acceptable if the one that comes back
    admits it. A process that answers "ready" between crashes is the same lie as
    one that answers "ready" while every AWS lane fails.

    The third row is the one that stops the rule being written as "any restart
    ever recorded is a fault": a crash from weeks ago is reported and is *not*
    degrading, because otherwise the field is useless for spotting the crash
    three minutes ago.
    """

    cases: tuple[tuple[str, RestartHistory, dict, tuple[str, ...]], ...] = (
        (
            "three recent restarts",
            RestartHistory(
                restarts=3,
                recent=3,
                last_at="2026-08-21T02:14:09+00:00",
                last_reason="SIGKILL (9)",
            ),
            {
                "status": "degraded",
                "degraded": True,
                "restarts": 3,
                "restarts_recent": 3,
                "last_restart_reason": "SIGKILL (9)",
                "supervisor_gave_up": False,
                "restart_history_state": "read",
            },
            ("RESTARTED 3 TIME(S) RECENTLY", "SIGKILL (9)"),
        ),
        (
            "a supervisor that has given up",
            RestartHistory(
                restarts=6,
                recent=6,
                last_at="2026-08-21T02:14:09+00:00",
                last_reason="exit code 1, and 6 restarts inside 600s did not fix it",
                gave_up=True,
            ),
            {"supervisor_gave_up": True},
            ("STOPPED RESTARTING IT",),
        ),
        (
            "one restart, weeks ago",
            RestartHistory(
                restarts=1,
                recent=0,
                last_at="2026-07-01T02:14:09+00:00",
                last_reason="exit code 1",
            ),
            {
                "status": "ready",
                "degraded": False,
                "restarts": 1,
                "restarts_recent": 0,
            },
            (),
        ),
    )

    for name, history, expected_fields, detail_fragments in cases:
        with pytest.MonkeyPatch.context() as patch:
            _deployed_app_runtime(patch, deployed_manifest())
            patch.setattr(app_module, "_restart_history", lambda h=history: h)

            async with app.router.lifespan_context(app):
                async with AsyncClient(
                    transport=ASGITransport(app=app), base_url="http://anti-demo.test"
                ) as client:
                    payload = (await client.get("/readyz")).json()

        for field, expected in expected_fields.items():
            actual = payload[field]
            assert actual is expected if isinstance(expected, bool) else actual == expected, (
                f"{name}: {field} was {actual!r}"
            )
        for fragment in detail_fragments:
            assert fragment in payload["degraded_detail"], f"{name}: missing {fragment!r}"


async def test_the_supervisors_record_is_what_readyz_reports(
    monkeypatch,
    tmp_path,
) -> None:
    """The whole chain, with no stub in the middle of it, in both runtimes.

    A real `RestartJournal` writes a real file, the real path resolution finds
    it, and `/readyz` reports what it says. Stubbing the history proves the
    reporting; this proves the reporting is wired to the thing the supervisor
    actually writes.

    The deployed half is the one that was wrong, and it is wrong in the
    direction this project keeps having to close. `app.yaml` binds
    `ANTI_DEMO_MANIFEST_JSON`, never `ANTI_DEMO_MANIFEST`, so
    `state_dir_from_environ` finds nothing, `restart_record_path()` is `None`,
    and `read_restart_history(None)` hands back the same zeroed history a first
    run has. `/readyz` published that as `restarts: 0` and
    `supervisor_gave_up: false` -- two permanent literals that no container
    event could move, on the surface a monitor reads to decide whether the
    deployed app is healthy. A field that cannot be known here has to say so.
    """

    async def readyz() -> dict:
        async with app.router.lifespan_context(app):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://anti-demo.test"
            ) as client:
                return (await client.get("/readyz")).json()

    _deployed_app_runtime(monkeypatch, deployed_manifest())

    # The deployed shape. `contain_ambient_installation_environment` has already
    # unset `ANTI_DEMO_MANIFEST`, which is exactly the environment `app.yaml`
    # gives the container.
    assert restart_record_path() is None
    deployed = await readyz()

    assert deployed["restart_history_state"] == "unavailable"
    for field in (
        "restarts",
        "restarts_recent",
        "last_restart_at",
        "last_restart_reason",
        "supervisor_gave_up",
    ):
        assert deployed[field] is None, f"{field} claims an answer it never read"
    assert "neither confirmed nor ruled out" in deployed["restart_history_detail"]
    # Reported, not alarmed on: this is the permanent condition of the deployed
    # runtime, so degrading on it would leave the box degraded for its whole
    # life and make `status` worth less to the person checking it before a demo.
    assert deployed["status"] == "ready"
    assert deployed["degraded"] is False
    assert deployed["degraded_detail"] is None
    # The measurement the unknown points at has to exist, or the sentence above
    # sends its reader to a field that is not there. Timezone-aware, because a
    # naive instant read by a monitor in another zone is a new guess.
    assert datetime.fromisoformat(deployed["process_started_at"]).tzinfo is not None

    # The local shape, where a supervisor really does write the record.
    manifest_file = tmp_path / "manifest.json"
    manifest_file.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(manifest_file))
    monkeypatch.setenv("ANTI_DEMO_SERVER_PORT", "8412")
    record = restart_record_path()
    assert record == tmp_path / "server-8412.restarts.json"
    RestartJournal(record).record("SIGKILL (9)", now=time.time())

    payload = await readyz()

    assert payload["restart_history_state"] == "read"
    assert payload["restarts"] == 1
    assert payload["restarts_recent"] == 1
    assert payload["last_restart_reason"] == "SIGKILL (9)"
    assert payload["restart_history_detail"] is None
    assert payload["status"] == "degraded"


async def test_a_credential_fault_outranks_a_restart_for_the_one_detail_slot(
    monkeypatch,
) -> None:
    """Both are reported; only one gets the sentence.

    Why the credentials win: "this process is a replacement" describes how the
    server got here, and "AWS is refusing the keys" describes what it cannot do
    now. The second is what an operator has to act on.
    """

    manifest = deployed_manifest()
    _deployed_app_runtime(monkeypatch, manifest)
    monkeypatch.setattr(
        app_module,
        "_restart_history",
        lambda: RestartHistory(restarts=2, recent=2, last_reason="SIGKILL (9)"),
    )
    _fixed_credential_verdict(
        monkeypatch,
        CredentialVerdict(
            state="rejected",
            detail="AWS REJECTED THE CREDENTIALS IN THIS PROCESS",
            recovery=RecoveryState("escalated", attempts=2, error="rejected"),
            checked_at_monotonic=1.0,
        ),
    )

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            payload = (await client.get("/readyz")).json()

    assert payload["degraded_detail"] == "AWS REJECTED THE CREDENTIALS IN THIS PROCESS"
    assert payload["restarts_recent"] == 2
    assert payload["credentials_state"] == "rejected"


@pytest.mark.parametrize(
    ("state", "expected_detail"),
    [
        ("retrying", "RECOVERY IS IN PROGRESS"),
        ("escalated", "RECOVERY HAS OUTLASTED ITS BUDGET"),
        ("given_up", "RECOVERY HAS STOPPED"),
    ],
)
async def test_a_ready_ring_still_recovering_is_not_reported_as_ready(
    monkeypatch,
    state,
    expected_detail,
) -> None:
    """The gap the three-value status exists to close, on the main ring.

    Every other degraded signal lowers `status`; the ring gate's own recovery
    was the one that set `degraded` and then left `status` saying "ready", so a
    monitor comparing against that single field saw a healthy server while the
    gate was escalating or had given up entirely. It stays a 200 -- the ring is
    ready and the app belongs in rotation -- but it must not read as "ready".
    """

    manifest = deployed_manifest()
    _deployed_app_runtime(monkeypatch, manifest)

    class ReadyButRecoveringGate(_ReadyGate):
        recovery = RecoveryState(
            state,
            attempts=4,
            detail="LEASE RENEWAL FAILING",
            error="OperationalError",
        )

    monkeypatch.setattr(app_module, "ShowtimeReadinessGate", ReadyButRecoveringGate)

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            response = await client.get("/readyz")

    payload = response.json()
    assert response.status_code == 200
    assert payload["ring_ready"] is True
    assert payload["status"] == "degraded"
    assert payload["degraded"] is True
    assert payload["recovery_state"] == state
    assert expected_detail in payload["degraded_detail"]


@pytest.mark.parametrize("state", ["retrying", "escalated", "given_up"])
async def test_recovery_never_upgrades_a_not_ready_ring_to_degraded(
    monkeypatch,
    state,
) -> None:
    """The downgrade is one-way: "not_ready" is worse than "degraded".

    Lowering `status` for a recovering gate must not accidentally lift a ring
    that cannot serve at all into the milder word, which would tell a monitor
    the app was still usable.
    """

    manifest = deployed_manifest()
    _deployed_app_runtime(monkeypatch, manifest)

    class UnreadyRecoveringGate(_ReadyGate):
        status = SimpleNamespace(
            ring_ready=False,
            maintenance_state="blocked",
            maintenance_detail="RING NOT OPENED",
        )
        round5_status = status
        recovery = RecoveryState(state, attempts=1, detail="RING NOT OPENED")

    monkeypatch.setattr(app_module, "ShowtimeReadinessGate", UnreadyRecoveringGate)

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            response = await client.get("/readyz")

    assert response.status_code == 503
    assert response.json()["status"] == "not_ready"


async def test_the_catalog_never_offers_a_round_readyz_says_cannot_arm(
    monkeypatch,
) -> None:
    """The round-select screen and the health surface may not contradict.

    The incident: `/api/catalog` reported all six rounds `availability: "ready"`
    with no reason attached -- including Round 5 -- at the same moment `/readyz`
    was returning 503 `not_ready` with `recovery_state: "given_up"`, no round
    could arm, and every bout was failing at the starting line. A monitor reads
    `/readyz`; a room full of customers reads the catalog, which is why this one
    is worse than its size suggests.

    Driven through the real app on purpose, both endpoints in one process against
    one gate. The whole defect was a surface disagreeing with reality, and that
    is exactly what a per-surface mock can be made to hide -- so the guard has to
    put the two answers side by side rather than assert each in isolation.
    """

    manifest = deployed_manifest()
    _deployed_app_runtime(monkeypatch, manifest)

    class GivenUpGate(_ReadyGate):
        status = SimpleNamespace(
            ring_ready=False,
            maintenance_state="blocked",
            maintenance_detail=(
                "BACKSTAGE CLEANUP BLOCKED · NOT RETRYING · OPERATOR ACTION REQUIRED"
            ),
        )
        round5_status = status
        recovery = RecoveryState("given_up", attempts=1, detail="RING NOT OPENED")

    monkeypatch.setattr(app_module, "ShowtimeReadinessGate", GivenUpGate)

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            readyz = await client.get("/readyz")
            catalog_response = await client.get("/api/catalog")

    assert readyz.status_code == 503
    assert readyz.json()["status"] == "not_ready"

    assert catalog_response.status_code == 200
    rounds = catalog_response.json()["rounds"]
    assert rounds, "the catalog must still answer; it just may not lie"
    # Not one green round while the ring cannot arm any of them.
    assert [item for item in rounds if item["availability"] == "ready"] == []
    # And not one bare refusal either: an operator has to be able to act on it.
    assert all(
        item["availability_reason"]
        for item in rounds
        if item["availability"] == "unavailable"
    )


async def test_a_gate_that_has_not_reported_yet_is_not_a_green_light(
    monkeypatch,
) -> None:
    """A present-but-silent gate must read the same way on both surfaces.

    The two defaults here point in opposite directions and both are deliberate.
    *No gate at all* is the local in-memory path and the unit-test path, where
    there is genuinely nothing to disagree with, and it reads as ready --
    the same reading `RunManager.bout_status` takes.

    *A gate that exists and has not answered* is the first moments of a real
    process's life, and `/readyz` reads a missing `ring_ready` as False and
    returns 503. Letting the catalog default permissively there would rebuild
    the same disagreement in the narrow window where it does the most damage:
    startup is exactly when somebody is loading the screen.
    """

    manifest = deployed_manifest()
    _deployed_app_runtime(monkeypatch, manifest)

    class SilentGate(_ReadyGate):
        status = None
        round5_status = None

    monkeypatch.setattr(app_module, "ShowtimeReadinessGate", SilentGate)

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            readyz = await client.get("/readyz")
            catalog_response = await client.get("/api/catalog")

    assert readyz.status_code == 503
    assert readyz.json()["ring_ready"] is False
    assert [
        item["availability"]
        for item in catalog_response.json()["rounds"]
        if item["availability"] == "ready"
    ] == []


def _arm_operator_ingress(monkeypatch, *, sealed: str, observed) -> None:
    """Point the real drift detector at a stubbed address, with no network.

    `tests/conftest.py` seals the probe for the whole suite by seeding its cache
    with a never-expiring "no drift"; this clears that so one real observation
    happens, against a stubbed `detect_operator_cidr`. Pass an exception for
    `observed` to play an unreachable probe. `tests/test_operator_ingress.py`
    covers the detector itself -- these only pin how /readyz folds its verdict.
    """

    lifecycle.reset_operator_ingress_cache()
    monkeypatch.setattr(
        lifecycle,
        "load_manifest",
        lambda: SimpleNamespace(aws=SimpleNamespace(operator_cidr=sealed)),
    )

    def _probe(*, timeout_seconds: float = 10.0) -> str:
        if isinstance(observed, Exception):
            raise observed
        return observed

    monkeypatch.setattr(lifecycle, "detect_operator_cidr", _probe)


async def test_what_the_ingress_probe_makes_readyz_say(monkeypatch) -> None:
    """The three answers the probe can give, and none of them is a 503.

    Drift is the silent killer this detector exists for: the laptop changed
    address, so the AWS security groups allow an address nobody holds and every
    round that dials Aurora or RDS directly times out with nothing on screen
    explaining it. Still a 200 -- the Lakebase lanes are fine and a 503 would
    turn a diagnosable fault into an outage.

    The matching row is what stops the detector from being a permanent alarm,
    and the unreachable row is the one that matters most: a probe that cannot
    answer must not invent a fault. Telling an operator to re-apply Terraform
    because the laptop was briefly offline is a false alarm, and a health
    surface that raises when an external service is unreachable is worse than
    the drift it was added to catch.
    """

    cases: tuple[tuple[str, str | Exception, dict, tuple[str, ...], bool], ...] = (
        (
            "the observed address has drifted",
            "198.51.100.4/32",
            {"status": "degraded", "degraded": True, "ring_ready": True},
            ("OPERATOR INGRESS IS STALE", "203.0.113.7/32", "198.51.100.4/32"),
            True,
        ),
        (
            "the observed address still matches",
            "203.0.113.7/32",
            {
                "status": "ready",
                "degraded": False,
                "degraded_detail": None,
                "degraded_capabilities": [],
            },
            (),
            False,
        ),
        (
            "the probe cannot reach anything",
            OSError("no route to host"),
            {"status": "ready", "degraded": False},
            (),
            False,
        ),
    )

    for name, observed, expected_fields, detail_fragments, expect_capability in cases:
        with pytest.MonkeyPatch.context() as patch:
            _deployed_app_runtime(patch, deployed_manifest())
            _arm_operator_ingress(patch, sealed="203.0.113.7/32", observed=observed)

            async with app.router.lifespan_context(app):
                async with AsyncClient(
                    transport=ASGITransport(app=app), base_url="http://anti-demo.test"
                ) as client:
                    response = await client.get("/readyz")

        payload = response.json()
        assert response.status_code == 200, name
        for field, expected in expected_fields.items():
            actual = payload[field]
            assert actual is expected if isinstance(expected, bool) else actual == expected, (
                f"{name}: {field} was {actual!r}"
            )
        for fragment in detail_fragments:
            assert fragment in payload["degraded_detail"], f"{name}: missing {fragment!r}"
        names_the_lanes = any(
            "Aurora and RDS" in loss for loss in payload["degraded_capabilities"]
        )
        assert names_the_lanes is expect_capability, name


async def test_ingress_drift_never_lifts_a_not_ready_ring_out_of_503(
    monkeypatch,
) -> None:
    """Folding in an observation must not change who is in rotation.

    The ring decides the status code. Drift is degrading, and degrading is
    strictly milder than a ring that cannot serve.
    """

    manifest = deployed_manifest()
    _deployed_app_runtime(monkeypatch, manifest)
    _arm_operator_ingress(monkeypatch, sealed="203.0.113.7/32", observed="198.51.100.4/32")

    class UnreadyGate(_ReadyGate):
        status = SimpleNamespace(
            ring_ready=False,
            maintenance_state="blocked",
            maintenance_detail="RING NOT OPENED",
        )
        round5_status = status

    monkeypatch.setattr(app_module, "ShowtimeReadinessGate", UnreadyGate)

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            response = await client.get("/readyz")

    payload = response.json()
    assert response.status_code == 503
    assert payload["status"] == "not_ready"
    # Still reported, just not promoted over the ring's own word for the slot.
    assert any("Aurora and RDS" in loss for loss in payload["degraded_capabilities"])


async def test_an_unsettled_ring_outranks_ingress_drift_for_the_detail(
    monkeypatch,
) -> None:
    """One sentence, ordered by breadth of outage.

    A ring being recovered is the wider fault than a class of rounds losing
    their direct route, so it keeps the sentence -- and drift is still fully
    readable in `degraded_capabilities`, which is what stops the precedence
    from hiding anything.
    """

    manifest = deployed_manifest()
    _deployed_app_runtime(monkeypatch, manifest)
    _arm_operator_ingress(monkeypatch, sealed="203.0.113.7/32", observed="198.51.100.4/32")

    class ReadyButRecoveringGate(_ReadyGate):
        recovery = RecoveryState("given_up", attempts=4, detail="LEASE RENEWAL FAILING")

    monkeypatch.setattr(app_module, "ShowtimeReadinessGate", ReadyButRecoveringGate)

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            payload = (await client.get("/readyz")).json()

    assert payload["status"] == "degraded"
    assert "RECOVERY HAS STOPPED" in payload["degraded_detail"]
    assert "OPERATOR INGRESS IS STALE" not in payload["degraded_detail"]
    assert any("Aurora and RDS" in loss for loss in payload["degraded_capabilities"])


def _real_credential_sentry(wiring: list[str] | None = None):
    from server.aws_credential_probe import CredentialSentry

    def build(expectations, *, on_recovered=None, **kwargs):
        if wiring is not None and on_recovered is not None:
            # Recorded by name so the assertion says which listener was wired,
            # rather than merely that something was.
            wiring.append(getattr(on_recovered, "__name__", repr(on_recovered)))
        return CredentialSentry(
            expectations,
            session_factory=lambda **_kwargs: (_ for _ in ()).throw(
                AssertionError("nothing to authenticate with; no session may be built")
            ),
            on_recovered=on_recovered,
            **kwargs,
        )

    return build
