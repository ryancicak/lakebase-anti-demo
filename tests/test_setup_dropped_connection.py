"""Setup carries on from where it stopped when a database drops one of its connections.

rc22's fresh install (2026-10-05) stopped in its seed on AdminShutdown, SQLSTATE 57P01:
Lakebase shut Round 3's new compute down a minute after its last query, as the seed's
connection landed. The same connection made again wakes the compute, and the resume the
failure tells an operator to run finishes the install.
"""

from __future__ import annotations

from types import SimpleNamespace

import psycopg
import pytest

from server import lifecycle
from server.lifecycle import (
    SETUP_DROPPED_CONNECTION_RETRY_SECONDS,
    Check,
    _dropped_connection,
    setup,
)


def _connection_closed() -> psycopg.OperationalError:
    return psycopg.OperationalError("consuming input failed: server closed the connection")


def _connect_gave_up() -> RuntimeError:
    """The shape `_connect` raises after two minutes: its own error, `from None`."""

    try:
        raise psycopg.OperationalError("connection failed")
    except psycopg.OperationalError:
        try:
            raise RuntimeError("did not become ready within 120 seconds") from None
        except RuntimeError as gave_up:
            return gave_up


def _wrapped(error: BaseException) -> RuntimeError:
    try:
        raise error
    except BaseException as cause:
        try:
            raise RuntimeError("Round 3 seed failed") from cause
        except RuntimeError as wrapper:
            return wrapper


@pytest.mark.parametrize(
    ("error", "dropped"),
    [
        pytest.param(psycopg.errors.AdminShutdown("terminating"), True, id="admin-shutdown"),
        pytest.param(psycopg.errors.CrashShutdown("crash"), True, id="crash-shutdown"),
        pytest.param(psycopg.errors.CannotConnectNow("starting"), True, id="starting-up"),
        pytest.param(psycopg.errors.ConnectionFailure("lost"), True, id="class-08"),
        pytest.param(_connection_closed(), True, id="closed-with-no-sqlstate"),
        pytest.param(_wrapped(psycopg.errors.AdminShutdown("x")), True, id="raised-from"),
        pytest.param(_connect_gave_up(), False, id="connect-already-waited"),
        pytest.param(psycopg.errors.QueryCanceled("timeout"), False, id="query-canceled"),
        pytest.param(psycopg.errors.InsufficientPrivilege("denied"), False, id="a-grant"),
        pytest.param(psycopg.errors.UndefinedTable("missing"), False, id="a-missing-table"),
        pytest.param(RuntimeError("Terraform said no"), False, id="not-postgres"),
    ],
)
def test_a_dropped_connection_is_told_apart_from_an_answer(error, dropped) -> None:
    assert (_dropped_connection(error) is not None) is dropped


def _stub_the_setup_tail(monkeypatch, calls: list[str]) -> None:
    monkeypatch.setattr(lifecycle, "_follow_operator_address", lambda candidate: None)
    for stage in ("round4", "round5", "round6", "round4_aws", "round6_aws"):
        monkeypatch.setattr(
            lifecycle,
            f"_prepare_and_reseal_{stage}",
            lambda candidate, *, timeout, stage=stage: calls.append(stage) or candidate,
        )
    monkeypatch.setattr(lifecycle, "_keep_lease_current", lambda candidate: calls.append("lease"))
    monkeypatch.setattr(
        lifecycle,
        "doctor",
        lambda competitor, *, timeout_seconds: (
            calls.append(f"doctor:{competitor}") or [Check("ready", True, "ready")]
        ),
    )


def _run_setup(ttl_hours: float | None = 12):
    return setup(
        databricks_profile="profile",
        aws_profile="",
        aws_region="us-west-2",
        expected_account="123456789012",
        owner="",
        operator_cidr=None,
        ttl_hours=ttl_hours,
        timeout_seconds=321,
    )


def test_a_fresh_install_whose_seed_was_dropped_carries_on(monkeypatch, tmp_path, capsys) -> None:
    owned_manifest = tmp_path / "manifest.json"
    seeding = SimpleNamespace(
        status="seeding",
        round5_ready=False,
        round6_ready=False,
        round4_aws_pending=False,
        round6_aws_pending=False,
    )
    calls: list[str] = []
    pauses: list[float] = []

    def provision(**options):
        calls.append(f"provision:{options['ttl_hours']:g}")
        # The first billable step comes after the manifest is written.
        owned_manifest.touch()
        raise psycopg.errors.AdminShutdown("terminating connection due to administrator command")

    monkeypatch.setattr(lifecycle, "manifest_path", lambda: owned_manifest)
    monkeypatch.setattr(lifecycle, "provision", provision)
    monkeypatch.setattr(lifecycle, "load_manifest", lambda: seeding)
    monkeypatch.setattr(
        lifecycle, "resume_provision", lambda timeout: calls.append("resume") or seeding
    )
    monkeypatch.setattr(lifecycle.time, "sleep", pauses.append)
    _stub_the_setup_tail(monkeypatch, calls)

    assert _run_setup() is seeding

    # bootstrap.sh passes --ttl-hours to a first install, and setup refuses it on a manifest
    # that exists, so the second attempt goes without it.
    assert calls == [
        "provision:12",
        "resume",
        "round6",
        "round4_aws",
        "round6_aws",
        "lease",
        "doctor:aurora",
        "doctor:rds",
    ]
    assert pauses == [SETUP_DROPPED_CONNECTION_RETRY_SECONDS[0]]
    assert (
        "WAIT  10s: a database dropped one of setup's connections (AdminShutdown), "
        "then setup carries on from where it stopped"
    ) in capsys.readouterr().out


def test_any_other_failure_stops_setup_at_once(monkeypatch, tmp_path) -> None:
    owned_manifest = tmp_path / "manifest.json"
    pauses: list[float] = []

    def provision(**_options):
        owned_manifest.touch()
        raise RuntimeError("Terraform said no")

    monkeypatch.setattr(lifecycle, "manifest_path", lambda: owned_manifest)
    monkeypatch.setattr(lifecycle, "provision", provision)
    monkeypatch.setattr(
        lifecycle, "resume_provision", lambda timeout: pytest.fail("only a dropped connection")
    )
    monkeypatch.setattr(lifecycle.time, "sleep", pauses.append)

    with pytest.raises(RuntimeError, match="Terraform said no"):
        _run_setup()
    assert pauses == []


def test_with_no_manifest_there_is_nothing_to_carry_on_from(monkeypatch, tmp_path) -> None:
    owned_manifest = tmp_path / "manifest.json"
    pauses: list[float] = []

    def provision(**_options):
        raise psycopg.errors.AdminShutdown("terminating connection due to administrator command")

    monkeypatch.setattr(lifecycle, "manifest_path", lambda: owned_manifest)
    monkeypatch.setattr(lifecycle, "provision", provision)
    monkeypatch.setattr(lifecycle.time, "sleep", pauses.append)

    with pytest.raises(psycopg.errors.AdminShutdown):
        _run_setup()
    assert pauses == []


def test_a_connection_that_keeps_dropping_fails_after_the_last_pause(monkeypatch, tmp_path) -> None:
    owned_manifest = tmp_path / "manifest.json"
    owned_manifest.touch()
    seeding = SimpleNamespace(
        status="seeding",
        round5_ready=False,
        round6_ready=False,
        round4_aws_pending=False,
        round6_aws_pending=False,
    )
    resumes: list[float] = []
    pauses: list[float] = []

    def resume_provision(timeout):
        resumes.append(timeout)
        raise _wrapped(psycopg.errors.AdminShutdown("terminating"))

    monkeypatch.setattr(lifecycle, "manifest_path", lambda: owned_manifest)
    monkeypatch.setattr(lifecycle, "load_manifest", lambda: seeding)
    monkeypatch.setattr(lifecycle, "resume_provision", resume_provision)
    monkeypatch.setattr(lifecycle.time, "sleep", pauses.append)

    with pytest.raises(RuntimeError, match="Round 3 seed failed"):
        _run_setup(ttl_hours=None)
    assert pauses == list(SETUP_DROPPED_CONNECTION_RETRY_SECONDS)
    assert len(resumes) == len(SETUP_DROPPED_CONNECTION_RETRY_SECONDS) + 1
