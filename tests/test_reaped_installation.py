"""A `ready` installation a sandbox reaper has deleted, as a re-run finds it.

`installation_remnants` is what bootstrap.sh asks before refusing to re-provision
an installation that says `ready`. It may only answer `gone` when every sealed
AWS resource and every sealed Lakebase project was read and found absent -- the
one state in which moving the old records aside and installing afresh cannot
orphan anything that still bills. Every other answer keeps the refusal.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from botocore.exceptions import ClientError

from server import cli as cli_module
from server import lifecycle
from server.reconcile import (
    PRESENCE_MISSING,
    PRESENCE_PRESENT,
    PRESENCE_UNVERIFIED,
    InstallationPresence,
)


def _manifest(*, v7: bool = True) -> SimpleNamespace:
    rounds = {
        n: SimpleNamespace(lakebase=SimpleNamespace(project_id=f"p-r{n}")) for n in range(1, 7)
    }
    return SimpleNamespace(
        run_id="ad-test-reaped",
        round_environments=rounds if v7 else None,
        coordination_lakebase=SimpleNamespace(project_id="p-coord") if v7 else None,
        aws=SimpleNamespace(runtime_role_arn="arn:aws:iam::123456789012:role/anti-demo-runtime-x"),
    )


def _world(monkeypatch, *, aws: InstallationPresence, projects: dict[str, object], app="absent"):
    monkeypatch.setattr(lifecycle, "reconcile_live", lambda manifest, factory: object())
    monkeypatch.setattr(lifecycle, "presence_from_report", lambda report: aws)

    def get_project(manifest, *, project_id):
        answer = projects.get(project_id)
        if isinstance(answer, Exception):
            raise answer
        return answer

    monkeypatch.setattr(lifecycle, "_get_lakebase_project_or_none", get_project)
    monkeypatch.setattr(
        lifecycle,
        "_owned_app",
        lambda manifest: SimpleNamespace(
            name="lakebase-anti-demo-3",
            unreadable="" if app != "unreadable" else "TimeoutError",
            present=app == "present",
            owned=app == "present",
        ),
    )


ALL_PROJECTS = [*(f"p-r{n}" for n in range(1, 7)), "p-coord"]
AWS_GONE = InstallationPresence(PRESENCE_MISSING, sealed=13, absent=13)


def test_everything_read_and_absent_is_gone(monkeypatch) -> None:
    _world(monkeypatch, aws=AWS_GONE, projects={})

    report = lifecycle.installation_remnants(_manifest())

    assert report["gone"] is True
    assert report["aws"] == {"state": PRESENCE_MISSING, "sealed": 13, "absent": 13, "reason": ""}
    assert report["lakebase"] == {"expected": 7, "absent": 7, "unreadable": 0}
    assert report["app"]["state"] == "absent"


def test_an_app_that_survived_does_not_stop_a_fresh_install(monkeypatch) -> None:
    """The directory that created it re-adopts it on its bootstrap.json."""

    _world(monkeypatch, aws=AWS_GONE, projects={}, app="present")

    assert lifecycle.installation_remnants(_manifest())["gone"] is True


def test_aws_gone_but_the_workspace_intact_is_not_gone(monkeypatch) -> None:
    """Rebuilding the AWS side in place is --reset-ready's job, not a fresh install's."""

    _world(monkeypatch, aws=AWS_GONE, projects={pid: {"uid": "u"} for pid in ALL_PROJECTS})

    report = lifecycle.installation_remnants(_manifest())

    assert report["gone"] is False
    assert report["lakebase"]["absent"] == 0


def test_a_project_that_cannot_be_read_is_never_counted_absent(monkeypatch) -> None:
    _world(monkeypatch, aws=AWS_GONE, projects={"p-r3": RuntimeError("workspace unreachable")})

    report = lifecycle.installation_remnants(_manifest())

    assert report["gone"] is False
    assert report["lakebase"] == {"expected": 7, "absent": 6, "unreadable": 1}


def test_some_aws_left_is_not_gone(monkeypatch) -> None:
    partial = InstallationPresence(PRESENCE_MISSING, sealed=13, absent=4)
    _world(monkeypatch, aws=partial, projects={})

    assert lifecycle.installation_remnants(_manifest())["gone"] is False


@pytest.mark.parametrize(
    "aws",
    [
        InstallationPresence(PRESENCE_UNVERIFIED, sealed=13, reason="AccessDenied"),
        InstallationPresence(PRESENCE_PRESENT, sealed=13),
    ],
)
def test_an_unverified_or_present_fleet_is_not_gone(monkeypatch, aws) -> None:
    _world(monkeypatch, aws=aws, projects={})

    assert lifecycle.installation_remnants(_manifest())["gone"] is False


def test_an_older_seal_is_never_declared_gone(monkeypatch) -> None:
    """Only a v7 seal lists every project it owns."""

    _world(monkeypatch, aws=AWS_GONE, projects={})

    report = lifecycle.installation_remnants(_manifest(v7=False))

    assert report["gone"] is False
    assert report["lakebase"]["unreadable"] == 1


def test_an_unreadable_manifest_is_reported_not_raised(monkeypatch) -> None:
    def refuse():
        raise FileNotFoundError("manifest.json")

    monkeypatch.setattr(lifecycle, "load_manifest", refuse)

    report = lifecycle.installation_remnants()

    assert report["gone"] is False
    assert "FileNotFoundError" in report["error"]


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, "Op")


def _sessions(monkeypatch, *, get_role_error: str | None):
    source = SimpleNamespace(
        client=lambda name: SimpleNamespace(
            get_role=lambda RoleName: (_ for _ in ()).throw(_client_error(get_role_error))
            if get_role_error
            else {"Role": {"RoleName": RoleName}}
        )
    )

    def refuse(manifest):
        raise _client_error("AccessDenied")

    monkeypatch.setattr(lifecycle, "_aws_session", refuse)
    monkeypatch.setattr(lifecycle, "_aws_source_session", lambda manifest: source)
    return source


def test_a_swept_runtime_role_falls_back_to_the_operators_own_keys(monkeypatch) -> None:
    source = _sessions(monkeypatch, get_role_error="NoSuchEntity")

    assert lifecycle._presence_session(_manifest()) is source


def test_a_role_that_exists_but_refuses_is_still_a_refusal(monkeypatch) -> None:
    _sessions(monkeypatch, get_role_error=None)

    with pytest.raises(ClientError):
        lifecycle._presence_session(_manifest())


def test_an_unreadable_role_is_still_a_refusal(monkeypatch) -> None:
    _sessions(monkeypatch, get_role_error="AccessDenied")

    with pytest.raises(ClientError):
        lifecycle._presence_session(_manifest())


def test_the_presence_command_prints_the_report_as_json(monkeypatch, capsys, tmp_path) -> None:
    monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(tmp_path / ".anti-demo-v7" / "manifest.json"))
    monkeypatch.setattr(cli_module, "installation_remnants", lambda: {"gone": True, "run_id": "x"})
    monkeypatch.setattr("sys.argv", ["antidemo", "presence"])

    assert cli_module.main() == 0

    assert json.loads(capsys.readouterr().out.strip().splitlines()[-1]) == {
        "gone": True,
        "run_id": "x",
    }
