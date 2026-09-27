"""A `ready` installation a sandbox reaper has deleted, as a re-run finds it.

`installation_remnants` is what bootstrap.sh asks before refusing to re-provision
an installation that says `ready`. It may only answer `gone` when every sealed
AWS resource and every sealed Lakebase project was read and found absent -- the
one state in which moving the old records aside and installing afresh cannot
orphan anything that still bills. Every other answer keeps the refusal.
"""

from __future__ import annotations

import io
import json
import socket
import urllib.error
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

ACCOUNT = "123456789012"
HOST = "https://dbc-reaped-0000.cloud.databricks.com"


def _manifest(*, v7: bool = True) -> SimpleNamespace:
    rounds = {
        n: SimpleNamespace(lakebase=SimpleNamespace(project_id=f"p-r{n}")) for n in range(1, 7)
    }
    return SimpleNamespace(
        run_id="ad-test-reaped",
        round_environments=rounds if v7 else None,
        coordination_lakebase=SimpleNamespace(project_id="p-coord") if v7 else None,
        aws=SimpleNamespace(
            account_id=ACCOUNT,
            region="us-west-2",
            runtime_role_arn=f"arn:aws:iam::{ACCOUNT}:role/anti-demo-runtime-x",
        ),
        databricks=SimpleNamespace(profile="anti-demo-dbc-reaped-0000"),
    )


def _world(
    monkeypatch,
    *,
    aws: InstallationPresence,
    projects: dict[str, object],
    app="absent",
    workspace=(lifecycle.WORKSPACE_LIVE, "answered as a workspace"),
):
    monkeypatch.setattr(lifecycle, "reconcile_live", lambda manifest, factory: object())
    monkeypatch.setattr(lifecycle, "presence_from_report", lambda report: aws)
    monkeypatch.setattr(lifecycle, "_databricks_profile_host", lambda profile: HOST)
    monkeypatch.setattr(lifecycle, "databricks_workspace_liveness", lambda host: workspace)

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


WORKSPACE_GONE = (
    lifecycle.WORKSPACE_GONE,
    "answered HTTP 400: Unable to determine workspace context",
)
UNREADABLE = {pid: RuntimeError("cannot configure default credentials") for pid in ALL_PROJECTS}


def test_a_deleted_workspace_takes_its_projects_and_app_with_it(monkeypatch) -> None:
    """The reaper deleted the workspace as well: its projects cannot be asked about."""

    _world(
        monkeypatch, aws=AWS_GONE, projects=UNREADABLE, app="unreadable", workspace=WORKSPACE_GONE
    )

    report = lifecycle.installation_remnants(_manifest())

    assert report["gone"] is True
    assert report["lakebase"] == {"expected": 7, "absent": 7, "unreadable": 0}
    assert report["app"]["state"] == "absent"
    assert report["workspace"] == {
        "host": HOST,
        "state": lifecycle.WORKSPACE_GONE,
        "reason": WORKSPACE_GONE[1],
    }


def test_a_project_that_answers_is_never_hidden_by_a_gone_verdict(monkeypatch) -> None:
    projects = {**UNREADABLE, "p-r5": {"uid": "still-here"}}
    _world(monkeypatch, aws=AWS_GONE, projects=projects, workspace=WORKSPACE_GONE)

    report = lifecycle.installation_remnants(_manifest())

    assert report["gone"] is False
    assert report["lakebase"] == {"expected": 7, "absent": 6, "unreadable": 0}


def test_a_workspace_that_could_not_be_asked_counts_nothing_absent(monkeypatch) -> None:
    unverified = (lifecycle.WORKSPACE_UNVERIFIED, "could not be asked (TimeoutError)")
    _world(monkeypatch, aws=AWS_GONE, projects=UNREADABLE, app="unreadable", workspace=unverified)

    report = lifecycle.installation_remnants(_manifest())

    assert report["gone"] is False
    assert report["lakebase"] == {"expected": 7, "absent": 0, "unreadable": 7}
    assert report["app"]["state"] == "unreadable"


def test_an_unreadable_manifest_is_reported_not_raised(monkeypatch) -> None:
    def refuse():
        raise FileNotFoundError("manifest.json")

    monkeypatch.setattr(lifecycle, "load_manifest", refuse)

    report = lifecycle.installation_remnants()

    assert report["gone"] is False
    assert "FileNotFoundError" in report["error"]


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, "Op")


def _sessions(monkeypatch, *, get_role_error: str | None, account: str = ACCOUNT):
    iam = SimpleNamespace(
        get_role=lambda RoleName: (_ for _ in ()).throw(_client_error(get_role_error))
        if get_role_error
        else {"Role": {"RoleName": RoleName}}
    )
    sts = SimpleNamespace(get_caller_identity=lambda: {"Account": account})
    source = SimpleNamespace(client=lambda name, **kwargs: {"iam": iam, "sts": sts}[name])

    def refuse(manifest):
        raise _client_error("AccessDenied")

    monkeypatch.setattr(lifecycle, "_aws_session", refuse)
    monkeypatch.setattr(lifecycle, "_aws_source_session", lambda manifest: source)
    return source


def test_a_swept_runtime_role_falls_back_to_the_operators_own_keys(monkeypatch) -> None:
    source = _sessions(monkeypatch, get_role_error="NoSuchEntity")

    assert lifecycle._presence_session(_manifest()) is source


def test_keys_from_another_account_are_never_read_as_an_empty_installation(monkeypatch) -> None:
    """In another account the role is missing too, and so is everything else."""

    _sessions(monkeypatch, get_role_error="NoSuchEntity", account="210987654321")

    with pytest.raises(lifecycle._ForeignAccountKeys, match="not the sealed 123456789012"):
        lifecycle._presence_session(_manifest())


def test_the_foreign_account_is_named_in_the_report(monkeypatch) -> None:
    def refuse(manifest, factory):
        raise lifecycle._ForeignAccountKeys("the AWS keys are for account A, not the sealed B")

    _world(monkeypatch, aws=AWS_GONE, projects={})
    monkeypatch.setattr(lifecycle, "reconcile_live", refuse)

    report = lifecycle.installation_remnants(_manifest())

    assert report["gone"] is False
    assert report["aws"]["state"] == PRESENCE_UNVERIFIED
    assert report["aws"]["reason"] == "the AWS keys are for account A, not the sealed B"


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


# What a workspace's own front door says, as `databricks_workspace_liveness`
# reads it. Recorded answers: on 2026-09-27 a workspace deleted weeks before still
# resolved and answered its OAuth discovery with HTTP 400 "Unable to determine
# workspace context"; a live one answered 200 with a token endpoint.
DELETED_BODY = (
    b'{"error_description":"Invalid request: Unable to determine workspace context for '
    b'dbc-reaped-0000.cloud.databricks.com","error":"invalid_request"}'
)


class _Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _front_door(monkeypatch, *, dns: dict[str, int | None], answer=None) -> None:
    """`dns` maps a name to None (resolves) or a gaierror code; `answer` is urlopen's."""

    def getaddrinfo(name, port):
        code = dns.get(name)
        if code is not None:
            raise socket.gaierror(code, "lookup failed")
        return [("stub",)]

    def urlopen(request, timeout):
        if isinstance(answer, BaseException):
            raise answer
        return _Response(answer)

    monkeypatch.setattr(lifecycle.socket, "getaddrinfo", getaddrinfo)
    monkeypatch.setattr(lifecycle.urllib.request, "urlopen", urlopen)


WITNESS = "accounts.cloud.databricks.com"
WORKSPACE = "dbc-reaped-0000.cloud.databricks.com"


def test_a_workspace_that_answers_with_a_token_endpoint_is_live(monkeypatch) -> None:
    _front_door(monkeypatch, dns={}, answer=b'{"token_endpoint":"https://x/oidc/v1/token"}')

    assert lifecycle.databricks_workspace_liveness(HOST)[0] == lifecycle.WORKSPACE_LIVE


def test_a_deleted_workspaces_own_answer_is_gone(monkeypatch) -> None:
    error = urllib.error.HTTPError(HOST, 400, "Bad Request", None, io.BytesIO(DELETED_BODY))
    _front_door(monkeypatch, dns={}, answer=error)

    state, why = lifecycle.databricks_workspace_liveness(HOST)

    assert state == lifecycle.WORKSPACE_GONE
    assert why == f"{WORKSPACE} answered HTTP 400: Unable to determine workspace context"


def test_any_other_refusal_is_unverified(monkeypatch) -> None:
    error = urllib.error.HTTPError(HOST, 403, "Forbidden", None, io.BytesIO(b"denied"))
    _front_door(monkeypatch, dns={}, answer=error)

    assert lifecycle.databricks_workspace_liveness(HOST) == (
        lifecycle.WORKSPACE_UNVERIFIED,
        f"{WORKSPACE} answered HTTP 403",
    )


def test_a_name_gone_from_dns_is_gone_while_databricks_resolves(monkeypatch) -> None:
    _front_door(monkeypatch, dns={WORKSPACE: socket.EAI_NONAME})

    assert lifecycle.databricks_workspace_liveness(HOST) == (
        lifecycle.WORKSPACE_GONE,
        f"{WORKSPACE} no longer resolves",
    )


def test_nothing_resolving_is_offline_not_gone(monkeypatch) -> None:
    _front_door(monkeypatch, dns={WORKSPACE: socket.EAI_NONAME, WITNESS: socket.EAI_NONAME})

    assert lifecycle.databricks_workspace_liveness(HOST)[0] == lifecycle.WORKSPACE_UNVERIFIED


def test_a_lookup_that_failed_for_another_reason_is_unverified(monkeypatch) -> None:
    _front_door(monkeypatch, dns={WORKSPACE: socket.EAI_AGAIN})

    assert lifecycle.databricks_workspace_liveness(HOST)[0] == lifecycle.WORKSPACE_UNVERIFIED


@pytest.mark.parametrize(
    "answer",
    [
        urllib.error.URLError(TimeoutError("timed out")),
        b"<html>proxy login</html>",
        b'{"issuer":"x"}',
    ],
)
def test_no_answer_or_a_foreign_one_is_unverified(monkeypatch, answer) -> None:
    _front_door(monkeypatch, dns={}, answer=answer)

    assert lifecycle.databricks_workspace_liveness(HOST)[0] == lifecycle.WORKSPACE_UNVERIFIED


def test_no_recorded_host_is_unverified() -> None:
    assert lifecycle.databricks_workspace_liveness("")[0] == lifecycle.WORKSPACE_UNVERIFIED
