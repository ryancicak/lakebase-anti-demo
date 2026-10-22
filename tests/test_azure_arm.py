"""server/azure_arm.py against a scripted transport: no network, no Azure.

Each behavior pinned here was measured in Phase 0 (2026-10-18) and is named in the module
docstring: a restore accepted and then refused, firewall writes one at a time, PgBouncer
parameters that do not exist while it is off, and throttling.
"""

from __future__ import annotations

import base64
import http.client
import json
import time
import urllib.error
from collections.abc import Mapping
from typing import Any

import pytest

from server.azure_arm import (
    TOKEN_EXPIRY_MARGIN_SECONDS,
    TOKEN_REUSE_SECONDS,
    ArmClient,
    ArmError,
    AzureCredentials,
    Response,
    RestoreNotReady,
    token_reuse_seconds,
)

SUB = "00000000-1111-2222-3333-444444444444"
CREDENTIALS = AzureCredentials(tenant_id="tenant", client_id="client", client_secret="s3cret",
                               subscription_id=SUB)
TOKEN = Response(200, {"access_token": "tok", "expires_in": 3599}, {})


class ScriptedTransport:
    """Answers each call with the next scripted response whose (method, fragment) matches. A
    scripted exception is raised instead, as urllib raises a call that got no answer."""

    def __init__(self, script: list[tuple[str, str, Response | Exception]]) -> None:
        self.script = list(script)
        self.calls: list[tuple[str, str, Any]] = []
        #: Each call's (method, url fragment-free path, timeout), in order.
        self.timeouts: list[tuple[str, str, float]] = []

    def __call__(self, method: str, url: str, headers: Mapping[str, str], data: bytes | None,
                 timeout: float) -> Response:
        self.calls.append((method, url, data))
        self.timeouts.append((method, url.split("?", 1)[0], timeout))
        for index, (want_method, fragment, response) in enumerate(self.script):
            if want_method == method and fragment in url:
                del self.script[index]
                if isinstance(response, Exception):
                    raise response
                return response
        raise AssertionError(f"unscripted call {method} {url}")


def client(
    script: list[tuple[str, str, Response | Exception]],
) -> tuple[ArmClient, ScriptedTransport]:
    transport = ScriptedTransport(script)
    return ArmClient(CREDENTIALS, "rg-test", transport=transport, sleep=lambda _: None), transport


def accepted(operation_url: str) -> Response:
    return Response(202, None, {"Azure-AsyncOperation": operation_url})


SOURCE = Response(200, {"id": f"/subscriptions/{SUB}/resourceGroups/rg-test/providers/"
                              "Microsoft.DBforPostgreSQL/flexibleServers/src"}, {})


def test_the_token_is_fetched_once_and_reused() -> None:
    arm, transport = client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("GET", "/flexibleServers/a?", Response(404, None, {})),
        ("GET", "/flexibleServers/b?", Response(404, None, {})),
    ])
    assert arm.get_server("a") is None
    assert arm.get_server("b") is None
    assert sum(1 for method, url, _ in transport.calls if "oauth2" in url) == 1


def jwt(expires_at: float) -> str:
    """A token shaped like Entra ID's: a JWT whose payload carries `exp`. Nothing here checks a
    signature, so it has none."""

    payload = base64.urlsafe_b64encode(json.dumps({"exp": expires_at}).encode()).decode()
    return f"header.{payload.rstrip('=')}.signature"


def test_a_token_is_reused_only_until_shortly_before_its_own_expiry() -> None:
    # `az account get-access-token` hands back its cached token, which may expire in minutes: an
    # uninstall reused one for 29 minutes, past its expiry, and ARM answered 401 (2026-10-22).
    now = 1_800_000_000.0
    assert token_reuse_seconds(jwt(now + 3600), now=lambda: now) == TOKEN_REUSE_SECONDS
    assert token_reuse_seconds(jwt(now + 600), now=lambda: now) == (
        600 - TOKEN_EXPIRY_MARGIN_SECONDS)
    assert token_reuse_seconds(jwt(now + 60), now=lambda: now) == 0.0
    # A token whose expiry can't be read keeps the ceiling, as every token did before.
    for opaque in ("tok", "a.!!!.c", f"a.{base64.urlsafe_b64encode(b'[1]').decode()}.c"):
        assert token_reuse_seconds(opaque, now=lambda: now) == TOKEN_REUSE_SECONDS


class RecordingTransport:
    """Each call's bearer token, answered from a script in order."""

    def __init__(self, answers: list[Response]) -> None:
        self.answers = list(answers)
        self.bearers: list[str] = []

    def __call__(self, method: str, url: str, headers: Mapping[str, str], data: bytes | None,
                 timeout: float) -> Response:
        self.bearers.append(headers["Authorization"].removeprefix("Bearer "))
        return self.answers.pop(0)


def test_a_token_that_expires_soon_is_minted_again_before_it_does() -> None:
    clock = [0.0]
    minted: list[str] = []

    def source() -> str:  # each with ten minutes left, as `az` may hand one back
        minted.append(jwt(time.time() + 600))
        return minted[-1]

    transport = RecordingTransport([Response(404, None, {})] * 3)
    arm = ArmClient(CREDENTIALS, "rg-test", transport=transport, sleep=lambda _: None,
                    clock=lambda: clock[0], token_source=source)
    arm.get_server("a")
    clock[0] = 400.0  # inside the token's 480 s of reuse
    arm.get_server("a")
    assert len(minted) == 1
    clock[0] = 500.0  # past it, though well inside the 30-minute ceiling
    arm.get_server("a")
    assert len(minted) == 2 and transport.bearers == [minted[0], minted[0], minted[1]]


def test_a_401_mints_a_new_token_and_sends_the_call_once_more() -> None:
    expired = Response(401, {"error": {"code": "ExpiredAuthenticationToken",
                                       "message": "The access token expiry has passed."}}, {})
    minted: list[str] = []

    def source() -> str:
        minted.append(f"token-{len(minted)}")
        return minted[-1]

    transport = RecordingTransport([expired, Response(404, None, {})])
    arm = ArmClient(CREDENTIALS, "rg-test", transport=transport, sleep=lambda _: None,
                    token_source=source)
    assert arm.get_server("a") is None
    assert transport.bearers == ["token-0", "token-1"]

    # A call that isn't idempotent is sent again too: ARM refuses a 401 before it acts on it.
    transport.answers = [expired, Response(200, {"runId": "run-1"}, {})]
    assert arm.request("POST", "/createRun", {}, idempotent=False).status == 200
    assert transport.bearers[2:] == ["token-1", "token-2"]

    # A second 401 in a row is ARM's answer, not a stale token's: it comes back as it was.
    transport.answers = [expired, expired]
    assert arm.request("GET", "/flexibleServers/b").status == 401
    assert transport.bearers[4:] == ["token-2", "token-3"]


def test_a_name_still_held_by_a_server_being_deleted_reads_unavailable() -> None:
    # A copy whose delete was issued reads 404 while Azure still holds its name (2026-10-20).
    arm, transport = client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("POST", "/checkNameAvailability", Response(200, {
            "name": "copy", "nameAvailable": False, "reason": "AlreadyExists"}, {})),
        ("POST", "/checkNameAvailability", Response(200, {"name": "copy", "nameAvailable": True},
                                                    {})),
    ])
    assert arm.server_name_available("copy") is False
    assert arm.server_name_available("copy") is True
    method, url, data = transport.calls[1]
    assert url.startswith(f"https://management.azure.com/subscriptions/{SUB}/providers/"
                          "Microsoft.DBforPostgreSQL/checkNameAvailability?api-version=")
    assert json.loads(data) == {"name": "copy", "type": "Microsoft.DBforPostgreSQL/flexibleServers"}


GATEWAY_TIMEOUT = Response(504, {"error": {
    "code": "GatewayTimeout", "message": "The gateway did not receive a response from "
    "'Microsoft.DBforPostgreSQL' within the specified time period."}}, {})


def test_a_gateway_timeout_is_sent_again_when_the_call_is_idempotent() -> None:
    # Round 3's Prepare ended on one, twice in a run (2026-10-20). It comes with a body.
    arm, _ = client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("GET", "/flexibleServers/a?", GATEWAY_TIMEOUT),
        ("GET", "/flexibleServers/a?", Response(404, None, {})),
    ])
    assert arm.get_server("a") is None


def test_a_gateway_timeout_on_a_call_that_isnt_idempotent_is_not_sent_again() -> None:
    arm, transport = client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("POST", "/createRun", GATEWAY_TIMEOUT),
    ])
    response = arm.request("POST", "/factories/f/pipelines/p/createRun?api-version=x",
                           {}, idempotent=False)
    assert response.status == 504
    assert sum(1 for _, url, _ in transport.calls if "createRun" in url) == 1


def test_a_name_check_the_gateway_never_answers_is_unknown() -> None:
    arm, _ = client([("POST", "/oauth2/v2.0/token", TOKEN),
                     *[("POST", "/checkNameAvailability", GATEWAY_TIMEOUT)] * 5])
    assert arm.server_name_available("copy") is None


@pytest.mark.parametrize("silence", [
    TimeoutError("timed out"), ConnectionResetError(54, "Connection reset by peer"),
    http.client.RemoteDisconnected("Remote end closed connection without response"),
    urllib.error.URLError("unreachable")], ids=type)
def test_a_call_that_got_no_answer_is_sent_again_when_the_call_is_idempotent(silence) -> None:
    # urllib raises these where boto3 would have sent the call again, so they ended a lane.
    arm, _ = client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("GET", "/flexibleServers/a?", silence),
        ("GET", "/flexibleServers/a?", Response(404, None, {})),
    ])
    assert arm.get_server("a") is None


INTERNAL_ERROR = Response(500, {"error": {"code": "InternalServerError",
                                          "message": "An unexpected error occured."}}, {})


def test_a_500_with_its_error_body_is_sent_again_unless_the_call_isnt_idempotent() -> None:
    # boto3 retries 500-504 for the AWS lanes; a read or a delete is safe to send again.
    arm, _ = client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("GET", "/flexibleServers/a?", INTERNAL_ERROR),
        ("GET", "/flexibleServers/a?", Response(404, None, {})),
    ])
    assert arm.get_server("a") is None
    start, transport = client([("POST", "/oauth2/v2.0/token", TOKEN),
                               ("POST", "/createRun?", INTERNAL_ERROR)])
    with pytest.raises(ArmError):
        start.create_pipeline_run("adf", "r4_pipeline", {"version": 4})
    assert sum(1 for _, url, _ in transport.calls if "createRun" in url) == 1


def test_a_read_waits_twenty_seconds_for_an_answer_and_a_write_two_minutes() -> None:
    # A read of a copy not made yet sat until ARM's 60 s gateway timeout three times on
    # 2026-10-20, and one more read answered at once: a read that waits less is sent again sooner.
    from server.azure_arm import READ_TIMEOUT_SECONDS, WRITE_TIMEOUT_SECONDS

    arm, transport = client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("GET", "/flexibleServers/a?", TimeoutError("timed out")),
        ("GET", "/flexibleServers/a?", Response(404, None, {})),
        ("POST", "/checkNameAvailability", Response(200, {"nameAvailable": True}, {})),
        ("DELETE", "/flexibleServers/a?", Response(204, None, {})),
    ])
    assert arm.get_server("a") is None
    assert arm.server_name_available("a") is True
    assert arm.delete_server("a") == "absent"
    waits = [(method, timeout) for method, path, timeout in transport.timeouts
             if "oauth2" not in path]
    assert waits == [("GET", READ_TIMEOUT_SECONDS), ("GET", READ_TIMEOUT_SECONDS),
                     ("POST", READ_TIMEOUT_SECONDS), ("DELETE", WRITE_TIMEOUT_SECONDS)]
    assert READ_TIMEOUT_SECONDS < 60.0 < WRITE_TIMEOUT_SECONDS


def test_a_start_that_got_no_answer_is_not_sent_again() -> None:
    # It may have landed, and a second createRun starts a second run.
    arm, transport = client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("POST", "/createRun?", TimeoutError("timed out")),
    ])
    with pytest.raises(ArmError, match="no answer to a POST"):
        arm.create_pipeline_run("adf", "r4_pipeline", {"version": 4})
    assert sum(1 for _, url, _ in transport.calls if "createRun" in url) == 1


def test_a_call_never_answered_is_an_arm_error_naming_no_subscription() -> None:
    arm, transport = client([("POST", "/oauth2/v2.0/token", TOKEN),
                             *[("GET", "/flexibleServers/a?", TimeoutError("timed out"))] * 5])
    with pytest.raises(ArmError, match="no answer to a GET") as raised:
        arm.get_server("a")
    assert SUB not in str(raised.value)
    assert sum(1 for _, url, _ in transport.calls if "/flexibleServers/a?" in url) == 5


def test_a_refused_token_is_an_arm_error_without_the_secret() -> None:
    arm, _ = client([("POST", "/oauth2/v2.0/token", Response(
        401, {"error": "invalid_client", "error_description": "bad"}, {}))])
    with pytest.raises(ArmError) as raised:
        arm.get_server("a")
    assert "s3cret" not in str(raised.value)


def test_a_restore_accepted_then_failed_with_internal_server_error_is_not_ready() -> None:
    # Measured: a restore before the source's first full backup is answered 202, and its
    # operation fails ~22 s later with a bare InternalServerError.
    arm, _ = client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("GET", "/flexibleServers/src?", SOURCE),
        ("PUT", "/flexibleServers/copy?", accepted("https://management.azure.com/op/1")),
        ("GET", "/op/1", Response(200, {"status": "InProgress"}, {})),
        ("GET", "/op/1", Response(200, {"status": "Failed", "error": {
            "code": "InternalServerError", "message": "An unexpected error occured"}}, {})),
    ])
    with pytest.raises(RestoreNotReady):
        arm.restore_point_in_time(source_server="src", name="copy",
                                  point_in_time_utc="2026-10-19T03:10:25Z", location="eastus2",
                                  tags={})


def test_a_restore_that_succeeds_returns_the_new_server_and_sends_the_point() -> None:
    restored = {"id": "/x/copy", "properties": {"state": "Ready"}}
    arm, transport = client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("GET", "/flexibleServers/src?", SOURCE),
        ("PUT", "/flexibleServers/copy?", accepted("https://management.azure.com/op/2")),
        ("GET", "/op/2", Response(200, {"status": "Succeeded"}, {})),
        ("GET", "/flexibleServers/copy?", Response(200, restored, {})),
    ])
    result = arm.restore_point_in_time(source_server="src", name="copy",
                                       point_in_time_utc="2026-10-19T03:14:00Z",
                                       location="eastus2", tags={"Owner": "o@x"})
    assert result == restored
    put = next(data for method, url, data in transport.calls if method == "PUT")
    assert b'"createMode": "PointInTimeRestore"' in put
    assert b'"pointInTimeUTC": "2026-10-19T03:14:00Z"' in put


def test_a_restore_names_a_zone_only_when_given_one() -> None:
    # Phase 0: B2s was refused in westus2's zone 3, so a copy is restored into its source's zone.
    arm, transport = client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("PUT", "/flexibleServers/zoned?", accepted("https://management.azure.com/op/z")),
        ("PUT", "/flexibleServers/unzoned?", accepted("https://management.azure.com/op/u")),
    ])
    for name, zone in (("zoned", "1"), ("unzoned", None)):
        arm.begin_restore(source_server="src", name=name, point_in_time_utc="2026-10-19T04:40:00Z",
                          location="westus2", tags={}, source_id=SOURCE.body["id"],
                          availability_zone=zone)
    zoned, unzoned = (json.loads(data) for method, _, data in transport.calls if method == "PUT")
    assert zoned["properties"]["availabilityZone"] == "1"
    assert "availabilityZone" not in unzoned["properties"]


def test_a_restored_server_that_reads_absent_at_first_is_waited_for() -> None:
    # Measured: after a Succeeded restore that reused a just-deleted name, the first read was 404.
    restored = {"id": "/x/copy", "properties": {"state": "Ready"}}
    arm, _ = client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("GET", "/flexibleServers/src?", SOURCE),
        ("PUT", "/flexibleServers/copy?", accepted("https://management.azure.com/op/v")),
        ("GET", "/op/v", Response(200, {"status": "Succeeded"}, {})),
        ("GET", "/flexibleServers/copy?", Response(404, None, {})),
        ("GET", "/flexibleServers/copy?", Response(404, None, {})),
        ("GET", "/flexibleServers/copy?", Response(200, restored, {})),
    ])
    assert arm.restore_point_in_time(source_server="src", name="copy",
                                     point_in_time_utc="2026-10-19T04:34:58Z",
                                     location="eastus2", tags={}) == restored


def test_any_other_failed_restore_is_an_arm_error_naming_no_subscription() -> None:
    arm, _ = client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("GET", "/flexibleServers/src?", SOURCE),
        ("PUT", "/flexibleServers/copy?", accepted("https://management.azure.com/op/3")),
        ("GET", "/op/3", Response(200, {"status": "Failed", "error": {
            "code": "QuotaExceeded", "message": "no vCores"}}, {})),
    ])
    with pytest.raises(ArmError) as raised:
        arm.restore_point_in_time(source_server="src", name="copy",
                                  point_in_time_utc="2026-10-19T03:14:00Z", location="eastus2",
                                  tags={})
    assert not isinstance(raised.value, RestoreNotReady)
    assert "QuotaExceeded" in str(raised.value)
    assert SUB not in str(raised.value)


def test_firewall_rules_are_sent_one_at_a_time_in_order_with_cidr_bounds() -> None:
    arm, transport = client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("PUT", "/firewallRules/operator?", accepted("https://management.azure.com/op/a")),
        ("GET", "/op/a", Response(200, {"status": "Succeeded"}, {})),
        ("PUT", "/firewallRules/egress-01?", accepted("https://management.azure.com/op/b")),
        ("GET", "/op/b", Response(200, {"status": "Succeeded"}, {})),
    ])
    # RFC 5737 documentation ranges: the repository refuses real addresses on shape alone.
    timings = arm.put_firewall_rules("copy", [("operator", "192.0.2.10/32"),
                                              ("egress-01", "198.51.100.0/24")])
    assert [name for name, _ in timings] == ["operator", "egress-01"]
    puts = [(url, data) for method, url, data in transport.calls if method == "PUT"]
    assert b'"startIpAddress": "192.0.2.10", "endIpAddress": "192.0.2.10"' in puts[0][1]
    assert b'"startIpAddress": "198.51.100.0", "endIpAddress": "198.51.100.255"' in puts[1][1]
    # The second PUT waits for the first rule's operation to finish.
    order = [url for _, url, _ in transport.calls if "oauth2" not in url]
    assert order.index(next(u for u in order if "/op/a" in u)) < order.index(
        next(u for u in order if "egress-01" in u))


def test_a_busy_server_is_waited_out_not_failed() -> None:
    # Measured: a firewall rule sent just after a tag merge answered ServerIsBusy.
    arm, transport = client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("PUT", "/firewallRules/operator?", Response(409, {"error": {
            "code": "ServerIsBusy", "message": "another operation is in progress"}}, {})),
        ("PUT", "/firewallRules/operator?", accepted("https://management.azure.com/op/busy")),
        ("GET", "/op/busy", Response(200, {"status": "Succeeded"}, {})),
    ])
    assert [name for name, _ in arm.put_firewall_rules("copy", [("operator", "192.0.2.10/32")])] \
        == ["operator"]
    assert sum(1 for method, url, _ in transport.calls if method == "PUT") == 2


def test_a_host_bits_cidr_is_refused_before_any_call() -> None:
    arm, transport = client([])
    with pytest.raises(ValueError):
        arm.put_firewall_rules("copy", [("bad", "198.51.100.7/24")])
    assert transport.calls == []


def test_a_pgbouncer_parameter_that_does_not_exist_while_off_reads_as_none() -> None:
    arm, _ = client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("GET", "/configurations/pgbouncer.max_client_conn?", Response(404, {"error": {
            "code": "ConfigurationNotExists", "message": "does not exist"}}, {})),
    ])
    assert arm.get_configuration("r5", "pgbouncer.max_client_conn") is None


def test_setting_a_parameter_azure_refuses_while_pgbouncer_is_off_is_an_arm_error() -> None:
    arm, _ = client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("PUT", "/configurations/pgbouncer.max_client_conn?",
         accepted("https://management.azure.com/op/c")),
        ("GET", "/op/c", Response(200, {"status": "Failed", "error": {
            "code": "ServerParameterToCMSBlockedUpdateForDisabledPgBouncer",
            "message": "isn't supported while PgBouncer isn't enabled"}}, {})),
    ])
    with pytest.raises(ArmError) as raised:
        arm.set_configuration("r5", "pgbouncer.max_client_conn", "10100")
    assert raised.value.code == "ServerParameterToCMSBlockedUpdateForDisabledPgBouncer"


def test_throttling_is_retried_after_its_retry_after() -> None:
    slept: list[float] = []
    transport = ScriptedTransport([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("GET", "/flexibleServers/a?", Response(429, None, {"Retry-After": "3"})),
        ("GET", "/flexibleServers/a?", Response(404, None, {})),
    ])
    arm = ArmClient(CREDENTIALS, "rg-test", transport=transport, sleep=slept.append)
    assert arm.get_server("a") is None
    assert slept == [3.0]


def test_cancelling_a_run_that_already_ended_is_already_parked() -> None:
    arm, _ = client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("POST", "/pipelineruns/run-1/cancel?", Response(400, {"error": {
            "code": "PipelineRunNotInProgress", "message": "not in progress"}}, {})),
    ])
    arm.cancel_pipeline_run("adf", "run-1")


def test_only_runs_still_in_flight_count_as_in_progress() -> None:
    arm, transport = client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("POST", "/queryPipelineRuns?", Response(200, {"value": [
            {"runId": "a", "status": "Succeeded"}, {"runId": "b", "status": "InProgress"},
            {"runId": "c", "status": "Queued"}, {"runId": "d", "status": "Cancelled"}]}, {})),
    ])
    runs = arm.pipeline_runs("adf", "r4_pipeline", since_utc="2026-10-19T00:00:00Z",
                             until_utc="2026-10-19T06:00:00Z")
    assert [run["runId"] for run in runs] == ["b", "c"]
    body = next(data for method, url, data in transport.calls if "queryPipelineRuns" in url)
    assert b'"values": ["r4_pipeline"]' in body
    assert b'"operand": "Status", "operator": "In"' in body  # ADF filters by status itself


def test_every_page_of_runs_is_read() -> None:
    # One page of a busy two-day window could hide a run still billing (adversarial review).
    arm, transport = client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("POST", "/queryPipelineRuns?", Response(200, {"value": [
            {"runId": "a", "status": "InProgress"}], "continuationToken": "page-2"}, {})),
        ("POST", "/queryPipelineRuns?", Response(200, {"value": [
            {"runId": "b", "status": "Queued"}]}, {})),
    ])
    runs = arm.pipeline_runs("adf", "r4_pipeline", since_utc="2026-10-19T00:00:00Z",
                             until_utc="2026-10-19T06:00:00Z")
    assert [run["runId"] for run in runs] == ["a", "b"]
    second = [data for _, url, data in transport.calls if "queryPipelineRuns" in url][1]
    assert b'"continuationToken": "page-2"' in second


def test_a_start_is_never_sent_twice_on_a_silent_server_error() -> None:
    # A 5xx with no body may mean the run was created; a second createRun starts a second run.
    arm, transport = client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("POST", "/createRun?", Response(502, None, {})),
    ])
    with pytest.raises(ArmError, match="starting pipeline r4_pipeline"):
        arm.create_pipeline_run("adf", "r4_pipeline", {"version": 4})
    assert sum(1 for _, url, _ in transport.calls if "createRun" in url) == 1


TARGET = (f"/subscriptions/{SUB}/resourceGroups/rg-test/providers/Microsoft.DBforPostgreSQL/"
          "flexibleServers/r4")


MANAGED = "/subscriptions/managed-vnet/resourceGroups/vnet-rg/providers/Microsoft.Network"


def factory_view(status: str) -> Response:
    """The factory's own managed private endpoint, as ARM reports it."""
    return Response(200, {"properties": {"connectionState": {"status": status}}}, {})


FACTORY_APPROVED = factory_view("Approved")


def connections(*rows: tuple[str, str] | tuple[str, str, str]) -> Response:
    """(connection name, state[, the private endpoint it comes from]); the endpoint defaults to
    ADF's ``adf-1.mpe-r4`` in its managed network."""
    return Response(200, {"value": [
        {"id": f"{TARGET}/privateEndpointConnections/{row[0]}", "name": row[0], "properties": {
            "privateEndpoint": {"id": row[2] if len(row) > 2 else
                                f"{MANAGED}/privateEndpoints/adf-1.mpe-r4"},
            "privateLinkServiceConnectionState": {"status": row[1]}}} for row in rows]}, {})


def test_only_the_factory_endpoints_own_connection_is_approved() -> None:
    # Anyone can request a private endpoint to a known resource ID; theirs stays Pending. A
    # storage account names the connection "<account>.<id>", so it is matched by its endpoint.
    stranger = "/subscriptions/other/resourceGroups/x/providers/Microsoft.Network/" \
               "privateEndpoints/strangers-endpoint"
    arm, transport = client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("GET", "/privateEndpointConnections?", connections(
            ("account.1111", "Pending"), ("account.2222", "Pending", stranger))),
        ("PUT", "/privateEndpointConnections/account.1111?", accepted("https://op/p")),
        ("GET", "/privateEndpointConnections?", connections(
            ("account.1111", "Approved"), ("account.2222", "Pending", stranger))),
        ("GET", "/managedPrivateEndpoints/mpe-r4?", FACTORY_APPROVED),
    ])
    approved = arm.approve_private_endpoint_connection(TARGET, "2025-08-01", factory="adf-1",
                                                       endpoint="mpe-r4")
    assert approved.endswith("/account.1111")
    puts = [url.split("?")[0] for method, url, _ in transport.calls if method == "PUT"]
    assert len(puts) == 1 and puts[0].endswith("/account.1111")


def test_two_connections_claiming_the_factorys_endpoint_are_refused() -> None:
    lookalike = "/subscriptions/other/resourceGroups/x/providers/Microsoft.Network/" \
                "privateEndpoints/adf-1.mpe-r4"
    arm, transport = client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("GET", "/privateEndpointConnections?", connections(
            ("account.1111", "Pending"), ("account.2222", "Pending", lookalike)))])
    with pytest.raises(ArmError, match="2 private endpoint connections claim"):
        arm.approve_private_endpoint_connection(TARGET, "2025-08-01", factory="adf-1",
                                                endpoint="mpe-r4")
    assert not [url for method, url, _ in transport.calls if method == "PUT"]


def sleeping_client(script: list[tuple[str, str, Response]]) -> tuple[ArmClient, ScriptedTransport]:
    """A client whose clock moves only when it sleeps, so polling arithmetic is exact."""
    now = [0.0]
    transport = ScriptedTransport(script)
    arm = ArmClient(CREDENTIALS, "rg-test", transport=transport,
                    sleep=lambda seconds: now.__setitem__(0, now[0] + seconds),
                    clock=lambda: now[0])
    return arm, transport


def test_an_approval_left_unanswered_is_sent_again_and_the_state_decides() -> None:
    # The connection's state decides, never an async operation; an approval still unanswered
    # after `resend_seconds` is sent again.
    pending = connections(("adf-1.mpe-r4.abc", "Pending"))
    arm, transport = sleeping_client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("GET", "/privateEndpointConnections?", pending),                      # t=0: send
        ("PUT", "/privateEndpointConnections/adf-1.mpe-r4.abc?", accepted("https://op/1")),
        ("GET", "/privateEndpointConnections?", pending),                      # t=5: wait
        ("GET", "/privateEndpointConnections?", pending),                      # t=10: resend
        ("PUT", "/privateEndpointConnections/adf-1.mpe-r4.abc?", accepted("https://op/2")),
        ("GET", "/privateEndpointConnections?", connections(("adf-1.mpe-r4.abc", "Approved"))),
        ("GET", "/managedPrivateEndpoints/mpe-r4?", FACTORY_APPROVED),
    ])
    arm.approve_private_endpoint_connection(TARGET, "2025-08-01", factory="adf-1",
                                            endpoint="mpe-r4", resend_seconds=10)
    assert not [url for _, url, _ in transport.calls if "/op/" in url]  # operations never awaited
    assert sum(1 for method, _, _ in transport.calls if method == "PUT") == 2


def test_an_approval_the_factory_never_confirms_is_rejected_again() -> None:
    # A stranger named their endpoint "<factory>.<endpoint>" and got in first: the target shows
    # it Approved, but the factory's own endpoint never does (security review, 2026-10-19).
    approved = connections(("account.9999", "Approved",
                            "/subscriptions/other/resourceGroups/x/providers/Microsoft.Network/"
                            "privateEndpoints/adf-1.mpe-r4"))
    script: list[tuple[str, str, Response]] = [("POST", "/oauth2/v2.0/token", TOKEN)]
    for _ in range(5):  # t = 0, 5, 10, 15, 20
        script += [("GET", "/privateEndpointConnections?", approved),
                   ("GET", "/managedPrivateEndpoints/mpe-r4?", factory_view("Pending"))]
    script.append(("PUT", "/privateEndpointConnections/account.9999?", Response(200, {}, {})))
    arm, transport = sleeping_client(script)
    with pytest.raises(ArmError, match="factory never confirmed it, so it was rejected"):
        arm.approve_private_endpoint_connection(TARGET, "2025-08-01", factory="adf-1",
                                                endpoint="mpe-r4", bound_seconds=20)
    rejection = next(data for method, url, data in transport.calls if method == "PUT")
    assert b'"status": "Rejected"' in rejection


def test_an_endpoint_whose_connection_never_appears_is_bounded() -> None:
    arm, _ = sleeping_client([("POST", "/oauth2/v2.0/token", TOKEN)] + [
        ("GET", "/privateEndpointConnections?", connections()) for _ in range(6)])
    with pytest.raises(ArmError, match="not there after 20s"):
        arm.approve_private_endpoint_connection(TARGET, "2025-08-01", factory="adf-1",
                                                endpoint="mpe-r4", bound_seconds=20)


def test_a_rejected_endpoint_connection_is_refused_by_name() -> None:
    arm, _ = sleeping_client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("GET", "/privateEndpointConnections?", connections(("adf-1.mpe-r4.abc", "Rejected")))])
    with pytest.raises(ArmError, match="mpe-r4 private endpoint connection is Rejected"):
        arm.approve_private_endpoint_connection(TARGET, "2025-08-01", factory="adf-1",
                                                endpoint="mpe-r4")


def test_an_arm_refusal_naming_the_subscription_has_it_cut_out() -> None:
    # An ArmError can reach the room; authorization and policy refusals name the subscription.
    arm, _ = client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("GET", "/flexibleServers/a?", Response(403, {"error": {
            "code": "AuthorizationFailed", "message": f"The client may not read "
            f"/subscriptions/{SUB}/resourceGroups/rg-test/providers/x"}}, {}))])
    with pytest.raises(ArmError) as raised:
        arm.get_server("a")
    assert SUB not in str(raised.value) and "/subscriptions/<subscription>/" in str(raised.value)


def test_empty_credentials_are_refused_at_construction() -> None:
    with pytest.raises(ValueError):
        AzureCredentials(tenant_id="t", client_id="", client_secret="s", subscription_id=SUB)


FAILED = Response(200, {"status": "Failed"}, {})  # as Azure answered at 5:08 AM: no error code


def test_a_delete_that_failed_with_the_server_still_there_is_sent_again() -> None:
    # Phase 0, 5:07 AM: a new copy's delete met another write on the server and ended Failed;
    # sent again later, it succeeded.
    arm, transport = sleeping_client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("DELETE", "/flexibleServers/copy?", accepted("https://management.azure.com/op/d1")),
        ("GET", "/op/d1", FAILED),
        ("GET", "/flexibleServers/copy?", Response(200, {"id": "/x/copy"}, {})),
        ("DELETE", "/flexibleServers/copy?", accepted("https://management.azure.com/op/d2")),
        ("GET", "/op/d2", Response(200, {"status": "Succeeded"}, {})),
    ])
    assert arm.delete_server("copy") == "deleted"
    assert sum(1 for method, _, _ in transport.calls if method == "DELETE") == 2


def test_a_delete_refused_while_the_server_is_busy_is_sent_again() -> None:
    arm, transport = sleeping_client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("DELETE", "/flexibleServers/copy?", Response(409, {"error": {
            "code": "ServerIsBusy", "message": "another operation is in progress"}}, {})),
        ("DELETE", "/flexibleServers/copy?", accepted("https://management.azure.com/op/d4")),
        ("GET", "/op/d4", Response(200, {"status": "Succeeded"}, {})),
    ])
    assert arm.delete_server("copy") == "deleted"
    assert sum(1 for method, _, _ in transport.calls if method == "DELETE") == 2


def test_a_failed_delete_of_a_server_already_gone_is_done() -> None:
    arm, transport = sleeping_client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("DELETE", "/flexibleServers/copy?", accepted("https://management.azure.com/op/d3")),
        ("GET", "/op/d3", FAILED),
        ("GET", "/flexibleServers/copy?", Response(404, None, {})),
    ])
    assert arm.delete_server("copy") == "deleted"
    assert sum(1 for method, _, _ in transport.calls if method == "DELETE") == 1


def test_a_delete_that_keeps_failing_stops_before_the_bound_with_its_own_error() -> None:
    # Retries every 15 s against a 40 s bound: sent at 0, 15 and 30 s, and not again at 45.
    failed = Response(200, {"status": "Failed", "error": {
        "code": "ResourceOperationFailure", "message": "terminal provisioning state"}}, {})
    script: list[tuple[str, str, Response]] = [("POST", "/oauth2/v2.0/token", TOKEN)]
    for n in range(4):
        script += [
            ("DELETE", "/flexibleServers/copy?", accepted(f"https://management.azure.com/op/k{n}")),
            ("GET", f"/op/k{n}", failed),
            ("GET", "/flexibleServers/copy?", Response(200, {"id": "/x/copy"}, {}))]
    arm, transport = sleeping_client(script)
    with pytest.raises(ArmError, match="ended Failed: ResourceOperationFailure") as raised:
        arm.delete_server("copy", bound_seconds=40)
    assert raised.value.code == "ResourceOperationFailure"
    assert sum(1 for method, _, _ in transport.calls if method == "DELETE") == 3


def test_a_delete_still_busy_at_the_bound_is_an_arm_error_naming_it() -> None:
    busy = Response(409, {"error": {"code": "ServerIsBusy", "message": "busy"}}, {})
    arm, transport = sleeping_client(
        [("POST", "/oauth2/v2.0/token", TOKEN)] + [("DELETE", "/flexibleServers/copy?", busy)] * 4)
    with pytest.raises(ArmError) as raised:
        arm.delete_server("copy", bound_seconds=12)  # sent at 0, 5 and 10 s, not at 15
    assert raised.value.code == "ServerIsBusy"
    assert sum(1 for method, _, _ in transport.calls if method == "DELETE") == 3


# Run Command (Round 5's runners) ---------------------------------------------------------------

RUN_OPERATION = "https://management.azure.com/operations/run-1"


def finished(message: str, status: str = "Succeeded") -> Response:
    # A finished Run Command operation, as Compute answers its Azure-AsyncOperation poll.
    return Response(200, {"status": status, "properties": {"output": {"value": [
        {"code": "ProvisioningState/succeeded", "level": "Info", "message": message}]}}}, {})


def test_run_command_sends_the_script_and_returns_both_streams() -> None:
    arm, transport = client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("POST", "/virtualMachines/vm-r5-0/runCommand?", accepted(RUN_OPERATION)),
        ("GET", "/operations/run-1", Response(200, {"status": "InProgress"}, {})),
        ("GET", "/operations/run-1", finished(
            "Enable succeeded: \n[stdout]\nOLD_PID=0\nANTI_DEMO_EXIT=0\n\n[stderr]\nwarning\n")),
    ])
    output = arm.run_shell_script("vm-r5-0", ["set -eu", "echo OLD_PID=0"], bound_seconds=120)
    assert output.stdout.splitlines() == ["OLD_PID=0", "ANTI_DEMO_EXIT=0"]
    assert output.stderr.strip() == "warning"
    (sent,) = [json.loads(data) for method, url, data in transport.calls if "runCommand" in url]
    assert sent == {"commandId": "RunShellScript", "script": ["set -eu", "echo OLD_PID=0"]}


def test_run_command_waits_out_a_vm_busy_with_another() -> None:
    busy = Response(409, {"error": {"code": "Conflict", "message": "Run command extension "
                                    "execution is in progress."}}, {})
    arm, transport = sleeping_client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("POST", "/runCommand?", busy),
        ("POST", "/runCommand?", accepted(RUN_OPERATION)),
        ("GET", "/operations/run-1", finished("Enable succeeded: \n[stdout]\nok\n[stderr]\n")),
    ])
    assert arm.run_shell_script("vm-r5-1", ["echo ok"], bound_seconds=60).stdout == "ok"
    assert sum(1 for method, url, _ in transport.calls if "runCommand" in url) == 2


def test_run_command_is_never_sent_again_once_it_may_have_run() -> None:
    # An install appends its archive a chunk at a time: a second send of a chunk would corrupt it.
    for answer in (ConnectionResetError("reset"), Response(503, None, {})):
        arm, transport = client([
            ("POST", "/oauth2/v2.0/token", TOKEN),
            ("POST", "/runCommand?", answer),
            ("POST", "/runCommand?", accepted(RUN_OPERATION)),
        ])
        with pytest.raises(ArmError):
            arm.run_shell_script("vm-r5-0", ["echo chunk"], bound_seconds=60)
        assert sum(1 for method, url, _ in transport.calls if "runCommand" in url) == 1


def test_a_script_the_extension_reports_failed_still_returns_its_streams() -> None:
    # The streams can come back inside the operation's error, quoted.
    failed = Response(200, {"status": "Failed", "error": {
        "code": "VMExtensionProvisioningError",
        "message": "Enable failed: failed to execute command: command terminated with exit "
                   "status=1\\n[stdout]\\nANTI_DEMO_EXIT=1\\n\\n[stderr]\\nANTI_DEMO_STAGE=venv\\n"}},
        {})
    arm, _ = client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("POST", "/runCommand?", accepted(RUN_OPERATION)),
        ("GET", "/operations/run-1", failed),
    ])
    output = arm.run_shell_script("vm-r5-0", ["false"], bound_seconds=60)
    assert output.stdout.splitlines() == ["ANTI_DEMO_EXIT=1"]
    assert "ANTI_DEMO_STAGE=venv" in output.stderr
    # With no streams at all, the operation's own error is the answer.
    arm, _ = client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("POST", "/runCommand?", accepted(RUN_OPERATION)),
        ("GET", "/operations/run-1", Response(200, {"status": "Failed", "error": {
            "code": "VMAgentStatusCommunicationError", "message": "the VM agent is not ready"}},
            {})),
    ])
    with pytest.raises(ArmError, match="VMAgentStatusCommunicationError"):
        arm.run_shell_script("vm-r5-0", ["true"], bound_seconds=60)


def test_a_runner_is_ready_when_running_with_its_agent_ready() -> None:
    def view(power: str, agent: str) -> Response:
        return Response(200, {"statuses": [{"code": "ProvisioningState/succeeded"},
                                           {"code": f"PowerState/{power}"}],
                              "vmAgent": {"statuses": [{"code": "ProvisioningState/succeeded",
                                                        "displayStatus": agent}]}}, {})

    arm, _ = client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("GET", "/virtualMachines/vm-r5-0/instanceView?", view("running", "Ready")),
        ("GET", "/virtualMachines/vm-r5-0/instanceView?", view("running", "Not Ready")),
        ("GET", "/virtualMachines/vm-r5-0/instanceView?", view("deallocated", "Ready")),
        ("GET", "/virtualMachines/vm-r5-0/instanceView?", Response(404, None, {})),
    ])
    assert [arm.vm_ready("vm-r5-0") for _ in range(4)] == [True, False, False, None]


def test_a_run_command_that_outlives_its_bound_is_an_error_naming_the_vm_alone() -> None:
    arm, _ = sleeping_client(
        [("POST", "/oauth2/v2.0/token", TOKEN), ("POST", "/runCommand?", accepted(RUN_OPERATION))]
        + [("GET", "/operations/run-1", Response(200, {"status": "InProgress"}, {}))] * 40)
    with pytest.raises(ArmError) as raised:
        arm.run_shell_script("vm-r5-0", ["sleep 600"], bound_seconds=30)
    assert "vm-r5-0" in str(raised.value) and SUB not in str(raised.value)
