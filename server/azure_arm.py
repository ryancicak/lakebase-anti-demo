"""A small Azure Resource Manager client for the Azure edition, on the standard library alone.

The Azure edition drives about a dozen ARM operations: Flexible Server restores, firewall rules and
server parameters for Rounds 2, 3 and 5, and Data Factory pipeline runs for Rounds 4 and 6. It
does not take the Azure SDK for them. Every wheel the App installs is one more download through a
build proxy that has failed deploys before (pyproject.toml), and these calls are plain REST.

Tokens come from a `token_source`: the deployed App's is a Unity Catalog service credential on its
own Access Connector, and the operator's laptop's is `az login` (`server/azure_identity.py`). So
neither holds an Azure secret. The client-credentials grant against a service
principal is still here for an operator who has one.

**What Phase 0 measured that the API reference does not say** (2026-10-18, eastus2):

* **A restore Azure cannot serve yet is accepted, then fails.** A point-in-time restore requested
  before the source's first full backup exists is answered 202, and its operation fails about
  22 s later with a bare ``InternalServerError``. That is reported as `RestoreNotReady`, which a
  caller retries, never as a broken installation.
* **Firewall writes are serialized per server, and parallel ones queue worse.** 27 rules one at a
  time took 176 s (about 6.3 s each); 12 sent at once took 309 s. `put_firewall_rules` therefore
  sends them one at a time, on purpose.
* **ARM's operation status lags the data plane by 30-60 s, in both directions.** PgBouncer
  answered on 6432 about 9 s after the switch-on request while its operation still read
  InProgress, and refused about 5.5 s after switch-off while the operation took 38-70 s. A clock
  that matters stops on the data plane; `wait_operation` is for knowing a write has landed.

Synchronous, like boto3. The app calls it through ``asyncio.to_thread``.
"""

from __future__ import annotations

import base64
import http.client
import ipaddress
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

ARM = "https://management.azure.com"
LOGIN = "https://login.microsoftonline.com"
#: The newest generally available Flexible Server API on 2026-10-18.
POSTGRES_API = "2025-08-01"
DATA_FACTORY_API = "2018-06-01"
COMPUTE_API = "2024-07-01"

#: How long a token is reused at most. Entra ID issues them for about an hour.
TOKEN_REUSE_SECONDS = 1800.0
#: How long before its own expiry a token is minted again, so ARM still accepts a call sent with it.
#: Shorter than the life `az` leaves on a token it hands back (it refreshes one with five minutes
#: or less left), so a token is reused for minutes, not minted again for every call.
TOKEN_EXPIRY_MARGIN_SECONDS = 120.0
#: Bounded retries for throttling and transient front-door failures (429, 5xx before a body).
REQUEST_ATTEMPTS = 5
#: ARM's own gateway saying the service behind it didn't answer in time.
_GATEWAY_STATUSES = frozenset({502, 503, 504})
#: The server errors an idempotent call is sent again for: the gateway's, and a 500 with its
#: error body, as boto3 retries 500-504 for the AWS lanes.
_RETRIED_STATUSES = _GATEWAY_STATUSES | {500}
#: How long a read may go unanswered before it is sent again. ARM answers a server read in about
#: a second, but a read of a copy that doesn't exist yet, or is being made, sometimes sat until
#: the gateway's 60 s timeout (2026-10-20, three times: a preflight, a cooldown, the probe), and
#: one more read answered at once. A write keeps the long wait, since it may have landed.
READ_TIMEOUT_SECONDS = 20.0
WRITE_TIMEOUT_SECONDS = 120.0
#: What a call that got no answer at all raises: a connection refused or reset, a socket that
#: timed out, a response cut off mid-read. boto3 sends such a call again, so the AWS lanes ride
#: these out; urllib leaves it to the caller.
_UNANSWERED = (OSError, http.client.HTTPException)
#: How often an async operation is polled. One second keeps a measured switch time honest to
#: the second; the first spike polled every five and could only say "by then".
OPERATION_POLL_SECONDS = 1.0

#: What a restore ARM accepted and then could not serve looks like.
_NOT_READY_CODES = frozenset({"InternalServerError"})
#: A server refusing a write because another of its operations is still running.
SERVER_BUSY = "ServerIsBusy"
BUSY_RETRY_SECONDS = 5.0
BUSY_BOUND_SECONDS = 600.0
#: How long a restored server may read as absent after its operation Succeeded.
VISIBLE_RETRY_SECONDS = 5.0
VISIBLE_BOUND_SECONDS = 600.0
#: How long to wait before sending again a delete that ended Failed with the server still there.
DELETE_RETRY_SECONDS = 15.0
#: A factory endpoint's approval: how long to wait for its connection to read Approved, how long
#: one approval may go unanswered before it is sent again, and how often the state is read.
APPROVAL_BOUND_SECONDS = 900.0
#: A bound on `pipeline_runs`' paging: far past two days of one pipeline's runs.
PIPELINE_RUN_PAGES = 50
APPROVAL_RESEND_SECONDS = 120.0
APPROVAL_POLL_SECONDS = 5.0
#: A VM runs one Run Command at a time and refuses another with a 409 while it does. Nothing ran,
#: so it is sent again after this long, within the caller's bound.
RUN_COMMAND_BUSY_RETRY_SECONDS = 10.0
#: The two streams in Run Command's message: "Enable succeeded: \n[stdout]\n...\n[stderr]\n...".
_RUN_COMMAND_STREAMS = re.compile(r"\[stdout\]\n(?P<stdout>.*?)\n?\[stderr\]\n?(?P<stderr>.*)\Z",
                                  re.S)


class ArmError(RuntimeError):
    """An ARM call failed in a way the caller cannot fix by waiting.

    Its message carries ARM's error code and message and the resource's own name, never the
    subscription ID or a bearer token: the operator log quotes this server's messages.
    """

    def __init__(self, message: str, *, status: int | None = None, code: str | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.code = code


class RestoreNotReady(ArmError):
    """Azure accepted a point-in-time restore and then could not serve it yet. Retry later."""


@dataclass(frozen=True)
class AzureCredentials:
    """The subscription, and a service principal for the client-credentials grant, if any.

    The grant is used only when `ArmClient` is given no `token_source`. The App and the
    installer each give one, so they carry no client.
    """

    tenant_id: str
    subscription_id: str
    client_id: str = ""
    client_secret: str = field(default="", repr=False)

    def __post_init__(self) -> None:
        for name in ("tenant_id", "subscription_id"):
            if not str(getattr(self, name)).strip():
                raise ValueError(f"Azure credential {name} is empty")
        if bool(self.client_id.strip()) != bool(self.client_secret.strip()):
            raise ValueError("Azure credential client_id and client_secret come together")


@dataclass(frozen=True)
class Response:
    status: int
    body: Any
    headers: Mapping[str, str]


@dataclass(frozen=True)
class RunCommandOutput:
    """What a Run Command script printed: the last 4,096 bytes of each stream, Azure's limit."""

    stdout: str
    stderr: str


def _run_command_output(body: Any) -> RunCommandOutput:
    """The two streams, wherever the operation's answer carries them.

    A finished operation answers with ``properties.output.value[].message``; a Location poll
    answers with the ``value`` list itself; and a script the extension reports failed can come
    back as the operation's error message, with the streams inside it.
    """

    texts: list[str] = []
    if isinstance(body, dict):
        properties = body.get("properties") if isinstance(body.get("properties"), dict) else {}
        for holder in (properties.get("output"), body):
            statuses = holder.get("value") if isinstance(holder, dict) else None
            for status in statuses if isinstance(statuses, list) else []:
                if isinstance(status, dict) and status.get("message"):
                    texts.append(str(status["message"]))
        error = body.get("error")
        if not texts and isinstance(error, dict) and error.get("message"):
            texts.append(str(error["message"]))
    text = "\n".join(texts)
    if "\n" not in text and "\\n" in text:
        text = text.replace("\\n", "\n")  # the streams, quoted inside an error message
    match = _RUN_COMMAND_STREAMS.search(text)
    if match is None:
        return RunCommandOutput(stdout="", stderr=text[-4096:])
    return RunCommandOutput(stdout=match.group("stdout"), stderr=match.group("stderr"))


#: (method, url, headers, body bytes or None, timeout) -> Response. Injected by tests.
Transport = Callable[[str, str, Mapping[str, str], bytes | None, float], Response]


def urllib_transport(
    method: str, url: str, headers: Mapping[str, str], data: bytes | None, timeout: float
) -> Response:
    request = urllib.request.Request(url, data=data, method=method, headers=dict(headers))
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            return Response(response.status, _decode(raw), dict(response.headers))
    except urllib.error.HTTPError as error:
        return Response(error.code, _decode(error.read()), dict(error.headers or {}))


def _decode(raw: bytes) -> Any:
    if not raw:
        return None
    try:
        return json.loads(raw)
    except ValueError:
        return raw.decode("utf-8", errors="replace")[:2000]


def _header(headers: Mapping[str, str], name: str) -> str | None:
    lowered = name.lower()
    for key, value in headers.items():
        if key.lower() == lowered:
            return value
    return None


#: A subscription ID inside ARM's own words, e.g. "/subscriptions/<guid>/resourceGroups/...".
_SUBSCRIPTION = re.compile(r"(?i)(/subscriptions/)[0-9a-f-]{36}")


def _error_of(body: Any) -> tuple[str | None, str]:
    """ARM's error code and message. A message naming the subscription (authorization and
    policy refusals do) has the ID cut out, because an `ArmError` can reach the room."""

    if isinstance(body, dict):
        error = body.get("error") if isinstance(body.get("error"), dict) else body
        code, message = error.get("code"), str(error.get("message") or "")
    else:
        code, message = None, str(body or "")
    return code, _SUBSCRIPTION.sub(r"\1<subscription>", message)[:500]


def token_reuse_seconds(token: str, *, ceiling: float = TOKEN_REUSE_SECONDS,
                        now: Callable[[], float] = time.time) -> float:
    """How long `token` may be reused: `ceiling`, or until `TOKEN_EXPIRY_MARGIN_SECONDS` before
    the token's own expiry, if that comes first.

    The expiry is the token's JWT `exp` claim, read without checking anything else, because it
    can only shorten the reuse. `az account get-access-token` hands back the token it has cached,
    which may expire in minutes: an uninstall reused one for 29 minutes, past its expiry, and ARM
    answered 401 (2026-10-22). A token with no readable `exp` keeps the ceiling.
    """

    try:
        payload = token.split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        expires_at = float(claims["exp"])
    except (IndexError, KeyError, TypeError, ValueError):
        return ceiling
    return max(0.0, min(ceiling, expires_at - now() - TOKEN_EXPIRY_MARGIN_SECONDS))


class ArmClient:
    """One subscription, one resource group, one service principal."""

    def __init__(
        self,
        credentials: AzureCredentials,
        resource_group: str,
        *,
        transport: Transport = urllib_transport,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        token_source: Callable[[], str] | None = None,
    ) -> None:
        """`token_source`, when given, replaces the client-credentials grant: the App's service
        credential, or the operator's `az login` (`server/azure_identity.py`)."""

        if not resource_group.strip():
            raise ValueError("resource_group is empty")
        self._credentials = credentials
        self._resource_group = resource_group
        self._transport = transport
        self._sleep = sleep
        self._clock = clock
        self._token_source = token_source
        #: The token in use: the token, when it was minted, and how long it may be reused.
        self._token: tuple[str, float, float] | None = None

    @property
    def resource_group_path(self) -> str:
        return (f"/subscriptions/{self._credentials.subscription_id}"
                f"/resourceGroups/{self._resource_group}")

    def _bearer(self) -> str:
        if self._token and self._clock() - self._token[1] < self._token[2]:
            return self._token[0]
        if self._token_source is not None:
            token = self._token_source()
            self._token = (token, self._clock(), token_reuse_seconds(token))
            return token
        if not self._credentials.client_id:
            raise ArmError("no Azure token source: give a token_source or a service principal")
        form = urllib.parse.urlencode({
            "grant_type": "client_credentials",
            "client_id": self._credentials.client_id,
            "client_secret": self._credentials.client_secret,
            "scope": f"{ARM}/.default",
        }).encode()
        response = self._transport(
            "POST", f"{LOGIN}/{self._credentials.tenant_id}/oauth2/v2.0/token",
            {"Content-Type": "application/x-www-form-urlencoded"}, form, 30.0)
        token = response.body.get("access_token") if isinstance(response.body, dict) else None
        if response.status != 200 or not token:
            code, message = _error_of(response.body)
            raise ArmError(f"Entra ID refused the service principal's token request: "
                           f"{code or response.status}: {message}", status=response.status,
                           code=code)
        self._token = (str(token), self._clock(), token_reuse_seconds(str(token)))
        return self._token[0]

    def request(
        self, method: str, url_or_path: str, body: Any = None, *, idempotent: bool = True,
        timeout: float | None = None,
    ) -> Response:
        """One ARM call, retried on throttling, a front door that answered nothing, and a gateway.

        A 502, 503 or 504 is ARM's own gateway saying the service behind it didn't answer in time.
        It comes with a body (``GatewayTimeout``), and an idempotent call is safe to send again.
        Round 3's Prepare met one (2026-10-20, the release bar's second run), and it ended the
        Prepare. A 500 with its error body is sent again too, as boto3 does. A call that isn't
        `idempotent` (``createRun``) is retried on throttling alone. A 5xx may mean the first
        request landed, and a second would start a second run.

        A call that got no answer at all (`_UNANSWERED`) is sent again on the same terms, and a
        read waits `READ_TIMEOUT_SECONDS` for one (`timeout` overrides). Its last failure, or the
        first of a call that isn't idempotent, is an `ArmError` naming the method alone: the URL
        carries the subscription.

        A 401 drops the token, and the call is sent once more with a new one: a token can run out
        between its own reuse check and ARM's (`token_reuse_seconds`). ARM refuses a 401 before
        it acts on anything, so this is safe for a call that isn't idempotent too.
        """

        url = url_or_path if url_or_path.startswith("https://") else ARM + url_or_path
        data = json.dumps(body).encode() if body is not None else None
        if timeout is None:
            timeout = READ_TIMEOUT_SECONDS if method == "GET" else WRITE_TIMEOUT_SECONDS
        last: Response | None = None
        reminted = False
        for attempt in range(REQUEST_ATTEMPTS):
            headers = {"Authorization": f"Bearer {self._bearer()}",
                       "Content-Type": "application/json"}
            try:
                last = self._transport(method, url, headers, data, timeout)
            except _UNANSWERED as error:
                if not idempotent or attempt == REQUEST_ATTEMPTS - 1:
                    raise ArmError(f"ARM gave no answer to a {method} "
                                   f"({type(error).__name__})") from error
                self._sleep(min(2.0 ** attempt, 30.0))
                continue
            if last.status == 401 and not reminted and attempt < REQUEST_ATTEMPTS - 1:
                self._token, reminted = None, True
                continue
            retryable = last.status == 429 or (
                idempotent and last.status >= 500
                and (last.body is None or last.status in _RETRIED_STATUSES))
            if not retryable or attempt == REQUEST_ATTEMPTS - 1:
                return last
            wait = _header(last.headers, "Retry-After")
            self._sleep(min(float(wait) if wait and wait.isdigit() else 2.0 ** attempt, 30.0))
        assert last is not None
        return last

    def wait_operation(self, response: Response, *, bound_seconds: float) -> tuple[str, Any]:
        """Follow a 202's Azure-AsyncOperation (or Location) to a terminal status."""

        url = _header(response.headers, "Azure-AsyncOperation") or _header(
            response.headers, "Location")
        if response.status in (200, 201) and not url:
            return "Succeeded", response.body
        if not url:
            raise ArmError(f"ARM answered {response.status} with no operation to follow")
        started = self._clock()
        body: Any = None
        while self._clock() - started < bound_seconds:
            polled = self.request("GET", url)
            body = polled.body
            status = body.get("status") if isinstance(body, dict) else None
            if polled.status == 200 and status in ("Succeeded", "Failed", "Canceled"):
                return status, body
            resource = isinstance(body, dict) and body.get("id")
            if polled.status == 200 and status is None and resource:
                return "Succeeded", body  # a Location poll answers with the resource itself
            self._sleep(OPERATION_POLL_SECONDS)
        return "Timeout", body

    # Flexible Server -------------------------------------------------------------------------

    def server_path(self, name: str) -> str:
        return (f"{self.resource_group_path}/providers/Microsoft.DBforPostgreSQL/"
                f"flexibleServers/{name}")

    def get_server(self, name: str) -> dict[str, Any] | None:
        response = self.request("GET", f"{self.server_path(name)}?api-version={POSTGRES_API}")
        if response.status == 404:
            return None
        if response.status != 200 or not isinstance(response.body, dict):
            code, message = _error_of(response.body)
            raise ArmError(f"reading server {name}: {code or response.status}: {message}",
                           status=response.status, code=code)
        return response.body

    def server_name_available(self, name: str) -> bool | None:
        """Whether Azure would take a new server named `name` now; None when it didn't say.

        A server Azure is still deleting already reads 404 while its name stays taken, so a
        restore to it is refused ``ServerNameAlreadyExists``. A towel issues its copy's delete and
        returns, and the next bout's restore met exactly that (2026-10-20, the release bar's
        `finish-linger` wave after `rapid-rearm-towel`). Only this call sees that window.

        It is also the call that timed out at ARM's gateway, twice in one run, each time in the
        window it exists for. Still unanswered after `request`'s retries, it is None: not known to
        be free, which its callers wait out as they wait out a name still taken.
        """

        response = self.request(
            "POST",
            f"/subscriptions/{self._credentials.subscription_id}/providers/"
            f"Microsoft.DBforPostgreSQL/checkNameAvailability?api-version={POSTGRES_API}",
            {"name": name, "type": "Microsoft.DBforPostgreSQL/flexibleServers"},
            timeout=READ_TIMEOUT_SECONDS)  # a read, sent as a POST
        if response.status in _GATEWAY_STATUSES:
            return None
        if response.status != 200 or not isinstance(response.body, dict):
            code, message = _error_of(response.body)
            raise ArmError(f"checking the server name {name}: {code or response.status}: "
                           f"{message}", status=response.status, code=code)
        return bool(response.body.get("nameAvailable"))

    def restore_point_in_time(
        self,
        *,
        source_server: str,
        name: str,
        point_in_time_utc: str,
        location: str,
        tags: Mapping[str, str],
        bound_seconds: float = 4 * 3600.0,
    ) -> dict[str, Any]:
        """Restore `source_server` to a new server `name` at `point_in_time_utc`, and wait.

        Raises RestoreNotReady when Azure accepts the request and then cannot serve the point.
        """

        accepted = self.begin_restore(source_server=source_server, name=name,
                                      point_in_time_utc=point_in_time_utc, location=location,
                                      tags=tags)
        return self.finish_restore(accepted, source_server=source_server, name=name,
                                   point_in_time_utc=point_in_time_utc,
                                   bound_seconds=bound_seconds)

    def begin_restore(
        self,
        *,
        source_server: str,
        name: str,
        point_in_time_utc: str,
        location: str,
        tags: Mapping[str, str],
        source_id: str | None = None,
        availability_zone: str | None = None,
    ) -> Response:
        """Send the restore request and return ARM's acceptance, without waiting.

        Split from `finish_restore` so a caller can report "accepted" between the two: Round 2's
        protocol requires the first control-plane request to precede any presentation I/O.
        Pass `source_id` (read at preflight) to make the request the very first call, and the
        source's `availability_zone`. A size can be refused in one zone of a region and offered in
        the others (Phase 0: B2s in westus2's zone 3), and an unpinned copy lands where Azure picks.
        """

        if source_id is None:
            source = self.get_server(source_server)
            if source is None:
                raise ArmError(f"restore source {source_server} does not exist", status=404)
            source_id = str(source["id"])
        properties: dict[str, Any] = {
            "createMode": "PointInTimeRestore", "sourceServerResourceId": source_id,
            "pointInTimeUTC": point_in_time_utc}
        if availability_zone:
            properties["availabilityZone"] = availability_zone
        body = {"location": location, "tags": dict(tags), "properties": properties}
        response = self.request("PUT", f"{self.server_path(name)}?api-version={POSTGRES_API}", body)
        if response.status not in (200, 201, 202):
            code, message = _error_of(response.body)
            raise ArmError(f"restore of {source_server} to {name} refused: "
                           f"{code or response.status}: {message}", status=response.status,
                           code=code)
        return response

    def finish_restore(
        self,
        accepted: Response,
        *,
        source_server: str,
        name: str,
        point_in_time_utc: str,
        bound_seconds: float = 4 * 3600.0,
    ) -> dict[str, Any]:
        """Wait out an accepted restore. Raises RestoreNotReady for a point Azure can't serve."""

        status, operation = self.wait_operation(accepted, bound_seconds=bound_seconds)
        if status != "Succeeded":
            code, message = _error_of(operation)
            if code in _NOT_READY_CODES:
                raise RestoreNotReady(
                    f"Azure accepted the restore of {source_server} to {point_in_time_utc} and "
                    f"could not serve it yet ({code})", code=code)
            raise ArmError(f"restore of {source_server} to {name} ended {status}: "
                           f"{code}: {message}", code=code)
        # The restored server can read as absent for a while after its operation Succeeded: in
        # Phase 0's third live Round 2 run, it reused the name of a copy deleted 34 s earlier
        # (as every bout of a run does), and the first read answered 404 while the server stood
        # Ready. So the read is retried within a bound before the copy is called missing.
        visible_by = self._clock() + VISIBLE_BOUND_SECONDS
        while True:
            restored = self.get_server(name)
            if restored is not None:
                return restored
            if self._clock() >= visible_by:
                raise ArmError(
                    f"restore of {source_server} reported Succeeded but {name} is still absent "
                    f"after {VISIBLE_BOUND_SECONDS:.0f}s")
            self._sleep(VISIBLE_RETRY_SECONDS)

    def merge_tags(self, resource_id: str, tags: Mapping[str, str]) -> None:
        """Add `tags` to a resource through the Tags API, keeping the tags it already has.

        A point-in-time restore gives its copy the *source's* tags and ignores the ones in the
        request (Phase 0, 2026-10-18), so a copy's ownership tags are merged on afterwards.
        """

        response = self.request(
            "PATCH", f"{resource_id}/providers/Microsoft.Resources/tags/default"
                     "?api-version=2021-04-01",
            {"operation": "Merge", "properties": {"tags": dict(tags)}})
        if response.status not in (200, 202):
            code, message = _error_of(response.body)
            raise ArmError(f"tagging {resource_id.rsplit('/', 1)[-1]}: {code or response.status}: "
                           f"{message}", status=response.status, code=code)

    def list_full_backups(self, name: str) -> list[str]:
        """The completion times of a server's full backups, oldest first.

        A server has none for its first few minutes (4.2 min in Phase 0), and until it has one
        it cannot be restored at all.
        """

        response = self.request(
            "GET", f"{self.server_path(name)}/backups?api-version={POSTGRES_API}")
        if response.status != 200 or not isinstance(response.body, dict):
            code, message = _error_of(response.body)
            raise ArmError(f"listing backups of {name}: {code or response.status}: {message}",
                           status=response.status, code=code)
        return sorted(
            str(row["properties"]["completedTime"]) for row in response.body.get("value", [])
            # Azure spells it "Full"; an earlier API spelled it "FULL".
            if str((row.get("properties") or {}).get("backupType", "")).upper() == "FULL")

    def begin_delete_server(self, name: str) -> Response:
        """Issue a delete and return without waiting (a cancelled lane's teardown)."""

        return self.request("DELETE", f"{self.server_path(name)}?api-version={POSTGRES_API}")

    def delete_server(self, name: str, *, bound_seconds: float = 1800.0) -> str:
        """Delete a server and wait. Returns 'absent' when there was nothing to delete.

        A delete can end Failed while Azure is still finishing other work on a new server. In
        Phase 0 a copy's delete, sent 80 s after its restore finished, met a configuration write
        on the server that began the same second, and ended ``ResourceOperationFailure`` 62 s
        later. Sent again three minutes on, it succeeded. So a failed delete of a server that
        still exists is sent again, and so is one refused ``ServerIsBusy``. No delete is sent at
        or after the bound: the error raised then is the last refusal's own.
        """

        deadline = self._clock() + bound_seconds
        while True:
            response = self.request("DELETE",
                                    f"{self.server_path(name)}?api-version={POSTGRES_API}")
            if response.status in (204, 404):
                return "absent"
            if response.status not in (200, 202):
                code, message = _error_of(response.body)
                pause = BUSY_RETRY_SECONDS  # a wait, as for any write
                if code != SERVER_BUSY or self._clock() + pause >= deadline:
                    raise ArmError(f"delete of {name}: {code or response.status}: {message}",
                                   status=response.status, code=code)
            else:
                status, operation = self.wait_operation(
                    response, bound_seconds=max(deadline - self._clock(), OPERATION_POLL_SECONDS))
                if status == "Succeeded":
                    return "deleted"
                code, message = _error_of(operation)
                if status == "Failed" and self.get_server(name) is None:
                    return "deleted"
                pause = DELETE_RETRY_SECONDS
                if status != "Failed" or self._clock() + pause >= deadline:
                    raise ArmError(f"delete of {name} ended {status}: {code}: {message}",
                                   code=code)
            self._sleep(pause)

    def _server_write(self, url: str, body: Any, what: str, *, bound_seconds: float) -> None:
        """One write to a server's child resource, waiting out ``ServerIsBusy``.

        A server takes one operation at a time: in Phase 0's live Round 2 run, a firewall rule sent
        to a copy seconds after its restore, just after a tag merge, was refused ``ServerIsBusy``.
        (A settled server took writes at once after a merge.) That is a wait, not a failure.
        """

        started = self._clock()
        while True:
            response = self.request("PUT", url, body)
            code, message = _error_of(response.body)
            if response.status in (200, 201, 202):
                status, operation = self.wait_operation(response, bound_seconds=bound_seconds)
                if status == "Succeeded":
                    return
                code, message = _error_of(operation)
                if code != SERVER_BUSY:
                    raise ArmError(f"{what} ended {status}: {code}: {message}", code=code)
            elif code != SERVER_BUSY:
                raise ArmError(f"{what}: {code or response.status}: {message}",
                               status=response.status, code=code)
            if self._clock() - started >= BUSY_BOUND_SECONDS:
                raise ArmError(f"{what}: the server stayed busy for {BUSY_BOUND_SECONDS:.0f}s",
                               code=SERVER_BUSY)
            self._sleep(BUSY_RETRY_SECONDS)

    def put_firewall_rules(
        self, server: str, rules: Iterable[tuple[str, str]], *, bound_seconds: float = 900.0
    ) -> list[tuple[str, float]]:
        """Create (name, IPv4 CIDR) rules on `server`, one at a time. Returns seconds per rule.

        One at a time is the fast way, not the cautious one: see the module docstring.
        """

        timings: list[tuple[str, float]] = []
        for name, cidr in rules:
            network = ipaddress.IPv4Network(cidr, strict=True)
            started = self._clock()
            body = {"properties": {"startIpAddress": str(network[0]),
                                   "endIpAddress": str(network[-1])}}
            path = f"{self.server_path(server)}/firewallRules/{name}"
            self._server_write(f"{path}?api-version={POSTGRES_API}", body,
                               f"firewall rule {name} on {server}", bound_seconds=bound_seconds)
            timings.append((name, self._clock() - started))
        return timings

    def get_configuration(self, server: str, name: str) -> str | None:
        """A server parameter's value, or None when it does not exist right now.

        PgBouncer's parameters other than ``pgbouncer.enabled`` do not exist while PgBouncer is
        off (ConfigurationNotExists), so None is an answer, not an error.
        """

        response = self.request(
            "GET", f"{self.server_path(server)}/configurations/{name}?api-version={POSTGRES_API}")
        if response.status == 404:
            return None
        if response.status != 200 or not isinstance(response.body, dict):
            code, message = _error_of(response.body)
            raise ArmError(f"reading {name} on {server}: {code or response.status}: {message}",
                           status=response.status, code=code)
        return response.body.get("properties", {}).get("value")

    def set_configuration(
        self, server: str, name: str, value: str, *, bound_seconds: float = 1800.0
    ) -> None:
        self._server_write(
            f"{self.server_path(server)}/configurations/{name}?api-version={POSTGRES_API}",
            {"properties": {"value": value, "source": "user-override"}},
            f"setting {name}={value} on {server}", bound_seconds=bound_seconds)

    # Data Factory ----------------------------------------------------------------------------

    def factory_path(self, factory: str) -> str:
        return f"{self.resource_group_path}/providers/Microsoft.DataFactory/factories/{factory}"

    def create_pipeline_run(
        self, factory: str, pipeline: str, parameters: Mapping[str, Any] | None = None
    ) -> str:
        response = self.request(
            "POST",
            f"{self.factory_path(factory)}/pipelines/{pipeline}/createRun"
            f"?api-version={DATA_FACTORY_API}",
            dict(parameters or {}), idempotent=False)
        run_id = response.body.get("runId") if isinstance(response.body, dict) else None
        if response.status != 200 or not run_id:
            code, message = _error_of(response.body)
            raise ArmError(f"starting pipeline {pipeline}: {code or response.status}: {message}",
                           status=response.status, code=code)
        return str(run_id)

    def get_pipeline_run(self, factory: str, run_id: str) -> dict[str, Any]:
        response = self.request(
            "GET",
            f"{self.factory_path(factory)}/pipelineruns/{run_id}?api-version={DATA_FACTORY_API}")
        if response.status != 200 or not isinstance(response.body, dict):
            code, message = _error_of(response.body)
            raise ArmError(f"reading pipeline run {run_id}: {code or response.status}: {message}",
                           status=response.status, code=code)
        return response.body

    def pipeline_runs(
        self, factory: str, pipeline: str, *, since_utc: str, until_utc: str,
        statuses: Iterable[str] = ("Queued", "InProgress", "Canceling"),
    ) -> list[dict[str, Any]]:
        """Runs of `pipeline` updated in the window and in one of `statuses`, every page of them.

        Prepare asks this with the default statuses to confirm no run is in flight before the bell,
        the Azure counterpart of Glue's single run slot. ADF filters by status itself and pages
        what's left (``continuationToken``). Reading only the first page could miss a run still
        billing.
        """

        wanted = list(dict.fromkeys(statuses))
        query: dict[str, Any] = {
            "lastUpdatedAfter": since_utc, "lastUpdatedBefore": until_utc,
            "filters": [{"operand": "PipelineName", "operator": "Equals", "values": [pipeline]},
                        {"operand": "Status", "operator": "In", "values": wanted}]}
        runs: list[dict[str, Any]] = []
        for _ in range(PIPELINE_RUN_PAGES):
            response = self.request(
                "POST",
                f"{self.factory_path(factory)}/queryPipelineRuns?api-version={DATA_FACTORY_API}",
                query)
            if response.status != 200 or not isinstance(response.body, dict):
                code, message = _error_of(response.body)
                raise ArmError(f"querying runs of {pipeline}: {code or response.status}: "
                               f"{message}", status=response.status, code=code)
            runs.extend(run for run in response.body.get("value", [])
                        if run.get("status") in wanted)
            token = response.body.get("continuationToken")
            if not token:
                return runs
            query = {**query, "continuationToken": token}
        raise ArmError(f"querying runs of {pipeline}: more than {PIPELINE_RUN_PAGES} pages")

    def activity_runs(
        self, factory: str, run_id: str, *, since_utc: str, until_utc: str
    ) -> list[dict[str, Any]]:
        """A run's activities, whose output carries a data flow's sink metrics and error."""

        response = self.request(
            "POST",
            f"{self.factory_path(factory)}/pipelineruns/{run_id}/queryActivityruns"
            f"?api-version={DATA_FACTORY_API}",
            {"lastUpdatedAfter": since_utc, "lastUpdatedBefore": until_utc})
        if response.status != 200 or not isinstance(response.body, dict):
            code, message = _error_of(response.body)
            raise ArmError(f"reading activities of run {run_id}: {code or response.status}: "
                           f"{message}", status=response.status, code=code)
        return list(response.body.get("value", []))

    def approve_private_endpoint_connection(
        self,
        target_id: str,
        api_version: str,
        *,
        factory: str,
        endpoint: str,
        bound_seconds: float = APPROVAL_BOUND_SECONDS,
        resend_seconds: float = APPROVAL_RESEND_SECONDS,
    ) -> str:
        """Approve one factory endpoint's connection on `target_id`; wait until it reads Approved.

        A factory's managed private endpoint lands Pending on its target (a Flexible Server, the
        ADLS account), and Terraform's provider does not approve it, so the installer does, here.

        * **Only this endpoint's connection.** Anyone can request a private endpoint to a resource
          whose ID they know, and the request waits, Pending, for the owner, so approving every
          Pending connection would let a stranger in. ADF's endpoint is ``<factory>.<endpoint>``,
          the last segment of the connection's ``privateEndpoint.id``. The connection's own name
          varies by target (a Flexible Server names it ``<factory>.<endpoint>.<id>``, a storage
          account ``<account>.<id>``), so it isn't used. Two connections claiming the same
          endpoint is a refusal, not a choice. Other connections are left exactly as they are.
        * **The factory must agree.** A private endpoint's name is its creator's choice, so a
          stranger can name theirs ``<factory>.<endpoint>`` too. The name only picks the
          candidate. The connection counts as the factory's once the factory's own endpoint reads
          Approved as well (29 s after the target did, in Phase 0). A connection approved on the
          target that the factory never confirms within the bound is set back to Rejected, and
          the call fails.
        * **The connection's state is the record, not the approval's operation.** The state is
          read until it says Approved, and an approval unanswered for `resend_seconds` is sent
          again. In Phase 0 (2026-10-19) an approval took about a minute. A subscription's policy
          can spend minutes configuring a target just after it is created.

        Returns the connection's ID. Raises if it hasn't appeared, or isn't confirmed Approved by
        both sides, within `bound_seconds`.
        """

        expected = f"{factory}.{endpoint}".lower()

        def from_endpoint(connection: Mapping[str, Any]) -> bool:
            source = ((connection.get("properties") or {}).get("privateEndpoint") or {})
            return str(source.get("id") or "").rsplit("/", 1)[-1].lower() == expected

        deadline = self._clock() + bound_seconds
        sent_at: float | None = None
        while True:
            response = self.request(
                "GET", f"{target_id}/privateEndpointConnections?api-version={api_version}")
            if response.status != 200 or not isinstance(response.body, dict):
                code, message = _error_of(response.body)
                raise ArmError(f"listing private endpoint connections: {code or response.status}: "
                               f"{message}", status=response.status, code=code)
            ours = [connection for connection in response.body.get("value", [])
                    if from_endpoint(connection)]
            if len(ours) > 1:
                raise ArmError(f"{len(ours)} private endpoint connections claim to come from "
                               f"{endpoint}; none was approved")
            state = (((ours[0].get("properties") or {})
                      .get("privateLinkServiceConnectionState") or {}).get("status")
                     if ours else None)
            if (state == "Approved"
                    and self._factory_endpoint_state(factory, endpoint) == "Approved"):
                return str(ours[0]["id"])
            if state in ("Rejected", "Disconnected"):
                raise ArmError(f"the {endpoint} private endpoint connection is {state}; "
                               "delete the endpoint and create it again")
            now = self._clock()
            if now >= deadline:
                if state == "Approved":
                    # Approved on the target, never confirmed by the factory: someone else's.
                    rejection = {"status": "Rejected", "description":
                                 "Not the installation's factory endpoint"}
                    self.request("PUT", f"{ours[0]['id']}?api-version={api_version}",
                                 {"properties": {"privateLinkServiceConnectionState": rejection}})
                    raise ArmError(f"a connection claiming {endpoint} was approved, but the "
                                   "factory never confirmed it, so it was rejected again")
                raise ArmError(f"the {endpoint} private endpoint connection is "
                               f"{state or 'not there'} after {bound_seconds:.0f}s")
            if state == "Pending" and (sent_at is None or now - sent_at >= resend_seconds):
                approval = {"status": "Approved",
                            "description": "Approved by the lakebase-anti-demo installer"}
                put = self.request("PUT", f"{ours[0]['id']}?api-version={api_version}",
                                   {"properties": {"privateLinkServiceConnectionState": approval}})
                code, message = _error_of(put.body)
                if put.status in (200, 201, 202):
                    sent_at = now
                elif not (put.status == 409 or code == SERVER_BUSY):
                    raise ArmError(f"approving the {endpoint} private endpoint connection: "
                                   f"{code or put.status}: {message}", status=put.status,
                                   code=code)
            self._sleep(APPROVAL_POLL_SECONDS)

    def _factory_endpoint_state(self, factory: str, endpoint: str) -> str | None:
        """The factory's own view of its managed private endpoint's connection."""

        response = self.request(
            "GET", f"{self.factory_path(factory)}/managedVirtualNetworks/default/"
                   f"managedPrivateEndpoints/{endpoint}?api-version={DATA_FACTORY_API}")
        if response.status != 200 or not isinstance(response.body, dict):
            return None
        return (((response.body.get("properties") or {}).get("connectionState") or {})
                .get("status"))

    def cancel_pipeline_run(self, factory: str, run_id: str) -> None:
        """Park: cancel a run. A run that already ended is parked already."""

        response = self.request(
            "POST",
            f"{self.factory_path(factory)}/pipelineruns/{run_id}/cancel"
            f"?api-version={DATA_FACTORY_API}")
        if response.status in (200, 202, 204):
            return
        code, message = _error_of(response.body)
        if code in ("PipelineRunNotInProgress", "PipelineRunAlreadyCompleted"):
            return
        raise ArmError(f"cancelling pipeline run {run_id}: {code or response.status}: {message}",
                       status=response.status, code=code)

    # Virtual machines ------------------------------------------------------------------------

    def vm_path(self, name: str) -> str:
        return f"{self.resource_group_path}/providers/Microsoft.Compute/virtualMachines/{name}"

    def vm_ready(self, name: str) -> bool | None:
        """Whether the VM runs with its agent ready (what executes Run Command), None if absent.

        SSM's PingStatus ONLINE, on Azure: the instance view's ``PowerState/running`` and the VM
        agent reporting ``Ready``.
        """

        response = self.request(
            "GET", f"{self.vm_path(name)}/instanceView?api-version={COMPUTE_API}")
        if response.status == 404:
            return None
        if response.status != 200 or not isinstance(response.body, dict):
            code, message = _error_of(response.body)
            raise ArmError(f"reading VM {name}: {code or response.status}: {message}",
                           status=response.status, code=code)
        codes = {str(status.get("code") or "") for status in response.body.get("statuses") or []
                 if isinstance(status, dict)}
        agent = response.body.get("vmAgent")
        agent = agent if isinstance(agent, dict) else {}
        agent_ready = any(isinstance(status, dict) and status.get("displayStatus") == "Ready"
                          for status in agent.get("statuses") or [])
        return "PowerState/running" in codes and agent_ready

    def run_shell_script(self, vm: str, script: Iterable[str], *,
                         bound_seconds: float) -> RunCommandOutput:
        """Run Command's RunShellScript on `vm`, as root, and what it printed.

        Round 5's stand-in for SSM's AWS-RunShellScript: it installs and refreshes the runner and
        carries the setup requests, all off the clock (Phase 0 measured 31-32 s a call, even for
        `date`). A refusal because another Run Command is running (409) is sent again within the
        bound. Nothing else is: the script may have run, and an install appends its archive a
        chunk at a time. Run Command cannot be cancelled, so a script bounds its own steps.

        Whether the script succeeded is its own to say. The extension's status for a script that
        exits non-zero is not something the caller relies on: each script prints its exit status
        last, where the 4,096-byte limit keeps it.
        """

        url = f"{self.vm_path(vm)}/runCommand?api-version={COMPUTE_API}"
        body = {"commandId": "RunShellScript", "script": list(script)}
        started = self._clock()
        while True:
            response = self.request("POST", url, body, idempotent=False)
            if response.status != 409 or self._clock() - started >= bound_seconds:
                break
            self._sleep(RUN_COMMAND_BUSY_RETRY_SECONDS)
        if response.status not in (200, 202):
            code, message = _error_of(response.body)
            raise ArmError(f"Run Command on {vm} was refused: {code or response.status}: {message}",
                           status=response.status, code=code)
        remaining = max(bound_seconds - (self._clock() - started), OPERATION_POLL_SECONDS)
        status, result = self.wait_operation(response, bound_seconds=remaining)
        if status == "Timeout":
            raise ArmError(f"Run Command on {vm} did not finish within {bound_seconds:.0f} s")
        output = _run_command_output(result)
        if status != "Succeeded" and not output.stdout:
            code, message = _error_of(result)
            raise ArmError(f"Run Command on {vm} ended {status}: {code or ''}: {message}",
                           code=code)
        return output
