"""Shared plumbing for the release-bar scripts.

Two things every script here needs: an authenticated JSON client for a deployed
app, and the AWS key pair of the checkout the installation was made from. Nothing
is defaulted to a particular installation: the app URL and the Databricks CLI
profile come from `ANTI_DEMO_APP_URL` and `ANTI_DEMO_PROFILE`, as in
`scripts/r5_soak_monitor.py`, and a missing one stops the script with a message.
"""

from __future__ import annotations

import http.client
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

AWS_KEYS = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_DEFAULT_REGION")

ALL_ROUNDS = (
    "wake_idle_app",
    "make_schema_change_safely",
    "recover_deleted_order",
    "put_model_score_in_app",
    "survive_connection_spike",
    "analyze_live_orders_without_slowing_checkout",
)

#: A Databricks OAuth token lives for about an hour. Re-minting well inside that
#: is what kept a five-hour campaign from dying on 401s halfway through.
TOKEN_REFRESH_SECONDS = 300.0

#: Every name the AWS lanes of Rounds 4 and 6 give their buckets, Glue jobs and connections, and
#: Round 6's DMS instance, endpoints, tasks and subnet group, starts so.
GLUE_LANE_PREFIX = "lakebase-ant"


def dms_listing(dms, operation: str, key: str) -> list[dict]:
    """A DMS listing as a list: DMS answers a listing with nothing in it with a fault."""
    from botocore.exceptions import ClientError

    items: list[dict] = []
    arguments = {"WithoutSettings": True} if operation == "describe_replication_tasks" else {}
    try:
        for page in dms.get_paginator(operation).paginate(**arguments):
            items.extend(page.get(key) or [])
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") != "ResourceNotFoundFault":
            raise
    return items


class ResponseLost(ConnectionError):
    """A POST whose answer never arrived whole, so only the app knows whether it acted.

    rc8, 2026-10-01 18:12:50Z: the app answered an arm in full (the proxy logged 48,693
    bytes), 3,756 of them reached the harness, and five scenarios failed behind that
    one cut-off answer. A caller catches this and reads the session to settle it.
    """

    def __init__(self, method: str, path: str, cause: BaseException) -> None:
        super().__init__(
            f"{method} {path}: its response was lost ({type(cause).__name__}: {cause})"
        )
        self.method = method
        self.path = path


#: The pauses before a GET the Databricks Apps front end answered in the app's place is sent
#: again: fifteen seconds in all.
PLATFORM_RETRY_SECONDS = (1.0, 2.0, 4.0, 8.0)


class PlatformAnswered(RuntimeError):
    """The Databricks Apps front end answered a request in the app's place."""

    def __init__(self, status: int, payload: Any) -> None:
        super().__init__(f"the Databricks Apps front end answered {status}: {payload}")
        self.status = status


def answered_by_the_platform(status: int, payload: Any) -> bool:
    """Whether a 502, 503 or 504 came from the Databricks Apps front end, not from the app.

    The app's own errors are FastAPI's `{"detail": ...}`, and a 503 among them is a fault
    the bar has to see. The front end's is `{"error_code": "TEMPORARILY_UNAVAILABLE", ...}`,
    or a page that is not JSON at all. rc23's bar lost a Round 1 scenario to one of those
    on /api/bout/all (2026-10-05, 17:10:45Z): the app's own log shows it serving every
    request around that one, and the next request, half a second later, was answered.
    """

    return status in (502, 503, 504) and not (isinstance(payload, dict) and "detail" in payload)


#: A resource deleted between being listed and having its tags read is gone.
GONE_CODES = frozenset(
    {
        "NoSuchEntity",
        "DBProxyNotFoundFault",
        "ResourceNotFoundFault",
        "DBParameterGroupNotFound",
        "DBClusterParameterGroupNotFound",
        "AWS.SimpleQueueService.NonExistentQueue",
        "QueueDoesNotExist",
        "NoSuchBucket",
        "NoSuchTagSet",
        "EntityNotFoundException",
    }
)
#: A resource AWS is still deleting answers a tag read with this instead: DMS does,
#: for a replication instance or task in `deleting`. It is neither gone nor readable.
TRANSITIONAL_CODES = frozenset({"InvalidResourceStateFault"})
#: How long a leak check waits for a mid-deletion resource to finish, and how often it looks.
TRANSITION_WAIT_SECONDS = 15 * 60.0
TRANSITION_POLL_SECONDS = 30.0


class ResourceInTransition(RuntimeError):
    """A listed resource whose tags cannot be read yet because AWS is still deleting it.

    rc8's ALL GONE check, 2026-10-01 19:57:56Z, crashed on this: another installation's
    DMS replication instance was being deleted beside it. The count is taken again.
    """


def tags_of(read, key: str, **arguments) -> object:
    """One tag read, as None for a resource gone since its listing.

    Raises `ResourceInTransition` for one still being deleted.
    """
    from botocore.exceptions import ClientError

    try:
        return read(**arguments).get(key)
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code")
        if code in GONE_CODES:
            return None
        if code in TRANSITIONAL_CODES:
            target = next(iter(arguments.values()), "a resource")
            raise ResourceInTransition(f"{target} is still being deleted") from error
        raise


def counted_when_settled(count, *, wait: float = TRANSITION_WAIT_SECONDS, sleep=time.sleep):
    """`count()` once no resource it reads is mid-deletion, for up to `wait` seconds.

    Past that, it stops with the resource's name rather than guess whose it was.
    """
    deadline = time.monotonic() + wait
    while True:
        try:
            return count()
        except ResourceInTransition as busy:
            if time.monotonic() >= deadline:
                raise SystemExit(
                    f"{busy} after {wait / 60:.0f} minutes, so whose it is could not be read"
                ) from busy
            print(f"WAIT  {busy}; counting again in {TRANSITION_POLL_SECONDS:.0f}s", flush=True)
            sleep(TRANSITION_POLL_SECONDS)


def require_env(name: str, hint: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"{name} is not set: {hint}")
    return value


class AppClient:
    """JSON calls to a deployed anti-demo app, authenticated as a CLI profile."""

    def __init__(self, base_url: str | None = None, profile: str | None = None) -> None:
        self.base_url = (
            base_url or require_env("ANTI_DEMO_APP_URL", "the deployed app's https URL")
        ).rstrip("/")
        self.profile = profile or require_env(
            "ANTI_DEMO_PROFILE", "a Databricks CLI profile that can reach the app"
        )
        self._headers: dict[str, str] | None = None
        self._minted_at = 0.0

    def headers(self) -> dict[str, str]:
        if self._headers is None or time.monotonic() - self._minted_at > TOKEN_REFRESH_SECONDS:
            from databricks.sdk import WorkspaceClient

            self._headers = WorkspaceClient(profile=self.profile).config.authenticate()
            self._minted_at = time.monotonic()
        return dict(self._headers)

    def call(
        self,
        method: str,
        path: str,
        body: Any = None,
        timeout: float = 120,
    ) -> tuple[int, Any]:
        """Status and parsed body. A GET is retried on a truncated or reset response.

        Any other method raises `ResponseLost` instead: the app may already have acted on
        it, so sending it again blind could arm or towel twice.

        A body that is not JSON -- a proxy's error page, say -- comes back as
        `{"text": ...}` rather than raising, whatever the status.
        """
        attempts = 3 if method == "GET" else 1
        for attempt in range(attempts):
            data = None if body is None else json.dumps(body).encode()
            request = urllib.request.Request(
                self.base_url + path,
                data=data,
                method=method,
                headers={**self.headers(), "content-type": "application/json"},
            )
            try:
                with urllib.request.urlopen(request, timeout=timeout) as response:
                    return response.status, _parsed(response.read())
            except urllib.error.HTTPError as error:
                return error.code, _parsed(error.read())
            except (
                http.client.IncompleteRead,
                ConnectionError,
                urllib.error.URLError,
                TimeoutError,
            ) as error:
                if method != "GET":
                    raise ResponseLost(method, path, error) from error
                if attempt + 1 >= attempts:
                    raise
                time.sleep(1.0 + attempt)
        raise AssertionError("unreachable")

    def stop_start(self) -> str:
        """Stop the app serving `base_url` and start it again, deploying nothing.

        A process death: nothing it was doing gets to finish. Found by its URL, so
        no other app in the workspace can be the one stopped. Returns its name.
        """
        from databricks.sdk import WorkspaceClient

        workspace = WorkspaceClient(profile=self.profile)
        name = next(
            (
                app.name
                for app in workspace.apps.list()
                if app.name and (app.url or "").rstrip("/") == self.base_url
            ),
            None,
        )
        if name is None:
            raise RuntimeError(f"no app in this workspace serves {self.base_url}")
        workspace.apps.stop(name).result()
        workspace.apps.start(name).result()
        return name

    def rounds(self) -> dict[str, dict[str, Any]]:
        status, payload = self.call("GET", "/api/bout/all", timeout=60)
        if status != 200:
            raise RuntimeError(f"/api/bout/all returned {status}")
        return dict((payload or {}).get("rounds") or {})

    def all_ready(self) -> bool:
        return all_ready(self.rounds())


def _parsed(raw: bytes) -> Any:
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except ValueError:
        return {"text": raw.decode(errors="replace")[:2000]}


def all_ready(rounds: dict[str, dict[str, Any]]) -> bool:
    """Every one of the six rounds is READY and can start. A missing round is not."""
    return all(
        (rounds.get(round_id) or {}).get("state") == "ready"
        and (rounds.get(round_id) or {}).get("can_start") is True
        for round_id in ALL_ROUNDS
    )


def load_checkout_aws(checkout: Path) -> None:
    """Export only the AWS key pair and region from a checkout's `.env.bootstrap`.

    Nothing else in that file is exported: it also names the Databricks app, and a
    `DATABRICKS_APP_NAME` in this process's environment makes the manifest loader
    believe it is running inside the app.

    The region is the installation's own, where it has one. The file needs only five
    values and the region is not among them, and a region taken from the shell
    instead could be another one, where `gone.py` would find nothing and say ALL GONE.
    """
    for raw in (checkout / ".env.bootstrap").read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if "=" not in line or line.startswith("#"):
            continue
        key, value = line.split("=", 1)
        key = key.removeprefix("export ").strip()
        if key in AWS_KEYS:
            os.environ[key] = value.strip().strip('"').strip("'")
    region = installation_region(checkout)
    if region:
        os.environ["AWS_DEFAULT_REGION"] = region


def latest_manifest(checkout: Path) -> Path | None:
    """The newest generation's manifest, compared as numbers (v10 after v7)."""
    generations = [
        path
        for path in checkout.glob(".anti-demo-v*/manifest.json")
        if path.parent.name.removeprefix(".anti-demo-v").isdigit()
    ]
    if not generations:
        return None
    return max(generations, key=lambda path: int(path.parent.name.removeprefix(".anti-demo-v")))


def use_checkout(checkout: Path) -> Path | None:
    """Point this process at a checkout's installation: its AWS keys, code and manifest.

    An `ANTI_DEMO_MANIFEST` already set is kept, for an installation whose manifest
    lives outside the checkout.
    """
    checkout = checkout.resolve()
    load_checkout_aws(checkout)
    manifest = latest_manifest(checkout)
    if os.environ.get("ANTI_DEMO_MANIFEST"):
        manifest = Path(os.environ["ANTI_DEMO_MANIFEST"])
    elif manifest is not None:
        os.environ["ANTI_DEMO_MANIFEST"] = str(manifest)
    sys.path.insert(0, str(checkout))
    return manifest


def _installation_records(checkout: Path) -> list[tuple[str, dict[str, Any]]]:
    """Each generation's manifest or cleanup receipt, newest generation first.

    `./antidemo cleanup` deletes the manifest and leaves a `cleanup-receipt.json`
    beside it. An `ANTI_DEMO_MANIFEST` that is set names the generation instead.
    """
    generations = sorted(
        (
            path
            for path in checkout.resolve().glob(".anti-demo-v*")
            if path.is_dir() and path.name.removeprefix(".anti-demo-v").isdigit()
        ),
        key=lambda path: int(path.name.removeprefix(".anti-demo-v")),
        reverse=True,
    )
    if os.environ.get("ANTI_DEMO_MANIFEST"):
        generations = [Path(os.environ["ANTI_DEMO_MANIFEST"]).parent]
    return [
        (name, json.loads((generation / name).read_text(encoding="utf-8")))
        for generation in generations
        for name in ("manifest.json", "cleanup-receipt.json")
        if (generation / name).is_file()
    ]


def installation_run_id(checkout: Path) -> str | None:
    """The run id of the checkout's newest installation, installed or uninstalled."""
    for _name, record in _installation_records(checkout):
        if record.get("run_id"):
            return str(record["run_id"])
    return None


def installation_region(checkout: Path) -> str | None:
    """The AWS region the checkout's newest installation lives in, installed or uninstalled."""
    for name, record in _installation_records(checkout):
        region = (
            (record.get("aws") or {}).get("region")
            if name == "manifest.json"
            else record.get("aws_region")
        )
        if region:
            return str(region)
    return None


def installation_databricks_profile(checkout: Path) -> str | None:
    """The Databricks CLI profile the checkout's newest installation used, installed or not."""
    for name, record in _installation_records(checkout):
        if name == "manifest.json":
            profile = (record.get("databricks") or {}).get("profile")
        else:
            profiles = record.get("lakebase_profiles") or []
            profile = profiles[0] if profiles else None
        if profile:
            return str(profile)
    return None
