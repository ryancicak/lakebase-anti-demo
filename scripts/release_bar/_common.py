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
            ):
                if attempt + 1 >= attempts:
                    raise
                time.sleep(1.0 + attempt)
        raise AssertionError("unreachable")

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
    """
    for raw in (checkout / ".env.bootstrap").read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if "=" not in line or line.startswith("#"):
            continue
        key, value = line.split("=", 1)
        key = key.removeprefix("export ").strip()
        if key in AWS_KEYS:
            os.environ[key] = value.strip().strip('"').strip("'")


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


def installation_run_id(checkout: Path) -> str | None:
    """The run id of the checkout's newest installation, installed or uninstalled.

    `./antidemo cleanup` deletes the manifest and leaves a `cleanup-receipt.json`
    naming the same run id beside it, so this still answers after an uninstall.
    An `ANTI_DEMO_MANIFEST` that is set names the generation instead.
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
    for generation in generations:
        for name in ("manifest.json", "cleanup-receipt.json"):
            path = generation / name
            if path.is_file():
                run_id = json.loads(path.read_text(encoding="utf-8")).get("run_id")
                if run_id:
                    return str(run_id)
    return None
