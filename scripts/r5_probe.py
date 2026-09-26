"""Read-only live probe for the deployed Round 5 app (no paid bout).

Mints an M2M OAuth token from the given Databricks profile and GETs a path on the
app front door. Prints HTTP status + body. Used for /readyz and /catalog checks.

The app host is resolved from the Databricks SDK (``apps.get(name).url``) or the
``ANTI_DEMO_APP_URL`` environment variable, never hardcoded, so no real
deployment hostname is committed to the tree.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request

from databricks.sdk import WorkspaceClient

PROFILE = os.environ.get("ANTI_DEMO_PROFILE")  # a ~/.databrickscfg profile; unset uses SDK defaults
APP_NAME = os.environ.get("ANTI_DEMO_APP_NAME", "lakebase-anti-demo")


def _base(w: WorkspaceClient) -> str:
    override = os.environ.get("ANTI_DEMO_APP_URL")
    if override:
        return override.rstrip("/")
    app = w.apps.get(name=APP_NAME)
    url = str(getattr(app, "url", "") or "")
    if not url:
        raise SystemExit(
            f"Could not resolve a URL for app {APP_NAME!r}; set ANTI_DEMO_APP_URL."
        )
    return url.rstrip("/")


def probe(path: str) -> tuple[int, str]:
    w = WorkspaceClient(profile=PROFILE)
    base = _base(w)
    auth = w.config.authenticate()
    token = auth["Authorization"]
    req = urllib.request.Request(f"{base}{path}", headers={"Authorization": token})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", "replace")


def main() -> None:
    path = sys.argv[1] if len(sys.argv) > 1 else "/readyz"
    status, body = probe(path)
    print(f"HTTP {status} {path}")
    try:
        parsed = json.loads(body)
        print(json.dumps(parsed, indent=2)[:4000])
    except json.JSONDecodeError:
        print(body[:1500])


if __name__ == "__main__":
    main()
