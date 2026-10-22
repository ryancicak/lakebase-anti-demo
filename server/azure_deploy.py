"""The Azure edition's App deploy: the AWS deploy's steps (bootstrap.sh), with no AWS in them.

1. **The secret scope**, the installation's own, holding the sealed manifest and the servers'
   admin password. Written through the SDK, so no secret is ever on a command line.
2. **The App's resources:** those two secrets, READ, which is what lets the App's principal read
   them and nothing else in the scope.
3. **The source**, staged with `app.azure.yaml` as its `app.yaml`, so the App binds no AWS secret
   (design 11.4), and the built frontend. `.gitignore` keeps that out of a plain sync, so the
   sync names it back in (`SYNC_ARGS`), and the workspace is asked that it arrived.
4. **`databricks sync`, a build stamp, and `databricks apps deploy`**, as bootstrap.sh does them.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .manifest import PROJECT_ROOT, DemoManifest

MANIFEST_SECRET = "anti-demo-manifest-json"
ADMIN_PASSWORD_SECRET = "azure-postgres-admin-password"
#: What a staged tree leaves behind: the repository's own and the installation's local state.
_STAGE_IGNORE = (".git", ".venv", "node_modules", ".anti-demo*", ".env*", "__pycache__",
                 ".ruff_cache", ".pytest_cache", "*.tfstate*", ".terraform", ".isaac")
#: bootstrap.sh's SYNC_ARGS. `databricks sync` honours the staged tree's .gitignore, which
#: excludes frontend/dist deliberately, so the built frontend is named back in. Without it the
#: deploy succeeds and every page answers 503.
SYNC_ARGS = ("--exclude", ".anti-demo*", "--exclude", ".venv", "--exclude", "node_modules",
             "--exclude", ".git", "--exclude", ".env*", "--include", "frontend/dist")


class AzureDeployError(RuntimeError):
    """The deploy stopped on something it names."""


def secret_scope_name(app_name: str, run_id: str) -> str:
    return f"{app_name}-{run_id}"


def _run(args: list[str], *, timeout: float = 600) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout, check=False)
    if result.returncode != 0:
        tail = "\n".join(f"{result.stdout}\n{result.stderr}".strip().splitlines()[-4:])
        raise AzureDeployError(f"{' '.join(args[:3])} failed: {tail}")
    return result


def publish_secrets(
    workspace: Any, scope: str, manifest: DemoManifest, admin_password: str
) -> None:
    existing = {item.name for item in workspace.secrets.list_scopes()}
    if scope not in existing:
        print(f"CREATE secret scope {scope}", flush=True)
        workspace.secrets.create_scope(scope)
    workspace.secrets.put_secret(scope, MANIFEST_SECRET,
                                 string_value=manifest.model_dump_json(exclude_none=True))
    workspace.secrets.put_secret(scope, ADMIN_PASSWORD_SECRET, string_value=admin_password)
    print(f"OK    the manifest and the servers' password are in {scope}", flush=True)


def bind_resources(workspace: Any, app_name: str, scope: str) -> None:
    from databricks.sdk.service.apps import (
        App,
        AppResource,
        AppResourceSecret,
        AppResourceSecretSecretPermission,
    )

    resources = [AppResource(name=key, secret=AppResourceSecret(
        scope=scope, key=key, permission=AppResourceSecretSecretPermission.READ))
        for key in (MANIFEST_SECRET, ADMIN_PASSWORD_SECRET)]
    workspace.apps.update(app_name, App(name=app_name, resources=resources))
    print(f"OK    the App's resources are bound to {scope}", flush=True)


def build_frontend(root: Path = PROJECT_ROOT) -> None:
    """The bundle the App serves, built from this tree, as bootstrap.sh builds it."""

    frontend = root / "frontend"
    if not (frontend / "node_modules").is_dir():
        print("BUILD frontend dependencies (npm ci)", flush=True)
        _run(["npm", "ci", "--prefix", str(frontend)], timeout=1200)
    print("BUILD frontend (npm run build)", flush=True)
    _run(["npm", "run", "build", "--prefix", str(frontend)], timeout=1200)


def stage_source(root: Path = PROJECT_ROOT) -> Path:
    """A copy of the source with the Azure `app.yaml`, and the built frontend inside it."""

    dist = root / "frontend" / "dist"
    if not (dist / "index.html").is_file():
        raise AzureDeployError("frontend/dist is not built: run 'npm ci && npm run build' in "
                               "frontend/ first")
    stage = Path(tempfile.mkdtemp(prefix="anti-demo-azure-stage-"))
    target = stage / "src"
    shutil.copytree(root, target, ignore=shutil.ignore_patterns(*_STAGE_IGNORE))
    shutil.copy2(root / "app.azure.yaml", target / "app.yaml")
    (target / "app.azure.yaml").unlink()
    if "AWS_" in (target / "app.yaml").read_text(encoding="utf-8"):
        raise AzureDeployError("the staged app.yaml binds an AWS setting")
    return target


def build_stamp(root: Path = PROJECT_ROOT) -> dict[str, object] | None:
    commit = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, capture_output=True,
                            text=True, check=False).stdout.strip()
    if len(commit) != 40:
        return None
    dirty = bool(subprocess.run(["git", "status", "--porcelain", "--untracked-files=no"],
                                cwd=root, capture_output=True, text=True,
                                check=False).stdout.strip())
    return {"commit": commit, "dirty": dirty,
            "deployed_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")}


def deploy(
    workspace: Any,
    *,
    profile: str,
    app_name: str,
    manifest: DemoManifest,
    admin_password: str,
    principal: str,
) -> str:
    """Publish, bind, sync and deploy. The App's URL."""

    scope = secret_scope_name(app_name, manifest.run_id)
    publish_secrets(workspace, scope, manifest, admin_password)
    bind_resources(workspace, app_name, scope)
    build_frontend()
    staged = stage_source()
    remote = f"/Workspace/Users/{principal}/{app_name}"
    print(f"SYNC  the source to {remote}", flush=True)
    _run(["databricks", "sync", str(staged), remote, "--full", *SYNC_ARGS, "-p", profile],
         timeout=1800)
    # Asked of the workspace, as bootstrap.sh asks it: sync's output names only what changed.
    present = subprocess.run(["databricks", "workspace", "get-status",
                              f"{remote}/frontend/dist/index.html", "-p", profile],
                             capture_output=True, text=True, timeout=120, check=False)
    if present.returncode != 0:
        raise AzureDeployError("the synced tree has no frontend/dist/index.html, so every page "
                               "of the deployed App would answer 503")
    stamp = build_stamp()
    if stamp is not None:
        stamp_file = staged.parent / "build-info.json"
        stamp_file.write_text(json.dumps(stamp) + "\n", encoding="utf-8")
        _run(["databricks", "workspace", "import", f"{remote}/build-info.json", "--file",
              str(stamp_file), "--format", "AUTO", "--overwrite", "-p", profile])
    print(f"DEPLOY Databricks App {app_name}", flush=True)
    _run(["databricks", "apps", "deploy", app_name, "--source-code-path", remote, "-p", profile],
         timeout=2400)
    shutil.rmtree(staged.parent, ignore_errors=True)
    app = json.loads(_run(["databricks", "apps", "get", app_name, "-p", profile, "-o",
                           "json"]).stdout)
    return str(app.get("url") or "")
