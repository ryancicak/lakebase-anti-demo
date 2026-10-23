"""Round 5 on Azure, installed: both runners, their roles, and the seal.

The Azure counterpart of `lifecycle._prepare_and_reseal_round5`, in its order:

1. Both runners installed and their resident agents started (`configure_runners`). Each agent
   attests, then waits for its control DSN in Key Vault, as an AWS agent waits for its secret.
2. Each runner's sealing key (the harness's ``public_key``).
3. The Lakebase lane's roles, made with the Lakebase admin sealed to that runner's key
   (``prepare_lakebase``, exactly as on AWS).
4. The competitor's roles on the r5 server, made with the server's admin credential, which the
   installer writes into Key Vault as JSON for this one read (``prepare_rds_baseline``, the
   competitor's credential ``rds`` to the harness, port 5432: its observer and setup verification
   reach the server itself; its bout clients reach PgBouncer on 6432 through the runner's shim).
5. The seal (`Round5AzureResources`); then each lane's control DSN goes into Key Vault
   (`lifecycle.ensure_coordination`).

AWS's ``reassert_rds_credentials`` has no counterpart. It copies the burst role's password into
the RDS Proxy's own secret; PgBouncer authenticates clients against the server itself.

Every runner request goes over Run Command (`run_runner_request`), about 32 s a call, all of it
before any bout.
"""

from __future__ import annotations

import asyncio
import base64
import gzip
import hashlib
import json
import re
import secrets
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any
from urllib.parse import unquote, urlsplit

from . import lifecycle
from .azure_key_vault import KeyVault
from .azure_round5_engine import azure_round5_config_sha256
from .azure_round5_runner import (
    CONTROL_SECRET_NAMES,
    RunnerInstallError,
    RunnerTarget,
    azure_runner_sha256,
    configure_runners,
    run_runner_request,
    runner_targets,
    secret_identifier,
)
from .connection_spike import ConnectionSpikeContract
from .connection_spike_live import TRUST_BUNDLE_PATH, runner_harness_sha256
from .manifest import DemoManifest, Round5AzureResources, apply_manifest_environment

SETUP_PROTOCOL = "connection-spike-setup-v1"
#: The r5 server's admin credential, as JSON, for the competitor runner's one role setup.
MASTER_SECRET_NAME = "round5-competitor-master"
DIRECT_PORT = 5432
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def setup_request(run: Callable[..., Any], target: RunnerTarget,
                  payload: Mapping[str, Any]) -> dict[str, Any]:
    """One setup request to the harness, over Run Command; `lifecycle._round5_setup_request`."""

    encoded = base64.b64encode(gzip.compress(json.dumps(
        payload, sort_keys=True, separators=(",", ":")).encode("utf-8"))).decode("ascii")
    status, output = run_runner_request(run, target.vm_name, vault_uri=target.vault_uri,
                                        encoded=encoded)
    lines = output.splitlines()
    results = [line for line in lines if line.startswith("SETUP_RESULT:")]
    if status != 0 or len(results) != 1 or f"SETUP_SETTLED:{payload['nonce']}" not in lines:
        codes = re.findall(r"RUNNER_ERROR:([a-z_]{1,64})", output)
        raise RunnerInstallError(
            f"Round 5 runner {target.vm_name} returned no exact settled {payload['action']}"
            + (f": the runner refused with {codes[-1]}" if codes else ""))
    try:
        result = json.loads(results[0].split(":", 1)[1])
    except json.JSONDecodeError as exc:
        raise RunnerInstallError(f"Round 5 runner {target.vm_name} returned invalid JSON") from exc
    expected = {"protocol": SETUP_PROTOCOL, "action": payload["action"],
                "nonce": payload["nonce"], "status": "verified"}
    if not isinstance(result, dict) or any(result.get(key) != value
                                           for key, value in expected.items()):
        raise RunnerInstallError(
            f"Round 5 runner {target.vm_name}'s result did not match its request")
    return result


def public_key(run: Callable[..., Any], target: RunnerTarget) -> tuple[str, str]:
    result = setup_request(run, target, {"protocol": SETUP_PROTOCOL, "action": "public_key",
                                         "nonce": secrets.token_hex(16)})
    key, digest = str(result.get("public_key") or ""), str(result.get("public_key_sha256") or "")
    try:
        actual = hashlib.sha256(base64.b64decode(key, validate=True)).hexdigest()
    except ValueError as exc:
        raise RunnerInstallError(
            f"Round 5 runner {target.vm_name}'s public key is invalid") from exc
    if actual != digest:
        raise RunnerInstallError(f"Round 5 runner {target.vm_name}'s public-key digest is invalid")
    return key, digest


def _digests(result: Mapping[str, Any], target: RunnerTarget) -> tuple[str, str]:
    client = str(result.get("credential_sha256") or "")
    observer = str(result.get("observer_credential_sha256") or "")
    if _SHA256.fullmatch(client) is None or _SHA256.fullmatch(observer) is None:
        raise RunnerInstallError(
            f"Round 5 runner {target.vm_name} returned invalid baseline credential digests")
    return client, observer


def network_sha256(outputs: Mapping[str, Any]) -> str:
    runners = outputs.get("round5_runners") or {}
    return hashlib.sha256(json.dumps({
        "egress_ip": outputs.get("round5_runner_egress_ip"),
        "runners": {key: (value or {}).get("id") for key, value in sorted(runners.items())},
    }, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def published_password(dsn: str | None, role: str) -> str | None:
    """`lifecycle._published_resident_password`, read from Key Vault.

    The password already published for `role` is re-applied, never replaced: a running agent
    holds the DSN it read at start, and a new password would leave it refused (2026-09-27 on AWS).
    """

    if not dsn:
        return None
    parsed = urlsplit(dsn)
    if parsed.scheme not in {"postgresql", "postgres"}:
        return None
    if unquote(parsed.username or "") != role or not parsed.password:
        return None
    return unquote(parsed.password)


async def publish_resident_logins(manifest: DemoManifest, store: Any, host: str,
                                  vault: KeyVault) -> None:
    """Each lane's resident login on the coordination store, and its DSN in Key Vault.

    `lifecycle.ensure_coordination`'s AWS block, with Key Vault for Secrets Manager: the same
    lane-scoped roles and DSNs (`lifecycle._rotate_round5_resident_login`,
    `_round5_resident_dsn`), read back by each runner's agent as its VM's identity.
    """

    digest = hashlib.sha256((manifest.installation_id or manifest.run_id).encode()).hexdigest()[:16]
    roles = {"lakebase": f"anti_demo_r5_lakebase_{digest}",
             "competitor": f"anti_demo_r5_competitor_{digest}"}
    passwords: dict[str, str] = {}
    for lane, role in roles.items():
        current = await asyncio.to_thread(vault.get, CONTROL_SECRET_NAMES[lane])
        passwords[lane] = published_password(current, role) or secrets.token_urlsafe(48)

    async def rotate(cursor: Any) -> None:
        for lane, role in roles.items():
            await lifecycle._rotate_round5_resident_login(
                cursor, database=manifest.databricks.database, role=role,
                password=passwords[lane], lane_id=lane)
        await lifecycle._retire_round5_shared_resident_login(
            cursor, database=manifest.databricks.database, role=f"anti_demo_r5_{digest}")

    await store._run(rotate)
    for lane, role in roles.items():
        dsn = lifecycle._round5_resident_dsn(host=host, database=manifest.databricks.database,
                                             role=role, password=passwords[lane],
                                             trust_bundle_path=str(TRUST_BUNDLE_PATH))
        await asyncio.to_thread(vault.set, CONTROL_SECRET_NAMES[lane], dsn)


def ensure_round5(
    manifest: DemoManifest,
    *,
    run: Callable[..., Any],
    outputs: Mapping[str, Any],
    vault: KeyVault,
    admin_password: str,
    report: Callable[[str], None] = print,
) -> Round5AzureResources:
    """Install and set up both runners, and return the seal (not yet saved)."""

    azure = manifest.azure
    if azure is None or "r5" not in azure.servers:
        raise RunnerInstallError("Round 5 on Azure needs the r5 server sealed first")
    server = azure.servers["r5"]
    lakebase_target, competitor_target = runner_targets(outputs, region=azure.region)
    harness = runner_harness_sha256()
    database = manifest.databricks.database
    sealed = manifest.round5_azure

    report("Round 5: the r5 server's admin credential into Key Vault, for its runner's setup")
    vault.set(MASTER_SECRET_NAME, json.dumps(
        {"host": server.host, "port": DIRECT_PORT, "dbname": database,
         "username": server.admin_login, "password": admin_password},
        sort_keys=True, separators=(",", ":")))
    installs = configure_runners(
        run, (lakebase_target, competitor_target), expected_harness_sha256=harness,
        keep_trust_bundle_sha256=sealed.trust_bundle_sha256 if sealed is not None else None,
        report=report)
    trust = installs["lakebase"].trust_bundle_sha256

    report("Round 5: each runner's sealing key")
    lakebase_key, lakebase_key_sha256 = public_key(run, lakebase_target)
    _, competitor_key_sha256 = public_key(run, competitor_target)

    report("Round 5: the Lakebase lane's roles")
    direct_host, pooled_host = lifecycle._enable_round5_lakebase_native_login(manifest)
    apply_manifest_environment(manifest)
    admin = asyncio.run(lifecycle._round_lakebase_provider(manifest, 5).connection_material())
    if admin.host != direct_host:
        raise RunnerInstallError("Round 5 Lakebase admin host differs from the control plane")
    common = {
        "protocol": SETUP_PROTOCOL, "bout_id": f"baseline-{manifest.run_id}",
        "port": DIRECT_PORT, "dbname": database, "username": lifecycle.ROUND5_NATIVE_ROLE,
        "trust_bundle_path": str(TRUST_BUNDLE_PATH), "trust_bundle_sha256": trust,
    }
    lakebase = setup_request(run, lakebase_target, {
        **common, "action": "prepare_lakebase", "nonce": secrets.token_hex(16),
        "lane_id": "lakebase", "endpoint_host": pooled_host, "credential_host": direct_host,
        "sealed_admin": lifecycle._seal_round5_admin(lakebase_key, admin),
        "public_key_sha256": lakebase_key_sha256,
    })
    lakebase_client, lakebase_observer = _digests(lakebase, lakebase_target)

    report("Round 5: the competitor's roles on the r5 server")
    competitor = setup_request(run, competitor_target, {
        **common, "action": "prepare_rds_baseline", "nonce": secrets.token_hex(16),
        "lane_id": "rds", "endpoint_host": server.host, "credential_host": server.host,
        "master_secret_arn": secret_identifier(azure.region, MASTER_SECRET_NAME),
    })
    competitor_client, competitor_observer = _digests(competitor, competitor_target)

    runners = outputs["round5_runners"]
    service_bus = outputs["service_bus"]
    contract = ConnectionSpikeContract().sha256
    values: dict[str, Any] = {
        "lakebase_direct_host": direct_host,
        "lakebase_pooled_host": pooled_host,
        "server_name": server.server,
        "server_resource_id": server.resource_id,
        "server_host": server.host,
        "lakebase_runner_vm": lakebase_target.vm_name,
        "competitor_runner_vm": competitor_target.vm_name,
        "runner_vm_size": str(runners["runner-0"]["size"]),
        "lakebase_runner_principal_id": str(runners["runner-0"]["principal_id"]),
        "competitor_runner_principal_id": str(runners["runner-1"]["principal_id"]),
        "runner_egress_ip": str(outputs["round5_runner_egress_ip"]),
        "service_bus_namespace": str(service_bus["namespace_name"]),
        "lakebase_control_queue": lakebase_target.queue_address,
        "competitor_control_queue": competitor_target.queue_address,
        "key_vault_uri": lakebase_target.vault_uri,
        "lakebase_control_secret": secret_identifier(azure.region,
                                                     CONTROL_SECRET_NAMES["lakebase"]),
        "competitor_control_secret": secret_identifier(azure.region,
                                                       CONTROL_SECRET_NAMES["competitor"]),
        "competitor_master_secret": secret_identifier(azure.region, MASTER_SECRET_NAME),
        "network_sha256": network_sha256(outputs),
        "runner_harness_sha256": harness,
        "azure_runner_sha256": azure_runner_sha256(),
        "trust_bundle_sha256": trust,
        "runner_public_key_sha256": lakebase_key_sha256,
        "competitor_runner_public_key_sha256": competitor_key_sha256,
        "lakebase_credential_sha256": lakebase_client,
        "lakebase_observer_credential_sha256": lakebase_observer,
        "competitor_credential_sha256": competitor_client,
        "competitor_observer_credential_sha256": competitor_observer,
        "contract_sha256": contract,
    }
    baseline = hashlib.sha256(json.dumps(values, sort_keys=True,
                                         separators=(",", ":")).encode()).hexdigest()
    return Round5AzureResources(
        **values, baseline_sha256=baseline,
        config_sha256=azure_round5_config_sha256(baseline, contract),
        sealed_at=datetime.now(UTC))
