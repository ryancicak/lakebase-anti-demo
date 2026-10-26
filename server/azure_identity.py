"""Where the Azure edition's ARM tokens come from. No Azure secret is held anywhere.

* **The deployed App** asks Unity Catalog for one. The installation's own App Access Connector has
  a service credential, and the App's service principal holds ACCESS on it. So
  `generate_temporary_service_credential` mints a token for the connector's managed identity.
  - Phase 0 (2026-10-19) measured it: issued in about 2 s, for ARM, about 24 hours long.
  - The identity's one role is Contributor on the installation's resource group.
  - A metastore admin can mint the same token, as they can read anything the metastore governs.
* **The operator's laptop** uses `az login` in the installation's tenant, as Terraform does.

Both are `ArmClient` token sources: a callable returning a bearer token, called again as the
client's reuse window ends. Neither puts a token in a message.
"""

from __future__ import annotations

import subprocess
from collections.abc import Callable
from typing import Any

from .azure_arm import ARM, ArmError

#: The scope a service credential mints ARM tokens for. It must end in `/.default` (Phase 0).
ARM_SCOPE = f"{ARM}/.default"
#: Round 5's control transport: the App sends each runner its control events on Service Bus.
SERVICE_BUS_SCOPE = "https://servicebus.azure.net/.default"


def service_credential_token_source(
    workspace: Any, credential_name: str, *, scope: str = ARM_SCOPE
) -> Callable[[], str]:
    """The App's token for `scope`: Unity Catalog mints it as the App Access Connector."""

    from databricks.sdk.service.catalog import GenerateTemporaryServiceCredentialAzureOptions

    if not credential_name.strip():
        raise ValueError("the App's service credential name is empty")

    def mint() -> str:
        minted = workspace.credentials.generate_temporary_service_credential(
            credential_name,
            azure_options=GenerateTemporaryServiceCredentialAzureOptions(resources=[scope]),
        )
        token = minted.azure_aad.aad_token if minted.azure_aad else None
        if not token:
            raise ArmError(f"service credential {credential_name} returned no Azure token")
        return str(token)

    return mint


def az_cli_token_source(
    tenant_id: str, *, scope: str = ARM_SCOPE,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> Callable[[], str]:
    """The operator's token for `scope`, from `az login` in the installation's tenant."""

    if not tenant_id.strip():
        raise ValueError("the Azure tenant ID is empty")
    resource = scope.removesuffix(".default")

    def mint() -> str:
        result = run(
            ["az", "account", "get-access-token", "--tenant", tenant_id,
             "--resource", resource, "--query", "accessToken", "-o", "tsv"],
            capture_output=True, text=True, timeout=60, check=False,
        )
        token = (result.stdout or "").strip()
        if result.returncode != 0 or not token:
            raise ArmError(f"az could not mint an ARM token in tenant {tenant_id}: run "
                           f"'az login --tenant {tenant_id}', then try again")
        return token

    return mint
