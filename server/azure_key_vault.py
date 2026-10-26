"""The installation's Key Vault, as the installer writes it: Round 5's runner secrets.

Two kinds of secret go in, each read back by one runner as its VM's identity
(`runner/azure_io.py`): a lane's control DSN (the resident agent's login to the coordination
store, `lifecycle.ensure_coordination`), and the r5 server's admin credential as JSON, which the
competitor's runner uses once to make its own roles (the harness's ``prepare_rds_baseline``). The
installer holds Key Vault Secrets Officer on the vault (infra/azure/role_assignments.tf).

Values are never logged or put in an error: an error names the vault's host and the secret's name.
"""

from __future__ import annotations

import json
import re
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from typing import Any

KEY_VAULT_SCOPE = "https://vault.azure.net/.default"
API_VERSION = "7.4"
_NAME = re.compile(r"^[A-Za-z0-9-]{1,127}$")
_VAULT = re.compile(r"^https://(?P<name>[A-Za-z0-9-]{3,24})\.vault\.azure\.net/?$")


class KeyVaultError(RuntimeError):
    """A Key Vault call failed. Names the vault and secret, never the value."""


class KeyVault:
    def __init__(self, vault_uri: str, token_source: Callable[[], str], *,
                 opener: Callable[..., Any] = urllib.request.urlopen) -> None:
        match = _VAULT.fullmatch(vault_uri)
        if match is None:
            raise ValueError("not a Key Vault URI")
        self._base = f"https://{match.group('name')}.vault.azure.net"
        self._token = token_source
        self._open = opener

    def _url(self, name: str) -> str:
        if _NAME.fullmatch(name) is None:
            raise ValueError("not a Key Vault secret name")
        return f"{self._base}/secrets/{urllib.parse.quote(name)}?api-version={API_VERSION}"

    def _send(self, method: str, name: str, body: bytes | None = None) -> Any:
        request = urllib.request.Request(
            self._url(name), data=body, method=method,
            headers={"Authorization": f"Bearer {self._token()}",
                     "Content-Type": "application/json"})
        try:
            with self._open(request, timeout=30) as response:
                return json.loads(response.read() or b"null")
        except urllib.error.HTTPError as error:
            if method == "GET" and error.code == 404:
                return None
            raise KeyVaultError(f"Key Vault {self._base} refused {method} of secret {name}: "
                                f"HTTP {error.code}") from None
        except (OSError, ValueError) as error:
            raise KeyVaultError(f"Key Vault {self._base} gave no answer to {method} of secret "
                                f"{name} ({type(error).__name__})") from None

    def get(self, name: str) -> str | None:
        """The secret's current value, or None if it has none."""

        body = self._send("GET", name)
        return None if body is None else str(body.get("value") or "") or None

    def set(self, name: str, value: str) -> None:
        if not value:
            raise ValueError("a Key Vault secret needs a value")
        self._send("PUT", name, json.dumps({"value": value}).encode())
