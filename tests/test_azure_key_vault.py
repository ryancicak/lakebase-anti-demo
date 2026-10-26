"""server/azure_key_vault.py against a fake Key Vault: no network."""

from __future__ import annotations

import io
import json
import urllib.error
from typing import Any

import pytest

from server.azure_key_vault import KeyVault, KeyVaultError

VAULT = "https://kv-test-r5.vault.azure.net/"


class Response(io.BytesIO):
    def __enter__(self) -> Response:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


class FakeVault:
    def __init__(self) -> None:
        self.secrets: dict[str, str] = {}
        self.requests: list[Any] = []
        self.refuse: int | None = None

    def __call__(self, request: Any, timeout: float) -> Response:
        self.requests.append(request)
        name = request.full_url.split("/secrets/", 1)[1].split("?", 1)[0]
        if self.refuse is not None:
            raise urllib.error.HTTPError(request.full_url, self.refuse, "refused", None, None)
        if request.get_method() == "PUT":
            self.secrets[name] = json.loads(request.data)["value"]
            return Response(json.dumps({"value": self.secrets[name]}).encode())
        if name not in self.secrets:
            raise urllib.error.HTTPError(request.full_url, 404, "SecretNotFound", None, None)
        return Response(json.dumps({"value": self.secrets[name]}).encode())


def test_a_secret_is_written_and_read_back_as_the_installer() -> None:
    fake = FakeVault()
    vault = KeyVault(VAULT, lambda: "installer-token", opener=fake)
    assert vault.get("round5-lakebase-control") is None
    # The value is opaque to the vault; a placeholder, not a connection string.
    vault.set("round5-lakebase-control", "control-dsn-placeholder")
    assert vault.get("round5-lakebase-control") == "control-dsn-placeholder"
    put = fake.requests[1]
    assert put.full_url == ("https://kv-test-r5.vault.azure.net/secrets/round5-lakebase-control"
                            "?api-version=7.4")
    assert put.headers["Authorization"] == "Bearer installer-token"


def test_a_refusal_names_the_secret_and_never_the_value() -> None:
    fake = FakeVault()
    vault = KeyVault(VAULT, lambda: "t", opener=fake)
    fake.refuse = 403
    with pytest.raises(KeyVaultError) as raised:
        vault.set("round5-competitor-master", "super-secret-password")
    assert "round5-competitor-master" in str(raised.value) and "HTTP 403" in str(raised.value)
    assert "super-secret-password" not in str(raised.value)
    with pytest.raises(ValueError):
        vault.get("not_a_name")
    with pytest.raises(ValueError):
        KeyVault("https://example.test", lambda: "t")
