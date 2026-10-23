"""server/home_cloud.py: the Databricks side's cloud comes from the workspace host, and the egress
feed and the DNS witness follow it. The AWS edition's behavior is unchanged."""

from __future__ import annotations

import json
import socket

import pytest

from server import lifecycle
from server.home_cloud import AWS, AZURE, home_cloud_for_host


def test_the_edition_comes_from_the_workspace_host() -> None:
    assert home_cloud_for_host("https://dbc-0000-0000.cloud.databricks.com") is AWS
    assert home_cloud_for_host("adb-example.0.azuredatabricks.net") is AZURE
    with pytest.raises(ValueError, match="not an AWS or Azure"):
        home_cloud_for_host("https://1234567890123456.7.gcp.databricks.com")
    assert home_cloud_for_host("https://example.test", default=AWS) is AWS


class _FakeFeed:
    def __init__(self, body: str) -> None:
        self._body = body.encode()

    def __enter__(self) -> _FakeFeed:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def read(self, *_: object) -> bytes:
        return self._body


def feed(*rows: dict[str, object]) -> str:
    return json.dumps({"timestampSeconds": 1_700_000_000, "prefixes": list(rows)})


def row(platform: str, prefixes: list[str]) -> dict[str, object]:
    return {"platform": platform, "region": "westus2", "service": "Databricks",
            "type": "outbound", "ipv4Prefixes": prefixes}


def test_azure_reads_its_own_platform_and_admits_its_published_slash_23(monkeypatch) -> None:
    body = feed(row("aws", ["198.51.100.0/24"]), row("azure", ["198.18.0.0/23", "192.0.2.0/24"]))
    monkeypatch.setattr(lifecycle.urllib.request, "urlopen", lambda *_a, **_k: _FakeFeed(body))
    cidrs, published_at = lifecycle.fetch_serverless_egress_cidrs("westus2", home=AZURE)
    assert sorted(cidrs) == ["192.0.2.0/24", "198.18.0.0/23"] and published_at == 1_700_000_000
    # The AWS edition, reading the same feed, still sees only its own platform's rows.
    assert lifecycle.fetch_serverless_egress_cidrs("westus2")[0] == ("198.51.100.0/24",)


def test_the_aws_edition_still_refuses_a_slash_23(monkeypatch) -> None:
    body = feed(row("aws", ["198.18.0.0/23"]))
    monkeypatch.setattr(lifecycle.urllib.request, "urlopen", lambda *_a, **_k: _FakeFeed(body))
    with pytest.raises(ValueError, match="wider than a /24"):
        lifecycle.fetch_serverless_egress_cidrs("westus2")


def test_a_vanished_azure_workspace_is_judged_against_azures_own_dns(monkeypatch) -> None:
    looked_up: list[str] = []

    def getaddrinfo(name: str, *_: object) -> list[object]:
        looked_up.append(name)
        if name.endswith(".0.azuredatabricks.net"):
            raise socket.gaierror(socket.EAI_NONAME, "nodename nor servname provided")
        return []

    monkeypatch.setattr(lifecycle.socket, "getaddrinfo", getaddrinfo)
    state, _ = lifecycle.databricks_workspace_liveness("adb-example.0.azuredatabricks.net")
    assert state == lifecycle.WORKSPACE_GONE
    assert looked_up[-1] == "accounts.azuredatabricks.net"
