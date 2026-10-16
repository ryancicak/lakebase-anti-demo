"""Setup grants the app What It Cost's billing read, or says exactly what an admin must run.

No setup ever granted it. The deployed app's posted read was refused on every
installation (`[INSUFFICIENT_PERMISSIONS] ... USE SCHEMA on Schema 'system.billing'`,
2026-10-14), and What It Cost showed Lakebase as unpriced: the one contrast the panel
exists to draw. `system` is Databricks' own catalog and an installer may not be able to
share it, so this is the one grant in setup that is allowed to be refused.
"""

from __future__ import annotations

import re

import pytest

from server import lifecycle
from server.lifecycle import (
    NetworkFailure,
    _grant_posted_usage_read,
    _posted_usage_unity_catalog_grants,
)
from server.posted_usage import PostedUsageScope, posted_usage_query

APP = "11111111-1111-4111-8111-111111111111"


def test_the_grants_cover_every_table_the_posted_read_selects_from() -> None:
    """Derived from the statement the read actually sends, not from another list."""

    query = posted_usage_query(PostedUsageScope(endpoint_ids=("ep-example-one-d1000001",)))
    read = set(re.findall(r"\b(system\.[a-z_]+\.[a-z_]+)\b", query))
    assert read == {"system.billing.usage", "system.billing.list_prices"}

    grants = {grant.name: grant for grant in _posted_usage_unity_catalog_grants()}
    for table in read:
        assert grants[table].privileges == ("SELECT",)
    # Traversal down to them, and nothing that writes or manages.
    assert grants["system.billing"].privileges == ("USE SCHEMA",)
    assert grants["system"].privileges == ("USE CATALOG",)
    assert set(grants) == read | {"system.billing", "system"}


def test_a_granted_read_applies_every_statement_and_says_nothing(monkeypatch, capsys) -> None:
    applied: list[str] = []
    monkeypatch.setattr(
        "server.lifecycle._sql_statement",
        lambda profile, warehouse_id, statement: applied.append(statement),
    )

    assert _grant_posted_usage_read("profile", "warehouse", APP) == []
    assert applied == [grant.statement(APP) for grant in _posted_usage_unity_catalog_grants()]
    assert "WARN" not in capsys.readouterr().out


def test_a_refused_grant_does_not_fail_setup_and_prints_what_to_run(monkeypatch, capsys) -> None:
    refusal = (
        "Databricks SQL statement failed with state FAILED [BAD_REQUEST] "
        "(warehouse=w, statement=s, attempt 1/4): [INSUFFICIENT_PERMISSIONS] "
        "User does not have MANAGE on Schema 'system.billing'"
    )

    def refuse(profile: str, warehouse_id: str, statement: str):
        if "`system`" in statement:
            raise RuntimeError(refusal)
        return {}

    monkeypatch.setattr("server.lifecycle._sql_statement", refuse)

    unapplied = _grant_posted_usage_read("profile", "warehouse", APP)

    expected = [grant.statement(APP) for grant in _posted_usage_unity_catalog_grants()]
    assert unapplied == expected
    out = capsys.readouterr().out
    assert "WARN  What It Cost" in out
    assert "INSUFFICIENT_PERMISSIONS" in out
    # Every statement an admin needs, ready to paste.
    for statement in expected:
        assert f"        {statement};" in out


def test_a_refused_billing_grant_leaves_round_fours_grants_in_place(monkeypatch, capsys) -> None:
    """The billing grant runs last, so its refusal cannot cost Round 4 anything."""

    applied: list[str] = []

    def refuse_system(profile: str, warehouse_id: str, statement: str):
        if "`system`" in statement:
            raise RuntimeError("[INSUFFICIENT_PERMISSIONS] no")
        applied.append(statement)
        return {}

    monkeypatch.setattr("server.lifecycle._sql_statement", refuse_system)
    monkeypatch.setattr("server.lifecycle._run", lambda arguments, **kwargs: None)
    manifest = type(
        "Manifest", (), {"databricks": type("Databricks", (), {"profile": "profile"})()}
    )()
    names = {
        "catalog": "anti_demo_catalog",
        "source_schema": "source",
        "storage_schema": "storage",
        "online_schema": "online",
        "source_table": "anti_demo_catalog.source.model_scores_source",
        "synced_table_id": "anti_demo_catalog.online.model_scores",
    }

    lifecycle._grant_round4_uc_and_warehouse(manifest, names, "warehouse", "pipeline", APP)

    round4 = lifecycle._round4_unity_catalog_grants(names)
    assert applied == [grant.statement(APP) for grant in round4]
    assert "WARN  What It Cost" in capsys.readouterr().out


def test_a_lost_network_is_not_mistaken_for_a_refusal(monkeypatch) -> None:
    def lose_the_network(profile: str, warehouse_id: str, statement: str):
        raise NetworkFailure("no route to host")

    monkeypatch.setattr("server.lifecycle._sql_statement", lose_the_network)

    with pytest.raises(NetworkFailure):
        _grant_posted_usage_read("profile", "warehouse", APP)
