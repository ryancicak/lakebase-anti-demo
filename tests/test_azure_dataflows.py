"""server/azure_dataflows.py: the rules Phase 0's cold runs taught, pinned without a cold run."""

from __future__ import annotations

import pytest

from server.azure_dataflows import (
    pipeline,
    round4_flow,
    round4_guarded_upsert,
    round6_flow,
    script_string,
    sink_rows_written,
)

ROUND4 = dict(adls_linked_service="ls-adls", postgres_linked_service="ls-r4", container="r4source",
              folder="model_scores_source", version=4, schema="public", target="r4_scores",
              staging="r4_staging", delta_table_id="c3e3563c-0000-0000-0000-000000000000")


def test_a_quote_in_a_script_string_is_backslash_escaped_never_doubled() -> None:
    assert script_string("a > 'x'") == "'a > \\'x\\''"
    assert "''" not in script_string("it's")


def test_round4_reads_the_runs_version_and_table_and_stages_them_beside_each_row() -> None:
    flow = round4_flow(**ROUND4)["properties"]["typeProperties"]
    lines = flow["scriptLines"]
    # The defaults; each run passes its own (round4_adf_lane.py).
    assert lines[:4] == ["parameters{", "     version as integer (4),",
                         "     table_id as string ('c3e3563c-0000-0000-0000-000000000000')", "}"]
    assert "     versionAsOf: ($version)) ~> src" in lines
    assert lines[lines.index("src derive(delta_table_id = $table_id,") + 1] == (
        "     delta_commit_version = toLong($version)) ~> staged")
    assert flow["transformations"] == [{"name": "staged"}]
    sink_input = next(line for line in lines if line.startswith("     input("))
    assert sink_input.startswith("     input(entity_id as string")
    assert sink_input.endswith("delta_commit_version as long),")
    assert "     truncate: true," in lines


def test_round4s_guard_matches_the_glue_writers_rule_with_the_staged_version() -> None:
    sql = round4_guarded_upsert(schema="public", target="r4_scores", staging="r4_staging")
    assert "on conflict (entity_id) do update" in sql
    assert ("where public.r4_scores.delta_table_id is distinct from excluded.delta_table_id "
            "or public.r4_scores.delta_commit_version < excluded.delta_commit_version") in sql
    # The run's table and version, staged beside each row; no literal of either.
    assert "proof_nonce, delta_table_id, delta_commit_version, false" in sql


def test_identifiers_that_could_inject_are_refused() -> None:
    with pytest.raises(ValueError):
        round4_guarded_upsert(schema="public", target="r4; drop table x", staging="s")
    with pytest.raises(ValueError):
        round4_flow(**{**ROUND4, "version": -1})
    with pytest.raises(ValueError, match="not a Delta table ID"):
        round4_flow(**{**ROUND4, "delta_table_id": "x') ~> src"})


def test_a_pipeline_passes_its_parameters_to_the_flow() -> None:
    body = pipeline(flow="r4_move", runtime="ir-westus2",
                    parameters={"version": ("int", 4), "table_id": ("string", "t-1")})[
        "properties"]
    assert body["parameters"] == {"version": {"type": "int", "defaultValue": 4},
                                  "table_id": {"type": "string", "defaultValue": "t-1"}}
    # A flow reads what it's passed as an expression, so a string goes in quoted. Unquoted,
    # "probe-table-id-123" was read as a subtraction and failed live (DF-EXPR-010).
    assert body["activities"][0]["typeProperties"]["dataflow"]["parameters"] == {
        "version": {"value": "@pipeline().parameters.version", "type": "Expression"},
        "table_id": {"value": "'@{pipeline().parameters.table_id}'", "type": "Expression"}}
    assert "parameters" not in pipeline(flow="r6", runtime="ir-westus2")["properties"]


def test_round6_extracts_the_apps_orders_after_the_watermark_with_an_escaped_literal() -> None:
    lines = round6_flow(postgres_linked_service="ls-r6", adls_linked_service="ls-adls",
                        table="live_orders", watermark_utc="2026-10-19T03:27:17.851331+00:00",
                        container="r6history", folder="live_orders_history")[
        "properties"]["typeProperties"]["scriptLines"]
    query = next(line for line in lines if line.startswith("     query:"))
    assert "from round6.live_orders where committed_at > \\'2026-10-19T03:27:17.851331+00:00\\'" \
        in query
    # The app's live_orders row (round6_race_live's columns), then Azure's watermark column.
    assert ("select order_id, sku, store, quantity, total_cents, status, proof_nonce, "
            "committed_at") in query
    sink_input = next(line for line in lines if line.startswith("     input("))
    assert sink_input.startswith("     input(order_id as string, sku as string")
    assert "committed_at as timestamp" in sink_input


def test_a_runtime_name_with_an_underscore_is_refused_before_azure_refuses_it() -> None:
    with pytest.raises(ValueError, match="hyphens"):
        pipeline(flow="r4", runtime="ir_eastus2")
    assert pipeline(flow="r4", runtime="ir-westus2")["properties"]["activities"][0][
        "typeProperties"]["integrationRuntime"]["referenceName"] == "ir-westus2"


def test_rows_written_is_read_from_the_activity_output() -> None:
    assert sink_rows_written({"output": {"runStatus": {"metrics": {"sink1": {
        "rowsWritten": 575}}}}}) == 575
    assert sink_rows_written({"output": {}}) is None
