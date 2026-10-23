"""The Azure edition's two ADF data flows, as ARM bodies: Round 4's mover and Round 6's extract.

The installer writes these once; the app starts their pipelines at the bell and cancels them to
park (design sections 6.4 and 6.6). Every rule below was learned on a cold run in Phase 0
(2026-10-18), each run costing 4-7 minutes, so they are kept in one place with tests.

* **Escaping.** A quote inside a script string literal is ``\\'``. A doubled quote fails to
  compile (DF-DSL-001), and only after the cluster has spent its cold start.
* **A sink declares its columns.** With schema drift off, a sink with no ``input(...)`` maps
  nothing ("The result has 0 output columns").
* **Round 4 reads a version, not a change feed.** ADF's Delta reader is Delta Lake 2.4: no change
  feed, and no ``v2Checkpoint``, so the source table is created with classic checkpoints.
  ``versionAsOf`` reads the version just after Prepare's verified one.
* **The version is the run's parameter,** so one standing flow serves every bout and the bell
  stays one ``createRun``. The flow stages it beside each row, and the guard reads it from there,
  so no literal version is baked into the flow's post-SQL.
* **Round 4 writes through a staging table** and applies glue/round4_writer.py's guard in one
  post-SQL statement: a different Delta table, or a higher commit version, wins; a replay changes
  nothing (Phase 0: a second run of version 4 left all 18,001 rows as they were).
* **Both run on the factory's managed-VNet runtime.** Data flows don't leave from the published
  DataFactory ranges, so they reach the servers through managed private endpoints.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

ROUND4_COLUMNS = (("entity_id", "string"), ("score", "double"), ("model_version", "string"),
                  ("proof_nonce", "string"), ("updated_at", "timestamp"))
#: What Round 4's staging table holds: the Delta row, and the table and version the run read it
#: at. Both are the run's parameters, as Glue's run arguments are.
ROUND4_STAGED_COLUMNS = (*ROUND4_COLUMNS, ("delta_table_id", "string"),
                         ("delta_commit_version", "long"))
#: Round 6's source: the app's `round6.live_orders` row (server/round6_race_live.py's columns),
#: and Azure's watermark column, which the database defaults so checkout writes nothing new.
ROUND6_COLUMNS = (("order_id", "string"), ("sku", "string"), ("store", "string"),
                  ("quantity", "integer"), ("total_cents", "integer"), ("status", "string"),
                  ("proof_nonce", "string"), ("committed_at", "timestamp"))


def script_string(text: str) -> str:
    """A data flow script string literal: backslash-escaped, single-quoted."""

    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _columns(columns: Sequence[tuple[str, str]]) -> str:
    return ", ".join(f"{name} as {kind}" for name, kind in columns)


def _sql_literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _identifier(name: str) -> str:
    if not name or not all(ch.isalnum() or ch == "_" for ch in name):
        raise ValueError(f"not a plain SQL identifier: {name!r}")
    return name


def _table_id(value: str) -> str:
    """A Delta table ID, which reaches the flow as a quoted string parameter: a UUID's characters
    only, so nothing in it can close the quote."""

    if not value or not all(ch.isalnum() or ch == "-" for ch in value):
        raise ValueError(f"not a Delta table ID: {value!r}")
    return value


def round4_guarded_upsert(*, schema: str, target: str, staging: str) -> str:
    """glue/round4_writer.py's ledger guard, as one set-based statement from the staging table.

    Each staged row carries the Delta table and version its run read (``delta_table_id``,
    ``delta_commit_version``), so the statement holds neither. A source re-created since the last
    run (a new table ID, versions restarting) then wins, as the guard intends, instead of looking
    like an old, lower version.
    """

    t = f"{_identifier(schema)}.{_identifier(target)}"
    s = f"{_identifier(schema)}.{_identifier(staging)}"
    return (
        f"insert into {t} (entity_id, score, model_version, proof_nonce, delta_table_id, "
        "delta_commit_version, deleted, applied_at) "
        "select entity_id, score, model_version, proof_nonce, delta_table_id, "
        f"delta_commit_version, false, clock_timestamp() from {s} "
        "on conflict (entity_id) do update set score = excluded.score, "
        "model_version = excluded.model_version, proof_nonce = excluded.proof_nonce, "
        "delta_table_id = excluded.delta_table_id, "
        "delta_commit_version = excluded.delta_commit_version, deleted = false, "
        "applied_at = excluded.applied_at "
        f"where {t}.delta_table_id is distinct from excluded.delta_table_id "
        f"or {t}.delta_commit_version < excluded.delta_commit_version")


def _flow(source_ls: str, sink_ls: str, lines: Sequence[str],
          transformations: Sequence[str] = ()) -> dict[str, Any]:
    return {"properties": {"type": "MappingDataFlow", "typeProperties": {
        "sources": [{"name": "src", "linkedService": {"referenceName": source_ls,
                                                      "type": "LinkedServiceReference"}}],
        "sinks": [{"name": "sink1", "linkedService": {"referenceName": sink_ls,
                                                      "type": "LinkedServiceReference"}}],
        "transformations": [{"name": name} for name in transformations],
        "scriptLines": list(lines)}}}


def round4_flow(
    *, adls_linked_service: str, postgres_linked_service: str, container: str, folder: str,
    version: int, schema: str, target: str, staging: str, delta_table_id: str,
) -> dict[str, Any]:
    """Round 4's mover. `version` and `delta_table_id` are only the parameters' defaults: each
    run passes its own (`server/round4_adf_lane.py`)."""

    if version < 0:
        raise ValueError("version must be a Delta commit version")
    upsert = round4_guarded_upsert(schema=schema, target=target, staging=staging)
    return _flow(adls_linked_service, postgres_linked_service, [
        "parameters{",
        f"     version as integer ({int(version)}),",
        f"     table_id as string ({script_string(_table_id(delta_table_id))})",
        "}",
        f"source(output({_columns(ROUND4_COLUMNS)}),",
        "     allowSchemaDrift: false,",
        "     validateSchema: false,",
        "     ignoreNoFilesFound: false,",
        "     format: 'delta',",
        f"     fileSystem: {script_string(container)},",
        f"     folderPath: {script_string(folder)},",
        "     versionAsOf: ($version)) ~> src",
        "src derive(delta_table_id = $table_id,",
        "     delta_commit_version = toLong($version)) ~> staged",
        "staged sink(allowSchemaDrift: false,",
        "     validateSchema: false,",
        f"     input({_columns(ROUND4_STAGED_COLUMNS)}),",
        "     format: 'table',",
        f"     schemaName: {script_string(_identifier(schema))},",
        f"     tableName: {script_string(_identifier(staging))},",
        "     insertable: true,",
        "     updateable: false,",
        "     upsertable: false,",
        "     deletable: false,",
        "     recreate: false,",
        "     truncate: true,",
        "     preSQLs: [],",
        f"     postSQLs: [{script_string(upsert)}],",
        "     skipDuplicateMapInputs: true,",
        "     skipDuplicateMapOutputs: true) ~> sink1",
    ], transformations=("staged",))


def round6_flow(
    *, postgres_linked_service: str, adls_linked_service: str, table: str, watermark_utc: str,
    container: str, folder: str, schema: str = "round6",
) -> dict[str, Any]:
    """Rows committed after `watermark_utc`, appended to the Delta history in ADLS."""

    columns = ", ".join(name for name, _ in ROUND6_COLUMNS)
    query = (f"select {columns} from {_identifier(schema)}.{_identifier(table)} "
             f"where committed_at > {_sql_literal(watermark_utc)} order by committed_at, order_id")
    return _flow(postgres_linked_service, adls_linked_service, [
        f"source(output({_columns(ROUND6_COLUMNS)}),",
        "     allowSchemaDrift: false,",
        "     validateSchema: false,",
        "     isolationLevel: 'READ_COMMITTED',",
        f"     query: {script_string(query)},",
        "     format: 'query') ~> src",
        "src sink(allowSchemaDrift: false,",
        "     validateSchema: false,",
        f"     input({_columns(ROUND6_COLUMNS)}),",
        "     format: 'delta',",
        f"     fileSystem: {script_string(container)},",
        f"     folderPath: {script_string(folder)},",
        "     mergeSchema: false,",
        "     autoCompact: false,",
        "     optimizedWrite: false,",
        "     vacuum: 0,",
        "     deletable: false,",
        "     insertable: true,",
        "     updateable: false,",
        "     upsertable: false,",
        "     umask: 0022,",
        "     preCommands: [],",
        "     postCommands: [],",
        "     skipDuplicateMapInputs: true,",
        "     skipDuplicateMapOutputs: true) ~> sink1",
    ])


def pipeline(
    *, flow: str, runtime: str, cores: int = 8,
    parameters: Mapping[str, tuple[str, Any]] | None = None,
) -> dict[str, Any]:
    """One Execute Data Flow activity on the managed-VNet runtime (TTL 0: cold at the bell).

    `parameters` maps a name to its ADF type and default (``{"version": ("int", 4)}``). Each is
    the pipeline's own, passed to the flow's parameter of the same name, so a run's ``createRun``
    body sets it.

    A flow evaluates what it's passed as a data flow *expression*, so a string goes in quoted. Live,
    an unquoted ``probe-table-id-123`` was read as ``probe - table - id - 123`` and failed
    (DF-EXPR-010, Phase 0). An int passes as it is.
    """

    if "_" in runtime:
        raise ValueError("ADF integration runtime names allow letters, digits and hyphens only")
    declared = dict(parameters or {})
    reference: dict[str, Any] = {"referenceName": flow, "type": "DataFlowReference"}
    if declared:
        reference["parameters"] = {
            name: {"value": (f"'@{{pipeline().parameters.{name}}}'" if kind == "string"
                             else f"@pipeline().parameters.{name}"), "type": "Expression"}
            for name, (kind, _default) in declared.items()}
    body: dict[str, Any] = {"activities": [{
        "name": "move", "type": "ExecuteDataFlow", "typeProperties": {
            "dataflow": reference,
            "integrationRuntime": {"referenceName": runtime, "type": "IntegrationRuntimeReference"},
            "compute": {"computeType": "General", "coreCount": cores}, "traceLevel": "Fine"}}]}
    if declared:
        body["parameters"] = {name: {"type": kind, "defaultValue": default}
                              for name, (kind, default) in declared.items()}
    return {"properties": body}


def sink_rows_written(activity: Mapping[str, Any]) -> int | None:
    """The data flow's rows written, from an activity run's output (`ArmClient.activity_runs`)."""

    metrics = ((activity.get("output") or {}).get("runStatus") or {}).get("metrics") or {}
    rows = (metrics.get("sink1") or {}).get("rowsWritten")
    return int(rows) if rows is not None else None
