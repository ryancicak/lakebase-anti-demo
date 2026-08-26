"""What Databricks has actually posted for this installation, from one Delta query.

:mod:`server.standing_cost` prices the AWS side from a sealed shape and refuses to
make a provider call of its own. The Databricks side is the other half, and it is
the half that can be read against a bill: ``system.billing.usage`` publishes what
was metered and ``system.billing.list_prices`` publishes what it was metered at.
This module is the caller that reads them and hands the result over as a
:class:`~server.standing_cost.PostedDatabricksUsage`.

**This is a Delta query and wakes nothing, and it must stay that way.** Reading
CDF or change-feed status through the Lakebase control plane wakes the endpoint
and bills the thing being measured -- reproduced twice, on 137 samples.
``get-endpoint`` is the only safe control-plane call and nothing here needs it:
every identifier this query filters on is already sealed in the manifest. The
Lakebase endpoints are named by the hosts in ``round_environments``, the app by
``round4.app_service_principal_client_id`` and the synced-table pipeline by
``round4.pipeline_id``. So the scope is derived from a file on disk and the only
network call is a ``SELECT``.

Four properties are load-bearing:

1. **One query, not four.** The window, the two Lakebase quantities and every
   platform meter come back from a single statement, so the figures cannot be
   assembled from reads taken minutes apart and then compared as if they shared a
   window.
2. **No rate is known here either.** Prices come from ``list_prices`` joined at
   the usage row's own start time, and the price handed on is the effective one
   -- dollars divided by quantity -- so an interval that crossed a price change
   reports what it actually cost rather than whichever side of the change was
   picked.
3. **A row that did not price takes its whole meter out.** ``unpriced_rows``
   suppresses that component's price, :func:`observed_platform_components` then
   drops the component, and the platform lane reads ``unpriced`` rather than
   quietly smaller. An understated Databricks total is the one error that would
   flatter us.
4. **``predates_installation`` is derived, not asserted.** A meter whose first
   posted interval starts before ``manifest.created_at`` was billing before this
   run existed. The app is the case that matters -- it is why the two totals are
   separate figures -- and it comes out of the data rather than out of a
   constant.

An absence is not a failure. Not one sealed project has posted a single
``COMPUTE_NODE_ALWAYS_ON_MIN`` row: every endpoint scales to zero and there is no
always-on minimum to bill. That is a measured zero and travels with the basis
that makes it one. It is only reported as unavailable when the query itself could
not run.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation

from .standing_cost import (
    INTERMITTENT_PLATFORM_COMPONENTS,
    ROUND4_PIPELINE_LABEL,
    PlatformComponent,
    PostedDatabricksUsage,
    observed_platform_components,
)

SECONDS_PER_HOUR = Decimal(3600)

USAGE_TABLE = "system.billing.usage"
PRICE_TABLE = "system.billing.list_prices"

# Every identifier below is read from the manifest and interpolated into SQL. The
# pattern is narrower than any of them needs to be, and a value that does not
# match is refused rather than quoted -- a sealed identifier that has picked up a
# quote is a corrupted seal, not a string to escape.
_SAFE_IDENTIFIER = re.compile(r"\A[A-Za-z0-9._-]{1,255}\Z")

_ALWAYS_ON = "lakebase_always_on"
_STORAGE = "lakebase_storage"
_PLATFORM = "platform"
_WINDOW = "window"

_NO_ALWAYS_ON_MIN = (
    "no COMPUTE_NODE_ALWAYS_ON_MIN rows posted for the sealed projects over this "
    "window: every endpoint scales to zero and bills no always-on minimum"
)


class PostedUsageScopeError(ValueError):
    """The manifest did not name enough of the installation to scope a read."""


@dataclass(frozen=True, slots=True)
class PostedUsageScope:
    """Which meters belong to this installation, taken entirely from the seal.

    ``since`` is deliberately the *date* the installation was created rather than
    its timestamp. The day's earlier rows are what establish that the app was
    already billing before ``created_at``, and dropping them would make a meter
    that predates this run look like one this run started.
    """

    endpoint_ids: tuple[str, ...]
    app_ids: tuple[str, ...] = ()
    pipeline_ids: tuple[str, ...] = ()
    created_at: datetime | None = None
    since: date | None = None
    labels: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.endpoint_ids:
            raise PostedUsageScopeError(
                "no sealed Lakebase endpoint could be read from the manifest, so a "
                "posted read would have nothing to scope itself to"
            )
        for value in (*self.endpoint_ids, *self.app_ids, *self.pipeline_ids):
            if not _SAFE_IDENTIFIER.match(value):
                raise PostedUsageScopeError(
                    f"sealed identifier {value!r} is not a plain identifier and will "
                    "not be interpolated into a query"
                )

    def label_for(self, kind: str, identifier: str) -> str:
        return self.labels.get(identifier) or f"{kind} · {identifier}"


def _endpoint_id_from_host(host: object) -> str:
    """``ep-example-one-d1000001.database...`` is the endpoint billing names."""

    text = str(host or "").strip()
    if not text:
        return ""
    leading = text.split(".", 1)[0]
    return leading.removesuffix("-pooler")


def scope_from_manifest(manifest: object) -> PostedUsageScope:
    """Name this installation's Databricks meters without a control-plane call."""

    endpoints: list[str] = []
    labels: dict[str, str] = {}

    environments = getattr(manifest, "round_environments", None) or {}
    values = environments.values() if hasattr(environments, "values") else environments
    for environment in values:
        endpoint = _endpoint_id_from_host(
            getattr(getattr(environment, "lakebase", None), "direct_host", "")
        )
        if endpoint and endpoint not in endpoints:
            endpoints.append(endpoint)

    coordination = getattr(manifest, "coordination_environment", None)
    endpoint = _endpoint_id_from_host(getattr(coordination, "direct_host", ""))
    if endpoint and endpoint not in endpoints:
        endpoints.append(endpoint)

    app_ids: list[str] = []
    pipeline_ids: list[str] = []
    round4 = getattr(manifest, "round4", None)
    app_id = str(getattr(round4, "app_service_principal_client_id", "") or "").strip()
    if app_id:
        app_ids.append(app_id)
        labels[app_id] = "Databricks App compute"
    pipeline_id = str(getattr(round4, "pipeline_id", "") or "").strip()
    if pipeline_id:
        pipeline_ids.append(pipeline_id)
        # The label is the join between this read and the continuous-pipeline
        # paragraph in the disclosure, which finds its subject by name. Renaming
        # it here alone would withhold that paragraph rather than fail.
        labels[pipeline_id] = ROUND4_PIPELINE_LABEL

    created_at = getattr(manifest, "created_at", None)
    created = created_at if isinstance(created_at, datetime) else None
    if created is not None and created.tzinfo is None:
        created = created.replace(tzinfo=UTC)

    return PostedUsageScope(
        endpoint_ids=tuple(endpoints),
        app_ids=tuple(app_ids),
        pipeline_ids=tuple(pipeline_ids),
        created_at=created,
        since=created.astimezone(UTC).date() if created is not None else None,
        labels=labels,
    )


def _in_list(values: Sequence[str]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def posted_usage_query(scope: PostedUsageScope) -> str:
    """The one statement. Aggregated in SQL so the payload is nine rows, not ten thousand.

    The sealed projects are resolved from the sealed endpoints inside the query
    rather than configured, because storage rows carry a project and no endpoint:
    scoping storage by endpoint would silently drop the whole Lakebase storage
    lane.
    """

    since = scope.since.isoformat() if scope.since is not None else "1970-01-01"
    app_clause = (
        f"OR u.usage_metadata.app_id IN ({_in_list(scope.app_ids)})" if scope.app_ids else ""
    )
    pipeline_clause = (
        f"OR u.usage_metadata.dlt_pipeline_id IN ({_in_list(scope.pipeline_ids)})"
        if scope.pipeline_ids
        else ""
    )
    return f"""
WITH sealed AS (
  SELECT DISTINCT usage_metadata.project_id AS project_id
  FROM {USAGE_TABLE}
  WHERE usage_date >= DATE'{since}'
    AND usage_metadata.project_id IS NOT NULL
    AND usage_metadata.endpoint_id IN ({_in_list(scope.endpoint_ids)})
),
scoped AS (
  SELECT u.sku_name, u.cloud, u.usage_unit, u.usage_quantity,
         u.usage_start_time, u.usage_end_time,
         CASE
           WHEN u.billing_origin_product = 'LAKEBASE'
                AND u.product_features.lakebase.compute_type = 'COMPUTE_NODE_ALWAYS_ON_MIN'
             THEN '{_ALWAYS_ON}'
           WHEN u.billing_origin_product = 'LAKEBASE' AND u.usage_unit = 'DSU'
             THEN '{_STORAGE}'
           WHEN u.usage_metadata.app_id IS NOT NULL
                OR u.usage_metadata.dlt_pipeline_id IS NOT NULL
             THEN '{_PLATFORM}'
           ELSE 'lakebase_marginal'
         END AS kind,
         COALESCE(u.usage_metadata.app_id, u.usage_metadata.dlt_pipeline_id, '') AS identifier
  FROM {USAGE_TABLE} u
  WHERE u.usage_date >= DATE'{since}'
    AND (
      (u.billing_origin_product = 'LAKEBASE'
         AND u.usage_metadata.project_id IN (SELECT project_id FROM sealed))
      {app_clause}
      {pipeline_clause}
    )
),
priced AS (
  SELECT s.kind, s.identifier, s.usage_unit, s.usage_start_time, s.usage_end_time,
         CAST(s.usage_quantity AS DOUBLE) AS qty,
         CAST(s.usage_quantity AS DOUBLE)
           * CAST(lp.pricing.effective_list.default AS DOUBLE) AS usd,
         -- Each interval's own length. Summed below, it is the time the meter
         -- was actually up: for a resource that starts and stops that is not
         -- the same quantity as the span its first and last interval bound,
         -- and it is the one a rate has to be divided by.
         CAST(
           unix_timestamp(s.usage_end_time) - unix_timestamp(s.usage_start_time) AS DOUBLE
         ) AS metered_seconds,
         CASE WHEN lp.sku_name IS NULL THEN 1 ELSE 0 END AS unpriced
  FROM scoped s
  LEFT JOIN {PRICE_TABLE} lp
    ON  lp.sku_name = s.sku_name AND lp.cloud = s.cloud
    AND lp.currency_code = 'USD' AND lp.usage_unit = s.usage_unit
    AND s.usage_start_time >= lp.price_start_time
    AND (lp.price_end_time IS NULL OR s.usage_start_time < lp.price_end_time)
)
SELECT '{_WINDOW}' AS kind, '' AS identifier, '' AS usage_unit,
       SUM(qty) AS qty, SUM(usd) AS usd, SUM(unpriced) AS unpriced_rows,
       COUNT(*) AS rows_n,
       MIN(usage_start_time) AS first_start, MAX(usage_end_time) AS last_end,
       SUM(metered_seconds) AS metered_seconds
FROM priced
UNION ALL
SELECT kind, identifier, MAX(usage_unit),
       SUM(qty), SUM(usd), SUM(unpriced), COUNT(*),
       MIN(usage_start_time), MAX(usage_end_time), SUM(metered_seconds)
FROM priced
WHERE kind <> 'lakebase_marginal'
GROUP BY kind, identifier
ORDER BY kind, identifier
""".strip()


def _decimal(value: object) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _moment(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _hours(row: Mapping[str, object]) -> Decimal | None:
    """How long this meter's own posted coverage runs.

    Each meter is divided by its own window rather than by the read's overall
    window. The app posts hourly and the pipeline every ten minutes, so they stop
    at different points; dividing both by the later of the two would understate
    whichever finished first.
    """

    start = _moment(row.get("first_start"))
    end = _moment(row.get("last_end"))
    if start is None or end is None:
        return None
    seconds = Decimal(str((end - start).total_seconds()))
    return seconds / SECONDS_PER_HOUR if seconds > 0 else None


def _metered_hours(row: Mapping[str, object]) -> Decimal | None:
    """How long this meter was actually up, summed over its own posted intervals.

    The counterpart to :func:`_hours`, and the difference between them is the
    whole of the defect this exists for. ``_hours`` measures the *span* from the
    first posted interval to the last; this measures the intervals themselves. A
    resource that never stops posts back-to-back intervals and the two agree. A
    resource that starts and stops leaves gaps, and only this one excludes them.

    ``None`` when the query did not return the column -- an older cached payload,
    or a caller assembling rows by hand -- so an absent value degrades to the
    span rather than to a division by zero.
    """

    seconds = _decimal(row.get("metered_seconds"))
    if seconds is None or seconds <= 0:
        return None
    return seconds / SECONDS_PER_HOUR


def _per_hour(row: Mapping[str, object]) -> Decimal | None:
    quantity = _decimal(row.get("qty"))
    hours = _hours(row)
    if quantity is None or hours is None:
        return None
    return quantity / hours


def _platform_rows(
    rows: Sequence[Mapping[str, object]],
    scope: PostedUsageScope,
) -> list[dict[str, object]]:
    """Platform meters as :func:`observed_platform_components` wants them.

    The price handed on is the effective one -- dollars over quantity across the
    meter's own rows -- and is omitted entirely when any row failed to join a
    price, which is what turns a shortfall into an unpriced lane instead of a
    smaller total.

    **The meter is divided by its uptime for the components declared
    intermittent, and by its posted span for every other one.** That is a
    per-component property rather than a better method, and the distinction is
    load-bearing in both directions. The App's compute and the Lakebase lanes are
    up for the whole span they post over, so the span *is* their uptime and
    changing their denominator would move a figure that is already right. The
    Round 4 pipeline is started at arm and released once its bout has settled, so
    its span is mostly hours it was down -- and dividing by that produced a
    duty-cycle-blended average which the app rendered as a rate while every
    document published the while-running one.
    """

    parsed: list[dict[str, object]] = []
    for row in rows:
        if str(row.get("kind") or "") != _PLATFORM:
            continue
        identifier = str(row.get("identifier") or "")
        quantity = _decimal(row.get("qty"))
        usd = _decimal(row.get("usd"))
        unpriced = _decimal(row.get("unpriced_rows")) or Decimal(0)
        label = scope.label_for("Databricks platform meter", identifier)
        uptime = _metered_hours(row) if label in INTERMITTENT_PLATFORM_COMPONENTS else None
        span_hours = _hours(row)
        per_hour = (
            quantity / uptime
            if uptime is not None and quantity is not None
            else _per_hour(row)
        )
        first_start = _moment(row.get("first_start"))
        predates = bool(
            scope.created_at is not None
            and first_start is not None
            and first_start < scope.created_at
        )
        stamp = (
            f"{first_start:%Y-%m-%dT%H:%M:%SZ}" if first_start is not None else "an unread start"
        )
        attribution = (
            f"{USAGE_TABLE} rows for {identifier}, {int(_decimal(row.get('rows_n')) or 0)} "
            f"intervals from {stamp}"
        )
        if predates:
            attribution += ", which is before this installation was created"
        if uptime is not None:
            # The duty cycle travels with the figure, because a rate divided by
            # uptime and one divided by a span are different claims and a reader
            # cannot tell them apart from the number alone.
            attribution += (
                f", divided by {uptime:.1f} h of uptime"
                + (f" inside a {span_hours:.1f} h span" if span_hours is not None else "")
            )
        parsed.append(
            {
                "label": label,
                "dbu_per_hour": per_hour,
                "uptime_hours": uptime,
                "usd_per_dbu": (
                    usd / quantity
                    if usd is not None and quantity is not None and quantity > 0 and unpriced == 0
                    else None
                ),
                "attribution": attribution,
                "grade": "measured",
                "predates_installation": predates,
            }
        )
    return parsed


def posted_usage_from_rows(
    rows: Iterable[Mapping[str, object]],
    scope: PostedUsageScope,
) -> PostedDatabricksUsage:
    """Turn the query's nine-or-so rows into the observation the builder takes."""

    materialised = list(rows)
    by_kind = {str(row.get("kind") or ""): row for row in materialised}
    window = by_kind.get(_WINDOW)
    if window is None or not _decimal(window.get("rows_n")):
        return PostedDatabricksUsage(
            unavailable=(
                "the posted read returned no usage rows for this installation's "
                "sealed endpoints, app or pipeline"
            )
        )

    storage = by_kind.get(_STORAGE)
    always_on = by_kind.get(_ALWAYS_ON)
    platform: tuple[PlatformComponent, ...] = observed_platform_components(
        _platform_rows(materialised, scope)
    )

    return PostedDatabricksUsage(
        window_start=_moment(window.get("first_start")),
        window_end=_moment(window.get("last_end")),
        posted_usd=_decimal(window.get("usd")),
        # An absent always-on-min row is a measured absence: the read succeeded and
        # found none. It is a zero with its basis attached, never an unavailable.
        lakebase_dbu_per_hour=(
            _per_hour(always_on) if always_on is not None else Decimal(0)
        ),
        lakebase_dbu_basis=(
            f"{USAGE_TABLE} COMPUTE_NODE_ALWAYS_ON_MIN rows for the sealed projects"
            if always_on is not None
            else _NO_ALWAYS_ON_MIN
        ),
        lakebase_dsu_per_hour=_per_hour(storage) if storage is not None else None,
        lakebase_dsu_basis=(
            f"{USAGE_TABLE} DSU storage rows for the sealed projects"
            if storage is not None
            else "no DSU storage rows were posted for the sealed projects"
        ),
        platform=platform,
    )


QueryExecutor = Callable[[str], Iterable[Mapping[str, object]]]


def read_posted_databricks_usage(
    manifest: object | None,
    *,
    execute: QueryExecutor,
) -> PostedDatabricksUsage:
    """Read posted usage, and turn every failure into an unavailable rather than a raise.

    A disclosure that cannot read billing still has an AWS half, a projection and
    two totals. Losing the posted comparison suppresses the variance and nothing
    else, which is why every failure here lands as
    :attr:`PostedDatabricksUsage.unavailable` and none of them propagates.
    """

    if manifest is None:
        return PostedDatabricksUsage(
            unavailable="no manifest is configured, so no installation could be scoped"
        )
    try:
        scope = scope_from_manifest(manifest)
    except PostedUsageScopeError as error:
        return PostedDatabricksUsage(unavailable=str(error))
    try:
        rows = execute(posted_usage_query(scope))
    except Exception as error:  # noqa: BLE001 - a billing read may never take the app down
        return PostedDatabricksUsage(
            unavailable=f"the posted read could not run: {type(error).__name__}: {error}"
        )
    try:
        return posted_usage_from_rows(rows, scope)
    except Exception as error:  # noqa: BLE001 - nor may parsing its answer
        return PostedDatabricksUsage(
            unavailable=f"the posted read could not be parsed: {type(error).__name__}: {error}"
        )


def warehouse_query_executor(manifest: object) -> QueryExecutor | None:
    """A ``SELECT``-only executor over the warehouse the installation already owns.

    The seal names a SQL warehouse for Round 4 and another for Round 6, and both
    are warehouses this installation created and is already billed for. One of
    them is picked rather than a new one started: waking a second warehouse to
    measure standing cost would add standing cost, which is the mistake this whole
    module is written around.

    ``None`` when the seal names no warehouse, which
    :func:`read_posted_databricks_usage` turns into an unavailable rather than a
    raise.
    """

    profile = str(getattr(getattr(manifest, "databricks", None), "profile", "") or "").strip()
    warehouse = ""
    for round_name in ("round4", "round6"):
        candidate = str(
            getattr(getattr(manifest, round_name, None), "warehouse_id", "") or ""
        ).strip()
        if candidate:
            warehouse = candidate
            break
    if not profile or not warehouse:
        return None

    def execute(statement: str) -> list[Mapping[str, object]]:
        # Imported here rather than at module scope: server.lifecycle pulls in the
        # whole provisioning surface, and this module is imported by the session
        # manager on every start including the ones that never read billing.
        from .lifecycle import _sql_rows, _sql_statement

        return _sql_rows(_sql_statement(profile, warehouse, statement, timeout=180))

    return execute


class PostedUsageCache:
    """The posted read, held off the request path entirely.

    WHY A CACHE AND NOT A CALL. The disclosure is rebuilt on every read of a
    session, and this read is a warehouse round trip that took roughly fifteen
    seconds when it was first run against the sealed installation. Doing it inline
    would put that latency on every poll of a fight card, and would make a panel
    whose subject is cost into a thing that costs warehouse time to look at.

    So the value is refreshed on its own schedule and :meth:`current` only ever
    returns what is already in hand. Before the first refresh completes that is
    ``None``, which the builder renders as an unavailable posted read -- the same
    honest degraded state as a billing outage, and the reason it is safe to serve
    immediately at startup rather than blocking on a warehouse.

    Posted usage moves hourly at best: ``system.billing.usage`` publishes on an
    interval measured in tens of minutes, so a refresh far more often than that
    would re-read a table that has not changed.
    """

    def __init__(
        self,
        manifest: object | None,
        *,
        execute: QueryExecutor | None = None,
        interval_seconds: float = 900.0,
    ) -> None:
        self._manifest = manifest
        self._execute = execute
        self._interval = interval_seconds
        self._value: PostedDatabricksUsage | None = None

    @property
    def interval_seconds(self) -> float:
        return self._interval

    def current(self) -> PostedDatabricksUsage | None:
        """What the last refresh found. Never blocks, never calls a provider."""

        return self._value

    def refresh(self) -> PostedDatabricksUsage:
        """Read once, blocking. Belongs on a background task, never on a request."""

        execute = self._execute
        if execute is None and self._manifest is not None:
            execute = warehouse_query_executor(self._manifest)
        if execute is None:
            value = PostedDatabricksUsage(
                unavailable=(
                    "no SQL warehouse is named in the seal, so posted usage could not "
                    "be read"
                )
            )
        else:
            value = read_posted_databricks_usage(self._manifest, execute=execute)
        self._value = value
        return value
