from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from math import isfinite
from typing import Literal

from .capacity import RDS_INSTANCE_CLASS
from .models import CompetitorId, CostLineItem, CostReceiptSnapshot, RoundId

REGION = "us-west-2"
DBRICKS_AS_OF = datetime(2026, 8, 20, 3, 45, 20, tzinfo=UTC)
AWS_RDS_AS_OF = datetime(2026, 8, 18, 0, 11, 58, tzinfo=UTC)
AWS_EC2_AS_OF = datetime(2026, 8, 19, 16, 58, 43, tzinfo=UTC)
AWS_VPC_AS_OF = datetime(2026, 7, 24, 15, 42, 25, tzinfo=UTC)
AWS_SECRETS_AS_OF = datetime(2025, 8, 28, 15, 38, 4, tzinfo=UTC)
AWS_PROXY_AS_OF = AWS_RDS_AS_OF

DBRICKS_PRICES = "system.billing.list_prices pricing.effective_list.default; normal pricing.default"
AWS_RDS_PRICES = (
    "AWS Price List API · AmazonRDS · OnDemand · us-west-2 · "
    "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonRDS/current/us-west-2/index.json"
)
AWS_RDS_CROSS_AZ_PRICES = (
    f"{AWS_RDS_PRICES} · EC2 bytes sent + received counted once; no RDS-side duplicate"
)
AWS_EC2_PRICES = "AWS Price List API · AmazonEC2/AmazonVPC · OnDemand · us-west-2"
AWS_VPC_PRICES = (
    "AWS Price List API · AmazonVPC · OnDemand · us-west-2 · "
    "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonVPC/current/us-west-2/index.json"
)
AWS_SECRETS_PRICES = "AWS Price List API · AWSSecretsManager · OnDemand · us-west-2"
AWS_PROXY_PRICES = "Amazon RDS Proxy pricing · us-west-2 · per ACU/vCPU-hour · 10-minute minimum"

RDS_PROXY_UNIT_RATE_USD = 0.015
RDS_PROXY_MINIMUM_SECONDS = 600.0

# The RDS instance class every AWS lane runs is chosen once, in
# server/capacity.py, because that one choice has to move the provisioned box,
# the on-screen capacity disclosure, the `capacity_parity` doctor check and the
# price together. Pricing reads it rather than restating it, so a resize cannot
# change the box and leave the rate behind.
CONFIGURED_RDS_INSTANCE_CLASS = RDS_INSTANCE_CLASS


@dataclass(frozen=True, slots=True)
class RdsInstancePrice:
    """One on-demand instance-hour rate, with the AWS SKU that establishes it."""

    usd_per_hour: Decimal
    sku: str


# Single-AZ PostgreSQL on-demand rates, read from the us-west-2 Price List API
# region file `AWS_RDS_PRICES` already cites. Like every AWS figure in this
# project these are rate-card derived rather than invoice-verified:
# `ce:GetCostAndUsage` is denied to this installation and is not being pursued,
# so no AWS number here has been reconciled against a bill.
#
# Every class server/capacity.py is willing to approve appears here, and
# tests/test_cost_model.py asserts the two tables cover the same set, so a class
# that capacity parity would accept can never be one that cost cannot price.
RDS_INSTANCE_HOUR_PRICES: dict[str, RdsInstancePrice] = {
    "db.t4g.micro": RdsInstancePrice(Decimal("0.016"), "CT79XNCJJGH56FA8"),
    "db.t4g.small": RdsInstancePrice(Decimal("0.032"), "VNDRBDV7GJ67Z9MK"),
    "db.t4g.medium": RdsInstancePrice(Decimal("0.065"), "N2BHMKBGM78G338C"),
    "db.t4g.large": RdsInstancePrice(Decimal("0.129"), "QV6898M6ZB978GWA"),
    "db.m6g.large": RdsInstancePrice(Decimal("0.159"), "U83W4JXPWDSWZ252"),
}


class UnknownRdsInstanceClassError(LookupError):
    """Raised when a configured instance class has no published rate here.

    Deliberately fatal rather than defaulted. A plausible-looking rate applied to
    the wrong box puts a wrong dollar figure in front of a customer, which is the
    one failure this project cannot absorb; refusing to price is recoverable.
    """


def rds_instance_price(instance_class: str) -> RdsInstancePrice:
    """The published rate for one class, or a loud failure."""

    try:
        return RDS_INSTANCE_HOUR_PRICES[instance_class]
    except KeyError:
        known = ", ".join(sorted(RDS_INSTANCE_HOUR_PRICES))
        raise UnknownRdsInstanceClassError(
            f"no published instance-hour rate for RDS instance class {instance_class!r}; "
            f"add it to RDS_INSTANCE_HOUR_PRICES in server/pricing.py together with the "
            f"AWS SKU it was read from (priced classes: {known})"
        ) from None


def rds_instance_hour_usd(instance_class: str) -> Decimal:
    return rds_instance_price(instance_class).usd_per_hour


def rds_instance_compute_source(instance_class: str) -> str:
    """The rate-card citation for one class, naming the SKU it was read from."""

    return f"{AWS_RDS_PRICES} · SKU {rds_instance_price(instance_class).sku}"


LaneId = Literal["lakebase", "competitor", "shared"]
Cadence = Literal["bout", "hour", "month", "usage"]
CostStatus = Literal["estimate", "usage_pending", "selection_required"]
RateBasis = Literal["standard_list", "current_promotion"]
CostScope = Literal[
    "bout_estimate",
    "required_monthly_carrying_cost",
    "installation_overhead",
]
Confidence = Literal["high", "medium", "low", "pending"]
QuantityMethod = Literal[
    "rate_card_pending",
    "exact_session_window",
    "result_evidence",
    "provider_reconciliation",
    "selected_configuration",
    "selection_required",
]
ReconciliationStatus = Literal[
    "estimate",
    "pending",
    "reconciled",
    "estimate_only",
    "posted_partial",
    "posted_through_window",
    "corrected",
    "selection_required",
    "attribution_ambiguous",
    "unavailable",
]

DELAYED_DATABRICKS_USAGE = (
    "Databricks system.billing.usage delayed provider reconciliation; "
    "attribute by exact project/branch/endpoint metadata and usage interval"
)
DELTA_STORAGE_USAGE = (
    "Direct table-size observation or system.storage.table_metrics_history when populated; "
    "never infer bytes from row count"
)


def calculate_rds_proxy_cost(
    competitor_id: CompetitorId,
    billable_seconds: float,
) -> tuple[float, float]:
    """Return the capacity-hour quantity and cost for an RDS Proxy lifetime."""
    if not isfinite(billable_seconds) or billable_seconds < 0:
        raise ValueError("RDS Proxy billable seconds must be a finite non-negative value")
    capacity = 8.0 if competitor_id == CompetitorId.AURORA_SERVERLESS_V2 else 2.0
    priced_seconds = max(RDS_PROXY_MINIMUM_SECONDS, billable_seconds)
    quantity = capacity * priced_seconds / 3600.0
    # Preserve the provider formula's operation order so exact floor examples
    # serialize cleanly as 0.02 / 0.005 instead of binary-float artifacts.
    # Normalize the finite decimal rate result before it crosses the JSON boundary.
    # Without this, a real 1,296-second Aurora lifetime serializes as
    # 0.043199999999999995 instead of the auditable $0.043200 result.
    return round(quantity, 12), round(
        RDS_PROXY_UNIT_RATE_USD * capacity * priced_seconds / 3600.0,
        12,
    )


def _line(
    lane_id: LaneId,
    component: str,
    unit: str,
    rate: float | None,
    source: str,
    source_as_of: datetime,
    *,
    quantity: float | None = None,
    subtotal: float | None = None,
    reference_list_rate: float | None = None,
    rate_basis: RateBasis = "standard_list",
    cadence: Cadence = "usage",
    status: CostStatus = "usage_pending",
    scope: CostScope = "bout_estimate",
    confidence: Confidence = "pending",
    quantity_method: QuantityMethod = "rate_card_pending",
    reconciliation_status: ReconciliationStatus = "pending",
) -> CostLineItem:
    return CostLineItem(
        lane_id=lane_id,
        component=component,
        quantity=quantity,
        unit=unit,
        unit_rate_usd=rate,
        reference_list_unit_rate_usd=reference_list_rate,
        subtotal_usd=subtotal,
        rate_basis=rate_basis,
        cadence=cadence,
        status=status,
        scope=scope,
        confidence=confidence,
        quantity_method=quantity_method,
        reconciliation_status=reconciliation_status,
        source=source,
        source_as_of=source_as_of,
    )


def _lakebase_database_lines() -> list[CostLineItem]:
    return [
        _line(
            "lakebase",
            "Lakebase compute",
            "DBU",
            0.26,
            DBRICKS_PRICES,
            DBRICKS_AS_OF,
            reference_list_rate=0.52,
            rate_basis="current_promotion",
        ),
        _line(
            "lakebase",
            "Lakebase database, PITR, and snapshot storage",
            "DSU",
            0.023,
            DBRICKS_PRICES,
            DBRICKS_AS_OF,
            scope="required_monthly_carrying_cost",
        ),
    ]


def _aws_database_lines(competitor_id: CompetitorId) -> list[CostLineItem]:
    if competitor_id == CompetitorId.AURORA_SERVERLESS_V2:
        lines = [
            _line(
                "competitor",
                "Aurora Serverless v2 compute",
                "ACU-hour",
                0.12,
                AWS_RDS_PRICES,
                AWS_RDS_AS_OF,
            ),
            _line(
                "competitor",
                "Aurora database storage",
                "GB-month",
                0.10,
                AWS_RDS_PRICES,
                AWS_RDS_AS_OF,
                scope="required_monthly_carrying_cost",
            ),
            _line(
                "competitor",
                "Aurora standard I/O",
                "million requests",
                0.20,
                AWS_RDS_PRICES,
                AWS_RDS_AS_OF,
            ),
            _line(
                "competitor",
                "Aurora backup storage above free allocation",
                "GB-month",
                0.021,
                AWS_RDS_PRICES,
                AWS_RDS_AS_OF,
                scope="required_monthly_carrying_cost",
            ),
        ]
    else:
        lines = [
            _line(
                "competitor",
                f"RDS PostgreSQL {CONFIGURED_RDS_INSTANCE_CLASS} compute",
                "instance-hour",
                float(rds_instance_hour_usd(CONFIGURED_RDS_INSTANCE_CLASS)),
                rds_instance_compute_source(CONFIGURED_RDS_INSTANCE_CLASS),
                AWS_RDS_AS_OF,
                cadence="month",
                scope="required_monthly_carrying_cost",
            ),
            _line(
                "competitor",
                "RDS PostgreSQL gp3 storage",
                "GB-month",
                0.115,
                AWS_RDS_PRICES,
                AWS_RDS_AS_OF,
                quantity=20,
                subtotal=2.30,
                cadence="month",
                status="estimate",
                scope="required_monthly_carrying_cost",
                confidence="high",
                quantity_method="selected_configuration",
                reconciliation_status="estimate",
            ),
            _line(
                "competitor",
                "RDS backup storage above free allocation",
                "GB-month",
                0.095,
                AWS_RDS_PRICES,
                AWS_RDS_AS_OF,
                scope="required_monthly_carrying_cost",
            ),
        ]
    lines.extend(
        [
            _line(
                "competitor",
                "AWS-managed database credential",
                "secret-month",
                0.40,
                AWS_SECRETS_PRICES,
                AWS_SECRETS_AS_OF,
                quantity=1,
                subtotal=0.40,
                cadence="month",
                status="estimate",
                scope="required_monthly_carrying_cost",
                confidence="high",
                quantity_method="selected_configuration",
                reconciliation_status="estimate",
            ),
            _line(
                "competitor",
                "Database public IPv4",
                "address-hour",
                0.005,
                AWS_VPC_PRICES,
                AWS_VPC_AS_OF,
                cadence="month",
                scope="required_monthly_carrying_cost",
            ),
        ]
    )
    return lines


def _ephemeral_artifact_lines(
    round_id: RoundId,
    competitor_id: CompetitorId,
) -> list[CostLineItem]:
    purpose = (
        "isolated branch" if round_id == RoundId.MAKE_SCHEMA_CHANGE_SAFELY else "recovery branch"
    )
    lines = [
        _line(
            "lakebase",
            f"Lakebase temporary {purpose} compute",
            "DBU",
            0.26,
            DBRICKS_PRICES,
            DBRICKS_AS_OF,
            reference_list_rate=0.52,
            rate_basis="current_promotion",
        ),
        _line(
            "lakebase",
            f"Lakebase temporary {purpose} storage",
            "DSU",
            0.023,
            DBRICKS_PRICES,
            DBRICKS_AS_OF,
        ),
    ]
    aws_purpose = (
        "isolated clone" if round_id == RoundId.MAKE_SCHEMA_CHANGE_SAFELY else "PITR restore"
    )
    if competitor_id == CompetitorId.AURORA_SERVERLESS_V2:
        lines.extend(
            [
                _line(
                    "competitor",
                    f"Aurora temporary {aws_purpose} compute",
                    "ACU-hour",
                    0.12,
                    AWS_RDS_PRICES,
                    AWS_RDS_AS_OF,
                ),
                _line(
                    "competitor",
                    f"Aurora temporary {aws_purpose} storage",
                    "GB-month",
                    0.10,
                    AWS_RDS_PRICES,
                    AWS_RDS_AS_OF,
                ),
                _line(
                    "competitor",
                    f"Aurora temporary {aws_purpose} I/O",
                    "million requests",
                    0.20,
                    AWS_RDS_PRICES,
                    AWS_RDS_AS_OF,
                ),
            ]
        )
    else:
        lines.extend(
            [
                _line(
                    "competitor",
                    # RestoreDBInstanceToPointInTime is called with the source
                    # instance's own DBInstanceClass, so the restore is billed at
                    # the same class as the baseline box it was cloned from.
                    f"RDS PostgreSQL temporary {aws_purpose} compute",
                    "instance-hour",
                    float(rds_instance_hour_usd(CONFIGURED_RDS_INSTANCE_CLASS)),
                    rds_instance_compute_source(CONFIGURED_RDS_INSTANCE_CLASS),
                    AWS_RDS_AS_OF,
                ),
                _line(
                    "competitor",
                    f"RDS PostgreSQL temporary {aws_purpose} gp3 storage",
                    "GB-month",
                    0.115,
                    AWS_RDS_PRICES,
                    AWS_RDS_AS_OF,
                ),
            ]
        )
    lines.append(
        _line(
            "competitor",
            f"Temporary {aws_purpose} public IPv4",
            "address-hour",
            0.005,
            AWS_VPC_PRICES,
            AWS_VPC_AS_OF,
        )
    )
    return lines


def _databricks_pipeline_lines(round_id: RoundId) -> list[CostLineItem]:
    if round_id == RoundId.PUT_MODEL_SCORE_IN_APP:
        native_component = "Lakeflow Connect managed sync compute"
        delta_component = "Delta model-score table storage"
    else:
        native_component = "Lakebase native CDF change-feed processing"
        delta_component = "Delta live-orders table storage"
    return [
        _line(
            "lakebase",
            native_component,
            "provider billing unit",
            None,
            DELAYED_DATABRICKS_USAGE,
            DBRICKS_AS_OF,
        ),
        _line(
            "lakebase",
            "Databricks SQL warehouse query compute",
            "DBU",
            None,
            DELAYED_DATABRICKS_USAGE,
            DBRICKS_AS_OF,
        ),
        _line(
            "lakebase",
            delta_component,
            "GB-month",
            None,
            DELTA_STORAGE_USAGE,
            DBRICKS_AS_OF,
        ),
    ]


def _external_stack_lines(round_id: RoundId) -> list[CostLineItem]:
    product = (
        "Required external reverse-ETL product"
        if round_id == RoundId.PUT_MODEL_SCORE_IN_APP
        else "Required external CDC-to-Delta stack"
    )
    source = "Select a product and published plan before pricing this required layer"
    return [
        _line(
            "competitor",
            product,
            "vendor billing unit",
            None,
            source,
            DBRICKS_AS_OF,
            status="selection_required",
            scope="required_monthly_carrying_cost",
            confidence="pending",
            quantity_method="selection_required",
            reconciliation_status="selection_required",
        ),
        _line(
            "competitor",
            f"{product} installation and configuration",
            "engineering-hour",
            None,
            "Select the product and implementation plan before estimating installation effort",
            DBRICKS_AS_OF,
            status="selection_required",
            scope="installation_overhead",
            confidence="pending",
            quantity_method="selection_required",
            reconciliation_status="selection_required",
        ),
    ]


def _round_five_lines(competitor_id: CompetitorId) -> list[CostLineItem]:
    proxy_target = (
        "Aurora Serverless v2 · 8 ACU"
        if competitor_id == CompetitorId.AURORA_SERVERLESS_V2
        else "provisioned RDS · 2 vCPU"
    )
    proxy_unit = "ACU-hour" if competitor_id == CompetitorId.AURORA_SERVERLESS_V2 else "vCPU-hour"
    proxy_component = (
        f"RDS Proxy · {proxy_target} · published 10-minute minimum applies; "
        "provider lifetime pending"
    )
    return [
        _line(
            "competitor",
            "Cross-AZ runner ↔ database transfer",
            "GB",
            0.01,
            AWS_RDS_CROSS_AZ_PRICES,
            AWS_RDS_AS_OF,
        ),
        _line(
            "competitor",
            proxy_component,
            proxy_unit,
            RDS_PROXY_UNIT_RATE_USD,
            AWS_PROXY_PRICES,
            AWS_PROXY_AS_OF,
        ),
        _line(
            "competitor",
            "RDS Proxy credential in Secrets Manager",
            "secret-month",
            0.40,
            AWS_SECRETS_PRICES,
            AWS_SECRETS_AS_OF,
        ),
        _line(
            "competitor",
            "Secrets Manager API requests",
            "10,000 requests",
            0.05,
            AWS_SECRETS_PRICES,
            AWS_SECRETS_AS_OF,
        ),
        _line(
            "shared",
            "Neutral m6i.large runner",
            "instance-hour",
            0.096,
            AWS_EC2_PRICES,
            AWS_EC2_AS_OF,
            cadence="month",
            scope="installation_overhead",
        ),
        _line(
            "shared",
            "Neutral runner gp3 root volume",
            "GB-month",
            0.08,
            AWS_EC2_PRICES,
            AWS_EC2_AS_OF,
            quantity=20,
            subtotal=1.60,
            cadence="month",
            status="estimate",
            scope="installation_overhead",
            confidence="high",
            quantity_method="selected_configuration",
            reconciliation_status="estimate",
        ),
        _line(
            "shared",
            "Neutral runner public IPv4",
            "address-hour",
            0.005,
            AWS_VPC_PRICES,
            AWS_VPC_AS_OF,
            cadence="month",
            scope="installation_overhead",
        ),
    ]


def _known_total(lines: list[CostLineItem], scope: CostScope) -> float | None:
    subtotals = [
        line.subtotal_usd for line in lines if line.scope == scope and line.subtotal_usd is not None
    ]
    return round(sum(subtotals), 12) if subtotals else None


def _receipt(lines: list[CostLineItem]) -> CostReceiptSnapshot:
    for line in lines:
        if line.subtotal_usd is not None and line.original_estimate_usd is None:
            line.original_estimate_usd = line.subtotal_usd
        if line.reconciliation_status in {"estimate", "pending"}:
            line.reconciliation_status = "estimate_only"
    known_bout_estimate = _known_total(lines, "bout_estimate")
    return CostReceiptSnapshot(
        region=REGION,
        lines=lines,
        reconciliation_status="estimate_only",
        original_estimate_usd=known_bout_estimate,
        known_bout_estimate_usd=known_bout_estimate,
        known_monthly_carrying_cost_usd=_known_total(lines, "required_monthly_carrying_cost"),
        known_installation_overhead_usd=_known_total(lines, "installation_overhead"),
        note=(
            "Immediate estimate, not an invoice. Bout estimate includes only known incremental "
            "usage or temporary artifacts caused by this bout; required monthly carrying cost "
            "and installation overhead are shown separately and never added to it. Null quantity "
            "means pending evidence, not zero cost. Lakebase compute uses the published $0.26/DBU "
            "promotion against the normal $0.52/DBU list rate; storage remains $0.023/DSU. "
            "Databricks system billing and AWS billed usage arrive after the bout, so later "
            "provider reconciliation must retain its exact usage watermark."
        ),
    )


def build_cost_receipt(
    round_id: RoundId,
    competitor_id: CompetitorId,
    *,
    rds_proxy_billable_seconds: float | None = None,
) -> CostReceiptSnapshot:
    lines = _lakebase_database_lines()
    if round_id not in {RoundId.PUT_MODEL_SCORE_IN_APP, RoundId.ANALYZE_LIVE_ORDERS}:
        lines.extend(_aws_database_lines(competitor_id))
    if round_id in {RoundId.MAKE_SCHEMA_CHANGE_SAFELY, RoundId.RECOVER_DELETED_ORDER}:
        lines.extend(_ephemeral_artifact_lines(round_id, competitor_id))
    if round_id in {RoundId.PUT_MODEL_SCORE_IN_APP, RoundId.ANALYZE_LIVE_ORDERS}:
        lines.extend(_databricks_pipeline_lines(round_id))
        lines.extend(_external_stack_lines(round_id))
    if round_id == RoundId.SURVIVE_CONNECTION_SPIKE:
        lines.extend(_round_five_lines(competitor_id))
        if rds_proxy_billable_seconds is not None:
            receipt = _receipt(lines)
            return update_terminal_cost_receipt(
                receipt,
                competitor_id,
                terminal_at=datetime.now(UTC),
                run_started_at=None,
                rds_proxy_created=True,
                rds_proxy_billable_seconds=rds_proxy_billable_seconds,
            )
    return _receipt(lines)


def update_terminal_cost_receipt(
    receipt: CostReceiptSnapshot,
    competitor_id: CompetitorId,
    *,
    terminal_at: datetime,
    run_started_at: datetime | None,
    rds_proxy_created: bool = False,
    rds_proxy_billable_seconds: float | None = None,
) -> CostReceiptSnapshot:
    """Add only quantities proved by terminal session/result evidence.

    Session elapsed time is not a database, runner, storage, network, API, or CUR
    billing quantity, so those lines deliberately remain pending.
    """
    updated = receipt.model_copy(deep=True)
    if not rds_proxy_created:
        return updated
    proxy_line = next(
        (line for line in updated.lines if line.component.startswith("RDS Proxy ·")),
        None,
    )
    if proxy_line is None:
        return updated
    billed_seconds = 0.0 if rds_proxy_billable_seconds is None else rds_proxy_billable_seconds
    quantity, subtotal = calculate_rds_proxy_cost(competitor_id, billed_seconds)
    proxy_line.quantity = quantity
    proxy_line.subtotal_usd = subtotal
    proxy_line.status = "estimate"
    proxy_line.confidence = "high" if rds_proxy_billable_seconds is not None else "medium"
    proxy_line.quantity_method = "result_evidence"
    proxy_line.reconciliation_status = "estimate_only"
    proxy_line.original_estimate_usd = subtotal
    proxy_line.observed_from = run_started_at
    proxy_line.observed_through = terminal_at
    if rds_proxy_billable_seconds is None:
        proxy_line.component = proxy_line.component.replace(
            "published 10-minute minimum applies; provider lifetime pending",
            "published 10-minute minimum evidenced; final provider lifetime pending",
        )
    else:
        proxy_line.component = proxy_line.component.replace(
            "published 10-minute minimum applies; provider lifetime pending",
            "evidenced provider lifetime estimate; billed usage pending",
        )
    updated.known_bout_estimate_usd = _known_total(updated.lines, "bout_estimate")
    updated.original_estimate_usd = updated.known_bout_estimate_usd
    updated.known_monthly_carrying_cost_usd = _known_total(
        updated.lines,
        "required_monthly_carrying_cost",
    )
    updated.known_installation_overhead_usd = _known_total(
        updated.lines,
        "installation_overhead",
    )
    return updated
