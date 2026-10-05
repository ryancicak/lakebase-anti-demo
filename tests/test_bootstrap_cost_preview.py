"""The installer's cost preview prices the always-on fleet the way the cost model does.

2026-10-02. rc11's install printed "~$30.04/day fixed" against the model's $32.23 of
hourly-billed AWS. The preview had left out Round 6's DMS replication instance and the
eleven database writers' public addresses, and counted one runner's root volume of two.
bootstrap.sh copies its rates from server/cost_model.py, and nothing checked the copy.
"""

from __future__ import annotations

import re
import subprocess
from decimal import Decimal
from pathlib import Path

import pytest

from server.cost_model import (
    CarryingWindow,
    Cloud,
    InstallationShape,
    RateCard,
    estimate_carrying_cost,
)
from server.pricing import rds_instance_hour_usd

SOURCE = (Path(__file__).resolve().parents[1] / "bootstrap.sh").read_text(encoding="utf-8")
_DAY = CarryingWindow(seconds=Decimal(86400))
_HOURLY_UNITS = {"instance-hour", "address-hour", "ACU-hour"}


def _literal(name: str) -> Decimal:
    match = re.search(rf'^{name}="?([0-9.]+)"?$', SOURCE, re.MULTILINE)
    assert match is not None, f"{name} is no longer a literal in bootstrap.sh"
    return Decimal(match.group(1))


def _preview(function: str) -> Decimal:
    """Run one of the preview's own awk functions with the script's own constants."""

    constants = "\n".join(re.findall(r"^(?:RATE|COUNT)_[A-Z0-9_]+=.*$", SOURCE, re.MULTILINE))
    body = re.search(rf"^{function}\(\) \{{\n.*?^\}}$", SOURCE, re.MULTILINE | re.DOTALL)
    assert body is not None, f"bootstrap.sh no longer defines {function}()"
    result = subprocess.run(
        ["bash", "-c", f"{constants}\n{body.group(0)}\n{function}"],
        capture_output=True,
        text=True,
        check=True,
    )
    return Decimal(result.stdout)


def _model_aws(units: set[str], *, include: bool) -> Decimal:
    lines = estimate_carrying_cost(_DAY, shape=InstallationShape()).lines
    return sum(
        (
            line.usd
            for line in lines
            if line.cloud is Cloud.AWS and line.usd and (line.rate.unit in units) == include
        ),
        Decimal(0),
    )


@pytest.mark.parametrize(
    ("name", "model"),
    [
        ("RATE_RDS_T4G_MEDIUM_HOUR", rds_instance_hour_usd("db.t4g.medium")),
        ("RATE_EC2_C7I_2XLARGE_HOUR", RateCard().ec2_c7i_2xlarge_hour.usd),
        ("RATE_PUBLIC_IPV4_HOUR", RateCard().public_ipv4_hour.usd),
        ("RATE_RDS_GP3_GB_MONTH", RateCard().rds_gp3_gb_month.usd),
        ("RATE_EBS_GP3_GB_MONTH", RateCard().ebs_gp3_gb_month.usd),
        ("RATE_SECRET_MONTH", RateCard().secret_month.usd),
        ("RATE_AURORA_ACU_HOUR", RateCard().aurora_acu_hour.usd),
        ("RATE_DMS_T3_SMALL_HOUR", RateCard().dms_t3_small_hour.usd),
    ],
)
def test_each_rate_is_the_cost_models(name: str, model: Decimal) -> None:
    assert _literal(name) == model


@pytest.mark.parametrize(
    ("name", "field"),
    [
        ("COUNT_AURORA_CLUSTERS", "aurora_clusters"),
        ("COUNT_RDS_INSTANCES", "rds_instances"),
        ("COUNT_AURORA_ALWAYS_AWAKE", "aurora_replicating_clusters"),
        ("COUNT_RUNNERS", "runner_instances"),
        ("COUNT_DMS_INSTANCES", "dms_replication_instances"),
    ],
)
def test_each_count_is_the_shapes(name: str, field: str) -> None:
    assert _literal(name) == getattr(InstallationShape(), field)


def test_the_always_on_line_is_the_models_hourly_arithmetic() -> None:
    # Every hourly-billed AWS line: databases, runners, addresses, the awake cluster, DMS.
    assert _preview("fixed_daily") == _model_aws(_HOURLY_UNITS, include=True).quantize(
        Decimal("0.01")
    )


def test_storage_and_secrets_come_within_a_few_cents_of_the_model() -> None:
    # The preview divides a month by 30 rather than 730 hours, and leaves Aurora's
    # consumed storage to the usage-shaped list, so this one is close, not exact.
    monthly = _model_aws(_HOURLY_UNITS, include=False)
    assert abs(_preview("metered_daily") - monthly) < Decimal("0.05")


def test_the_summary_lists_what_the_always_on_line_charges_for() -> None:
    summary = SOURCE[SOURCE.index("  AWS, always on") : SOURCE.index("/day fixed")]
    assert "public IPv4 address, one per runner" in summary
    assert "public IPv4 address, one per database writer" in summary
    assert "DMS replication instance" in summary
