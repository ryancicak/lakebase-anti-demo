"""server/azure_round5_adapter.py: AWS's lane adapter with ARM and Run Command for its AWS calls.

The resident path (stage, release, result, cancel) is inherited and cloud-free. These pin the parts
Azure replaces: the dispatch capsule, the runner check, and the capacity preflight over Run Command,
whose answer AWS's own parser reads.
"""

from __future__ import annotations

import base64
import gzip
import json
import re
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from server.azure_arm import RunCommandOutput
from server.azure_round5_adapter import (
    CAPACITY_CLASS,
    AzureConnectionSpikeAdapter,
    AzureConnectionSpikeLiveConfig,
)
from server.azure_round5_runner import AZURE_RUNNER_PATH
from server.connection_spike_live import (
    FANIN_PROTOCOL,
    FANIN_SCHEMA_VERSION,
    SAFETY_EVIDENCE_VERSION,
    WORKER_COUNT,
    ConnectionSpikeLiveConfigurationError,
    ConnectionSpikeLiveOperationError,
    ConnectionSpikeLiveTransientError,
    ConnectionSpikeTarget,
    FanInContract,
    fanin_capacity_model_sha256,
    fanin_config_sha256,
    fanin_generator_sha256,
    fanin_preflight_request,
    runner_asset_sha256s,
    runner_harness_sha256,
)

VAULT = "https://kv-test-r5.vault.azure.net/"
NOW = datetime(2026, 10, 20, 23, 0, tzinfo=UTC)
RUN = "preflight-azure-1"


def targets() -> tuple[ConnectionSpikeTarget, ...]:
    host = "psql-test-r5.postgres.database.azure.com"
    return (
        ConnectionSpikeTarget(lane_id="lakebase", secret_arn="", endpoint_host="lakebase.test",
                              credential_host="lakebase.test", credential_sha256="d" * 64,
                              observer_credential_sha256="e" * 64),
        ConnectionSpikeTarget(
            lane_id="competitor", endpoint_host=host, credential_host=host,
            secret_arn="arn:aws:secretsmanager:westus2:000000000000:secret:round5-rds",
            competitor_id="azure_postgres", competitor_target_id="psql-test-r5",
            competitor_resource_id="/subscriptions/x/psql-test-r5",
            credential_sha256="d" * 64, observer_credential_sha256="e" * 64),
    )


def config(**overrides: Any) -> AzureConnectionSpikeLiveConfig:
    values: dict[str, Any] = dict(targets=targets(), runner_instance_id="vm-test-r5-runner-1",
                                  resident_installation_id="ad-test", vault_uri=VAULT,
                                  runner_harness_sha256=runner_harness_sha256(),
                                  trust_bundle_sha256="f" * 64)
    return AzureConnectionSpikeLiveConfig(**{**values, **overrides})


class Arm:
    def __init__(self, ready: bool | None = True) -> None:
        self.ready = ready

    def vm_ready(self, name: str) -> bool | None:
        assert name == "vm-test-r5-runner-1"
        return self.ready


def preflight_output(run_id: str, **changes: Any) -> str:
    payload = {
        "protocol": FANIN_PROTOCOL, "schema_version": FANIN_SCHEMA_VERSION, "action": "preflight",
        "safety_evidence_version": SAFETY_EVIDENCE_VERSION,
        "contract_sha256": FanInContract().sha256, "config_sha256": fanin_config_sha256(),
        "capacity_model_sha256": fanin_capacity_model_sha256(),
        "generator_sha256": fanin_generator_sha256(),
        "runner_harness_sha256": runner_harness_sha256(),
        "runner_asset_sha256s": runner_asset_sha256s(),
        "shard_process_preflight": {"worker_count": WORKER_COUNT,
                                    "unique_processes": WORKER_COUNT,
                                    "unique_cpus": WORKER_COUNT, "sufficient": True},
        "telemetry_advisories": [], **changes}
    return (f"PREFLIGHT_RESULT:{json.dumps(payload)}\nCLEANUP_CONFIRMED:{run_id}\n"
            f"RUNNER_FLOCK_RELEASED:{run_id}\n")


class RunCommand:
    """Run Command answering one runner request with the harness's stdout, as the VM's script."""

    def __init__(self, harness_stdout: str, status: int = 0) -> None:
        self.stdout, self.status = harness_stdout, status
        self.requests: list[dict[str, Any]] = []
        self.scripts: list[list[str]] = []

    def __call__(self, vm: str, script: list[str], *, bound_seconds: float) -> RunCommandOutput:
        self.scripts.append(script)
        (line,) = [entry for entry in script if AZURE_RUNNER_PATH in entry]
        encoded = re.search(re.escape(AZURE_RUNNER_PATH) + r" '([^']+)'", line).group(1)
        self.requests.append(json.loads(gzip.decompress(base64.urlsafe_b64decode(encoded))))
        blob = base64.b64encode(gzip.compress(self.stdout.encode())).decode()
        return RunCommandOutput(stdout=f"RUNNER_STATUS={self.status}\n"
                                       f"RUNNER_STDOUT_GZIP_BASE64={blob}\nANTI_DEMO_EXIT=0\n",
                                stderr="")


def adapter(run: RunCommand | None = None, arm: Arm | None = None) -> AzureConnectionSpikeAdapter:
    return AzureConnectionSpikeAdapter(config(), arm=arm or Arm(),  # type: ignore[arg-type]
                                       run_script=run or RunCommand(""), clock=lambda: NOW)


def request() -> dict[str, Any]:
    return fanin_preflight_request(
        run_id=RUN, runner_instance_type=CAPACITY_CLASS,
        contract_sha256=FanInContract().sha256, config_sha256=fanin_config_sha256(),
        generator_sha256=fanin_generator_sha256(),
        capacity_model_sha256=fanin_capacity_model_sha256(),
        runner_harness_sha256=runner_harness_sha256())


async def test_the_check_holds_an_hours_capsule_once_the_runner_vm_is_ready() -> None:
    lane = adapter()
    await lane.check()
    assert lane.prepared_expires_at == NOW + timedelta(hours=1)
    # AWS's "temporarily absent from SSM": retried, never a configuration fault.
    with pytest.raises(ConnectionSpikeLiveTransientError, match="not running with its agent"):
        await adapter(arm=Arm(ready=False)).check()


async def test_the_capacity_preflight_goes_over_run_command_and_awss_parser_reads_it() -> None:
    run = RunCommand(preflight_output(RUN))
    lane = adapter(run)
    parsed = await lane.execute(RUN, request())
    assert parsed["runner_harness_sha256"] == runner_harness_sha256()
    (sent,) = run.requests
    assert sent == request() and sent["runner_instance_type"] == CAPACITY_CLASS
    assert any(f"ANTI_DEMO_KEY_VAULT_URI='{VAULT}'" in line for line in run.scripts[0])
    assert lane.prepared_expires_at == NOW + timedelta(hours=1)


async def test_a_refused_or_unsettled_preflight_is_an_error_naming_the_refusal() -> None:
    refused = RunCommand("RUNNER_ERROR:runner_busy\n", status=69)
    with pytest.raises(ConnectionSpikeLiveOperationError, match="refused with runner_busy"):
        await adapter(refused).execute(RUN, request())
    unsettled = RunCommand(preflight_output(RUN).replace(f"RUNNER_FLOCK_RELEASED:{RUN}\n", ""))
    with pytest.raises(Exception, match="flock-release evidence"):
        await adapter(unsettled).execute(RUN, request())
    other = RunCommand(preflight_output(RUN, runner_harness_sha256="0" * 64))
    with pytest.raises(ConnectionSpikeLiveOperationError, match="provenance"):
        await adapter(other).execute(RUN, request())


async def test_only_the_preflight_goes_over_run_command() -> None:
    bout = {**request(), "action": "run_lane_v3"}
    with pytest.raises(ConnectionSpikeLiveConfigurationError, match="resident path only"):
        await adapter().execute(RUN, bout)


def test_the_config_keeps_the_harnesss_capacity_class_and_two_lanes() -> None:
    with pytest.raises(ConnectionSpikeLiveConfigurationError, match="capacity class"):
        config(runner_instance_type="Standard_D8ls_v7")
    with pytest.raises(ConnectionSpikeLiveConfigurationError, match="two distinct"):
        config(targets=targets()[:1])
