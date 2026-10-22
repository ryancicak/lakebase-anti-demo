"""Round 5's lane adapter on Azure: AWS's resident path, with Run Command where SSM was.

`LiveConnectionSpikeAdapter` carries one lane's bout to its runner. Everything after the bell rides
the resident transport (the control outbox to the runner's queue; on Azure, Service Bus), and that
part has no AWS in it. The AWS in it is before the bell: the STS "dispatch capsule" whose expiry
gates each dispatch, SSM's ping and EC2's description of the runner, RDS's Proxy describe, and the
capacity preflight sent over SSM. This subclass replaces exactly those (Ryan, 2026-10-20: AWS
untouched) and inherits the rest:

- **The capsule** is the ARM client's window. Azure tokens are minted as needed, so it's an hour,
  renewed at every warm.
- **The runner check** is ARM's instance view: running, with its agent ready (what runs Run
  Command).
- **The capacity preflight** goes over Run Command through the Azure wrapper
  (`server/azure_round5_runner.run_runner_request`), its answer parsed by AWS's own code. It's the
  only request this adapter sends that way. A bout goes resident, and the bounded path (AWS's
  pre-resident dispatch) isn't offered on Azure.

Three AWS names stay on inside AWS's protocol, never on a screen: the competitor's credential is
``rds`` to the harness, its timed setup's journal kind is ``rds_proxy``, and the runner's capacity
class is ``c7i.2xlarge``, the harness's name for the 8 vCPU / 16 GiB its model sizes. Azure's
`Standard_D8ls_v7` (`capacity.AZURE_ROUND5_RUNNER_SIZE`) is that shape.
"""

from __future__ import annotations

import asyncio
import base64
import gzip
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from .azure_arm import ArmClient
from .azure_round5_runner import AZURE_RUNNER_PATH, RunnerInstallError, run_runner_request
from .capacity import AZURE_ROUND5_RUNNER_SIZE
from .connection_spike_live import (
    SSM_TIMEOUT_SECONDS,
    TRUST_BUNDLE_PATH,
    ConnectionSpikeLiveConfigurationError,
    ConnectionSpikeLiveOperationError,
    ConnectionSpikeLiveTransientError,
    ConnectionSpikeTarget,
    LiveConnectionSpikeAdapter,
    _runner_error_code,
)

#: The harness's runner capacity class (round5_fanin.RUNNER_INSTANCE_TYPE): 8 vCPU, 16 GiB.
CAPACITY_CLASS = "c7i.2xlarge"
CAPSULE_LIFETIME_SECONDS = 3600.0


@dataclass(frozen=True)
class AzureConnectionSpikeLiveConfig:
    """One lane's adapter inputs: what the inherited resident path and the engine read
    (`targets`, `resident_installation_id`, `runner_harness_sha256`, `runner_instance_type`,
    `trust_bundle_sha256`, `runner_instance_id`), and the VM and vault Run Command needs."""

    targets: tuple[ConnectionSpikeTarget, ...]
    #: The lane's runner VM, by name: Run Command's target, and how a leak is named.
    runner_instance_id: str
    resident_installation_id: str
    runner_harness_sha256: str
    trust_bundle_sha256: str
    vault_uri: str
    resident_control_queue_url: str = ""
    resident_control_secret_arn: str = ""
    runner_vm_size: str = AZURE_ROUND5_RUNNER_SIZE
    runner_instance_type: str = CAPACITY_CLASS
    runner_path: str = AZURE_RUNNER_PATH
    trust_bundle_path: str = str(TRUST_BUNDLE_PATH)
    command_timeout_seconds: float = SSM_TIMEOUT_SECONDS
    contract_sha256: str = ""

    def __post_init__(self) -> None:
        if len(self.targets) != 2 or len({target.lane_id for target in self.targets}) != 2:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 requires two distinct, sealed target lanes")
        if not self.runner_instance_id.strip() or not self.resident_installation_id.strip():
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5's Azure runner VM and installation identity are required")
        if self.runner_instance_type != CAPACITY_CLASS:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5's runner capacity class is the harness's own")


@dataclass(frozen=True)
class AzureDispatchCapsule:
    """The inherited dispatch checks' "clients": a window, and no AWS client in it."""

    expires_at: datetime
    rds: None = None


class AzureConnectionSpikeAdapter(LiveConnectionSpikeAdapter):
    """AWS's lane adapter, with ARM and Run Command for its pre-bell AWS calls."""

    def __init__(
        self,
        config: AzureConnectionSpikeLiveConfig,
        *,
        arm: ArmClient,
        run_script: Callable[..., Any],
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        **kwargs: Any,
    ) -> None:
        super().__init__(config, session_factory=None, **kwargs)  # type: ignore[arg-type]
        self._arm = arm
        self._run_script = run_script
        self._clock = clock

    async def _assumed_clients(self, run_id: str) -> AzureDispatchCapsule:  # type: ignore[override]
        return AzureDispatchCapsule(
            expires_at=self._clock() + timedelta(seconds=CAPSULE_LIFETIME_SECONDS))

    async def _preflight_runner(self, clients: Any) -> None:
        ready = await asyncio.to_thread(self._arm.vm_ready, self.config.runner_instance_id)
        if not ready:
            # AWS's "temporarily absent from SSM": retried, never a configuration fault.
            raise ConnectionSpikeLiveTransientError(
                f"Round 5's runner VM {self.config.runner_instance_id} is not running with its "
                "agent ready")

    async def _preflight_targets(self, rds: Any, *,
                                 targets: Sequence[ConnectionSpikeTarget] | None = None) -> None:
        """Nothing to describe per lane: the setup orchestrator's warm proves the server."""

    async def execute(self, run_id: str, request: Mapping[str, object], *,
                      targets: Sequence[ConnectionSpikeTarget] | None = None) -> dict[str, object]:
        """The capacity preflight, over Run Command. Bouts go resident (`execute_prepared`)."""

        if request.get("action") != "preflight":
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5's Azure runners take bouts on the resident path only")
        if self._dispatch_lock.locked() or self._settlement_debt:
            raise ConnectionSpikeLiveOperationError(
                "A Round 5 runner command or unsettled prior job is already active in this app "
                "replica")
        async with self._dispatch_lock:
            self._validate_run_id(run_id)
            self._validate_request(run_id, request)
            clients = await self._assumed_clients(run_id)
            await self._preflight_runner(clients)
            encoded = base64.urlsafe_b64encode(gzip.compress(
                json.dumps(request, sort_keys=True, separators=(",", ":")).encode(),
                mtime=0)).decode()
            try:
                status, output = await asyncio.to_thread(
                    run_runner_request, self._run_script, self.config.runner_instance_id,
                    vault_uri=self.config.vault_uri, encoded=encoded)
            except RunnerInstallError as exc:
                raise ConnectionSpikeLiveOperationError(str(exc)) from exc
            if status != 0:
                code = _runner_error_code(output)
                raise ConnectionSpikeLiveOperationError(
                    "Round 5 runner command did not succeed"
                    + (f": the runner refused with {code}" if code else ""))
            self._require_settlement(run_id, output)
            parsed = self._parse_preflight_output(run_id, output)
            self._prepared_clients = clients  # type: ignore[assignment]
            return parsed
