"""Round 5's proxy target registration waits out AWS's own state refusals.

2026-10-02: rc10's Round 5 Aurora source read `backing-up` for about a minute and a half
after its automated snapshot, and the warm keeper made the ring unclaimable for 25 seconds.
The warm check now treats `backing-up` as serving, so a bout can start inside a source's daily
backup. Whether AWS registers a proxy target then is AWS's to answer; if it refuses with a
state fault, the bout waits it out on the AWS lane's own clock, a bounded number of times.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest
from botocore.exceptions import ClientError

from server.connection_spike_live import (
    PROXY_TARGET_STATE_ATTEMPTS,
    PROXY_TARGET_STATE_RETRY_SECONDS,
    LiveConnectionSpikeSetupOrchestrator,
)

REGISTRATION = {"DBProxyName": "rds-proxy", "DBInstanceIdentifiers": ["rds-source"]}


def _registering(register):
    """`_register_proxy_target` on a stand-in holding only what it uses."""

    slept: list[float] = []

    async def call(operation, **kwargs):
        return operation(**kwargs)

    async def sleep(seconds: float) -> None:
        slept.append(seconds)

    orchestrator = SimpleNamespace(
        _call=call,
        _error_code=LiveConnectionSpikeSetupOrchestrator._error_code,
        _sleep=sleep,
        config=SimpleNamespace(
            proxy_registration={"DBInstanceIdentifiers": ["rds-source"]},
            competitor_resource_id="db-RESOURCE",
        ),
        _observation=lambda spec, provider_id: (spec, provider_id),
    )

    async def run():
        return await LiveConnectionSpikeSetupOrchestrator._register_proxy_target(
            cast(Any, orchestrator),
            cast(Any, SimpleNamespace(rds=SimpleNamespace(register_db_proxy_targets=register))),
            cast(Any, SimpleNamespace(names=SimpleNamespace(proxy_name="rds-proxy"))),
            cast(Any, "proxy-target-spec"),
        )

    return run, slept


def _fault(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "not now"}}, "RegisterDBProxyTargets")


@pytest.mark.parametrize("code", ["InvalidDBInstanceStateFault", "InvalidDBClusterStateFault"])
async def test_registration_waits_out_a_source_state_refusal(code: str) -> None:
    calls: list[dict[str, object]] = []

    def register(**kwargs):
        calls.append(kwargs)
        if len(calls) < 3:
            raise _fault(code)
        return {}

    run, slept = _registering(register)

    assert await run() == ("proxy-target-spec", "db-RESOURCE")
    assert calls == [REGISTRATION] * 3
    assert slept == [PROXY_TARGET_STATE_RETRY_SECONDS] * 2


async def test_registration_raises_any_other_fault_at_once() -> None:
    calls: list[dict[str, object]] = []

    def register(**kwargs):
        calls.append(kwargs)
        raise _fault("DBProxyTargetAlreadyRegisteredFault")

    run, slept = _registering(register)

    with pytest.raises(ClientError, match="DBProxyTargetAlreadyRegisteredFault"):
        await run()
    assert len(calls) == 1
    assert slept == []


async def test_registration_gives_up_after_its_bound() -> None:
    calls: list[dict[str, object]] = []

    def register(**kwargs):
        calls.append(kwargs)
        raise _fault("InvalidDBInstanceStateFault")

    run, slept = _registering(register)

    with pytest.raises(ClientError, match="InvalidDBInstanceStateFault"):
        await run()
    assert len(calls) == PROXY_TARGET_STATE_ATTEMPTS
    assert len(slept) == PROXY_TARGET_STATE_ATTEMPTS - 1
    # About three minutes, the longest backup rc10 saw.
    assert 170 <= sum(slept) <= 200


async def test_a_registration_that_lands_first_time_does_not_wait() -> None:
    calls: list[dict[str, object]] = []

    def register(**kwargs):
        calls.append(kwargs)
        return {}

    run, slept = _registering(register)

    assert await run() == ("proxy-target-spec", "db-RESOURCE")
    assert calls == [REGISTRATION]
    assert slept == []
