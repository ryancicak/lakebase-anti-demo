"""The mutation-wait refusal never carries a driver's host or role.

`_MutationWaitGate.reason` is what every control action raises as its 409
detail while startup waits out somebody else's mutation, so it reaches the
screen an operator is presenting from. When the mutation clears and startup
then fails, a psycopg message names the coordination host and the role it
logged in as; the refusal names the failure's type and SQLSTATE instead.
"""

from __future__ import annotations

import psycopg
import pytest

import app as app_module
from server.manager import InvalidStateError

_HOST = "instance-0f00.database.cloud.databricks.com"
_ROLE = "c0ffee00-role"


def _driver_failure() -> psycopg.OperationalError:
    return psycopg.OperationalError(
        f'connection failed: connection to server at "{_HOST}" (203.0.113.7), port 5432 '
        f'failed: FATAL:  password authentication failed for user "{_ROLE}"'
    )


def _waiting_gate(
    monkeypatch: pytest.MonkeyPatch, failures: list[BaseException]
) -> app_module._MutationWaitGate:
    async def failing_open(_app: object, *, deployed: bool) -> None:
        del deployed
        raise failures.pop(0)

    monkeypatch.setattr(app_module, "MUTATION_WAIT_POLL_SECONDS", 0)
    monkeypatch.setattr(app_module, "_open_runtime", failing_open)
    return app_module._MutationWaitGate(
        app_module.ManifestMutationInProgressError(
            "Demo setup is currently SEEDING, not READY",
            status="seeding",
            lock_held=True,
        )
    )


async def test_a_transient_startup_failure_after_the_mutation_names_no_host_or_role(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failures: list[BaseException] = [_driver_failure(), RuntimeError("stop")]
    gate = _waiting_gate(monkeypatch, failures)
    reasons: list[str] = []
    real_status = app_module.ReadinessStatus

    def recording_status(*args: object, **kwargs: object) -> object:
        reasons.append(gate.reason)
        return real_status(*args, **kwargs)

    monkeypatch.setattr(app_module, "ReadinessStatus", recording_status)
    await app_module._open_runtime_when_manifest_settles(
        object(), deployed=True, gate=gate, opened=[]
    )

    assert not failures
    transient = reasons[0]
    assert transient.startswith("Runtime startup is retrying a transient failure: ")
    assert "OperationalError" in transient
    assert _HOST not in transient
    assert _ROLE not in transient


async def test_a_failed_startup_after_the_mutation_refuses_without_the_driver_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Not transient, so the loop reports it once and stops.
    refusal = psycopg.ProgrammingError(
        f'permission denied for schema anti_demo on "{_HOST}" for role "{_ROLE}"'
    )
    try:
        raise InvalidStateError("startup could not open its stores") from refusal
    except InvalidStateError as wrapped:
        failure = wrapped
    gate = _waiting_gate(monkeypatch, [failure])

    await app_module._open_runtime_when_manifest_settles(
        object(), deployed=True, gate=gate, opened=[]
    )

    assert gate.recovery.state == "given_up"
    with pytest.raises(InvalidStateError) as refused:
        gate.require_ready()
    detail = str(refused.value)
    assert detail.startswith("Runtime startup failed after the mutation finished: ")
    # Our own sentence is quoted; the driver link under it contributes its type alone.
    assert "startup could not open its stores" in detail
    assert "ProgrammingError" in detail
    assert _HOST not in detail
    assert _ROLE not in detail
