"""Preparation's findings must reach the bout, and a missing binding must refuse the arm.

Both come from one live failure. A bout died seconds after the bell with
`AuthorizeSecurityGroupEgress ... Source group ID missing`, because moving the untimed preflight
into `prepare` gave it a `_SetupResources` that `prepare` then discarded. `_preflight_baseline`
does not only verify: it writes what it discovers onto that object -- the database's security
group, the sealed secret and proxy role, anything an interrupted bout left journalled -- and
`setup` then built a fresh empty one and skipped the preflight because preparation was recorded.

Ryan's second point is the one that matters more: a rule that would be authorized with no source
group should stop the round before the bell, not after it. Nothing had been created and no clock
needed to start for that to be knowable.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from server.connection_spike_live import (
    ConnectionSpikeLiveConfigurationError,
    LiveConnectionSpikeSetupOrchestrator,
    _SetupResources,
)


def orchestrator(*, runner_security_group_id: str) -> LiveConnectionSpikeSetupOrchestrator:
    """The one field of the config this check reads, on an otherwise inert instance.

    Built without `__init__` on purpose: the binding check is pure, and standing up a real
    orchestrator would drag in AWS clients, a journal and a coordinator to test one refusal.
    """

    instance = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    instance.config = SimpleNamespace(runner_security_group_id=runner_security_group_id)
    return instance


def resources(*, rds_security_group_id: str) -> _SetupResources:
    return _SetupResources(
        SimpleNamespace(),  # type: ignore[arg-type]
        rds_security_group_id=rds_security_group_id,
    )


def test_complete_bindings_arm_without_complaint() -> None:
    orchestrator(runner_security_group_id="sg-runner")._require_rule_bindings(
        resources(rds_security_group_id="sg-database")
    )


def test_a_missing_database_group_refuses_before_anything_is_created() -> None:
    """The exact live failure. This is what "Source group ID missing" was."""

    with pytest.raises(ConnectionSpikeLiveConfigurationError) as caught:
        orchestrator(runner_security_group_id="sg-runner")._require_rule_bindings(
            resources(rds_security_group_id="")
        )
    message = str(caught.value)
    assert "cannot arm" in message
    # Named by binding, not by AWS operation: the provider's own wording sent an operator to look
    # at EC2 for a fault that was in this process.
    assert "competitor database's security group" in message
    assert "Nothing was created and no clock was started" in message


def test_a_missing_runner_group_refuses_too() -> None:
    with pytest.raises(ConnectionSpikeLiveConfigurationError, match="sealed runner security group"):
        orchestrator(runner_security_group_id="")._require_rule_bindings(
            resources(rds_security_group_id="sg-database")
        )


def test_both_missing_are_reported_together() -> None:
    """Both at once, so one repair does not just uncover the next."""

    with pytest.raises(ConnectionSpikeLiveConfigurationError) as caught:
        orchestrator(runner_security_group_id="")._require_rule_bindings(
            resources(rds_security_group_id="")
        )
    message = str(caught.value)
    assert "competitor database's security group" in message
    assert "sealed runner security group" in message


@pytest.mark.parametrize("blank", ["", "   ", "\t"])
def test_whitespace_is_not_a_binding(blank: str) -> None:
    """A group id of spaces reaches AWS as absent, so it is absent here."""

    with pytest.raises(ConnectionSpikeLiveConfigurationError):
        orchestrator(runner_security_group_id="sg-runner")._require_rule_bindings(
            resources(rds_security_group_id=blank)
        )
