"""The server and the runner must compute the same four fan-in digests.

The runner refuses a request whose digests differ from the files it is about to execute,
and it is right to: a generator that still answers while differing from the armed
contract produces numbers that look exactly like a measurement. That refusal is the last
line, not the first. The server computes each digest from its own copy of the contract,
and if the two copies disagree, every bout fails at the decoder with
`fanin_digest_mismatch` -- a token that names the symptom and not which of the four
surfaces drifted.

These are one-line assertions on purpose. They are the cheapest possible guard on the
thing most likely to break: someone changing a constant on one side of the boundary.
"""

from __future__ import annotations

import pytest

from runner import round5_fanin as runner_fanin
from server.connection_fanin import (
    ConnectionSpikeContract,
    capacity_model_sha256,
    fanin_config_sha256,
    fanin_generator_sha256,
)


def test_the_contract_digest_matches_the_runner() -> None:
    assert ConnectionSpikeContract().sha256 == runner_fanin.contract_sha256()


def test_the_config_digest_matches_the_runner() -> None:
    assert fanin_config_sha256() == runner_fanin.config_sha256()


def test_the_generator_digest_matches_the_runner() -> None:
    """The generator digest is the runner file's own bytes.

    So this is not a restatement of a shared constant, it is the server proving it can
    name the exact file the runner will execute. It breaks the moment the two copies of
    the repository diverge, which is precisely when a bout must not be armed.
    """

    assert fanin_generator_sha256() == runner_fanin.generator_sha256()


def test_the_capacity_model_digest_matches_the_runner() -> None:
    assert capacity_model_sha256() == runner_fanin.capacity_model_sha256()


def test_a_generator_digest_is_read_from_the_named_file() -> None:
    """A path is accepted so the digest is testable rather than only correct by default."""

    with pytest.raises(OSError):
        fanin_generator_sha256("/nonexistent/round5_fanin.py")


def test_the_runner_instance_shape_is_named_once() -> None:
    """Three surfaces used to restate it, and Terraform provisioned a fourth value.

    The result was a runner topology preflight that refused every dispatch while
    reporting a sealed-contract mismatch, which reads as a tampered installation rather
    than as two constants that disagreed.
    """

    from server.connection_fanin import RUNNER_INSTANCE_TYPE
    from server.connection_spike_live import ConnectionSpikeLiveConfig
    from server.manifest import Round5FrozenConstants

    assert RUNNER_INSTANCE_TYPE == runner_fanin.RUNNER_INSTANCE_TYPE
    assert ConnectionSpikeLiveConfig.runner_instance_type == RUNNER_INSTANCE_TYPE
    assert Round5FrozenConstants().runner_instance_type == RUNNER_INSTANCE_TYPE


def test_the_fanin_ssm_window_outlasts_the_runners_own_budget() -> None:
    """SSM must not end a bout the runner is still measuring.

    If it does, the failure arrives as "the command did not complete" and says nothing
    about the 10,000 clients that were up at the time.
    """

    from server.connection_spike_live import FANIN_SSM_TIMEOUT_SECONDS

    assert FANIN_SSM_TIMEOUT_SECONDS > runner_fanin.RUN_TIMEOUT_SECONDS
