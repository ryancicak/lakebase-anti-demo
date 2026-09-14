"""The server's fan-in request must satisfy the runner's own decoder.

These two sides are the seam Round 5 has never had. The runner has always been able to
decode and execute a fan-in bout; the server has never built the request, so the adapter
sent a v1 schedule while the finaliser expected a v2 result. Testing the builder against
`_decode_fanin_request` itself, rather than against a copy of its rules, is what stops
the two drifting apart again.
"""

from __future__ import annotations

import base64
import gzip
import json
import sys
from pathlib import Path

import pytest

RUNNER_DIRECTORY = str(Path(__file__).resolve().parent.parent / "runner")
if RUNNER_DIRECTORY not in sys.path:
    sys.path.insert(0, RUNNER_DIRECTORY)

import connection_spike_runner as runner  # noqa: E402
import round5_fanin as fanin  # noqa: E402

from server.connection_fanin import (  # noqa: E402
    FanInError,
    fanin_preflight_request,
    fanin_run_request,
)

SHA = "a" * 64

#: A target names the lane, the secret that holds its credential, the endpoint it
#: connects to and the host that credential was minted for. The runner compares this key
#: set for equality, so host/port/dbname belong to the credential rather than the target.
LAKEBASE_TARGET = {
    "lane_id": "lakebase",
    "secret_arn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:lakebase-fixture",
    "endpoint_host": "ep-example-fixture.database.us-west-2.cloud.databricks.com",
    "credential_host": "ep-example-fixture.database.us-west-2.cloud.databricks.com",
}
AURORA_TARGET = {
    "lane_id": "competitor",
    "secret_arn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:aurora-fixture",
    "endpoint_host": "aurora-fixture.cluster-example.us-west-2.rds.amazonaws.com",
    "credential_host": "aurora-fixture.cluster-example.us-west-2.rds.amazonaws.com",
}
RDS_TARGET = {**AURORA_TARGET, "endpoint_host": "rds-fixture.example.us-west-2.rds.amazonaws.com",
              "credential_host": "rds-fixture.example.us-west-2.rds.amazonaws.com"}


def encoded(request: dict[str, object]) -> str:
    """The wire form the runner is handed on argv: gzipped canonical JSON, base64."""
    return base64.b64encode(gzip.compress(json.dumps(request).encode("utf-8"))).decode("ascii")


def digests() -> dict[str, str]:
    """The runner's own digests. Anything else is refused as a mismatch."""
    return {
        "contract_sha256": fanin.contract_sha256(),
        "config_sha256": fanin.config_sha256(),
        "generator_sha256": fanin.generator_sha256(),
        "capacity_model_sha256": fanin.capacity_model_sha256(),
    }


def test_the_preflight_request_decodes_on_the_runner() -> None:
    request = fanin_preflight_request(
        run_id="ad-19700101-0000-f1x7",
        runner_instance_type=fanin.RUNNER_INSTANCE_TYPE,
        **digests(),
    )
    run_id, targets, trust, decoded = runner._decode_fanin_request(encoded(request))

    assert run_id == "ad-19700101-0000-f1x7"
    # A preflight names no lane and needs no trust bundle: it asks whether this shape
    # can hold 10,000 clients at all, before any endpoint exists to connect to.
    assert targets == ()
    assert trust == ""
    assert decoded["action"] == "preflight"


def test_the_preflight_request_carries_exactly_the_declared_keys() -> None:
    """The decoder compares the key set for equality, so an extra field is a refusal.

    That strictness is the point: a request carrying fields this runner does not know
    was built by a different version of the server, and a preflight answered by the
    wrong version is worse than no answer.
    """

    request = fanin_preflight_request(
        run_id="ad-1", runner_instance_type=fanin.RUNNER_INSTANCE_TYPE, **digests()
    )
    with pytest.raises(runner.RunnerContractError, match="fanin_preflight_request_invalid"):
        runner._decode_fanin_request(encoded({**request, "unexpected": 1}))


def test_a_stale_digest_is_refused_rather_than_run() -> None:
    """A generator that still answers is more dangerous than one that fails.

    Its numbers look exactly like a measurement, so the digests are compared before any
    lane is read.
    """

    request = fanin_preflight_request(
        run_id="ad-1", runner_instance_type=fanin.RUNNER_INSTANCE_TYPE, **digests()
    )
    with pytest.raises(runner.RunnerContractError, match="fanin_digest_mismatch"):
        runner._decode_fanin_request(encoded({**request, "generator_sha256": SHA}))


def test_the_run_request_decodes_and_keeps_both_lanes() -> None:
    request = fanin_run_request(
        run_id="ad-1",
        trust_bundle_sha256=SHA,
        lakebase_credential_sha256="1" * 64,
        lakebase_observer_credential_sha256="2" * 64,
        competitor_credential_sha256="3" * 64,
        competitor_observer_credential_sha256="4" * 64,
        competitor_credential_id="aurora",
        targets=[
            LAKEBASE_TARGET,
            AURORA_TARGET,
        ],
        **digests(),
    )
    _run_id, targets, trust, decoded = runner._decode_fanin_request(encoded(request))

    assert decoded["action"] == "run"
    assert trust == SHA
    assert {target.lane_id for target in targets} == set(runner.RUNTIME_LANE_IDS)


def test_the_competitor_lane_carries_a_credential_id_and_lakebase_does_not() -> None:
    """Aurora and RDS are two separately sealed credentials, so the competitor lane has
    to say which one it holds. Lakebase has exactly one, and the runner refuses an id
    there as an over-specified request."""

    request = fanin_run_request(
        run_id="ad-1",
        trust_bundle_sha256=SHA,
        lakebase_credential_sha256="1" * 64,
        lakebase_observer_credential_sha256="2" * 64,
        competitor_credential_sha256="3" * 64,
        competitor_observer_credential_sha256="4" * 64,
        competitor_credential_id="rds",
        targets=[
            LAKEBASE_TARGET,
            RDS_TARGET,
        ],
        **digests(),
    )
    auth = request["baseline_auth"]
    assert set(auth["lakebase"]) == {"credential_sha256", "observer_credential_sha256"}
    assert "credential_id" in auth["competitor"]

    polluted = json.loads(json.dumps(request))
    polluted["baseline_auth"]["lakebase"]["credential_id"] = "lakebase"
    with pytest.raises(runner.RunnerContractError, match="baseline_auth_invalid"):
        runner._decode_fanin_request(encoded(polluted))


@pytest.mark.parametrize(
    ("targets", "why"),
    (
        ([{"lane_id": "lakebase"}], "one lane is not a comparison"),
        (
            [{"lane_id": "lakebase"}, {"lane_id": "lakebase"}],
            "the same lane twice would race a database against itself",
        ),
        (
            [{"lane_id": "lakebase"}, {"lane_id": "competitor"}, {"lane_id": "extra"}],
            "a third lane has no sealed credential",
        ),
    ),
)
def test_a_request_that_is_not_exactly_two_named_lanes_is_refused(targets, why) -> None:
    with pytest.raises(FanInError, match="targets_invalid"):
        fanin_run_request(
            run_id="ad-1",
            trust_bundle_sha256=SHA,
            lakebase_credential_sha256="1" * 64,
            lakebase_observer_credential_sha256="2" * 64,
            competitor_credential_sha256="3" * 64,
            competitor_observer_credential_sha256="4" * 64,
            competitor_credential_id="aurora",
            targets=targets,
            **digests(),
        ), why
