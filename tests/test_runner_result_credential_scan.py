"""The result guard must pass an auth-method label and still refuse credential material.

A completed 10,000-client bout was discarded by this guard. The payload was clean; the
flattened-JSON substring check it used to run matched the word "password" inside
`"auth_method": "tls-cleartext-password"`, which is the name of one of the protocol's two
supported authentication methods and appears in every successful bout. The bout had already
been paid for: setup, ramp, hold and sampling all completed first.

So these tests fix both halves in place. A label is readable, a digest is readable, and
credential material is refused whether it arrives as an assignment inside a string or as a
bare value under a credential-named key. The second half is why this is not simply a looser
check: the substring version did catch `{"session_token": "..."}`, and replacing it with a
values-only scan would have traded a false positive for a real hole.
"""

from __future__ import annotations

import pytest

from server.connection_fanin import SUPPORTED_AUTH_METHODS
from server.connection_spike_live import _runner_result_has_forbidden_credential as scan


@pytest.mark.parametrize("method", sorted(SUPPORTED_AUTH_METHODS))
def test_every_supported_auth_method_is_readable(method: str) -> None:
    """Parametrised over the contract's own set, so adding a method cannot reintroduce this.

    `tls-cleartext-password` is the one that broke it, and the point is that no member of
    this set may be mistaken for a secret.
    """

    assert not scan({"lanes": [{"lane_id": "lakebase", "auth_method": method}]})


def test_credential_digests_are_evidence_and_stay_readable() -> None:
    """Digests are what prove the bout used the credential the seal named."""

    assert not scan(
        {
            "credential_sha256": "a" * 64,
            "observer_credential_sha256": "b" * 64,
            "trust_bundle_sha256": "c" * 64,
        }
    )


@pytest.mark.parametrize(
    "payload",
    (
        {"note": "password=hunter2"},
        {"connection": "host=db user=x password=y"},
        {"env": "PGPASSWORD=y"},
        {"blob": "secret: y"},
    ),
)
def test_an_assignment_inside_a_string_is_refused(payload: dict[str, object]) -> None:
    assert scan(payload)


@pytest.mark.parametrize(
    "payload",
    (
        {"password": "x"},
        {"session_token": "AQoDX"},
        {"access_key_id": "AKIA"},
        {"secretstring": "{}"},
        {"lanes": [{"diagnostics": {"session_token": "AQoDX"}}]},
    ),
)
def test_a_bare_value_under_a_credential_key_is_refused(payload: dict[str, object]) -> None:
    """The half a values-only scan would have lost.

    None of these contains an `=`, so the assignment pattern sees nothing. The key name is
    the whole signal, and the nested case is why the walk has to reach it.
    """

    assert scan(payload)


def test_a_secret_arn_is_refused_which_is_the_safe_direction() -> None:
    """Documented rather than fixed, because no result field carries an ARN.

    A Secrets Manager ARN contains the literal `:secret:`, which the assignment pattern
    reads as `secret:` followed by a value. `ConnectionSpikeLaneResult` names no ARN and
    neither does the capacity preflight, so this cannot arise from the payloads that exist.
    It is asserted so the behaviour is known: if a diagnostic ever adds an ARN, the bout is
    refused rather than published, and refusing is the direction to fail in.
    """

    assert scan({"master_secret_arn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:x"})
