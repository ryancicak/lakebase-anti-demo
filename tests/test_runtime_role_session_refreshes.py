"""The runtime-role session renews its credential, so a lane can outlive one.

rc23's Round 4 towel cleanup, retried 73 minutes after its bout, failed its Glue calls with
ExpiredTokenException (2026-10-05). The lane kept the session it was built with, and that
session held one assumed credential, good for an hour.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import boto3
import pytest

from server import targets

_ROLE = "arn:aws:iam::111122223333:role/anti-demo-runtime-test"


class _Sts:
    """STS, handing out a new credential on every call, each two hours longer-lived."""

    def __init__(self) -> None:
        self.calls = 0

    def assume_role(self, **arguments):
        self.calls += 1
        assert arguments["RoleArn"] == _ROLE
        return {
            "Credentials": {
                "AccessKeyId": f"test-key-{self.calls}",
                "SecretAccessKey": "test-secret",
                "SessionToken": f"test-token-{self.calls}",
                "Expiration": datetime.now(UTC) + timedelta(hours=2 * self.calls - 1),
            }
        }


class _Source:
    def __init__(self, sts: _Sts) -> None:
        self._sts = sts

    def client(self, service: str, **_options):
        assert service == "sts"
        return self._sts


@pytest.fixture
def sts(monkeypatch) -> _Sts:
    sts = _Sts()
    real = boto3.Session

    def session(*args, **options):
        # The session the app's calls are made with is real; only its source is faked.
        if "botocore_session" in options or "aws_access_key_id" in options:
            return real(*args, **options)
        return _Source(sts)

    monkeypatch.setenv("ANTI_DEMO_RUNTIME_ROLE_ARN", _ROLE)
    monkeypatch.setattr(targets, "session_arguments", lambda *_args: {})
    monkeypatch.setattr(targets.boto3, "Session", session)
    return sts


def test_a_runtime_session_renews_its_credential_before_it_expires(sts) -> None:
    runtime = targets._runtime_aws_session("environment", None, "us-west-2")
    credentials = runtime.get_credentials()
    assert credentials.get_frozen_credentials().access_key == "test-key-1"

    # An hour and a quarter on, as the stuck towel cleanup's retry was.
    later = datetime.now(UTC) + timedelta(minutes=75)
    credentials._time_fetcher = lambda: later

    assert credentials.get_frozen_credentials().access_key == "test-key-2"
    assert sts.calls == 2


def test_a_credential_still_good_is_not_asked_for_again(sts) -> None:
    runtime = targets._runtime_aws_session("environment", None, "us-west-2")

    for _ in range(3):
        assert runtime.get_credentials().get_frozen_credentials().access_key == "test-key-1"
    assert sts.calls == 1
    assert runtime.region_name == "us-west-2"
