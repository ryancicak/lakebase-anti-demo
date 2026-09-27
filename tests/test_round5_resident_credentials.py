"""A running Round 5 resident must never be left holding a password its role lost.

The resident agent reads its control DSN once, at startup. `ensure_coordination`
used to mint a new password for its role on every call, and it is called after
the agent restarts (every Round 5 preparation) and without any restart at all
(`reset`). On 2026-09-27 a resumed install came up with both residents failing
every control message with `RESIDENT_CONTROL_TRANSIENT:OperationalError`, and
Round 5 warmed until it gave up. The published password is now re-applied.
"""

from __future__ import annotations

import inspect

from botocore.exceptions import ClientError

from server import lifecycle

ROLE = "anti_demo_r5_lakebase_0123456789abcdef"
PASSWORD = "p@ss/w:rd?with&symbols=+%"


def _dsn(role: str = ROLE, password: str = PASSWORD) -> str:
    return lifecycle._round5_resident_dsn(
        host="instance.database.cloud.databricks.com",
        database="databricks_postgres",
        role=role,
        password=password,
        trust_bundle_path="/opt/lakebase-anti-demo/round5/trust-bundle.pem",
    )


class Secrets:
    def __init__(self, value: str | None = None, error: str | None = None) -> None:
        self.value = value
        self.error = error

    def get_secret_value(self, *, SecretId: str) -> dict:
        del SecretId
        if self.error:
            raise ClientError({"Error": {"Code": self.error, "Message": self.error}}, "Get")
        return {"SecretString": self.value}


def test_the_published_password_is_kept_for_the_same_role() -> None:
    assert lifecycle._published_resident_password(Secrets(_dsn()), "arn", ROLE) == PASSWORD


def test_anything_but_a_dsn_for_this_role_gets_a_new_password() -> None:
    other = "anti_demo_r5_competitor_0123456789abcdef"
    for secrets in (
        Secrets(_dsn(role=other)),
        Secrets("not a dsn"),
        Secrets(""),
        Secrets(_dsn().replace(f":{lifecycle.quote(PASSWORD, safe='')}@", "@")),
        Secrets(error="ResourceNotFoundException"),
    ):
        assert lifecycle._published_resident_password(secrets, "arn", ROLE) is None


def test_ensure_coordination_consults_the_published_password_before_minting_one() -> None:
    source = inspect.getsource(lifecycle.ensure_coordination)
    consulted = source.index("_published_resident_password")
    minted = source.index("secrets.token_urlsafe(48)")
    applied = source.index("await store._run(rotate_resident_login)")
    assert consulted < minted < applied
    assert "published or secrets.token_urlsafe(48)" in source
