"""The observer must be able to open the connection its evidence depends on.

This is the fault that killed the first fan-in bout ever dispatched, in a worker process,
at the observer preflight, with both lanes at zero clients. `execute_fanin` puts
`sslrootcert` into the observer descriptor, and the observer is the only path that reaches
`connect_runner_database`, whose allowlist is exactly the six libpq connection fields. The
extra key raised `ValueError: runner database descriptor contains unsupported fields`,
which the runner reported as `RUNNER_ERROR:ValueError`.

Nothing caught it because nothing had ever built a fan-in request, so these tests assert
against the real allowlist and the real descriptor shape rather than a copy of either.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from runner import round5_fanin as fanin
from runner.external_io import connect_runner_database

#: The descriptor `execute_fanin` actually hands a `DirectObserver`: the five validated
#: database fields, the credential digest the request sealed, and the trust bundle path it
#: adds for TLS. Written out rather than imported so a change on either side shows up here.
OBSERVER_DESCRIPTOR = {
    "host": "observer-fixture.example.test",
    "port": 5432,
    "dbname": "databricks_postgres",
    "user": fanin.OBSERVER_ROLE,
    "password": "fixture-only",
    "credential_sha256": "a" * 64,
    "sslrootcert": "/opt/lakebase-anti-demo/round5/round5-ca.pem",
}


def observer() -> fanin.DirectObserver:
    return fanin.DirectObserver("lakebase", dict(OBSERVER_DESCRIPTOR), "anti-demo-fixture")


def test_the_connect_descriptor_carries_only_fields_libpq_accepts() -> None:
    """Compared against the allowlist itself, not a restatement of it."""

    allowed = {"host", "port", "dbname", "user", "username", "password"}
    assert set(observer()._connect_database()) <= allowed


def test_the_credential_digest_and_bundle_path_are_not_connection_fields() -> None:
    descriptor = observer()._connect_database()
    assert "credential_sha256" not in descriptor
    assert "sslrootcert" not in descriptor


def test_a_future_descriptor_key_cannot_reach_the_connection() -> None:
    """The reason this selects fields rather than removing them.

    A removal list has to be updated every time one more non-libpq key is added beside
    the two that are there now, and forgetting is exactly what happened.
    """

    extended = fanin.DirectObserver(
        "lakebase",
        {**OBSERVER_DESCRIPTOR, "some_future_key": "value"},
        "anti-demo-fixture",
    )
    assert "some_future_key" not in extended._connect_database()


def test_the_observer_verifies_against_the_sealed_bundle() -> None:
    """verify-full without a root cert falls back to the system trust store.

    Workable for a public CA, wrong for Amazon RDS, and wrong in principle for the one
    connection whose purpose is to be independent evidence.
    """

    assert fanin.TLS_MODE == "verify-full"
    assert observer()._trust_bundle_path() == Path(OBSERVER_DESCRIPTOR["sslrootcert"])


def test_a_descriptor_without_a_bundle_says_so_rather_than_inventing_one() -> None:
    bare = {k: v for k, v in OBSERVER_DESCRIPTOR.items() if k != "sslrootcert"}
    assert fanin.DirectObserver("lakebase", bare, "fixture")._trust_bundle_path() is None


async def test_the_real_descriptor_passes_the_real_validator() -> None:
    """The end-to-end shape check, short of opening a socket.

    `connect_runner_database` raises `ValueError` for a descriptor it will not accept and
    only then attempts a connection, so reaching a connection error rather than a
    ValueError is the proof that the descriptor itself is now well formed.
    """

    with pytest.raises(Exception) as caught:  # noqa: PT011
        await connect_runner_database(
            observer()._connect_database(),
            application_name="anti-demo-fixture-observer",
            trust_bundle_path=None,
            tls_mode=fanin.TLS_MODE,
            connect_timeout_seconds=1,
        )
    assert not isinstance(caught.value, ValueError), (
        f"the descriptor was rejected before any connection was attempted: {caught.value}"
    )

def test_the_worker_ready_barrier_outlasts_the_observer_preflight() -> None:
    """A barrier shorter than the work it waits for does not bound anything.

    A worker reports ready only after its observer connects and sees the lane quiet, and
    the observer owns both budgets. When the barrier was a bare 60 seconds against those
    two 120-second budgets, a slow observer was reported as a missing worker: a live bout
    died on `fanin_worker_ready_timeout`, which sends an operator looking for a dead
    process instead of a cold database.

    The relation is asserted, not the number, so changing an observer budget cannot
    silently reintroduce this.
    """

    from runner import connection_spike_runner as runner

    assert runner.FANIN_WORKER_READY_BUDGET_SECONDS > (
        fanin.OBSERVER_READY_TIMEOUT_SECONDS + fanin.OBSERVER_QUIESCE_TIMEOUT_SECONDS
    )
    assert (
        runner.FANIN_WORKER_RUN_TIMEOUT_SECONDS
        > runner.FANIN_WORKER_READY_BUDGET_SECONDS
    )
