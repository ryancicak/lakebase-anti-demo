"""Operation-specific provider acquisition for the sealed Round 5 runner.

Callers pass only already-decoded runner operation descriptors. Destination
selection is derived here from those immutable descriptors; raw boto3 and
psycopg constructors are intentionally unavailable to runner application
modules. This is an operational cross-wiring control for trusted code, not a
same-process hostile-code sandbox (see ``docs/TARGET_AUTHORITY.md``).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any


def secrets_manager_for_runner_operation(secret_arns: Sequence[str]) -> Any:
    accounts: set[str] = set()
    regions: set[str] = set()
    for arn in secret_arns:
        parts = arn.split(":", 5)
        if (
            len(parts) != 6
            or parts[0] != "arn"
            or parts[2] != "secretsmanager"
            or not parts[3]
            or not parts[4].isdigit()
            or not parts[5].startswith("secret:")
        ):
            raise ValueError("runner secret descriptor is not an exact Secrets Manager ARN")
        regions.add(parts[3])
        accounts.add(parts[4])
    if not secret_arns or len(regions) != 1 or len(accounts) != 1:
        raise ValueError("runner secret descriptors must share one account and region")

    import boto3

    return boto3.Session(region_name=next(iter(regions))).client(
        "secretsmanager",
        region_name=next(iter(regions)),
    )


async def connect_runner_database(
    descriptor: Mapping[str, object],
    *,
    application_name: str,
    trust_bundle_path: Path | None,
    tls_mode: str,
    connect_timeout_seconds: int,
) -> Any:
    allowed = {"host", "port", "dbname", "user", "username", "password"}
    if set(descriptor) - allowed:
        raise ValueError("runner database descriptor contains unsupported fields")
    host = descriptor.get("host")
    port = descriptor.get("port")
    database = descriptor.get("dbname")
    user = descriptor.get("user", descriptor.get("username"))
    password = descriptor.get("password")
    if (
        not isinstance(host, str)
        or not host
        or not isinstance(port, int)
        or not 1 <= port <= 65535
        or not isinstance(database, str)
        or not database
        or not isinstance(user, str)
        or not user
        or not isinstance(password, str)
        or not password
        or not application_name
    ):
        raise ValueError("runner database descriptor is incomplete")

    arguments: dict[str, object] = {
        "host": host,
        "port": port,
        "dbname": database,
        "user": user,
        "password": password,
        "sslmode": tls_mode,
        "connect_timeout": connect_timeout_seconds,
        "prepare_threshold": None,
        "application_name": application_name,
    }
    if trust_bundle_path is not None:
        if not trust_bundle_path.is_absolute():  # noqa: ASYNC240
            raise ValueError("runner trust bundle path must be absolute")
        arguments["sslrootcert"] = str(trust_bundle_path)

    import psycopg

    return await psycopg.AsyncConnection.connect(**arguments)


__all__ = ["connect_runner_database", "secrets_manager_for_runner_operation"]
