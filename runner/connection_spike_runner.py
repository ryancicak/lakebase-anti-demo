#!/usr/bin/env python3.12
from __future__ import annotations

import asyncio
import base64
import fcntl
import gzip
import hashlib
import io
import json
import os
import re
import secrets
import shutil
import signal
import stat
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

import boto3
import psycopg
from psycopg import sql

PROTOCOL = "connection-spike-v1"
SETUP_PROTOCOL = "connection-spike-setup-v1"
# SETUP_PROTOCOL is the only credential-preparation transport. Its SSM-safe
# envelopes contain public data, sealed ciphertext, or secret ARNs--never a
# plaintext password/token. `public_key`, `prepare_lakebase`, and
# `prepare_rds_baseline` establish the clean baseline for each physical AWS
# source. After T0, `reassert_rds_credentials` copies the already-prepared
# ordinary login for the selected source to the per-bout Proxy secret, and
# `verify` proves the final pooled endpoint.
#
# Revised PROTOCOL requests add exactly:
#   baseline_auth = {
#     "lakebase": {"credential_sha256": "<hex>"},
#     "competitor": {
#       "credential_sha256": "<hex>",
#       "credential_id": "rds" | "aurora",
#     },
#   }
# The allowlisted credential ID selects a fixed root-owned path below; no
# request can choose a filesystem path. The burst lane/result remains named
# `competitor` and is otherwise unchanged.
PYTHON_VERSION = (3, 12)
PSYCOPG_VERSION = "3.3.4"
WARMUP_ATTEMPTS = 4
SCORED_ATTEMPTS = 128
MAX_CONCURRENCY = 64
WITNESS_CLIENTS = 64
WITNESS_CONCURRENCY = 8
CONNECT_TIMEOUT_SECONDS = 10
ATTEMPT_TIMEOUT_SECONDS = 20.0
RUN_TIMEOUT_SECONDS = 110.0
TLS_MODE = "verify-full"
TRUST_BUNDLE_PATH = Path("/opt/lakebase-anti-demo/round5/round5-ca.pem")
LOCK_PATH = Path("/run/lock/lakebase-anti-demo-round5.lock")
RUN_ROOT = Path("/run/lakebase-anti-demo/round5")
APP_PREFIX = "anti-demo-r5"
CREDENTIAL_ROOT = Path("/var/lib/lakebase-anti-demo/credentials")
BASELINE_CREDENTIAL_PATHS = {
    "lakebase": CREDENTIAL_ROOT / "lakebase.json",
    "rds": CREDENTIAL_ROOT / "rds.json",
    "aurora": CREDENTIAL_ROOT / "aurora.json",
}
RUNTIME_LANE_IDS = frozenset({"lakebase", "competitor"})
AWS_CREDENTIAL_IDS = frozenset({"rds", "aurora"})
SEALED_BOX_KEY_PATH = CREDENTIAL_ROOT / "sealed-box.key"
BASELINE_ROLE = "anti_demo_burst"
BASELINE_DATABASE_KEYS = frozenset({"host", "port", "dbname", "username", "password"})
RDS_BASELINE_KEYS = BASELINE_DATABASE_KEYS | {"master_secret_arn"}
_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,95}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
SEALED_ADMIN_MAX_ENCODED_LENGTH = 21_848
BASELINE_RESTART_RETRY_DELAYS = (1.0, 2.0, 4.0, 8.0)
_SECRET_ARN = re.compile(
    r"^arn:(?P<partition>aws|aws-us-gov|aws-cn):secretsmanager:"
    r"(?P<region>[a-z0-9-]+):\d{12}:secret:.+$"
)
_CREDENTIAL_ENVIRONMENT = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_PROFILE",
    "AWS_DEFAULT_PROFILE",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
)


class RunnerContractError(RuntimeError):
    pass


class RunnerCancelled(RuntimeError):
    pass


def _secrets_manager_region(values: Sequence[object], error: str) -> str:
    regions: set[str] = set()
    for value in values:
        match = _SECRET_ARN.fullmatch(str(value or ""))
        if match is None:
            raise RunnerContractError(error)
        regions.add(match.group("region"))
    if len(regions) != 1:
        raise RunnerContractError(error)
    return regions.pop()


@dataclass(frozen=True)
class Target:
    lane_id: str
    secret_arn: str
    endpoint_host: str
    credential_host: str
    baseline_sha256: str = ""
    baseline_credential_id: str = ""


@dataclass(frozen=True)
class Attempt:
    lane_id: str
    kind: str
    ordinal: int
    worker_slot: int
    row_uuid: UUID
    value: str
    attempt_id: UUID
    scheduled_at_ns: int


@dataclass
class LaneRuntime:
    target: Target
    database: dict[str, object]
    direct_database: dict[str, object]
    witness_connections: list[Any]
    witness_clients: list[dict[str, object]]
    peak_backend_sessions: int = 0


def _decode_payload(argument: str) -> dict[str, object]:
    try:
        padding = "=" * (-len(argument) % 4)
        compressed = base64.urlsafe_b64decode(argument + padding)
        with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as archive:
            encoded = archive.read(512_001)
        if len(encoded) > 512_000:
            raise ValueError("request_expanded_too_large")
        request = json.loads(encoded)
    except (ValueError, TypeError, OSError, json.JSONDecodeError) as exc:
        raise RunnerContractError("request_invalid") from exc
    if not isinstance(request, dict):
        raise RunnerContractError("request_invalid")
    return request


def _decode_request(
    argument: str,
) -> tuple[str, tuple[Target, ...], tuple[Attempt, ...], str]:
    request = _decode_payload(argument)
    if request.get("protocol") != PROTOCOL:
        raise RunnerContractError("protocol_invalid")
    if request.get("trust_bundle_path") != str(TRUST_BUNDLE_PATH) or not re.fullmatch(
        r"[0-9a-f]{64}", str(request.get("trust_bundle_sha256") or "")
    ):
        raise RunnerContractError("trust_bundle_contract_invalid")
    trust_bundle_sha256 = str(request["trust_bundle_sha256"])
    run_id = request.get("run_id")
    if not isinstance(run_id, str) or _SAFE_ID.fullmatch(run_id) is None:
        raise RunnerContractError("run_id_invalid")
    raw_baseline_auth = request.get("baseline_auth")
    baseline_hashes: dict[str, str] = {}
    baseline_credential_ids: dict[str, str] = {}
    if raw_baseline_auth is not None:
        if not isinstance(raw_baseline_auth, dict) or set(raw_baseline_auth) != RUNTIME_LANE_IDS:
            raise RunnerContractError("baseline_auth_invalid")
        for lane_id, value in raw_baseline_auth.items():
            if (
                not isinstance(value, dict)
                or _SHA256.fullmatch(str(value.get("credential_sha256") or "")) is None
            ):
                raise RunnerContractError("baseline_auth_invalid")
            fields = set(value)
            if lane_id == "lakebase":
                if fields != {"credential_sha256"}:
                    raise RunnerContractError("baseline_auth_invalid")
                baseline_credential_ids[lane_id] = "lakebase"
            else:
                # Digest-only is the sealed v4 RDS contract. Keep accepting it
                # so existing RDS bouts do not change behavior while new
                # requests bind the selected physical AWS source explicitly.
                if fields == {"credential_sha256"}:
                    baseline_credential_ids[lane_id] = "rds"
                elif (
                    fields == {"credential_sha256", "credential_id"}
                    and isinstance(value.get("credential_id"), str)
                    and value.get("credential_id") in AWS_CREDENTIAL_IDS
                ):
                    baseline_credential_ids[lane_id] = str(value["credential_id"])
                else:
                    raise RunnerContractError("baseline_auth_invalid")
            baseline_hashes[lane_id] = str(value["credential_sha256"])

    raw_targets = request.get("targets")
    if not isinstance(raw_targets, list) or len(raw_targets) != 2:
        raise RunnerContractError("targets_invalid")
    targets: list[Target] = []
    for value in raw_targets:
        if (
            not isinstance(value, dict)
            or any(
                not isinstance(value.get(name), str) or not value[name]
                for name in ("lane_id", "endpoint_host")
            )
            or not isinstance(value.get("secret_arn"), str)
            or not isinstance(value.get("credential_host"), str)
        ):
            raise RunnerContractError("target_invalid")
        targets.append(
            Target(
                lane_id=value["lane_id"],
                secret_arn=value["secret_arn"],
                endpoint_host=value["endpoint_host"],
                credential_host=value["credential_host"],
                baseline_sha256=baseline_hashes.get(value["lane_id"], ""),
                baseline_credential_id=baseline_credential_ids.get(value["lane_id"], ""),
            )
        )
    lane_ids = {target.lane_id for target in targets}
    if len(lane_ids) != 2:
        raise RunnerContractError("targets_invalid")
    _secrets_manager_region(
        [target.secret_arn for target in targets if target.secret_arn],
        "target_invalid",
    )
    if baseline_hashes:
        if lane_ids != RUNTIME_LANE_IDS:
            raise RunnerContractError("baseline_auth_invalid")
        by_lane = {target.lane_id: target for target in targets}
        if by_lane["lakebase"].secret_arn or not by_lane["competitor"].secret_arn:
            raise RunnerContractError("baseline_auth_invalid")
    raw_schedule = request.get("schedule")
    if not isinstance(raw_schedule, list):
        raise RunnerContractError("schedule_invalid")
    attempts: list[Attempt] = []
    try:
        for value in raw_schedule:
            proof = value["proof"]
            attempt = Attempt(
                lane_id=str(value["lane_id"]),
                kind=str(value["kind"]),
                ordinal=int(value["ordinal"]),
                worker_slot=int(value["worker_slot"]),
                row_uuid=UUID(str(proof["row_uuid"])),
                value=str(proof["value"]),
                attempt_id=UUID(str(proof["attempt_id"])),
                scheduled_at_ns=int(value["scheduled_at_ns"]),
            )
            attempts.append(attempt)
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise RunnerContractError("schedule_invalid") from exc
    for lane_id in lane_ids:
        warmups = [item for item in attempts if item.lane_id == lane_id and item.kind == "warmup"]
        scored = [item for item in attempts if item.lane_id == lane_id and item.kind == "scored"]
        if (
            len(warmups) != WARMUP_ATTEMPTS
            or len(scored) != SCORED_ATTEMPTS
            or {item.worker_slot for item in scored} != set(range(MAX_CONCURRENCY))
        ):
            raise RunnerContractError("schedule_invalid")
    if (
        len(attempts) != 2 * (WARMUP_ATTEMPTS + SCORED_ATTEMPTS)
        or len({item.attempt_id for item in attempts}) != len(attempts)
        or len({item.row_uuid for item in attempts}) != len(attempts)
        or any(
            item.lane_id not in lane_ids or item.value != f"round5-{item.row_uuid}"
            for item in attempts
        )
    ):
        raise RunnerContractError("schedule_invalid")
    return run_id, tuple(targets), tuple(attempts), trust_bundle_sha256


def _validate_runtime() -> None:
    if sys.version_info[:2] != PYTHON_VERSION:
        raise RunnerContractError("python_contract_invalid")
    if psycopg.__version__ != PSYCOPG_VERSION:
        raise RunnerContractError("psycopg_contract_invalid")
    if any(os.environ.get(name) for name in _CREDENTIAL_ENVIRONMENT):
        raise RunnerContractError("runner_credential_source_invalid")


def _validate_trust_bundle(expected_sha256: str) -> None:
    if (
        not TRUST_BUNDLE_PATH.is_file()
        or TRUST_BUNDLE_PATH.is_symlink()
        or hashlib.sha256(TRUST_BUNDLE_PATH.read_bytes()).hexdigest() != expected_sha256
    ):
        raise RunnerContractError("trust_bundle_contract_invalid")


def _canonical_json(value: Mapping[str, object]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _require_secure_path(path: Path, expected_mode: int) -> os.stat_result:
    try:
        metadata = path.stat(follow_symlinks=False)
    except (FileNotFoundError, OSError) as exc:
        raise RunnerContractError("baseline_auth_unavailable") from exc
    if (
        stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != 0
        or stat.S_IMODE(metadata.st_mode) != expected_mode
    ):
        raise RunnerContractError("baseline_auth_permissions_invalid")
    return metadata


def _ensure_credential_root() -> None:
    try:
        CREDENTIAL_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(CREDENTIAL_ROOT, 0o700, follow_symlinks=False)
        os.chown(CREDENTIAL_ROOT, 0, 0, follow_symlinks=False)
    except OSError as exc:
        raise RunnerContractError("baseline_auth_write_failed") from exc
    _require_secure_path(CREDENTIAL_ROOT, 0o700)


def _write_root_file(path: Path, contents: bytes) -> str:
    _ensure_credential_root()
    if path.parent != CREDENTIAL_ROOT or path.is_symlink():
        raise RunnerContractError("baseline_auth_path_invalid")
    temporary = CREDENTIAL_ROOT / f".{path.name}.{secrets.token_hex(12)}"
    descriptor: int | None = None
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
        )
        os.fchmod(descriptor, 0o600)
        os.fchown(descriptor, 0, 0)
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            descriptor = None
            handle.write(contents)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _require_secure_path(path, 0o600)
    except OSError as exc:
        raise RunnerContractError("baseline_auth_write_failed") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
    return hashlib.sha256(contents).hexdigest()


def _read_root_json(path: Path, expected_keys: frozenset[str]) -> dict[str, object]:
    _require_secure_path(CREDENTIAL_ROOT, 0o700)
    _require_secure_path(path, 0o600)
    try:
        encoded = path.read_bytes()
        if len(encoded) > 16_384:
            raise ValueError("oversized")
        value = json.loads(encoded)
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        raise RunnerContractError("baseline_auth_invalid") from exc
    if not isinstance(value, dict) or set(value) != expected_keys:
        raise RunnerContractError("baseline_auth_invalid")
    return value


def _validate_database_value(
    value: Mapping[str, object],
    *,
    expected_host: str | None = None,
) -> dict[str, object]:
    host = value.get("host")
    port = value.get("port")
    dbname = value.get("dbname")
    user = value.get("username")
    password = value.get("password")
    if (
        not isinstance(host, str)
        or not host
        or (expected_host is not None and host != expected_host)
        or not isinstance(port, int)
        or not 1 <= port <= 65535
        or not isinstance(dbname, str)
        or not dbname
        or not isinstance(user, str)
        or not user
        or not isinstance(password, str)
        or not password
    ):
        raise RunnerContractError("database_binding_invalid")
    return {
        "host": host,
        "port": port,
        "dbname": dbname,
        "user": user,
        "password": password,
    }


def _load_baseline_database(target: Target) -> dict[str, object]:
    credential_id = target.baseline_credential_id or target.lane_id
    path = BASELINE_CREDENTIAL_PATHS.get(credential_id)
    expected_keys = (
        BASELINE_DATABASE_KEYS if credential_id == "lakebase" else RDS_BASELINE_KEYS
    )
    if path is None:
        raise RunnerContractError("baseline_auth_invalid")
    value = _read_root_json(path, expected_keys)
    encoded = _canonical_json(value)
    if hashlib.sha256(encoded).hexdigest() != target.baseline_sha256:
        raise RunnerContractError("baseline_auth_hash_invalid")
    return _validate_database_value(value, expected_host=target.credential_host)


async def _database_config(
    client: Any,
    target: Target,
) -> tuple[dict[str, object], dict[str, object]]:
    if target.baseline_sha256:
        common = _load_baseline_database(target)
        direct = dict(common)
        return ({**common, "host": target.endpoint_host}, direct)
    response = await asyncio.to_thread(
        client.get_secret_value,
        SecretId=target.secret_arn,
        VersionStage="AWSCURRENT",
    )
    try:
        value = json.loads(response["SecretString"])
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise RunnerContractError("database_binding_invalid") from exc
    if not isinstance(value, dict) or not target.credential_host:
        raise RunnerContractError("database_binding_invalid")
    secret_host = value.get("host")
    if not isinstance(secret_host, str) or not secret_host or secret_host != target.credential_host:
        raise RunnerContractError("database_binding_invalid")
    port = value.get("port", 5432)
    dbname = value.get("dbname", value.get("database"))
    user = value.get("username", value.get("user"))
    password = value.get("password")
    if (
        not isinstance(port, int)
        or not 1 <= port <= 65535
        or not isinstance(dbname, str)
        or not dbname
        or not isinstance(user, str)
        or not user
        or not isinstance(password, str)
        or not password
    ):
        raise RunnerContractError("database_binding_invalid")
    common: dict[str, object] = {
        "port": port,
        "dbname": dbname,
        "user": user,
        "password": password,
    }
    return ({"host": target.endpoint_host, **common}, {"host": secret_host, **common})


async def _connect(database: Mapping[str, object], application_name: str) -> Any:
    return await psycopg.AsyncConnection.connect(
        **database,
        sslmode=TLS_MODE,
        sslrootcert=str(TRUST_BUNDLE_PATH),
        connect_timeout=CONNECT_TIMEOUT_SECONDS,
        prepare_threshold=None,
        application_name=application_name,
    )


def _setup_text(value: object, error: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 2048:
        raise RunnerContractError(error)
    return value


def _setup_credential_id(request: Mapping[str, object]) -> str:
    lane_id = str(request["lane_id"])
    # `competitor` is the legacy v4 spelling for the RDS setup slot. New
    # baseline and per-bout setup requests use the physical AWS ID so Aurora
    # and RDS credentials cannot overwrite one another.
    return "rds" if lane_id == "competitor" else lane_id


def _decode_setup_request(argument: str) -> dict[str, object]:
    request = _decode_payload(argument)
    if request.get("protocol") != SETUP_PROTOCOL:
        raise RunnerContractError("protocol_invalid")
    action = _setup_text(request.get("action"), "setup_action_invalid")
    nonce = _setup_text(request.get("nonce"), "setup_nonce_invalid")
    if _SAFE_ID.fullmatch(nonce) is None:
        raise RunnerContractError("setup_nonce_invalid")
    if action == "public_key":
        if set(request) != {"protocol", "action", "nonce"}:
            raise RunnerContractError("setup_request_invalid")
        return request
    common = {
        "protocol",
        "action",
        "nonce",
        "bout_id",
        "lane_id",
        "endpoint_host",
        "credential_host",
        "port",
        "dbname",
        "username",
        "trust_bundle_path",
        "trust_bundle_sha256",
    }
    allowed = {
        "prepare_lakebase": common | {"sealed_admin", "public_key_sha256"},
        "prepare_rds_baseline": common | {"master_secret_arn"},
        "reassert_rds_credentials": common
        | {"master_secret_arn", "destination_secret_arn", "credential_sha256"},
        "verify": common | {"credential_sha256"},
    }
    expected = allowed.get(action)
    if expected is None or set(request) != expected:
        raise RunnerContractError("setup_request_invalid")
    bout_id = _setup_text(request.get("bout_id"), "setup_bout_invalid")
    if _SAFE_ID.fullmatch(bout_id) is None:
        raise RunnerContractError("setup_bout_invalid")
    lane_id = _setup_text(request.get("lane_id"), "setup_lane_invalid")
    if lane_id not in BASELINE_CREDENTIAL_PATHS and lane_id != "competitor":
        raise RunnerContractError("setup_lane_invalid")
    if (
        request.get("trust_bundle_path") != str(TRUST_BUNDLE_PATH)
        or _SHA256.fullmatch(str(request.get("trust_bundle_sha256") or "")) is None
    ):
        raise RunnerContractError("trust_bundle_contract_invalid")
    for name in ("endpoint_host", "credential_host", "dbname", "username"):
        _setup_text(request.get(name), "setup_binding_invalid")
    if request["username"] != BASELINE_ROLE:
        raise RunnerContractError("setup_binding_invalid")
    port = request.get("port")
    if not isinstance(port, int) or not 1 <= port <= 65535:
        raise RunnerContractError("setup_binding_invalid")
    if action == "prepare_lakebase" and lane_id != "lakebase":
        raise RunnerContractError("setup_lane_invalid")
    credential_id = _setup_credential_id(request)
    if (
        action in {"prepare_rds_baseline", "reassert_rds_credentials"}
        and credential_id not in AWS_CREDENTIAL_IDS
    ):
        raise RunnerContractError("setup_lane_invalid")
    secret_arns = [
        request[name]
        for name in ("master_secret_arn", "destination_secret_arn")
        if name in request
    ]
    if secret_arns:
        _secrets_manager_region(secret_arns, "setup_secret_binding_invalid")
    for name in ("public_key_sha256", "credential_sha256"):
        if name in request and _SHA256.fullmatch(str(request.get(name) or "")) is None:
            raise RunnerContractError("setup_hash_invalid")
    if "sealed_admin" in request:
        sealed_admin = request["sealed_admin"]
        if (
            not isinstance(sealed_admin, str)
            or not sealed_admin
            or len(sealed_admin) > SEALED_ADMIN_MAX_ENCODED_LENGTH
        ):
            raise RunnerContractError("sealed_admin_invalid")
        try:
            decoded = base64.b64decode(sealed_admin, validate=True)
        except ValueError as exc:
            raise RunnerContractError("sealed_admin_invalid") from exc
        if not 48 <= len(decoded) <= 16_384:
            raise RunnerContractError("sealed_admin_invalid")
    return request


def _sealed_box_public_key() -> tuple[bytes, str]:
    try:
        from nacl.public import PrivateKey
    except ImportError as exc:
        raise RunnerContractError("sealed_box_runtime_invalid") from exc
    _ensure_credential_root()
    if SEALED_BOX_KEY_PATH.exists():
        _require_secure_path(SEALED_BOX_KEY_PATH, 0o600)
        private_bytes = SEALED_BOX_KEY_PATH.read_bytes()
        if len(private_bytes) != PrivateKey.SIZE:
            raise RunnerContractError("sealed_box_key_invalid")
        private_key = PrivateKey(private_bytes)
    else:
        private_key = PrivateKey.generate()
        _write_root_file(SEALED_BOX_KEY_PATH, bytes(private_key))
    public_key = bytes(private_key.public_key)
    return public_key, hashlib.sha256(public_key).hexdigest()


def _open_sealed_admin(ciphertext: str, expected_public_key_sha256: str) -> dict[str, object]:
    try:
        from nacl.public import PrivateKey, SealedBox
    except ImportError as exc:
        raise RunnerContractError("sealed_box_runtime_invalid") from exc
    public_key, public_key_sha256 = _sealed_box_public_key()
    if public_key_sha256 != expected_public_key_sha256:
        raise RunnerContractError("sealed_box_key_mismatch")
    del public_key
    try:
        private_key = PrivateKey(SEALED_BOX_KEY_PATH.read_bytes())
        plaintext = SealedBox(private_key).decrypt(base64.b64decode(ciphertext, validate=True))
        if len(plaintext) > 16_384:
            raise ValueError("oversized")
        value = json.loads(plaintext)
    except Exception as exc:
        # PyNaCl deliberately exposes several concrete failure classes. Keep
        # all of them, and all password-bearing JSON failures, behind one
        # non-secret contract error.
        raise RunnerContractError("sealed_admin_invalid") from exc
    if not isinstance(value, dict) or set(value) != BASELINE_DATABASE_KEYS:
        raise RunnerContractError("sealed_admin_invalid")
    return value


async def _configure_ordinary_role(
    admin_database: Mapping[str, object],
    ordinary_database: Mapping[str, object],
    *,
    create_if_missing: bool,
    retry_transient_restart: bool = False,
) -> None:
    retry_index = 0
    while True:
        connection: Any | None = None
        retry_delay: float | None = None
        try:
            async with asyncio.timeout(ATTEMPT_TIMEOUT_SECONDS):
                connection = await _connect(admin_database, f"{APP_PREFIX}-role-setup")
                async with connection.cursor() as cursor:
                    await cursor.execute(
                        "SET LOCAL password_encryption = 'scram-sha-256'",
                        prepare=False,
                    )
                    await cursor.execute(
                        "SELECT rolcanlogin, rolsuper, rolcreatedb, rolcreaterole, "
                        "rolreplication, rolbypassrls FROM pg_roles WHERE rolname = %s",
                        (BASELINE_ROLE,),
                        prepare=False,
                    )
                    attributes = await cursor.fetchone()
                    exists = attributes is not None
                    if exists and tuple(attributes) != (
                        True,
                        False,
                        False,
                        False,
                        False,
                        False,
                    ):
                        raise RunnerContractError("baseline_role_attributes_invalid")
                    if attributes is None and not create_if_missing:
                        raise RunnerContractError("baseline_role_missing")
                    role = sql.Identifier(BASELINE_ROLE)
                    password = sql.Literal(str(ordinary_database["password"]))
                    operation = (
                        sql.SQL("ALTER ROLE {} PASSWORD {}")
                        if exists
                        else sql.SQL(
                            "CREATE ROLE {} WITH LOGIN PASSWORD {} NOSUPERUSER NOCREATEDB "
                            "NOCREATEROLE NOREPLICATION NOBYPASSRLS"
                        )
                    )
                    await cursor.execute(operation.format(role, password), prepare=False)
                    database = sql.Identifier(str(ordinary_database["dbname"]))
                    await cursor.execute(
                        sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(database, role),
                        prepare=False,
                    )
                    await cursor.execute(
                        sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(role),
                        prepare=False,
                    )
                    await cursor.execute(
                        sql.SQL(
                            "GRANT SELECT, INSERT, DELETE ON TABLE public.anti_demo_probe TO {}"
                        ).format(role),
                        prepare=False,
                    )
                await connection.commit()
            return
        except RunnerContractError:
            if connection is not None:
                try:
                    await connection.rollback()
                except psycopg.Error:
                    pass
            raise
        except Exception as exc:
            if connection is not None:
                try:
                    await connection.rollback()
                except psycopg.Error:
                    pass
            sqlstate = exc.sqlstate if isinstance(exc, psycopg.OperationalError) else None
            transient_restart = (
                isinstance(exc, (OSError, TimeoutError))
                or (
                    isinstance(exc, psycopg.OperationalError)
                    and (
                        sqlstate is None
                        or sqlstate.startswith("08")
                        or sqlstate in {"57P01", "57P02", "57P03"}
                    )
                )
            )
            if (
                retry_transient_restart
                and transient_restart
                and retry_index < len(BASELINE_RESTART_RETRY_DELAYS)
            ):
                retry_delay = BASELINE_RESTART_RETRY_DELAYS[retry_index]
                retry_index += 1
            else:
                raise RunnerContractError("baseline_role_setup_failed") from exc
        finally:
            if connection is not None:
                try:
                    await connection.close()
                except psycopg.Error:
                    pass
        assert retry_delay is not None
        await asyncio.sleep(retry_delay)


def _new_ordinary_database(request: Mapping[str, object]) -> dict[str, object]:
    return {
        "host": request["credential_host"],
        "port": request["port"],
        "dbname": request["dbname"],
        "username": BASELINE_ROLE,
        "password": secrets.token_urlsafe(48),
    }


async def _read_master_database(
    secrets_client: Any,
    *,
    secret_arn: str,
    expected_host: str,
    expected_port: int,
    expected_database: str,
) -> dict[str, object]:
    response = await asyncio.to_thread(
        secrets_client.get_secret_value,
        SecretId=secret_arn,
        VersionStage="AWSCURRENT",
    )
    try:
        value = json.loads(response["SecretString"])
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise RunnerContractError("master_secret_invalid") from exc
    if not isinstance(value, dict):
        raise RunnerContractError("master_secret_invalid")
    if "host" in value and (
        not isinstance(value["host"], str) or value["host"] != expected_host
    ):
        raise RunnerContractError("master_secret_invalid")
    if "port" in value and (
        not isinstance(value["port"], int)
        or isinstance(value["port"], bool)
        or value["port"] != expected_port
    ):
        raise RunnerContractError("master_secret_invalid")
    for name in ("dbname", "database"):
        if name in value and (
            not isinstance(value[name], str) or value[name] != expected_database
        ):
            raise RunnerContractError("master_secret_invalid")
    username = value.get("username", value.get("user"))
    password = value.get("password")
    if (
        not isinstance(username, str)
        or not username
        or not isinstance(password, str)
        or not password
    ):
        raise RunnerContractError("master_secret_invalid")
    return {
        "host": expected_host,
        "port": expected_port,
        "dbname": expected_database,
        "user": username,
        "password": password,
    }


async def _verify_setup_transaction(
    request: Mapping[str, object],
    *,
    retry_transient_restart: bool = False,
) -> None:
    credential_id = _setup_credential_id(request)
    path = BASELINE_CREDENTIAL_PATHS[credential_id]
    keys = (
        BASELINE_DATABASE_KEYS if credential_id == "lakebase" else RDS_BASELINE_KEYS
    )
    value = _read_root_json(path, keys)
    if hashlib.sha256(_canonical_json(value)).hexdigest() != request["credential_sha256"]:
        raise RunnerContractError("baseline_auth_hash_invalid")
    database = _validate_database_value(
        value,
        expected_host=str(request["credential_host"]),
    )
    database["host"] = request["endpoint_host"]
    retry_index = 0
    while True:
        connection: Any | None = None
        retry_delay: float | None = None
        try:
            connection = await _connect(database, f"{APP_PREFIX}-setup-verify")
            async with connection.cursor() as cursor:
                await cursor.execute(
                    "SELECT %s::text, current_user",
                    (request["nonce"],),
                    prepare=False,
                )
                row = await cursor.fetchone()
            await connection.commit()
            if row != (request["nonce"], BASELINE_ROLE):
                raise RunnerContractError("setup_verify_failed")
            return
        except RunnerContractError:
            raise
        except Exception as exc:
            sqlstate = exc.sqlstate if isinstance(exc, psycopg.OperationalError) else None
            transient_restart = (
                isinstance(exc, (OSError, TimeoutError))
                or (
                    isinstance(exc, psycopg.OperationalError)
                    and (
                        sqlstate is None
                        or sqlstate.startswith("08")
                        or sqlstate in {"57P01", "57P02", "57P03"}
                    )
                )
            )
            if (
                retry_transient_restart
                and transient_restart
                and retry_index < len(BASELINE_RESTART_RETRY_DELAYS)
            ):
                retry_delay = BASELINE_RESTART_RETRY_DELAYS[retry_index]
                retry_index += 1
            else:
                raise RunnerContractError("setup_verify_failed") from exc
        finally:
            if connection is not None:
                try:
                    await connection.close()
                except psycopg.Error:
                    pass
        assert retry_delay is not None
        await asyncio.sleep(retry_delay)


async def _execute_setup(request: Mapping[str, object]) -> dict[str, object]:
    action = str(request["action"])
    if action == "public_key":
        public_key, public_key_sha256 = _sealed_box_public_key()
        return {
            "protocol": SETUP_PROTOCOL,
            "action": action,
            "nonce": request["nonce"],
            "status": "verified",
            "public_key": base64.b64encode(public_key).decode("ascii"),
            "public_key_sha256": public_key_sha256,
        }

    _validate_trust_bundle(str(request["trust_bundle_sha256"]))
    result: dict[str, object] = {
        "protocol": SETUP_PROTOCOL,
        "action": action,
        "bout_id": request["bout_id"],
        "lane_id": request["lane_id"],
        "nonce": request["nonce"],
        "status": "verified",
    }
    if action == "prepare_lakebase":
        admin_value = _open_sealed_admin(
            str(request["sealed_admin"]), str(request["public_key_sha256"])
        )
        admin = _validate_database_value(
            admin_value,
            expected_host=str(request["credential_host"]),
        )
        if admin["dbname"] != request["dbname"]:
            raise RunnerContractError("setup_binding_invalid")
        ordinary = _new_ordinary_database(request)
        await _configure_ordinary_role(
            admin,
            ordinary,
            create_if_missing=True,
            retry_transient_restart=True,
        )
        credential_sha256 = _write_root_file(
            BASELINE_CREDENTIAL_PATHS["lakebase"], _canonical_json(ordinary)
        )
        await _verify_setup_transaction({**request, "credential_sha256": credential_sha256})
        result["credential_sha256"] = credential_sha256
        return result

    secret_arns = [
        request[name]
        for name in ("master_secret_arn", "destination_secret_arn")
        if name in request
    ]
    secrets_client: Any | None = None
    if secret_arns:
        region = _secrets_manager_region(secret_arns, "setup_secret_binding_invalid")
        secrets_client = boto3.Session().client("secretsmanager", region_name=region)
    if action == "prepare_rds_baseline":
        assert secrets_client is not None
        master_secret_arn = str(request["master_secret_arn"])
        admin = await _read_master_database(
            secrets_client,
            secret_arn=master_secret_arn,
            expected_host=str(request["credential_host"]),
            expected_port=int(request["port"]),
            expected_database=str(request["dbname"]),
        )
        ordinary = _new_ordinary_database(request)
        await _configure_ordinary_role(
            admin,
            ordinary,
            create_if_missing=True,
            # Aurora is deliberately proven at scale zero immediately before
            # baseline sealing.  Its first fresh connection can therefore
            # land during the bounded automatic-resume window.  Retry only
            # that known restart race; RDS remains single-attempt.
            retry_transient_restart=request["lane_id"] == "aurora",
        )
        stored = {**ordinary, "master_secret_arn": master_secret_arn}
        credential_sha256 = _write_root_file(
            BASELINE_CREDENTIAL_PATHS[_setup_credential_id(request)],
            _canonical_json(stored),
        )
        await _verify_setup_transaction({**request, "credential_sha256": credential_sha256})
        result["credential_sha256"] = credential_sha256
        return result

    if action == "reassert_rds_credentials":
        assert secrets_client is not None
        stored = _read_root_json(
            BASELINE_CREDENTIAL_PATHS[_setup_credential_id(request)],
            RDS_BASELINE_KEYS,
        )
        if hashlib.sha256(_canonical_json(stored)).hexdigest() != request["credential_sha256"]:
            raise RunnerContractError("baseline_auth_hash_invalid")
        if stored["master_secret_arn"] != request["master_secret_arn"]:
            raise RunnerContractError("master_secret_binding_invalid")
        ordinary = _validate_database_value(stored, expected_host=str(request["credential_host"]))
        admin = await _read_master_database(
            secrets_client,
            secret_arn=str(request["master_secret_arn"]),
            expected_host=str(request["credential_host"]),
            expected_port=int(request["port"]),
            expected_database=str(request["dbname"]),
        )
        await _configure_ordinary_role(admin, ordinary, create_if_missing=False)
        secret_payload = {
            "host": request["credential_host"],
            "port": ordinary["port"],
            "dbname": ordinary["dbname"],
            "username": ordinary["user"],
            "password": ordinary["password"],
        }
        try:
            await asyncio.to_thread(
                secrets_client.put_secret_value,
                SecretId=request["destination_secret_arn"],
                ClientRequestToken=secrets.token_hex(32),
                SecretString=_canonical_json(secret_payload).decode("utf-8"),
            )
        except Exception as exc:
            raise RunnerContractError("destination_secret_write_failed") from exc
        return result

    if action == "verify":
        # Aurora may still be inside its automatic-resume window after the
        # backstage scale-zero proof. Retry only transient restart failures
        # here; RDS and timed bout traffic remain single-attempt.
        await _verify_setup_transaction(
            request,
            retry_transient_restart=request["lane_id"] == "aurora",
        )
        return result
    raise RunnerContractError("setup_action_invalid")


async def _run_setup_bounded(
    request: Mapping[str, object], cancelled: asyncio.Event
) -> tuple[dict[str, object] | None, bool]:
    setup = asyncio.create_task(_execute_setup(request))
    cancellation = asyncio.create_task(cancelled.wait())
    try:
        done, _ = await asyncio.wait((setup, cancellation), return_when=asyncio.FIRST_COMPLETED)
        if cancellation in done and cancelled.is_set():
            setup.cancel()
            await asyncio.gather(setup, return_exceptions=True)
            return None, True
        return await setup, False
    finally:
        cancellation.cancel()
        await asyncio.gather(cancellation, return_exceptions=True)
        if not setup.done():
            setup.cancel()
            await asyncio.gather(setup, return_exceptions=True)


async def _execute_attempt(
    attempt: Attempt,
    database: Mapping[str, object],
    application_name: str,
) -> dict[str, object]:
    connection: Any | None = None
    try:
        async with asyncio.timeout(ATTEMPT_TIMEOUT_SECONDS):
            connection = await _connect(database, application_name)
            async with connection.cursor() as cursor:
                await cursor.execute(
                    """
                    SELECT probe_id, expected_value, %s::uuid, pg_backend_pid()
                    FROM public.anti_demo_probe
                    WHERE probe_id = %s AND expected_value = %s
                    """,
                    (attempt.attempt_id, attempt.row_uuid, attempt.value),
                    prepare=False,
                )
                response = await cursor.fetchone()
            await connection.commit()
            completed_ns = time.monotonic_ns()
            if response is None:
                return {
                    "attempt_id": str(attempt.attempt_id),
                    "status": "error",
                    "completed_ns": completed_ns,
                    "error": "probe_contract_failed",
                }
            try:
                response_proof = {
                    "row_uuid": str(response[0]),
                    "value": str(response[1]),
                    "attempt_id": str(response[2]),
                }
                backend_pid = int(response[3])
            except (IndexError, TypeError, ValueError):
                return {
                    "attempt_id": str(attempt.attempt_id),
                    "status": "error",
                    "completed_ns": completed_ns,
                    "error": "probe_contract_failed",
                }
            expected = {
                "row_uuid": str(attempt.row_uuid),
                "value": attempt.value,
                "attempt_id": str(attempt.attempt_id),
            }
            if response_proof != expected or backend_pid <= 0:
                return {
                    "attempt_id": str(attempt.attempt_id),
                    "status": "error",
                    "completed_ns": completed_ns,
                    "error": "probe_contract_failed",
                }
            return {
                "attempt_id": str(attempt.attempt_id),
                "status": "success",
                "completed_ns": completed_ns,
                # Only emitted after the parameterized SELECT matched the
                # immutable proof byte-for-byte and its transaction committed.
                # The app already owns that proof, so this keeps bounded SSM
                # output below its truncation limit without weakening the gate.
                "exact": True,
                "backend_pid": backend_pid,
            }
    except (psycopg.Error, TimeoutError, OSError):
        if connection is not None:
            try:
                await connection.rollback()
            except psycopg.Error:
                pass
        return {
            "attempt_id": str(attempt.attempt_id),
            "status": "error",
            "completed_ns": time.monotonic_ns(),
            "error": "attempt_failed",
        }
    finally:
        if connection is not None:
            try:
                await connection.close()
            except psycopg.Error:
                pass


async def _execute_service_attempt(
    attempt: Attempt,
    database: Mapping[str, object],
    application_name: str,
) -> dict[str, object]:
    # Callers invoke this only after acquiring their lane semaphore. Queueing
    # behind MAX_CONCURRENCY is deliberately excluded from raw service time;
    # connect, TLS, transaction, exact response validation, and commit remain
    # included. Keep _execute_attempt independently testable and decorate its
    # settled observation here so every emitted warm/scored attempt has a real
    # start instead of a fabricated barrier timestamp.
    started_ns = time.monotonic_ns()
    observation = await _execute_attempt(attempt, database, application_name)
    completed_ns = observation.get("completed_ns")
    if (
        isinstance(completed_ns, bool)
        or not isinstance(completed_ns, int)
        or completed_ns < started_ns
    ):
        raise RunnerContractError("attempt_timing_invalid")
    return {**observation, "started_ns": started_ns}


async def _warmup(
    runtime: LaneRuntime,
    attempts: Sequence[Attempt],
    run_id: str,
) -> list[dict[str, object]]:
    return list(
        await asyncio.gather(
            *(
                _execute_service_attempt(item, runtime.database, f"{APP_PREFIX}-{run_id}-warmup")
                for item in attempts
            )
        )
    )


async def _open_witness_clients(runtime: LaneRuntime, run_id: str) -> None:
    semaphore = asyncio.Semaphore(WITNESS_CONCURRENCY)

    async def open_one(ordinal: int) -> tuple[Any, dict[str, object]]:
        async with semaphore:
            client_id = f"w{ordinal:02d}"
            connection = await _connect(
                runtime.database,
                f"{APP_PREFIX}-{run_id}-witness",
            )
            client = {
                "client_id": client_id,
                "retained": True,
                "verified": False,
                "backend_pid": 0,
            }
            runtime.witness_connections.append(connection)
            runtime.witness_clients.append(client)
            return connection, client

    await asyncio.gather(*(open_one(index) for index in range(WITNESS_CLIENTS)))


async def _verify_witness_clients(runtime: LaneRuntime) -> None:
    semaphore = asyncio.Semaphore(WITNESS_CONCURRENCY)

    async def verify_one(index: int) -> None:
        async with semaphore:
            connection = runtime.witness_connections[index]
            client = runtime.witness_clients[index]
            async with connection.cursor() as cursor:
                await cursor.execute(
                    "SELECT %s::text, pg_backend_pid()",
                    (client["client_id"],),
                    prepare=False,
                )
                row = await cursor.fetchone()
            await connection.commit()
            client["verified"] = bool(
                row is not None and row[0] == client["client_id"] and int(row[1]) > 0
            )
            if client["verified"]:
                client["backend_pid"] = int(row[1])

    await asyncio.gather(*(verify_one(index) for index in range(WITNESS_CLIENTS)))
    if not all(client["verified"] for client in runtime.witness_clients):
        raise RunnerContractError("witness_contract_failed")


async def _observe_backend_peak(
    runtime: LaneRuntime,
    run_id: str,
    stop: asyncio.Event,
) -> None:
    connection = await _connect(runtime.direct_database, f"{APP_PREFIX}-{run_id}-observer")
    try:
        while not stop.is_set():
            async with connection.cursor() as cursor:
                await cursor.execute(
                    "SELECT count(*) FROM pg_stat_activity "
                    "WHERE application_name = %s AND pid <> pg_backend_pid()",
                    (f"{APP_PREFIX}-{run_id}-witness",),
                    prepare=False,
                )
                row = await cursor.fetchone()
            await connection.commit()
            if row is not None:
                runtime.peak_backend_sessions = max(runtime.peak_backend_sessions, int(row[0]))
            try:
                await asyncio.wait_for(stop.wait(), timeout=0.02)
            except TimeoutError:
                pass
    finally:
        await connection.close()


async def _run_scored(
    runtimes: Sequence[LaneRuntime],
    attempts: Sequence[Attempt],
    run_id: str,
    cancelled: asyncio.Event,
) -> tuple[list[dict[str, object]], int, dict[str, int]]:
    ordered_attempts = sorted(
        attempts,
        key=lambda item: (item.ordinal, item.lane_id),
    )
    barrier = asyncio.Event()
    ready = asyncio.Event()
    ready_count = 0
    ready_lock = asyncio.Lock()
    first_launch: dict[str, int] = {}
    semaphores = {
        runtime.target.lane_id: asyncio.Semaphore(MAX_CONCURRENCY) for runtime in runtimes
    }
    databases = {runtime.target.lane_id: runtime.database for runtime in runtimes}

    async def staged(item: Attempt) -> dict[str, object]:
        nonlocal ready_count
        async with ready_lock:
            ready_count += 1
            if ready_count == len(ordered_attempts):
                ready.set()
        await barrier.wait()
        if cancelled.is_set():
            raise RunnerCancelled
        first_launch.setdefault(item.lane_id, time.monotonic_ns())
        async with semaphores[item.lane_id]:
            return await _execute_service_attempt(
                item,
                databases[item.lane_id],
                f"{APP_PREFIX}-{run_id}-scored",
            )

    tasks = [asyncio.create_task(staged(item)) for item in ordered_attempts]
    cancellation = asyncio.create_task(cancelled.wait())
    try:
        await asyncio.wait_for(ready.wait(), timeout=5)
        release_ns = time.monotonic_ns()
        barrier.set()
        gathered = asyncio.gather(*tasks)
        done, _ = await asyncio.wait((gathered, cancellation), return_when=asyncio.FIRST_COMPLETED)
        if cancellation in done and cancelled.is_set():
            gathered.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise RunnerCancelled
        results = list(await gathered)
        if set(first_launch) != {runtime.target.lane_id for runtime in runtimes}:
            raise RunnerContractError("barrier_contract_failed")
        return results, release_ns, first_launch
    finally:
        cancellation.cancel()
        await asyncio.gather(cancellation, return_exceptions=True)
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def _close_witness_clients(runtimes: Sequence[LaneRuntime]) -> None:
    results = await asyncio.gather(
        *(connection.close() for runtime in runtimes for connection in runtime.witness_connections),
        return_exceptions=True,
    )
    if any(isinstance(value, BaseException) for value in results):
        raise RunnerContractError("witness_close_failed")


async def _prepare_rows(
    runtimes: Sequence[LaneRuntime],
    attempts: Sequence[Attempt],
) -> None:
    async def prepare_lane(runtime: LaneRuntime) -> None:
        owned = [item for item in attempts if item.lane_id == runtime.target.lane_id]
        connection = await _connect(runtime.direct_database, f"{APP_PREFIX}-prepare")
        try:
            async with connection.cursor() as cursor:
                for item in owned:
                    await cursor.execute(
                        "INSERT INTO public.anti_demo_probe (probe_id, expected_value) "
                        "VALUES (%s, %s)",
                        (item.row_uuid, item.value),
                        prepare=False,
                    )
                await cursor.execute(
                    "SELECT count(*) FROM public.anti_demo_probe WHERE probe_id = ANY(%s::uuid[])",
                    ([item.row_uuid for item in owned],),
                    prepare=False,
                )
                row = await cursor.fetchone()
            await connection.commit()
            if row is None or int(row[0]) != len(owned):
                raise RunnerContractError("prepare_rows_failed")
        finally:
            await connection.close()

    tasks = [asyncio.create_task(prepare_lane(runtime)) for runtime in runtimes]
    try:
        await asyncio.gather(*tasks)
    except BaseException:
        # asyncio.gather propagates the first failure without cancelling its
        # siblings. Settle every preparation task before lifecycle cleanup can
        # certify deletion, so a sibling cannot commit a late probe afterward.
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


async def _cleanup_rows(runtimes: Sequence[LaneRuntime], attempts: Sequence[Attempt]) -> None:
    by_lane = {
        runtime.target.lane_id: [
            item for item in attempts if item.lane_id == runtime.target.lane_id
        ]
        for runtime in runtimes
    }

    async def cleanup_lane(runtime: LaneRuntime) -> None:
        connection = await _connect(runtime.direct_database, f"{APP_PREFIX}-cleanup")
        try:
            probe_ids = [item.row_uuid for item in by_lane[runtime.target.lane_id]]
            async with connection.cursor() as cursor:
                await cursor.execute(
                    "DELETE FROM public.anti_demo_probe WHERE probe_id = ANY(%s::uuid[])",
                    (probe_ids,),
                    prepare=False,
                )
                await cursor.execute(
                    "SELECT count(*) FROM public.anti_demo_probe WHERE probe_id = ANY(%s::uuid[])",
                    (probe_ids,),
                    prepare=False,
                )
                row = await cursor.fetchone()
            await connection.commit()
            if row is None or int(row[0]) != 0:
                raise RunnerContractError("cleanup_incomplete")
        finally:
            await connection.close()

    async with asyncio.timeout(9):
        await asyncio.gather(*(cleanup_lane(runtime) for runtime in runtimes))


async def _lifecycle(
    run_id: str,
    targets: Sequence[Target],
    attempts: Sequence[Attempt],
    cancelled: asyncio.Event,
    run_directory: Path,
) -> tuple[dict[str, object] | None, bool]:
    runtimes: list[LaneRuntime] = []
    result: dict[str, object] | None = None
    was_cancelled = False
    observers_stop = asyncio.Event()
    observers: list[asyncio.Task[None]] = []
    try:
        region = _secrets_manager_region(
            [target.secret_arn for target in targets if target.secret_arn],
            "target_invalid",
        )
        secrets_client = boto3.Session().client(
            "secretsmanager",
            region_name=region,
        )
        bindings = await asyncio.gather(
            *(_database_config(secrets_client, target) for target in targets)
        )
        runtimes = [
            LaneRuntime(target, scored, direct, [], [])
            for target, (scored, direct) in zip(targets, bindings, strict=True)
        ]
        await _prepare_rows(runtimes, attempts)
        warmup_results = await asyncio.gather(
            *(
                _warmup(
                    runtime,
                    [
                        item
                        for item in attempts
                        if item.lane_id == runtime.target.lane_id and item.kind == "warmup"
                    ],
                    run_id,
                )
                for runtime in runtimes
            )
        )
        if any(item["status"] != "success" for lane in warmup_results for item in lane):
            raise RunnerContractError("warmup_failed")
        scored_attempts = sorted(
            (item for item in attempts if item.kind == "scored"),
            key=lambda item: (item.ordinal, item.lane_id),
        )
        scored_results, release_ns, first_launch = await _run_scored(
            runtimes, scored_attempts, run_id, cancelled
        )
        await asyncio.gather(*(_open_witness_clients(runtime, run_id) for runtime in runtimes))
        observers = [
            asyncio.create_task(_observe_backend_peak(runtime, run_id, observers_stop))
            for runtime in runtimes
        ]
        # Let both direct observers enter the untimed witness phase before its
        # retained-client transactions begin. No observer or witness connection
        # exists during the scored starting state.
        await asyncio.sleep(0)
        await asyncio.gather(*(_verify_witness_clients(runtime) for runtime in runtimes))
        observers_stop.set()
        observer_results = await asyncio.gather(*observers, return_exceptions=True)
        if any(isinstance(value, BaseException) for value in observer_results):
            raise RunnerContractError("observer_failed")
        observers.clear()
        lane_results = []
        for runtime, warmups in zip(runtimes, warmup_results, strict=True):
            observations = list(warmups) + [
                observation
                for item, observation in zip(scored_attempts, scored_results, strict=True)
                if item.lane_id == runtime.target.lane_id
            ]
            lane_results.append(
                {
                    "lane_id": runtime.target.lane_id,
                    "observations": observations,
                    "witness": {
                        "clients": runtime.witness_clients,
                        "peak_backend_sessions": runtime.peak_backend_sessions,
                    },
                }
            )
        result = {
            "protocol": PROTOCOL,
            "run_id": run_id,
            "release_ns": release_ns,
            "first_launch_ns_by_lane": first_launch,
            "lanes": lane_results,
            "contracts_verified": True,
        }
    except RunnerCancelled:
        was_cancelled = True
    finally:

        async def settle_cleanup() -> None:
            observers_stop.set()
            for observer in observers:
                if not observer.done():
                    observer.cancel()
            await asyncio.gather(*observers, return_exceptions=True)
            await _close_witness_clients(runtimes)
            if runtimes:
                await _cleanup_rows(runtimes, attempts)
            _cleanup_owned(run_id, run_directory)
            print(f"CLEANUP_CONFIRMED:{run_id}", flush=True)

        cleanup_task = asyncio.create_task(settle_cleanup())
        try:
            await asyncio.shield(cleanup_task)
        except asyncio.CancelledError:
            # A repeated SSM cancellation cannot interrupt exact row deletion,
            # retained-client close, run-directory removal, or flock evidence.
            await asyncio.shield(cleanup_task)
            raise
    return result, was_cancelled


def _prepare_run_directory(run_id: str) -> Path:
    RUN_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
    run_directory = RUN_ROOT / run_id
    if run_directory.exists():
        raise RunnerContractError("run_already_exists")
    run_directory.mkdir(mode=0o700)
    (run_directory / "owner").write_text(run_id, encoding="utf-8")
    return run_directory


def _cleanup_owned(run_id: str, run_directory: Path) -> None:
    marker = run_directory / "owner"
    if (
        run_directory != RUN_ROOT / run_id
        or run_directory.is_symlink()
        or not marker.is_file()
        or marker.is_symlink()
        or marker.read_text(encoding="utf-8") != run_id
    ):
        raise RunnerContractError("cleanup_ownership_invalid")
    shutil.rmtree(run_directory)
    if run_directory.exists():
        raise RunnerContractError("cleanup_incomplete")


def _encode_result(result: Mapping[str, object]) -> str:
    encoded_result = base64.urlsafe_b64encode(
        gzip.compress(
            json.dumps(
                result,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8"),
            mtime=0,
        )
    ).decode("ascii")
    if len(encoded_result) > 23_500:
        raise RunnerContractError("result_too_large")
    return encoded_result


def main() -> int:
    run_id = "unknown"
    lock_file: Any | None = None
    lock_acquired = False
    setup_nonce: str | None = None
    setup_cancelled: asyncio.Event | None = None
    exit_code = 1
    try:
        if len(sys.argv) != 2:
            raise RunnerContractError("request_missing")
        envelope = _decode_payload(sys.argv[1])
        protocol = envelope.get("protocol")
        setup_request: dict[str, object] | None = None
        if protocol == SETUP_PROTOCOL:
            setup_request = _decode_setup_request(sys.argv[1])
            run_id = str(setup_request.get("bout_id") or setup_request["nonce"])
            setup_nonce = str(setup_request["nonce"])
            setup_cancelled = asyncio.Event()

            def stop_setup(*unused: object) -> None:
                assert setup_cancelled is not None
                setup_cancelled.set()

            # Install setup handlers immediately after the secret-free
            # envelope is validated, before runtime checks or flock work.
            signal.signal(signal.SIGTERM, stop_setup)
            signal.signal(signal.SIGINT, stop_setup)
        elif protocol == PROTOCOL:
            run_id, targets, attempts, trust_bundle_sha256 = _decode_request(sys.argv[1])
        else:
            raise RunnerContractError("protocol_invalid")
        LOCK_PATH.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
        lock_file = LOCK_PATH.open("a+", encoding="utf-8")
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RunnerContractError("runner_busy") from exc
        lock_acquired = True
        _validate_runtime()
        if setup_request is not None:
            assert setup_cancelled is not None
            setup_result, was_setup_cancelled = asyncio.run(
                _run_setup_bounded(setup_request, setup_cancelled)
            )
            if was_setup_cancelled:
                print(f"RUNNER_CANCELLED:{run_id}", flush=True)
            elif setup_result is not None:
                encoded_setup = _canonical_json(setup_result).decode("utf-8")
                if len(encoded_setup.encode("utf-8")) > 4096:
                    raise RunnerContractError("setup_result_too_large")
                print("SETUP_RESULT:" + encoded_setup, flush=True)
                exit_code = 0
            return exit_code

        _validate_trust_bundle(trust_bundle_sha256)
        run_directory = _prepare_run_directory(run_id)
        cancelled = asyncio.Event()

        def stop(*unused: object) -> None:
            cancelled.set()

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)

        async def bounded() -> tuple[dict[str, object] | None, bool]:
            async with asyncio.timeout(RUN_TIMEOUT_SECONDS):
                lifecycle = asyncio.create_task(
                    _lifecycle(run_id, targets, attempts, cancelled, run_directory)
                )
                cancellation = asyncio.create_task(cancelled.wait())
                done, _ = await asyncio.wait(
                    (lifecycle, cancellation), return_when=asyncio.FIRST_COMPLETED
                )
                if cancellation in done and cancelled.is_set():
                    lifecycle.cancel()
                    await asyncio.gather(lifecycle, return_exceptions=True)
                    return None, True
                cancellation.cancel()
                await asyncio.gather(cancellation, return_exceptions=True)
                return await lifecycle

        result, was_cancelled = asyncio.run(bounded())
        if was_cancelled:
            print(f"RUNNER_CANCELLED:{run_id}", flush=True)
        elif result is not None:
            encoded_result = _encode_result(result)
            print(
                "RESULT_GZIP_BASE64:" + encoded_result,
                flush=True,
            )
            exit_code = 0
    except RunnerContractError as exc:
        print(f"RUNNER_ERROR:{exc.args[0] if exc.args else 'contract_failed'}", flush=True)
    except (TimeoutError, RunnerCancelled, asyncio.CancelledError):
        print(f"RUNNER_CANCELLED:{run_id}", flush=True)
    except Exception:
        print("RUNNER_ERROR:operation_failed", flush=True)
    finally:
        if setup_nonce is not None and lock_acquired:
            # Emitted only after the setup coroutine has completed, failed, or
            # fully handled cancellation. asyncio.run also waits for any
            # in-flight to_thread executor work before returning, so this
            # marker cannot race a late secret write.
            print(f"SETUP_SETTLED:{setup_nonce}", flush=True)
        if lock_file is not None:
            if lock_acquired:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            lock_file.close()
            if lock_acquired:
                print(f"RUNNER_FLOCK_RELEASED:{run_id}", flush=True)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
