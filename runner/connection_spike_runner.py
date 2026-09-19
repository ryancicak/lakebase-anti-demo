#!/usr/bin/env python3.12
from __future__ import annotations

import asyncio
import base64
import collections
import contextlib
import fcntl
import gzip
import hashlib
import io
import json
import math
import multiprocessing as mp
import os
import queue
import re
import secrets
import shutil
import signal
import stat
import sys
import time
import traceback
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, NamedTuple
from uuid import UUID

import boto3
import psycopg
from psycopg import sql

# Same isolated-import problem `round5_fanin` solves below, and for the same reason:
# run_connection_spike.sh execs the interpreter with -I, which implies -P, so the
# script's own directory is not on sys.path. An absolute `runner.external_io` import
# also assumes a package that does not exist on the instance, where these files are
# installed side by side under /opt/lakebase-anti-demo/round5. Try the package, then
# the sibling.
try:
    from .external_io import (
        connect_runner_database,
        secrets_manager_for_runner_operation,
    )
except ImportError:
    import sys as _external_sys
    from pathlib import Path as _ExternalPath

    _external_directory = str(_ExternalPath(__file__).resolve().parent)
    if _external_directory not in _external_sys.path:
        _external_sys.path.insert(0, _external_directory)
    from external_io import (
        connect_runner_database,
        secrets_manager_for_runner_operation,
    )

WORKER_CRASH_PREFIX = "WORKER_CRASH_JSON:"
WORKER_STOP_PREFIX = "WORKER_STOP_JSON:"
WORKER_RESULT_QUEUE_PROFILE_PREFIX = "WORKER_RESULT_QUEUE_PROFILE_JSON:"
PARENT_RESULT_PROFILE_PREFIX = "PARENT_RESULT_PROFILE_JSON:"


try:
    from . import round5_fanin as fanin
except ImportError:
    # run_connection_spike.sh execs the venv interpreter with -I, and isolated
    # mode implies -P: the script's own directory is not placed on sys.path. The
    # sealed installer therefore has to copy round5_fanin.py into the venv's
    # site-packages, and resolving that directory with
    # sysconfig.get_path("purelib") returns the *system* path on RHEL-derived
    # images such as Amazon Linux 2023. The copy then succeeds as root into a
    # directory the venv never reads, so the install reports no failure and the
    # import dies here instead.
    #
    # round5_fanin.py is always installed beside this file in the sealed runner
    # directory, so resolve it from __file__ rather than trusting any packaging
    # scheme. This adds exactly the sealed directory and nothing ambient, which
    # keeps the -I isolation the protocol requires.
    import sys as _sys
    from pathlib import Path as _Path

    _runner_directory = str(_Path(__file__).resolve().parent)
    if _runner_directory not in _sys.path:
        _sys.path.insert(0, _runner_directory)
    import round5_fanin as fanin
PROTOCOL = fanin.PROTOCOL
BOUNDED_PROTOCOL = "connection-spike-v1"
SETUP_PROTOCOL = "connection-spike-setup-v1"
JOB_PROTOCOL = "round5-job-v3"
JOB_RESULT_PREFIX = "JOB_RESULT:"
JOB_RESULT_CHUNK_CHARS = 6_000
#: The per-job ownership lock file. Held (via flock) by whichever invocation is
#: currently running one logical fan-in, and released by the kernel the instant
#: that process exits -- which is what lets a rejoining invocation take over from
#: a dead owner instead of waiting out the whole SSM command timeout.
JOB_OWNER_LOCK_NAME = "owner.lock"
#: How often a rejoining invocation re-checks a live owner's terminal state and
#: re-probes the ownership lock for owner death.
JOB_REJOIN_POLL_SECONDS = 0.1
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
SCORED_ATTEMPTS = 128  # v1 decoder compatibility only
MAX_CONCURRENCY = 64  # v1 decoder compatibility only
WITNESS_CLIENTS = 64  # v1 decoder compatibility only
WITNESS_CONCURRENCY = 8  # v1 decoder compatibility only
CONNECT_TIMEOUT_SECONDS = 10
ATTEMPT_TIMEOUT_SECONDS = 20.0
RUN_TIMEOUT_SECONDS = 110.0
# This file is copied onto the neutral runner and deliberately cannot import the
# app's server package. Keep this boundary equal to
# server.connection_spike_live.SSM_TIMEOUT_SECONDS; a regression test binds the
# two standalone constants together.
SSM_COMMAND_TIMEOUT_SECONDS = 120.0
FANIN_SSM_SAFETY_MARGIN_SECONDS = 60.0
FANIN_SSM_COMMAND_TIMEOUT_SECONDS = fanin.RUN_TIMEOUT_SECONDS + FANIN_SSM_SAFETY_MARGIN_SECONDS
# The setup verification deadline starts only after the Python process has
# imported, decoded and validated its request, acquired the flock, checked the
# trust bundle, and verified the sealed credential digest. It therefore cannot
# consume the whole SSM executionTimeout. Twenty seconds remains for that
# pre-deadline work, result serialization/printing, flock release, and the SSM
# agent to report terminal completion.
SETUP_VERIFY_SSM_SAFETY_MARGIN_SECONDS = 20.0
SETUP_VERIFY_DEADLINE_SECONDS = SSM_COMMAND_TIMEOUT_SECONDS - SETUP_VERIFY_SSM_SAFETY_MARGIN_SECONDS
SETUP_VERIFY_MAX_RETRY_DELAY_SECONDS = 8.0
TLS_MODE = "verify-full"
TRUST_BUNDLE_PATH = Path("/opt/lakebase-anti-demo/round5/round5-ca.pem")
LOCK_PATH = Path("/run/lock/lakebase-anti-demo-round5.lock")
# The resident agent's staged burst holds its OWN exclusive lock, separate from
# LOCK_PATH. In the v4 two-runner control plane the resident holds this while a
# job is staged/prepared/running, and a setup runner invocation (the Aurora
# pending-capacity wake, credential prep) must be able to run in parallel over
# LOCK_PATH -- which the app still requires to print RUNNER_FLOCK_RELEASED as
# proof it settled. Sharing one lock made the Aurora wake fail with runner_busy
# against the resident's own staged burst. Two locks let both proceed; the
# resident is still the sole burst runner, serialized by its own active-job set.
RESIDENT_LOCK_PATH = Path("/run/lock/lakebase-anti-demo-round5-resident.lock")
RUN_ROOT = Path("/run/lakebase-anti-demo/round5")
JOB_ROOT = Path("/var/lib/lakebase-anti-demo/round5-jobs")
APP_PREFIX = "anti-demo-r5"
CREDENTIAL_ROOT = Path("/var/lib/lakebase-anti-demo/credentials")
BASELINE_CREDENTIAL_PATHS = {
    "lakebase": CREDENTIAL_ROOT / "lakebase.json",
    "rds": CREDENTIAL_ROOT / "rds.json",
    "aurora": CREDENTIAL_ROOT / "aurora.json",
}
OBSERVER_CREDENTIAL_PATHS = {
    lane_id: CREDENTIAL_ROOT / f"{lane_id}-observer.json" for lane_id in BASELINE_CREDENTIAL_PATHS
}
RUNTIME_LANE_IDS = frozenset({"lakebase", "competitor"})
RUNNER_HARNESS_ASSETS = (
    "connection_spike_runner.py",
    "round5_fanin.py",
    "external_io.py",
    "run_connection_spike.sh",
    "requirements-round5.txt",
)
#: The pre-release readiness budget: the deadline `await_stage("ready")` runs
#: under, before the authoritative release exists.  It bounds ONLY the readiness
#: barrier (process spawn, CPU pinning, the observer connect+quiesce), never the
#: unscored dwell behind the Proxy exact gate and never the scored ramp.
#:
#: A worker reports ready only once its observer has connected and seen the lane quiet, and
#: the observer owns both of those budgets. Derived from them rather than chosen, because a
#: barrier shorter than the work it waits for does not bound anything: it just relabels a
#: slow observer as a missing worker, and `fanin_worker_ready_timeout` sends the operator
#: looking for a dead process instead of a cold database.
#:
#: The margin covers process spawn, CPU pinning and the queue write. Ceiling, not cost: an
#: observer on a warm lane connects and quiesces in well under a second.
FANIN_WORKER_READY_BUDGET_SECONDS = (
    fanin.OBSERVER_READY_TIMEOUT_SECONDS + fanin.OBSERVER_QUIESCE_TIMEOUT_SECONDS + 60.0
)
# The scored worker deadline covers ramp, the fresh hold prepare, hold, sampling
# and result transfer.  It is anchored at the authoritative release (T0), NOT at
# resident stage: a competitor lane dwells minutes behind the Proxy exact gate
# between "ready" and release, and anchoring at stage spent this budget on that
# unscored wait, timing out await_stage("ramp_ready") the instant release fired.
# _execute_sharded_fanin re-bases run_deadline off the release instant so this
# budget always begins at T0.  The pre-release readiness barrier uses
# FANIN_WORKER_READY_BUDGET_SECONDS above; the two never share a clock.
FANIN_WORKER_RUN_TIMEOUT_SECONDS = fanin.RUN_TIMEOUT_SECONDS
AWS_CREDENTIAL_IDS = frozenset({"rds", "aurora"})
SEALED_BOX_KEY_PATH = CREDENTIAL_ROOT / "sealed-box.key"
BASELINE_ROLE = "anti_demo_burst"
OBSERVER_ROLE = "anti_demo_observer"
BASELINE_DATABASE_KEYS = frozenset({"host", "port", "dbname", "username", "password"})
RDS_BASELINE_KEYS = BASELINE_DATABASE_KEYS | {"master_secret_arn"}
_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,95}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SQLSTATE = re.compile(r"^[0-9A-Z]{5}$")
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


def _runner_harness_evidence() -> tuple[dict[str, str], str]:
    root = Path(__file__).resolve().parent
    assets: dict[str, str] = {}
    digest = hashlib.sha256()
    for name in RUNNER_HARNESS_ASSETS:
        value = (root / name).read_bytes()
        assets[name] = hashlib.sha256(value).hexdigest()
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(value)
        digest.update(b"\0")
    return assets, digest.hexdigest()


def runner_harness_sha256() -> str:
    return _runner_harness_evidence()[1]


LOADED_RUNNER_HARNESS_SHA256 = runner_harness_sha256()


class RunnerCancelled(RuntimeError):
    pass


def _secrets_manager_region(values: Sequence[object], error: str) -> str:
    """The one region every ARN here names, or "" when there are no ARNs to name one.

    The purpose is to refuse a request whose secrets straddle two regions. An empty list
    straddles nothing: the Lakebase lane holds its credential outside Secrets Manager and
    carries no ARN, so a request naming only that lane has none, and treating that as a
    disagreement rejected the lane as `target_invalid`. Callers that require a region still
    get one or a refusal; callers that only need agreement get agreement.
    """

    regions: set[str] = set()
    for value in values:
        match = _SECRET_ARN.fullmatch(str(value or ""))
        if match is None:
            raise RunnerContractError(error)
        regions.add(match.group("region"))
    if not regions:
        return ""
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
    observer_sha256: str = ""


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


def _decode_job_control(argument: str) -> dict[str, object]:
    request = _decode_payload(argument)
    action = request.get("action")
    expected_fields = (
        {"protocol", "schema_version", "action", "job_id", "chunk_index"}
        if action == "job_result"
        else {"protocol", "schema_version", "action", "job_id"}
    )
    if (
        set(request) != expected_fields
        or request.get("protocol") != JOB_PROTOCOL
        or request.get("schema_version") != 3
        or action not in {"cancel_job", "job_status", "job_result"}
        or _SHA256.fullmatch(str(request.get("job_id") or "")) is None
        or (
            action == "job_result"
            and (
                isinstance(request.get("chunk_index"), bool)
                or not isinstance(request.get("chunk_index"), int)
                or int(request["chunk_index"]) < 0
            )
        )
    ):
        raise RunnerContractError("fanin_job_control_invalid")
    return request


def _decode_fanin_request(
    argument: str,
) -> tuple[str, tuple[Target, ...], str, dict[str, object]]:
    request = _decode_payload(argument)
    if (
        request.get("protocol") != fanin.PROTOCOL
        or request.get("schema_version") != fanin.SCHEMA_VERSION
        or request.get("action") not in {"preflight", "run", "run_lane_v3"}
    ):
        raise RunnerContractError("fanin_schema_invalid")
    run_id = request.get("run_id")
    if not isinstance(run_id, str) or _SAFE_ID.fullmatch(run_id) is None:
        raise RunnerContractError("run_id_invalid")
    for name, expected in (
        ("contract_sha256", fanin.contract_sha256()),
        ("config_sha256", fanin.config_sha256()),
        ("generator_sha256", fanin.generator_sha256()),
        ("capacity_model_sha256", fanin.capacity_model_sha256()),
        ("runner_harness_sha256", runner_harness_sha256()),
    ):
        if request.get(name) != expected:
            raise RunnerContractError("fanin_digest_mismatch")
    if request["action"] == "preflight":
        if set(request) != {
            "protocol",
            "schema_version",
            "action",
            "run_id",
            "runner_instance_type",
            "contract_sha256",
            "config_sha256",
            "generator_sha256",
            "capacity_model_sha256",
            "runner_harness_sha256",
        }:
            raise RunnerContractError("fanin_preflight_request_invalid")
        return run_id, (), "", request
    if request["action"] == "run_lane_v3":
        job_id = request.get("job_id")
        prepared_digest = request.get("prepared_request_digest")
        if (
            not isinstance(job_id, str)
            or _SHA256.fullmatch(job_id) is None
            or not isinstance(prepared_digest, str)
            or _SHA256.fullmatch(prepared_digest) is None
        ):
            raise RunnerContractError("fanin_job_identity_invalid")
        digest_payload = {
            key: value for key, value in request.items() if key != "prepared_request_digest"
        }
        observed = hashlib.sha256(_canonical_json(digest_payload)).hexdigest()
        if observed != prepared_digest:
            raise RunnerContractError("fanin_prepared_request_digest_invalid")
    if (
        request.get("trust_bundle_path") != str(TRUST_BUNDLE_PATH)
        or _SHA256.fullmatch(str(request.get("trust_bundle_sha256") or "")) is None
    ):
        raise RunnerContractError("trust_bundle_contract_invalid")
    raw_auth = request.get("baseline_auth")
    # A subset, because a request may run one lane. Checked against the targets below, so
    # authentication for a lane that is not run, or a lane run without authentication, is
    # still refused: what is relaxed is the count, never the correspondence.
    if not isinstance(raw_auth, dict) or not raw_auth or not set(raw_auth) <= RUNTIME_LANE_IDS:
        raise RunnerContractError("baseline_auth_invalid")
    client_hashes: dict[str, str] = {}
    observer_hashes: dict[str, str] = {}
    credential_ids: dict[str, str] = {}
    for lane_id, raw in raw_auth.items():
        if not isinstance(raw, dict):
            raise RunnerContractError("baseline_auth_invalid")
        expected_fields = (
            {"credential_sha256", "observer_credential_sha256"}
            if lane_id == "lakebase"
            else {
                "credential_sha256",
                "observer_credential_sha256",
                "credential_id",
            }
        )
        if (
            set(raw) != expected_fields
            or _SHA256.fullmatch(str(raw.get("credential_sha256") or "")) is None
            or _SHA256.fullmatch(str(raw.get("observer_credential_sha256") or "")) is None
        ):
            raise RunnerContractError("baseline_auth_invalid")
        credential_id = "lakebase" if lane_id == "lakebase" else raw.get("credential_id")
        if credential_id not in BASELINE_CREDENTIAL_PATHS:
            raise RunnerContractError("baseline_auth_invalid")
        client_hashes[lane_id] = str(raw["credential_sha256"])
        observer_hashes[lane_id] = str(raw["observer_credential_sha256"])
        credential_ids[lane_id] = str(credential_id)
    raw_targets = request.get("targets")
    if not isinstance(raw_targets, list) or not 1 <= len(raw_targets) <= len(RUNTIME_LANE_IDS):
        raise RunnerContractError("targets_invalid")
    if request["action"] == "run_lane_v3" and len(raw_targets) != 1:
        raise RunnerContractError("targets_invalid")
    targets: list[Target] = []
    for raw in raw_targets:
        if not isinstance(raw, dict):
            raise RunnerContractError("target_invalid")
        lane_id = raw.get("lane_id")
        if (
            lane_id not in RUNTIME_LANE_IDS
            or set(raw) != {"lane_id", "secret_arn", "endpoint_host", "credential_host"}
            or not isinstance(raw.get("secret_arn"), str)
            or not isinstance(raw.get("endpoint_host"), str)
            or not raw["endpoint_host"]
            or not isinstance(raw.get("credential_host"), str)
            or not raw["credential_host"]
        ):
            raise RunnerContractError("target_invalid")
        targets.append(
            Target(
                lane_id=str(lane_id),
                secret_arn=str(raw["secret_arn"]),
                endpoint_host=str(raw["endpoint_host"]),
                credential_host=str(raw["credential_host"]),
                baseline_sha256=client_hashes[str(lane_id)],
                baseline_credential_id=credential_ids[str(lane_id)],
                observer_sha256=observer_hashes[str(lane_id)],
            )
        )
    lane_ids = {target.lane_id for target in targets}
    if len(lane_ids) != len(targets) or lane_ids != set(raw_auth):
        # Exact correspondence, both ways: a duplicated lane, a lane with no credentials, or
        # credentials for a lane that is not being run are all refused here rather than
        # discovered as a missing key partway through a ramp.
        raise RunnerContractError("targets_invalid")
    _secrets_manager_region(
        [target.secret_arn for target in targets if target.secret_arn],
        "target_invalid",
    )
    return run_id, tuple(targets), str(request["trust_bundle_sha256"]), request


def _decode_request(
    argument: str,
) -> tuple[str, tuple[Target, ...], tuple[Attempt, ...], str]:
    request = _decode_payload(argument)
    if request.get("protocol") != BOUNDED_PROTOCOL:
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
    expected_keys = BASELINE_DATABASE_KEYS if credential_id == "lakebase" else RDS_BASELINE_KEYS
    if path is None:
        raise RunnerContractError("baseline_auth_invalid")
    value = _read_root_json(path, expected_keys)
    encoded = _canonical_json(value)
    if hashlib.sha256(encoded).hexdigest() != target.baseline_sha256:
        raise RunnerContractError("baseline_auth_hash_invalid")
    return _validate_database_value(value, expected_host=target.credential_host)


def _load_observer_database(target: Target) -> dict[str, object]:
    credential_id = target.baseline_credential_id or target.lane_id
    path = OBSERVER_CREDENTIAL_PATHS.get(credential_id)
    if path is None:
        raise RunnerContractError("observer_auth_invalid")
    value = _read_root_json(path, BASELINE_DATABASE_KEYS)
    if hashlib.sha256(_canonical_json(value)).hexdigest() != target.observer_sha256:
        raise RunnerContractError("observer_auth_hash_invalid")
    database = _validate_database_value(value, expected_host=target.credential_host)
    if database["user"] != OBSERVER_ROLE:
        raise RunnerContractError("observer_role_invalid")
    return database


def _validate_capacity_receipt(request: Mapping[str, object]) -> None:
    receipt = request.get("capacity_receipt")
    if (
        not isinstance(receipt, Mapping)
        or set(receipt)
        != {
            "protocol",
            "schema_version",
            "safety_evidence_version",
            "boot_id",
            "capacity_model_sha256",
            "runner_harness_sha256",
            "hard_safety_verified",
        }
        or receipt.get("protocol") != fanin.PROTOCOL
        or receipt.get("schema_version") != fanin.SCHEMA_VERSION
        or receipt.get("safety_evidence_version") != fanin.SAFETY_EVIDENCE_VERSION
        or receipt.get("boot_id") != fanin._runner_boot_id()
        or receipt.get("capacity_model_sha256") != fanin.capacity_model_sha256()
        or receipt.get("runner_harness_sha256") != runner_harness_sha256()
        or receipt.get("hard_safety_verified") is not True
    ):
        raise RunnerContractError("fanin_capacity_receipt_invalid")


async def _execute_fanin_request(
    request: Mapping[str, object],
    targets: Sequence[Target],
    cancelled: asyncio.Event,
    *,
    resident_pool: ResidentShardPool | None = None,
    resident_release_gate: asyncio.Event | None = None,
    on_resident_prepared: Callable[[], Awaitable[None]] | None = None,
) -> dict[str, object]:
    expanded_targets: list[dict[str, object]] = []
    for target in targets:
        client = _load_baseline_database(target)
        observer = _load_observer_database(target)
        expanded_targets.append(
            {
                "lane_id": target.lane_id,
                "database": {
                    **client,
                    "host": target.endpoint_host,
                    "credential_sha256": target.baseline_sha256,
                },
                "observer_database": {
                    **observer,
                    "credential_sha256": target.observer_sha256,
                },
            }
        )
    expanded = {
        **request,
        "targets": expanded_targets,
    }
    _validate_capacity_receipt(expanded)
    live_safety = fanin.TelemetrySummary()
    live_started_ns = time.monotonic_ns()
    live_observation = await fanin._telemetry_off_loop(
        time.process_time(),
        live_started_ns,
        fanin._network_bytes(),
        0.0,
    )
    live_safety.observe(live_observation)
    if int(live_observation["ephemeral_ports_remaining"]) < (
        fanin.TARGET_CLIENTS_PER_LANE + fanin.EPHEMERAL_PORT_RESERVE_PER_LANE
    ):
        live_safety.hard_failures.add(fanin.HardSafetyCode.EPHEMERAL_PORT_RESERVE_EXHAUSTED.value)
    if live_safety.hard_failures:
        code = sorted(live_safety.hard_failures)[0]
        fanin.classify_safety_code(code)
        raise RunnerContractError(code)
    harness_assets, harness_sha256 = _runner_harness_evidence()
    if fanin.WORKER_COUNT == 1:
        result = await fanin.execute_fanin(
            expanded,
            cancelled=cancelled,
            trust_bundle_path=TRUST_BUNDLE_PATH,
        )
    else:
        result = await _execute_sharded_fanin(
            expanded,
            cancelled,
            resident_pool=resident_pool,
            resident_release_gate=resident_release_gate,
            on_resident_prepared=on_resident_prepared,
        )
    result["runner_harness_sha256"] = harness_sha256
    result["runner_asset_sha256s"] = harness_assets
    result["runner_boot_id"] = fanin._runner_boot_id()
    return result


async def _await_process_event(
    event: Any,
    *,
    cancel_event: Any | None = None,
    poll_seconds: float = 0.01,
) -> None:
    """Wait for a multiprocessing event without occupying an executor thread."""

    if cancel_event is not None and cancel_event.is_set():
        raise RunnerCancelled("fanin_cancelled")
    while not event.is_set():
        if cancel_event is not None and cancel_event.is_set():
            raise RunnerCancelled("fanin_cancelled")
        await asyncio.sleep(poll_seconds)
    # Cleanup sets the cancellation and barrier events to wake every process.  Cancellation
    # wins that race: a worker may never interpret the cleanup wakeup as permission to sample.
    if cancel_event is not None and cancel_event.is_set():
        raise RunnerCancelled("fanin_cancelled")


def _sanitized_worker_crash(
    exc: BaseException,
    *,
    worker_index: int,
    worker_cpu: int,
) -> dict[str, object]:
    raw_sqlstate = getattr(exc, "sqlstate", None)
    sqlstate = (
        raw_sqlstate.upper()
        if isinstance(raw_sqlstate, str) and _SQLSTATE.fullmatch(raw_sqlstate)
        else None
    )
    frames = []
    for frame in traceback.extract_tb(exc.__traceback__):
        filename = Path(frame.filename)
        if filename.name not in {"connection_spike_runner.py", "round5_fanin.py"}:
            continue
        function = re.sub(r"[^A-Za-z0-9_]", "_", frame.name)[:64]
        frames.append(
            {
                "file": filename.name,
                "function": function,
                "line": frame.lineno,
            }
        )
    context = fanin.worker_crash_context()
    return {
        **context,
        "worker_id": worker_index,
        "worker_cpu": worker_cpu,
        "error_type": type(exc).__name__,
        "sqlstate": sqlstate,
        "frames": frames[-12:],
    }


def _preserve_worker_crash(value: object) -> None:
    if not isinstance(value, Mapping):
        raise RunnerContractError("fanin_worker_crash_envelope_invalid")
    print(
        WORKER_CRASH_PREFIX + fanin.canonical_json(dict(value)).decode("utf-8"),
        flush=True,
    )


def _preserve_worker_stop(value: object) -> None:
    if not isinstance(value, Mapping):
        raise RunnerContractError("fanin_worker_stop_envelope_invalid")
    print(
        WORKER_STOP_PREFIX + fanin.canonical_json(dict(value)).decode("utf-8"),
        flush=True,
    )


def _validated_ramp_proof(
    value: object,
    *,
    expected_run_id: str | None = None,
    expected_worker_index: int | None = None,
    expected_release_ns: int | None = None,
) -> dict[str, dict[str, object]]:
    if not isinstance(value, Mapping) or not value:
        raise RunnerContractError("fanin_worker_ramp_proof_invalid")
    expected_fields = {
        "initiated",
        "authenticated",
        "held",
        "terminal_failures",
        "cancelled",
        "target_elapsed_ns",
        "run_id",
        "lane_id",
        "worker_index",
        "release_ns",
    }
    proof: dict[str, dict[str, object]] = {}
    for raw_lane_id, raw_counts in value.items():
        lane_id = str(raw_lane_id)
        if lane_id not in RUNTIME_LANE_IDS or not isinstance(raw_counts, Mapping):
            raise RunnerContractError("fanin_worker_ramp_proof_invalid")
        if set(raw_counts) != expected_fields:
            raise RunnerContractError("fanin_worker_ramp_proof_invalid")
        counts: dict[str, object] = {}
        for field in {
            "initiated",
            "authenticated",
            "held",
            "terminal_failures",
            "cancelled",
            "target_elapsed_ns",
            "worker_index",
            "release_ns",
        }:
            raw_count = raw_counts[field]
            if isinstance(raw_count, bool) or not isinstance(raw_count, int):
                raise RunnerContractError("fanin_worker_ramp_proof_invalid")
            counts[field] = raw_count
        for field in {"run_id", "lane_id"}:
            raw_identity = raw_counts[field]
            if not isinstance(raw_identity, str) or not raw_identity:
                raise RunnerContractError("fanin_worker_ramp_proof_invalid")
            counts[field] = raw_identity
        if (
            counts["initiated"] != fanin.PARTITION_CLIENTS_PER_LANE
            or counts["authenticated"] != fanin.PARTITION_CLIENTS_PER_LANE
            or counts["held"] != fanin.PARTITION_CLIENTS_PER_LANE
            or counts["terminal_failures"] != 0
            or counts["cancelled"] != 0
            or int(counts["target_elapsed_ns"]) <= 0
            or counts["lane_id"] != lane_id
            or (expected_run_id is not None and counts["run_id"] != expected_run_id)
            or (
                expected_worker_index is not None
                and counts["worker_index"] != expected_worker_index
            )
            or (expected_release_ns is not None and counts["release_ns"] != expected_release_ns)
        ):
            raise RunnerContractError("fanin_worker_ramp_proof_invalid")
        proof[lane_id] = counts
    return proof


def _worker_stop_envelope(
    result: Mapping[str, object],
    *,
    outcome: str,
    worker_index: int,
    worker_cpu: int,
) -> dict[str, object]:
    raw_lanes = result.get("lanes")
    lanes = []
    if isinstance(raw_lanes, list):
        for raw_lane in raw_lanes:
            if not isinstance(raw_lane, Mapping):
                continue
            lane_id = str(raw_lane.get("lane_id") or "")
            if lane_id not in RUNTIME_LANE_IDS:
                continue
            lanes.append(
                {
                    "lane_id": lane_id,
                    "initiated": int(raw_lane.get("initiated_clients", 0)),
                    "authenticated": int(raw_lane.get("authenticated_clients", 0)),
                    "cancelled": int(raw_lane.get("cancelled_clients", 0)),
                    "held": int(raw_lane.get("held_clients_at_gate", 0)),
                    "terminal_failures": int(raw_lane.get("terminal_failures", 0)),
                    "failure_codes": dict(raw_lane.get("failure_codes") or {}),
                    "samples_succeeded": int(raw_lane.get("sampled_queries_succeeded", 0)),
                }
            )
    telemetry = result.get("telemetry")
    diagnostics = result.get("runtime_diagnostics")
    failure_values = (
        telemetry.get("telemetry_failures", []) if isinstance(telemetry, Mapping) else []
    )
    envelopes = (
        diagnostics.get("significant_stall_envelopes", [])
        if isinstance(diagnostics, Mapping)
        else []
    )
    worst_stall = envelopes[0] if isinstance(envelopes, list) and envelopes else None
    return {
        "worker_id": worker_index,
        "worker_cpu": worker_cpu,
        "outcome": outcome,
        "lanes": lanes,
        "telemetry_failures": sorted(str(value) for value in failure_values),
        "telemetry_peak_generator_owned_loop_lag_ms": (
            float(
                diagnostics.get("peak_generator_owned_loop_lag_ms", 0.0)
                if isinstance(diagnostics, Mapping)
                else 0.0
            )
        ),
        "worst_stall": worst_stall,
    }


def _fanin_worker_process(
    request: Mapping[str, object],
    worker_index: int,
    control_queue: Any,
    result_queue: Any,
    release_event: Any,
    release_ns: Any,
    hold_prepare_event: Any,
    hold_epoch_event: Any,
    hold_ns: Any,
    sample_release_event: Any,
    teardown_event: Any,
    cancel_event: Any,
) -> None:
    worker_cpu = _pin_fanin_worker(worker_index)

    def publish_worker_progress(value: Mapping[str, object]) -> None:
        started_ns = time.perf_counter_ns()
        snapshot_fields = {
            name: value.get(name)
            for name in (
                "phase",
                "initiated_clients",
                "authenticated_clients",
                "held_clients",
                "peak_authenticated_clients",
                "peak_held_clients",
                "terminal_failures",
                "sampled_queries_attempted",
                "sampled_queries_succeeded",
                "sampled_queries_failed",
                "retries",
                "cancelled_clients",
                "disconnected_during_hold",
                "failure_codes",
                "observer_sample_attempts",
                "observer_sample_failures",
                "observer_reconnect_attempts",
                "observer_reconnect_successes",
                "observer_failure_codes",
            )
        }
        enriched = {
            **dict(value),
            "release_ns": int(release_ns.value),
            "hold_ns": int(hold_ns.value) if int(hold_ns.value) > 0 else None,
            "snapshot_epoch": hashlib.sha256(_canonical_json(snapshot_fields)).hexdigest(),
        }
        control_queue.put(
            (
                "milestone" if value.get("milestone") else "progress",
                worker_index,
                enriched,
            )
        )
        fanin._record_phase("worker_progress_queue_write", started_ns)

    fanin._progress_callback = publish_worker_progress

    async def run() -> dict[str, object]:
        cancelled = asyncio.Event()
        owner = asyncio.current_task()
        assert owner is not None

        async def watch_cancel() -> None:
            await _await_process_event(cancel_event)
            cancelled.set()
            # Cooperative checks exist between waves, but a connect wave can be waiting on
            # network I/O and the hold sampler can be sleeping.  Cancel the owning coroutine
            # as well so a towel enters execute_fanin's shielded socket cleanup immediately
            # instead of waiting for the next phase boundary.
            owner.cancel()

        async def await_release() -> int:
            control_queue.put(
                (
                    "ready",
                    worker_index,
                    {
                        "run_id": str(request.get("run_id") or ""),
                        "worker_index": worker_index,
                        "worker_pid": os.getpid(),
                        "runner_harness_sha256": LOADED_RUNNER_HARNESS_SHA256,
                        "request_sha256": str(request.get("prepared_request_digest") or ""),
                    },
                )
            )
            await _await_process_event(release_event, cancel_event=cancel_event)
            return int(release_ns.value)

        async def await_hold(
            proof_reader: Callable[[], Mapping[str, Mapping[str, object]]],
        ) -> int:
            proof_identity = {
                "expected_run_id": str(request.get("run_id") or ""),
                "expected_worker_index": worker_index,
                "expected_release_ns": int(release_ns.value),
            }
            initial_proof = _validated_ramp_proof(
                proof_reader(),
                **proof_identity,
            )
            control_queue.put(("ramp_ready", worker_index, initial_proof))
            await _await_process_event(
                hold_prepare_event,
                cancel_event=cancel_event,
            )
            # This is a second observation, after all four initial proofs have
            # crossed the parent queue.  A socket lost at the barrier therefore
            # prevents the hold epoch instead of being hidden by the earlier
            # count.
            prepared_proof = _validated_ramp_proof(
                proof_reader(),
                **proof_identity,
            )
            control_queue.put(("hold_prepared", worker_index, prepared_proof))
            await _await_process_event(hold_epoch_event, cancel_event=cancel_event)
            shared_hold_ns = int(hold_ns.value)
            if shared_hold_ns <= int(release_ns.value):
                raise RunnerContractError("fanin_worker_hold_epoch_invalid")
            committed_proof = _validated_ramp_proof(
                proof_reader(),
                **proof_identity,
            )
            control_queue.put(
                (
                    "hold_committed",
                    worker_index,
                    committed_proof,
                    shared_hold_ns,
                )
            )
            await _await_process_event(
                sample_release_event,
                cancel_event=cancel_event,
            )
            return shared_hold_ns

        async def before_teardown() -> None:
            control_queue.put(("teardown_ready", worker_index))
            await _await_process_event(
                teardown_event,
                cancel_event=cancel_event,
            )

        watcher = asyncio.create_task(watch_cancel())
        try:
            result = await fanin.execute_fanin(
                request,
                cancelled=cancelled,
                trust_bundle_path=TRUST_BUNDLE_PATH,
                worker_index=worker_index,
                worker_count=fanin.WORKER_COUNT,
                partition_target_clients=fanin.PARTITION_CLIENTS_PER_LANE,
                await_release=await_release,
                await_hold=await_hold,
                before_teardown=before_teardown,
                worker_cpu=worker_cpu,
                capacity_preflight_verified=True,
            )
            return result
        finally:
            watcher.cancel()
            await asyncio.gather(watcher, return_exceptions=True)

    try:
        result = asyncio.run(run())
        outcome = str(result.get("worker_outcome") or "partial_result")
        if outcome not in {
            "completed",
            "partial_result",
            "hard_safety_failed",
            "cancelled",
        }:
            outcome = "partial_result"
        if outcome != "completed":
            code = "fanin_cancelled" if outcome == "cancelled" else f"fanin_worker_{outcome}"
            envelope = _worker_stop_envelope(
                result,
                outcome=outcome,
                worker_index=worker_index,
                worker_cpu=worker_cpu,
            )
            control_queue.put((outcome, worker_index, code, envelope))
            result_queue.put((outcome, worker_index, code, envelope))
            return
        queue_started_ns = time.perf_counter_ns()
        result_queue.put(("completed", worker_index, result))
        queue_elapsed_ms = (time.perf_counter_ns() - queue_started_ns) / 1_000_000
        print(
            WORKER_RESULT_QUEUE_PROFILE_PREFIX
            + json.dumps(
                {
                    "worker_id": worker_index,
                    "worker_cpu": worker_cpu,
                    "elapsed_ms": queue_elapsed_ms,
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
            flush=True,
        )
        fanin._worker_execution_context = None
    except BaseException as exc:
        code = str(exc)
        if not code or len(code) > 96 or not code.replace("_", "").isalnum():
            code = type(exc).__name__
        if isinstance(exc, (RunnerCancelled, asyncio.CancelledError)):
            code = "fanin_cancelled"
            control_queue.put(("cancelled", worker_index, code))
            result_queue.put(("cancelled", worker_index, code))
            return
        envelope = _sanitized_worker_crash(
            exc,
            worker_index=worker_index,
            worker_cpu=worker_cpu,
        )
        control_queue.put(("crashed", worker_index, code, envelope))
        result_queue.put(("crashed", worker_index, code, envelope))
    finally:
        fanin._progress_callback = None


def _pin_fanin_worker(worker_index: int) -> int:
    if not hasattr(os, "sched_getaffinity") or not hasattr(os, "sched_setaffinity"):
        raise RunnerContractError("fanin_worker_affinity_unavailable")
    available = sorted(os.sched_getaffinity(0))
    if len(available) < fanin.WORKER_COUNT:
        raise RunnerContractError("fanin_worker_affinity_insufficient")
    worker_cpu = int(available[worker_index])
    os.sched_setaffinity(0, {worker_cpu})
    if os.sched_getaffinity(0) != {worker_cpu}:
        raise RunnerContractError("fanin_worker_affinity_failed")
    return worker_cpu


def _resident_worker_entry(
    request_queue: Any,
    preload_ready_queue: Any,
    worker_index: int,
    control_queue: Any,
    result_queue: Any,
    release_event: Any,
    release_ns: Any,
    hold_prepare_event: Any,
    hold_epoch_event: Any,
    hold_ns: Any,
    sample_release_event: Any,
    teardown_event: Any,
    cancel_event: Any,
) -> None:
    preload_ready_queue.put(
        (
            "worker_ready",
            worker_index,
            os.getpid(),
            LOADED_RUNNER_HARNESS_SHA256,
        )
    )
    request = request_queue.get()
    if not isinstance(request, Mapping):
        raise RunnerContractError("resident_worker_request_invalid")
    _fanin_worker_process(
        request,
        worker_index,
        control_queue,
        result_queue,
        release_event,
        release_ns,
        hold_prepare_event,
        hold_epoch_event,
        hold_ns,
        sample_release_event,
        teardown_event,
        cancel_event,
    )


@dataclass
class ResidentShardPool:
    """Four imported, pinned-capable workers waiting before READY is published."""

    context: Any
    control_queue: Any
    result_queue: Any
    release_event: Any
    hold_prepare_event: Any
    hold_epoch_event: Any
    sample_release_event: Any
    teardown_event: Any
    cancel_event: Any
    release_ns: Any
    hold_ns: Any
    request_queues: list[Any]
    processes: list[Any]
    worker_ready_indexes: tuple[int, ...]

    @classmethod
    def start(cls) -> ResidentShardPool:
        context = mp.get_context("spawn")
        control_queue = context.Queue()
        result_queue = context.Queue()
        release_event = context.Event()
        hold_prepare_event = context.Event()
        hold_epoch_event = context.Event()
        sample_release_event = context.Event()
        teardown_event = context.Event()
        cancel_event = context.Event()
        release_ns = context.Value("q", 0)
        hold_ns = context.Value("q", 0)
        request_queues = [context.Queue(maxsize=1) for _ in range(fanin.WORKER_COUNT)]
        preload_ready_queue = context.Queue()
        processes = [
            context.Process(
                target=_resident_worker_entry,
                args=(
                    request_queues[index],
                    preload_ready_queue,
                    index,
                    control_queue,
                    result_queue,
                    release_event,
                    release_ns,
                    hold_prepare_event,
                    hold_epoch_event,
                    hold_ns,
                    sample_release_event,
                    teardown_event,
                    cancel_event,
                ),
                name=f"round5-resident-fanin-{index}",
            )
            for index in range(fanin.WORKER_COUNT)
        ]
        for process in processes:
            process.start()
        ready: dict[int, tuple[int, str]] = {}
        try:
            while len(ready) < fanin.WORKER_COUNT:
                message = preload_ready_queue.get(timeout=30)
                if (
                    not isinstance(message, tuple)
                    or len(message) != 4
                    or message[0] != "worker_ready"
                    or isinstance(message[1], bool)
                    or not isinstance(message[1], int)
                    or message[1] not in range(fanin.WORKER_COUNT)
                    or message[1] in ready
                    or isinstance(message[2], bool)
                    or not isinstance(message[2], int)
                    or message[2] <= 0
                    or message[3] != LOADED_RUNNER_HARNESS_SHA256
                ):
                    raise RunnerContractError("resident_worker_ready_invalid")
                ready[message[1]] = (message[2], message[3])
        except BaseException:
            for process in processes:
                if process.is_alive():
                    process.terminate()
                process.join(5)
            raise
        return cls(
            context=context,
            control_queue=control_queue,
            result_queue=result_queue,
            release_event=release_event,
            hold_prepare_event=hold_prepare_event,
            hold_epoch_event=hold_epoch_event,
            sample_release_event=sample_release_event,
            teardown_event=teardown_event,
            cancel_event=cancel_event,
            release_ns=release_ns,
            hold_ns=hold_ns,
            request_queues=request_queues,
            processes=processes,
            worker_ready_indexes=tuple(sorted(ready)),
        )

    def submit(self, request: Mapping[str, object]) -> None:
        for request_queue in self.request_queues:
            request_queue.put(dict(request))


async def _execute_sharded_fanin(
    request: Mapping[str, object],
    cancelled: asyncio.Event,
    *,
    resident_pool: ResidentShardPool | None = None,
    resident_release_gate: asyncio.Event | None = None,
    on_resident_prepared: Callable[[], Awaitable[None]] | None = None,
) -> dict[str, object]:
    context = resident_pool.context if resident_pool is not None else mp.get_context("spawn")
    control_queue = resident_pool.control_queue if resident_pool is not None else context.Queue()
    result_queue = resident_pool.result_queue if resident_pool is not None else context.Queue()
    release_event = resident_pool.release_event if resident_pool is not None else context.Event()
    hold_prepare_event = (
        resident_pool.hold_prepare_event if resident_pool is not None else context.Event()
    )
    hold_epoch_event = (
        resident_pool.hold_epoch_event if resident_pool is not None else context.Event()
    )
    sample_release_event = (
        resident_pool.sample_release_event if resident_pool is not None else context.Event()
    )
    teardown_event = resident_pool.teardown_event if resident_pool is not None else context.Event()
    cancel_event = resident_pool.cancel_event if resident_pool is not None else context.Event()
    release_ns = resident_pool.release_ns if resident_pool is not None else context.Value("q", 0)
    hold_ns = resident_pool.hold_ns if resident_pool is not None else context.Value("q", 0)
    processes = (
        resident_pool.processes
        if resident_pool is not None
        else [
            context.Process(
                target=_fanin_worker_process,
                args=(
                    request,
                    index,
                    control_queue,
                    result_queue,
                    release_event,
                    release_ns,
                    hold_prepare_event,
                    hold_epoch_event,
                    hold_ns,
                    sample_release_event,
                    teardown_event,
                    cancel_event,
                ),
                name=f"round5-fanin-{index}",
            )
            for index in range(fanin.WORKER_COUNT)
        ]
    )
    if resident_pool is not None:
        resident_pool.submit(request)
    latest_progress: dict[tuple[int, str], Mapping[str, object]] = {}
    peak_held_by_worker_lane: dict[tuple[int, str], int] = {}
    latest_system_socket_states: Mapping[str, object] = {}
    parent_profile: dict[str, dict[str, float | int]] = {}
    hold_committed = False
    target_elapsed_ms: float | None = None
    first_socket_ns: int | None = None
    first_authentication_ns: int | None = None
    first_socket_by_worker: dict[int, int] = {}
    first_authentication_by_worker: dict[int, int] = {}
    barrier_state = "READY"
    # The scored run budget must originate at the authoritative release (T0), not
    # here at resident stage.  A competitor lane dwells minutes behind the Proxy
    # exact gate between "ready" and release; anchoring FANIN_WORKER_RUN_TIMEOUT
    # at stage burned that budget on the wait and made await_stage("ramp_ready")
    # time out mechanically the instant release fired.  Pre-release, run_deadline
    # holds only the readiness budget (process spawn + observer connect/quiesce);
    # it is re-based off the release instant below.  See _release_run below.
    stage_entered_at = asyncio.get_running_loop().time()
    ready_reached_at: float | None = None
    run_deadline = stage_entered_at + FANIN_WORKER_READY_BUDGET_SECONDS
    parent_safety = fanin.TelemetrySummary()
    last_parent_safety_at = 0.0
    teardown_ready: set[int] = set()
    teardown_safety_sampled = False

    async def sample_parent_safety(*, force: bool = False) -> None:
        nonlocal last_parent_safety_at
        now = asyncio.get_running_loop().time()
        if not force and now - last_parent_safety_at < fanin.RESOURCE_TELEMETRY_INTERVAL_SECONDS:
            return
        live_processes = [process for process in processes if process.is_alive()]
        if len(live_processes) != fanin.WORKER_COUNT:
            return
        process_ids = [
            int(process.pid)
            for process in live_processes
            if isinstance(getattr(process, "pid", None), int)
            and not isinstance(process.pid, bool)
            and process.pid > 0
        ]
        if len(process_ids) != fanin.WORKER_COUNT:
            return
        parent_safety.observe(await asyncio.to_thread(fanin.host_safety_telemetry, process_ids))
        last_parent_safety_at = now
        if parent_safety.hard_failures:
            cancel_event.set()
            raise RunnerContractError(sorted(parent_safety.hard_failures)[0])

    def transition_barrier(expected: str, next_state: str) -> None:
        nonlocal barrier_state
        if barrier_state != expected:
            raise RunnerContractError("fanin_parent_barrier_state_invalid")
        barrier_state = next_state

    def record_parent_phase(phase: str, started_ns: int) -> None:
        elapsed_ms = (time.perf_counter_ns() - started_ns) / 1_000_000
        value = parent_profile.setdefault(
            phase,
            {"calls": 0, "total_ms": 0.0, "max_ms": 0.0},
        )
        value["calls"] = int(value["calls"]) + 1
        value["total_ms"] = float(value["total_ms"]) + elapsed_ms
        value["max_ms"] = max(float(value["max_ms"]), elapsed_ms)

    def publish_progress(index: int, value: object) -> None:
        nonlocal latest_system_socket_states, target_elapsed_ms
        started_ns = time.perf_counter_ns()
        if (
            index not in range(fanin.WORKER_COUNT)
            or not isinstance(value, Mapping)
            or value.get("protocol") != fanin.PROTOCOL
            or value.get("schema_version") != fanin.SCHEMA_VERSION
        ):
            raise RunnerContractError("fanin_worker_progress_invalid")
        lane_id = str(value.get("lane_id") or "")
        if lane_id not in RUNTIME_LANE_IDS:
            raise RunnerContractError("fanin_worker_progress_invalid")
        bounded_counts: dict[str, int] = {}
        for field, maximum in (
            ("initiated_clients", fanin.PARTITION_CLIENTS_PER_LANE),
            ("authenticated_clients", fanin.PARTITION_CLIENTS_PER_LANE),
            ("held_clients", fanin.PARTITION_CLIENTS_PER_LANE),
            ("terminal_failures", fanin.PARTITION_CLIENTS_PER_LANE),
            ("sampled_queries_succeeded", fanin.CLIENTS_PER_SAMPLE_GROUP * 2),
            ("sampled_queries_failed", fanin.CLIENTS_PER_SAMPLE_GROUP * 2),
        ):
            raw = value.get(field, 0)
            if isinstance(raw, bool) or not isinstance(raw, int) or not 0 <= raw <= maximum:
                raise RunnerContractError("fanin_worker_progress_invalid")
            bounded_counts[field] = raw
        prior = latest_progress.get((index, lane_id))
        if prior is not None and any(
            bounded_counts[field] < int(prior.get(field, 0))
            for field in (
                "initiated_clients",
                "authenticated_clients",
                "terminal_failures",
                "sampled_queries_succeeded",
                "sampled_queries_failed",
            )
        ):
            raise RunnerContractError("fanin_worker_progress_regressed")
        socket_states = value.get("socket_states")
        if index == 0 and isinstance(socket_states, Mapping) and socket_states:
            latest_system_socket_states = dict(socket_states)
        latest_progress[(index, lane_id)] = value
        peak_held_by_worker_lane[(index, lane_id)] = max(
            peak_held_by_worker_lane.get((index, lane_id), 0),
            bounded_counts["held_clients"],
        )
        if not hold_committed and (
            bounded_counts["sampled_queries_succeeded"] or bounded_counts["sampled_queries_failed"]
        ):
            raise RunnerContractError("fanin_worker_progress_before_hold")
        peers = [
            latest_progress.get((worker_index, lane_id), {})
            for worker_index in range(fanin.WORKER_COUNT)
        ]
        snapshot_epochs = {str(peer.get("snapshot_epoch") or "") for peer in peers if peer}
        if (
            any(not peer for peer in peers)
            or len(snapshot_epochs) != 1
            or not next(iter(snapshot_epochs))
            or any(int(peer.get("release_ns") or 0) != int(release_ns.value) for peer in peers)
            or any(peer.get("hold_ns") not in {None, int(hold_ns.value)} for peer in peers)
        ):
            return
        aggregate = dict(value)
        for field in (
            "initiated_clients",
            "authenticated_clients",
            "held_clients",
            "terminal_failures",
            "sampled_queries_succeeded",
            "sampled_queries_failed",
        ):
            aggregate[field] = sum(int(peer.get(field, 0)) for peer in peers)
        aggregate["peak_held_clients"] = sum(
            peak_held_by_worker_lane.get((worker_index, lane_id), 0)
            for worker_index in range(fanin.WORKER_COUNT)
        )
        if latest_system_socket_states:
            aggregate["socket_states"] = dict(latest_system_socket_states)
        # Phase and target time belong to the parent barrier, never to one
        # shard's local projection.  Until all four fresh prepare proofs have
        # committed, 2,500 on one worker is still ramping—not a held 10K gate.
        aggregate["phase"] = "holding" if hold_committed else "ramping"
        aggregate["time_to_target_ms"] = target_elapsed_ms
        # This clock is parent-owned. A shard's wall/loop scheduling cannot
        # move the aggregate elapsed value forward or backward.
        aggregate["elapsed_ms"] = (
            max(0.0, (time.monotonic_ns() - int(release_ns.value)) / 1_000_000)
            if int(release_ns.value) > 0
            else 0.0
        )
        aggregate["hold_remaining_ms"] = (
            max(
                0.0,
                (int(hold_ns.value) + fanin.HOLD_SECONDS * 1_000_000_000 - time.monotonic_ns())
                / 1_000_000,
            )
            if hold_committed
            else None
        )
        aggregate["first_socket_initiated_ms"] = (
            (first_socket_ns - int(release_ns.value)) / 1_000_000
            if first_socket_ns is not None
            else None
        )
        aggregate["first_client_authenticated_ms"] = (
            (first_authentication_ns - int(release_ns.value)) / 1_000_000
            if first_authentication_ns is not None
            else None
        )
        fanin._progress(aggregate)
        record_parent_phase("progress_snapshot_aggregate_json", started_ns)

    def observe_milestone(index: int, value: object) -> None:
        nonlocal first_socket_ns, first_authentication_ns
        if (
            index not in range(fanin.WORKER_COUNT)
            or not isinstance(value, Mapping)
            or value.get("protocol") != fanin.PROTOCOL
            or value.get("schema_version") != fanin.SCHEMA_VERSION
            or value.get("lane_id") not in RUNTIME_LANE_IDS
        ):
            raise RunnerContractError("fanin_worker_milestone_invalid")
        milestone = value.get("milestone")
        observed_ns = value.get("milestone_monotonic_ns")
        if (
            milestone not in {"first_socket_initiated", "first_client_authenticated"}
            or isinstance(observed_ns, bool)
            or not isinstance(observed_ns, int)
            or observed_ns < int(release_ns.value)
        ):
            raise RunnerContractError("fanin_worker_milestone_invalid")
        if milestone == "first_socket_initiated":
            prior = first_socket_by_worker.setdefault(index, observed_ns)
            if prior != observed_ns:
                raise RunnerContractError("fanin_worker_milestone_changed")
            if len(first_socket_by_worker) == fanin.WORKER_COUNT and first_socket_ns is None:
                first_socket_ns = min(first_socket_by_worker.values())
        else:
            prior = first_authentication_by_worker.setdefault(index, observed_ns)
            if prior != observed_ns:
                raise RunnerContractError("fanin_worker_milestone_changed")
            if (
                len(first_authentication_by_worker) == fanin.WORKER_COUNT
                and first_authentication_ns is None
            ):
                first_authentication_ns = min(first_authentication_by_worker.values())

    def raise_worker_outcome(kind: str, detail: list[object]) -> None:
        cancel_event.set()
        if kind == "crashed" and len(detail) > 1:
            _preserve_worker_crash(detail[1])
        elif kind in {"partial_result", "hard_safety_failed"} and len(detail) > 1:
            _preserve_worker_stop(detail[1])
        code = str(detail[0]) if detail else f"fanin_worker_{kind}"
        envelope = detail[1] if len(detail) > 1 and isinstance(detail[1], Mapping) else {}
        if kind == "hard_safety_failed":
            hard_codes = envelope.get("telemetry_failures", [])
            if isinstance(hard_codes, list) and len(hard_codes) == 1:
                candidate = str(hard_codes[0])
                if fanin.classify_safety_code(candidate) == "hard":
                    code = candidate
        elif kind == "partial_result":
            lanes = envelope.get("lanes", [])
            if isinstance(lanes, list):
                connection_codes = sorted(
                    str(candidate)
                    for lane in lanes
                    if isinstance(lane, Mapping) and isinstance(lane.get("failure_codes"), Mapping)
                    for candidate, count in lane["failure_codes"].items()
                    if isinstance(count, int) and count > 0
                )
                if connection_codes:
                    code = connection_codes[0]
        if kind == "cancelled" and cancelled.is_set():
            raise RunnerCancelled("fanin_cancelled")
        raise RunnerContractError(code)

    async def await_stage(
        stage: str,
    ) -> dict[int, dict[str, dict[str, object]]]:
        observed: set[int] = set()
        proofs: dict[int, dict[str, dict[str, object]]] = {}
        while len(observed) < fanin.WORKER_COUNT:
            await sample_parent_safety()
            if cancelled.is_set():
                raise RunnerCancelled("fanin_cancelled")
            if asyncio.get_running_loop().time() >= run_deadline:
                raise RunnerContractError(f"fanin_worker_{stage}_timeout")
            try:
                message = await asyncio.to_thread(
                    control_queue.get,
                    True,
                    0.25,
                )
            except queue.Empty:
                crashed = [
                    process.name for process in processes if process.exitcode not in (None, 0)
                ]
                if crashed:
                    raise RunnerContractError("fanin_worker_crashed") from None
                continue
            if not isinstance(message, tuple) or len(message) < 2:
                raise RunnerContractError("fanin_worker_message_invalid")
            kind, raw_index, *detail = message
            index = int(raw_index)
            if kind == "progress":
                publish_progress(index, detail[0] if detail else None)
                continue
            if kind == "milestone":
                observe_milestone(index, detail[0] if detail else None)
                continue
            if kind == "teardown_ready":
                if index not in range(fanin.WORKER_COUNT) or index in teardown_ready:
                    raise RunnerContractError("fanin_worker_teardown_barrier_invalid")
                teardown_ready.add(index)
                await sample_parent_safety(force=True)
                teardown_event.set()
                continue
            if kind in {"partial_result", "hard_safety_failed", "cancelled", "crashed"}:
                raise_worker_outcome(str(kind), detail)
            if kind != stage or index not in range(fanin.WORKER_COUNT):
                raise RunnerContractError(f"fanin_worker_barrier_invalid_{kind}"[:96])
            if stage in {"ramp_ready", "hold_prepared", "hold_committed"}:
                proof = _validated_ramp_proof(
                    detail[0] if detail else None,
                    expected_run_id=str(request.get("run_id") or ""),
                    expected_worker_index=index,
                    expected_release_ns=int(release_ns.value),
                )
                expected_lanes = {
                    str(target.get("lane_id") or "")
                    for target in request.get("targets", [])
                    if isinstance(target, Mapping)
                }
                if set(proof) != expected_lanes:
                    raise RunnerContractError("fanin_worker_ramp_proof_invalid")
                if stage == "hold_committed" and (
                    len(detail) != 2
                    or isinstance(detail[1], bool)
                    or not isinstance(detail[1], int)
                    or int(detail[1]) != int(hold_ns.value)
                ):
                    raise RunnerContractError("fanin_worker_hold_commit_invalid")
                proofs[index] = proof
            elif stage == "ready" and resident_pool is not None:
                ready = detail[0] if detail else None
                if (
                    not isinstance(ready, Mapping)
                    or ready.get("run_id") != request.get("run_id")
                    or ready.get("worker_index") != index
                    or isinstance(ready.get("worker_pid"), bool)
                    or not isinstance(ready.get("worker_pid"), int)
                    or int(ready["worker_pid"]) <= 0
                    or ready.get("runner_harness_sha256") != LOADED_RUNNER_HARNESS_SHA256
                    or ready.get("request_sha256")
                    != str(request.get("prepared_request_digest") or "")
                ):
                    raise RunnerContractError("fanin_worker_ready_proof_invalid")
            if index in observed:
                raise RunnerContractError("fanin_worker_barrier_duplicate")
            observed.add(index)
        return proofs

    if resident_pool is None:
        for process in processes:
            process.start()
    try:
        await sample_parent_safety(force=True)
        await await_stage("ready")
        ready_reached_at = asyncio.get_running_loop().time()
        if on_resident_prepared is not None:
            await on_resident_prepared()
        if resident_release_gate is not None:
            # Wait for release-or-cancel WITHOUT subtracting the scored run
            # budget.  This dwell is the competitor's Proxy build (~10-11 min);
            # it is not scored and must not consume ramp/hold time.  The outer
            # resident/SSM control plane owns the ceiling on this wait; a towel
            # sets `cancelled`, which settles this promptly via cancel_wait.
            release_wait = asyncio.create_task(resident_release_gate.wait())
            cancel_wait = asyncio.create_task(cancelled.wait())
            try:
                done, _pending = await asyncio.wait(
                    (release_wait, cancel_wait),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if cancel_wait in done and cancelled.is_set():
                    raise RunnerCancelled("fanin_cancelled")
            finally:
                release_wait.cancel()
                cancel_wait.cancel()
                await asyncio.gather(
                    release_wait,
                    cancel_wait,
                    return_exceptions=True,
                )
        with release_ns.get_lock():
            if release_ns.value != 0:
                raise RunnerContractError("fanin_release_epoch_already_set")
            release_ns.value = time.monotonic_ns()
        # Authoritative release (T0).  The scored run budget originates HERE.
        released_at = asyncio.get_running_loop().time()
        run_deadline = released_at + FANIN_WORKER_RUN_TIMEOUT_SECONDS
        # Pre-release parent telemetry (the readiness barrier and the unscored
        # Proxy dwell) must not contaminate the scored ramp telemetry window.
        parent_safety = fanin.TelemetrySummary()
        last_parent_safety_at = 0.0
        # Cheap structured deadline trace: proves stage->ready and the unscored
        # ready->release dwell, and that the ramp deadline is release-anchored.
        print(
            "FANIN_RELEASE_TRACE_JSON:"
            + _canonical_json(
                {
                    "worker_count": fanin.WORKER_COUNT,
                    "stage_to_ready_ms": round(
                        ((ready_reached_at or released_at) - stage_entered_at) * 1000.0,
                        3,
                    ),
                    "ready_to_release_ms": round(
                        (released_at - (ready_reached_at or released_at)) * 1000.0,
                        3,
                    ),
                    "run_budget_seconds": FANIN_WORKER_RUN_TIMEOUT_SECONDS,
                    "deadline_origin": "release",
                }
            ).decode("utf-8"),
            flush=True,
        )
        transition_barrier("READY", "RELEASED")
        release_event.set()
        ramp_proofs = await await_stage("ramp_ready")
        committed_target_candidate_ms = max(
            int(lane["target_elapsed_ns"]) / 1_000_000
            for proof in ramp_proofs.values()
            for lane in proof.values()
        )
        transition_barrier("RELEASED", "RAMP_READY")
        hold_prepare_event.set()
        prepared_proofs = await await_stage("hold_prepared")
        if prepared_proofs != ramp_proofs:
            raise RunnerContractError("fanin_worker_hold_prepare_mismatch")
        transition_barrier("RAMP_READY", "PREPARED")
        with hold_ns.get_lock():
            hold_ns.value = time.monotonic_ns()
        hold_epoch_event.set()
        committed_proofs = await await_stage("hold_committed")
        if cancelled.is_set() or cancel_event.is_set():
            raise RunnerCancelled("fanin_cancelled")
        if committed_proofs != prepared_proofs:
            raise RunnerContractError("fanin_worker_hold_commit_mismatch")
        target_elapsed_ms = max(
            int(lane["target_elapsed_ns"]) / 1_000_000
            for proof in committed_proofs.values()
            for lane in proof.values()
        )
        if target_elapsed_ms != committed_target_candidate_ms:
            raise RunnerContractError("fanin_worker_target_epoch_changed")
        hold_committed = True
        transition_barrier("PREPARED", "COMMITTED")
        for lane_id in {
            str(target.get("lane_id") or "")
            for target in request.get("targets", [])
            if isinstance(target, Mapping)
        }:
            speaker = next(
                (
                    (index, latest_progress[(index, lane_id)])
                    for index in range(fanin.WORKER_COUNT)
                    if (index, lane_id) in latest_progress
                ),
                None,
            )
            if speaker is not None:
                publish_progress(*speaker)
        transition_barrier("COMMITTED", "SAMPLING")
        sample_release_event.set()
        results: dict[int, Mapping[str, object]] = {}
        while len(results) < fanin.WORKER_COUNT:
            await sample_parent_safety()
            if cancelled.is_set():
                raise RunnerCancelled("fanin_cancelled")
            if asyncio.get_running_loop().time() >= run_deadline:
                raise RunnerContractError("fanin_worker_result_timeout")
            while True:
                try:
                    control_message = control_queue.get_nowait()
                except queue.Empty:
                    break
                if (
                    isinstance(control_message, tuple)
                    and len(control_message) == 3
                    and control_message[0] in {"progress", "milestone"}
                ):
                    if control_message[0] == "progress":
                        publish_progress(
                            int(control_message[1]),
                            control_message[2],
                        )
                    else:
                        observe_milestone(
                            int(control_message[1]),
                            control_message[2],
                        )
                elif isinstance(control_message, tuple) and len(control_message) >= 2:
                    kind, _raw_index, *detail = control_message
                    if kind == "teardown_ready":
                        worker_index = int(_raw_index)
                        if (
                            worker_index not in range(fanin.WORKER_COUNT)
                            or worker_index in teardown_ready
                        ):
                            raise RunnerContractError("fanin_worker_teardown_barrier_invalid")
                        teardown_ready.add(worker_index)
                        if len(teardown_ready) == fanin.WORKER_COUNT:
                            await sample_parent_safety(force=True)
                            teardown_safety_sampled = True
                            teardown_event.set()
                        continue
                    if kind in {
                        "partial_result",
                        "hard_safety_failed",
                        "cancelled",
                        "crashed",
                    }:
                        raise_worker_outcome(str(kind), detail)
            try:
                message = await asyncio.to_thread(
                    result_queue.get,
                    True,
                    0.25,
                )
            except queue.Empty:
                crashed = [
                    process.name for process in processes if process.exitcode not in (None, 0)
                ]
                if crashed:
                    raise RunnerContractError("fanin_worker_crashed") from None
                continue
            if not isinstance(message, tuple) or len(message) not in {3, 4}:
                raise RunnerContractError("fanin_worker_result_invalid")
            if message[0] in {
                "partial_result",
                "hard_safety_failed",
                "cancelled",
                "crashed",
            }:
                raise_worker_outcome(str(message[0]), list(message[2:]))
            if message[0] != "completed" or not isinstance(message[2], Mapping):
                raise RunnerContractError("fanin_worker_result_invalid")
            index = int(message[1])
            if index not in range(fanin.WORKER_COUNT) or index in results:
                raise RunnerContractError("fanin_worker_result_invalid")
            results[index] = message[2]
        for index, result in results.items():
            if (
                result.get("run_id") != request.get("run_id")
                or result.get("worker_index") != index
                or result.get("worker_count") != fanin.WORKER_COUNT
                or result.get("release_ns") != int(release_ns.value)
                or result.get("hold_ns") != int(hold_ns.value)
            ):
                raise RunnerContractError("fanin_worker_result_epoch_identity_invalid")
            lanes = result.get("lanes")
            if not isinstance(lanes, list):
                raise RunnerContractError("fanin_worker_result_invalid")
            by_lane = {
                str(lane.get("lane_id") or ""): lane for lane in lanes if isinstance(lane, Mapping)
            }
            if set(by_lane) != set(ramp_proofs[index]):
                raise RunnerContractError("fanin_worker_result_ramp_proof_mismatch")
            if any(
                int(by_lane[lane_id].get("time_to_target_ns") or 0)
                != int(proof["target_elapsed_ns"])
                for lane_id, proof in ramp_proofs[index].items()
            ):
                raise RunnerContractError("fanin_worker_result_ramp_proof_mismatch")
        if not teardown_safety_sampled:
            raise RunnerContractError("fanin_teardown_safety_evidence_missing")
        aggregate_started_ns = time.perf_counter_ns()
        aggregate = fanin.aggregate_worker_results(
            [results[index] for index in range(fanin.WORKER_COUNT)]
        )
        transition_barrier("SAMPLING", "COMPLETE")
        aggregate["barrier_state"] = barrier_state
        aggregate["hold_commit_count"] = 1
        if parent_safety.samples:
            host_safety = parent_safety.public_dict()
            host_fields = {
                name: host_safety[name]
                for name in (
                    "safety_evidence_version",
                    "telemetry_samples",
                    "telemetry_physical_memory_bytes",
                    "telemetry_min_available_memory_bytes",
                    "telemetry_peak_rss_bytes",
                    "telemetry_fd_soft_limit",
                    "telemetry_peak_open_fds",
                    "telemetry_ephemeral_port_count",
                    "telemetry_peak_ephemeral_ports_in_use",
                    "telemetry_min_ephemeral_port_reserve",
                    "hard_safety_verified",
                    "telemetry_failures",
                )
            }
            aggregate["telemetry"] = {
                **dict(aggregate.get("telemetry") or {}),
                **host_fields,
            }
            for lane in aggregate.get("lanes", []):
                if isinstance(lane, dict):
                    lane.update(host_fields)
        record_parent_phase("worker_result_aggregation", aggregate_started_ns)
        aggregate["parent_phase_profile"] = parent_profile
        return aggregate
    finally:
        primary_error = sys.exc_info()[1]
        cleanup_error: BaseException | None = None
        if barrier_state != "COMPLETE":
            barrier_state = "ABORT"
        try:
            if all(process.is_alive() for process in processes):
                await sample_parent_safety(force=True)
        except BaseException as exc:
            cleanup_error = exc
        finally:
            for event in (
                cancel_event,
                release_event,
                hold_prepare_event,
                hold_epoch_event,
                sample_release_event,
                teardown_event,
            ):
                try:
                    event.set()
                except BaseException as exc:
                    cleanup_error = cleanup_error or exc
            try:
                for process in processes:
                    try:
                        await asyncio.to_thread(process.join, 5.0)
                    except BaseException as exc:
                        cleanup_error = cleanup_error or exc
                for process in processes:
                    try:
                        if process.is_alive():
                            process.terminate()
                    except BaseException as exc:
                        cleanup_error = cleanup_error or exc
                for process in processes:
                    try:
                        await asyncio.to_thread(process.join, 5.0)
                    except BaseException as exc:
                        cleanup_error = cleanup_error or exc
                try:
                    if any(process.is_alive() for process in processes):
                        cleanup_error = cleanup_error or RunnerContractError(
                            "fanin_worker_cleanup_incomplete"
                        )
                except BaseException as exc:
                    cleanup_error = cleanup_error or exc
            finally:
                for process_queue in (control_queue, result_queue):
                    try:
                        close = getattr(process_queue, "close", None)
                        if callable(close):
                            close()
                    except BaseException as exc:
                        cleanup_error = cleanup_error or exc
                    try:
                        join_thread = getattr(process_queue, "join_thread", None)
                        if callable(join_thread):
                            join_thread()
                    except BaseException as exc:
                        cleanup_error = cleanup_error or exc
        if primary_error is None and cleanup_error is not None:
            raise cleanup_error


def _shard_smoke_worker(result_queue: Any, worker_index: int) -> None:
    try:
        worker_cpu = _pin_fanin_worker(worker_index)
        loop_p99_ms = asyncio.run(fanin._event_loop_microbatch_benchmark())
        result_queue.put(("ok", worker_index, os.getpid(), worker_cpu, loop_p99_ms))
    except BaseException:
        result_queue.put(("error", worker_index, os.getpid(), -1, math.inf))


def shard_process_preflight() -> dict[str, object]:
    context = mp.get_context("spawn")
    result_queue = context.Queue()
    processes = [
        context.Process(
            target=_shard_smoke_worker,
            args=(result_queue, index),
            name=f"round5-preflight-{index}",
        )
        for index in range(fanin.WORKER_COUNT)
    ]
    for process in processes:
        process.start()
    results = [result_queue.get(timeout=30.0) for _ in processes]
    for process in processes:
        process.join(30.0)
    if (
        any(process.exitcode != 0 for process in processes)
        or any(result[0] != "ok" for result in results)
        or len({int(result[2]) for result in results}) != fanin.WORKER_COUNT
        or len({int(result[3]) for result in results}) != fanin.WORKER_COUNT
    ):
        raise RunnerContractError("fanin_shard_preflight_failed")
    loop_values = [float(result[4]) for result in results]
    advisories = (
        [fanin.AdvisoryTelemetryCode.EVENT_LOOP_MICROBATCH_PRESSURE.value]
        if max(loop_values) > fanin.RUNTIME_MAX_EVENT_LOOP_P99_MS
        else []
    )
    return {
        "worker_count": fanin.WORKER_COUNT,
        "unique_processes": len({int(result[2]) for result in results}),
        "unique_cpus": len({int(result[3]) for result in results}),
        "peak_worker_loop_microbatch_p99_ms": max(loop_values),
        "telemetry_advisories": advisories,
        "sufficient": True,
    }


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
    return await connect_runner_database(
        database,
        application_name=application_name,
        trust_bundle_path=TRUST_BUNDLE_PATH,
        tls_mode=TLS_MODE,
        connect_timeout_seconds=CONNECT_TIMEOUT_SECONDS,
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
        request[name] for name in ("master_secret_arn", "destination_secret_arn") if name in request
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
    role_name: str = BASELINE_ROLE,
    observer: bool = False,
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
                        (role_name,),
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
                    role = sql.Identifier(role_name)
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
                    if observer:
                        await cursor.execute(
                            sql.SQL("GRANT pg_monitor TO {}").format(role),
                            prepare=False,
                        )
                    else:
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
            # Aurora can accept the first post-pause socket and then reject a
            # statement while PostgreSQL is still settling. Connection-class
            # failures are retryable. The observed wake-up anomaly can also
            # surface as UndefinedObject while CREATE ROLE and its following
            # GRANT briefly disagree; retry that exact create path without
            # masking unrelated schema, SQL, or authorization defects.
            transient_restart = isinstance(
                exc,
                (OSError, TimeoutError, psycopg.OperationalError, psycopg.InterfaceError),
            ) or (
                create_if_missing
                and isinstance(exc, psycopg.Error)
                and getattr(exc, "sqlstate", None) == "42704"
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


def _new_ordinary_database(
    request: Mapping[str, object],
    *,
    role_name: str = BASELINE_ROLE,
) -> dict[str, object]:
    return {
        "host": request["credential_host"],
        "port": request["port"],
        "dbname": request["dbname"],
        "username": role_name,
        "password": secrets.token_urlsafe(48),
    }


async def _verify_database_role(
    database: Mapping[str, object],
    expected_role: str,
) -> None:
    connection: Any | None = None
    try:
        connection = await _connect(database, f"{APP_PREFIX}-role-verify")
        async with connection.cursor() as cursor:
            await cursor.execute("SELECT current_user", prepare=False)
            row = await cursor.fetchone()
        await connection.commit()
        if row != (expected_role,):
            raise RunnerContractError("baseline_role_verify_failed")
    except RunnerContractError:
        raise
    except Exception as exc:
        raise RunnerContractError("baseline_role_verify_failed") from exc
    finally:
        if connection is not None:
            await connection.close()


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
    if "host" in value and (not isinstance(value["host"], str) or value["host"] != expected_host):
        raise RunnerContractError("master_secret_invalid")
    if "port" in value and (
        not isinstance(value["port"], int)
        or isinstance(value["port"], bool)
        or value["port"] != expected_port
    ):
        raise RunnerContractError("master_secret_invalid")
    for name in ("dbname", "database"):
        if name in value and (not isinstance(value[name], str) or value[name] != expected_database):
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
    _monotonic: Callable[[], float] | None = None,
    _sleep: Callable[[float], Awaitable[None]] | None = None,
) -> None:
    """Verify the sealed ordinary login, with a deadline only for Aurora wake.

    RDS and Lakebase retain their single-attempt behavior. Aurora retries only
    connection-class failures until a monotonic deadline below SSM's own
    execution boundary. Every emitted failure token is assembled exclusively
    from fixed labels, a validated SQLSTATE, and bounded counters.
    """

    clock = time.monotonic if _monotonic is None else _monotonic
    sleep = asyncio.sleep if _sleep is None else _sleep
    credential_id = _setup_credential_id(request)
    path = BASELINE_CREDENTIAL_PATHS[credential_id]
    keys = BASELINE_DATABASE_KEYS if credential_id == "lakebase" else RDS_BASELINE_KEYS
    value = _read_root_json(path, keys)
    if hashlib.sha256(_canonical_json(value)).hexdigest() != request["credential_sha256"]:
        raise RunnerContractError("baseline_auth_hash_invalid")
    database = _validate_database_value(
        value,
        expected_host=str(request["credential_host"]),
    )
    database["host"] = request["endpoint_host"]
    started_at = clock()
    deadline = started_at + SETUP_VERIFY_DEADLINE_SECONDS
    attempts = 0
    last_sqlstate = "none"

    def refusal(reason: str) -> RunnerContractError:
        elapsed = max(0.0, min(SETUP_VERIFY_DEADLINE_SECONDS, clock() - started_at))
        elapsed_seconds = int(math.ceil(elapsed))
        return RunnerContractError(
            f"setup_verify_{reason}_state_{last_sqlstate}"
            f"_attempts_{attempts}_elapsed_{elapsed_seconds}s"
        )

    while True:
        remaining = deadline - clock()
        if retry_transient_restart and remaining <= 0:
            raise refusal("deadline")
        attempts += 1
        connection: Any | None = None
        retry_delay: float | None = None
        try:

            async def verify() -> object:
                nonlocal connection
                connection = await _connect(database, f"{APP_PREFIX}-setup-verify")
                async with connection.cursor() as cursor:
                    await cursor.execute(
                        "SELECT %s::text, current_user",
                        (request["nonce"],),
                        prepare=False,
                    )
                    row = await cursor.fetchone()
                await connection.commit()
                return row

            if retry_transient_restart:
                async with asyncio.timeout(remaining):
                    row = await verify()
            else:
                row = await verify()
            if row != (request["nonce"], BASELINE_ROLE):
                raise RunnerContractError("setup_verify_failed")
            return
        except RunnerContractError:
            raise
        except Exception as exc:
            raw_sqlstate = getattr(exc, "sqlstate", None)
            sqlstate = (
                raw_sqlstate
                if isinstance(raw_sqlstate, str) and _SQLSTATE.fullmatch(raw_sqlstate)
                else None
            )
            last_sqlstate = sqlstate.lower() if sqlstate is not None else "none"
            transient_restart = isinstance(exc, (OSError, TimeoutError)) or (
                isinstance(exc, psycopg.OperationalError)
                and (
                    sqlstate is None
                    or sqlstate.startswith("08")
                    or sqlstate in {"57P01", "57P02", "57P03"}
                )
            )
            if not retry_transient_restart:
                raise RunnerContractError("setup_verify_failed") from exc
            if not transient_restart:
                raise refusal("nonretryable") from exc
            remaining = deadline - clock()
            if remaining <= 0:
                raise refusal("deadline") from exc
            retry_delay = min(
                2.0 ** (attempts - 1),
                SETUP_VERIFY_MAX_RETRY_DELAY_SECONDS,
                remaining,
            )
        finally:
            if connection is not None:
                try:
                    await connection.close()
                except psycopg.Error:
                    pass
        assert retry_delay is not None
        await sleep(retry_delay)


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
        observer = _new_ordinary_database(request, role_name=OBSERVER_ROLE)
        await _configure_ordinary_role(
            admin,
            ordinary,
            create_if_missing=True,
            retry_transient_restart=True,
        )
        await _configure_ordinary_role(
            admin,
            observer,
            create_if_missing=True,
            retry_transient_restart=True,
            role_name=OBSERVER_ROLE,
            observer=True,
        )
        credential_sha256 = _write_root_file(
            BASELINE_CREDENTIAL_PATHS["lakebase"], _canonical_json(ordinary)
        )
        observer_credential_sha256 = _write_root_file(
            OBSERVER_CREDENTIAL_PATHS["lakebase"], _canonical_json(observer)
        )
        await _verify_setup_transaction({**request, "credential_sha256": credential_sha256})
        await _verify_database_role(
            _validate_database_value(observer, expected_host=str(request["credential_host"])),
            OBSERVER_ROLE,
        )
        result["credential_sha256"] = credential_sha256
        result["observer_credential_sha256"] = observer_credential_sha256
        return result

    secret_arns = [
        request[name] for name in ("master_secret_arn", "destination_secret_arn") if name in request
    ]
    secrets_client: Any | None = None
    if secret_arns:
        _secrets_manager_region(secret_arns, "setup_secret_binding_invalid")
        secrets_client = secrets_manager_for_runner_operation(secret_arns)
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
        observer = _new_ordinary_database(request, role_name=OBSERVER_ROLE)
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
        await _configure_ordinary_role(
            admin,
            observer,
            create_if_missing=True,
            retry_transient_restart=request["lane_id"] == "aurora",
            role_name=OBSERVER_ROLE,
            observer=True,
        )
        stored = {**ordinary, "master_secret_arn": master_secret_arn}
        credential_sha256 = _write_root_file(
            BASELINE_CREDENTIAL_PATHS[_setup_credential_id(request)],
            _canonical_json(stored),
        )
        observer_credential_sha256 = _write_root_file(
            OBSERVER_CREDENTIAL_PATHS[_setup_credential_id(request)],
            _canonical_json(observer),
        )
        await _verify_setup_transaction({**request, "credential_sha256": credential_sha256})
        await _verify_database_role(
            _validate_database_value(observer, expected_host=str(request["credential_host"])),
            OBSERVER_ROLE,
        )
        result["credential_sha256"] = credential_sha256
        result["observer_credential_sha256"] = observer_credential_sha256
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
    request: Mapping[str, object],
    cancelled: asyncio.Event,
    *,
    _timeout: float = SETUP_VERIFY_DEADLINE_SECONDS,
) -> tuple[dict[str, object] | None, bool]:
    setup = asyncio.create_task(_execute_setup(request))
    cancellation = asyncio.create_task(cancelled.wait())
    try:
        done, _ = await asyncio.wait(
            (setup, cancellation),
            timeout=_timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if not done:
            setup.cancel()
            await asyncio.gather(setup, return_exceptions=True)
            raise RunnerContractError("setup_deadline")
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
        secret_arns = [target.secret_arn for target in targets if target.secret_arn]
        _secrets_manager_region(secret_arns, "target_invalid")
        secrets_client = secrets_manager_for_runner_operation(secret_arns)
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
            "protocol": BOUNDED_PROTOCOL,
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


def _reclaim_stale_run_directory(run_id: str) -> None:
    """Remove a run directory abandoned by a dead owner before re-preparing it.

    Only ever called by a takeover owner that holds the per-job flock, so the
    previous owner is provably gone and this cannot race a live writer. The path
    is fully determined by ``run_id`` and refused if it is a symlink, so it can
    never delete anything outside this job's own run directory.
    """

    stale = RUN_ROOT / run_id
    if stale.is_symlink():
        raise RunnerContractError("cleanup_ownership_invalid")
    if not stale.exists():
        return
    marker = stale / "owner"
    if (
        marker.is_file()
        and not marker.is_symlink()
        and marker.read_text(encoding="utf-8") == run_id
    ):
        _cleanup_owned(run_id, stale)
        return
    # The dead owner created the directory but died before writing its marker.
    # Under the per-job lock this is still unambiguously ours to remove.
    if stale.is_dir():
        shutil.rmtree(stale)
    if stale.exists():
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
        # The size and the biggest contributors, because "too large" without a number cost a
        # twelve-minute Proxy build to characterise. Integers and this repository's own key
        # names only: nothing here names a host, an ARN or a credential.
        try:
            contributors = sorted(
                ((len(_canonical_json({key: value})), key) for key, value in result.items()),
                reverse=True,
            )[:5]
            print(
                "RESULT_SIZE_JSON:"
                + _canonical_json(
                    {
                        "encoded_length": len(encoded_result),
                        "budget": 23_500,
                        "largest_keys": [name for _size, name in contributors],
                        "largest_key_bytes": [size for size, _name in contributors],
                    }
                ).decode("utf-8"),
                flush=True,
            )
        except Exception:  # noqa: BLE001
            # A diagnostic must never replace the refusal it is describing.
            pass
        raise RunnerContractError("result_too_large")
    return encoded_result


def _atomic_job_write(job_directory: Path, name: str, value: str) -> None:
    """Replace one bounded registry value without exposing a partial write."""

    if (
        job_directory.parent != JOB_ROOT
        or job_directory.is_symlink()
        or _SHA256.fullmatch(job_directory.name) is None
        or not name.replace("_", "").isalnum()
    ):
        raise RunnerContractError("fanin_job_registry_ownership_invalid")
    temporary = job_directory / f".{name}.{os.getpid()}.{secrets.token_hex(4)}"
    temporary.write_text(value, encoding="utf-8")
    temporary.chmod(0o600)
    temporary.replace(job_directory / name)


def _read_job_value(job_directory: Path, name: str) -> str:
    path = job_directory / name
    if (
        job_directory.parent != JOB_ROOT
        or job_directory.is_symlink()
        or path.is_symlink()
        or not path.is_file()
    ):
        return ""
    value = path.read_text(encoding="utf-8")
    if len(value.encode("utf-8")) > 32_768:
        raise RunnerContractError("fanin_job_registry_value_too_large")
    return value


def _open_job_lock(job_directory: Path) -> Any:
    """Open the per-job ownership lock file guarding one logical fan-in.

    The file lives inside the job's registry directory and is never mutated by
    ``_atomic_job_write`` (its name is not a registry-key shape), so it persists
    for the life of the durable job record. Ownership is expressed purely by an
    ``flock`` held on this file, which the kernel releases the instant the owning
    process exits -- crash, ``SIGKILL``, or clean shutdown alike. That is what
    lets a rejoining invocation distinguish a live owner from a dead one without
    waiting out the whole SSM command timeout.
    """

    if (
        job_directory.parent != JOB_ROOT
        or job_directory.is_symlink()
        or _SHA256.fullmatch(job_directory.name) is None
    ):
        raise RunnerContractError("fanin_job_registry_ownership_invalid")
    lock_path = job_directory / JOB_OWNER_LOCK_NAME
    if lock_path.is_symlink():
        raise RunnerContractError("fanin_job_registry_ownership_invalid")
    return lock_path.open("a+", encoding="utf-8")


def _release_job_lock(lock: Any) -> None:
    """Release and close a per-job ownership lock, tolerating a torn-down fd."""

    with contextlib.suppress(Exception):
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
    with contextlib.suppress(Exception):
        lock.close()


def _claim_job(request: Mapping[str, object]) -> tuple[Path, bool, Any | None]:
    """Atomically own, or defer to, one logical fan-in before opening a socket.

    The winner of the directory ``mkdir`` acquires the per-job ownership flock
    *before* publishing its immutable identity, so any later invocation that
    observes the published ``prepared_request_digest`` is guaranteed to also
    observe the lock held. A contender (its ``mkdir`` lost the race) returns
    ``job_owner=False`` with no lock and defers to
    :func:`_rejoin_or_takeover_job`, which decides between rejoining a live
    owner, replaying a settled result, or taking over from a dead one.
    """

    job_id = str(request.get("job_id") or "")
    prepared_digest = str(request.get("prepared_request_digest") or "")
    if _SHA256.fullmatch(job_id) is None or _SHA256.fullmatch(prepared_digest) is None:
        raise RunnerContractError("fanin_job_identity_invalid")
    JOB_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
    job_directory = JOB_ROOT / job_id
    try:
        job_directory.mkdir(mode=0o700)
    except FileExistsError:
        # The winning mkdir may still be acquiring the flock and writing its two
        # small identity files. Wait only for that bounded publication edge --
        # never for the job itself, which is the whole point of the takeover
        # path -- then verify the immutable identity before deferring.
        deadline = time.monotonic() + 1.0
        existing_digest = ""
        while time.monotonic() < deadline and not existing_digest:
            existing_digest = _read_job_value(
                job_directory,
                "prepared_request_digest",
            )
            if not existing_digest:
                time.sleep(0.01)
        if job_directory.is_symlink() or existing_digest != prepared_digest:
            raise RunnerContractError("fanin_job_identity_conflict") from None
        return job_directory, False, None
    lock: Any | None = None
    try:
        lock = _open_job_lock(job_directory)
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:  # pragma: no cover - a fresh mkdir cannot be pre-locked
            raise RunnerContractError("fanin_job_owner_lock_unavailable") from exc
        # Publish identity only after the lock is held, so the ordering a
        # contender relies on ("saw the digest => the owner had locked") holds.
        _atomic_job_write(job_directory, "prepared_request_digest", prepared_digest)
        _atomic_job_write(job_directory, "state", "claimed")
        return job_directory, True, lock
    except BaseException:
        if lock is not None:
            _release_job_lock(lock)
        shutil.rmtree(job_directory, ignore_errors=True)
        raise


class _JobRejoin(NamedTuple):
    """Outcome of a contender consulting an already-claimed job.

    ``disposition`` is ``"replay"`` when the job reached (or can be finalized to)
    a terminal state -- the contender emits the recorded outcome and exits -- or
    ``"takeover"`` when a dead owner left the job unfinished and this invocation
    must run it to completion while holding ``lock`` for the rest of its life.
    """

    disposition: str
    encoded_result: str | None = None
    was_cancelled: bool = False
    lock: Any | None = None


def _settled_terminal(job_directory: Path) -> _JobRejoin | None:
    """Return the replay outcome for a job the owner already settled, else None."""

    if _read_job_value(job_directory, "settled") != "true":
        return None
    result = _read_job_value(job_directory, "result_gzip_base64")
    if result:
        return _JobRejoin("replay", encoded_result=result)
    state = _read_job_value(job_directory, "state")
    return _JobRejoin("replay", was_cancelled=state == "cancelled")


def _recover_unsettled_terminal(job_directory: Path) -> _JobRejoin | None:
    """Recover a terminal outcome a dead owner reached but never settled.

    Called only under the per-job lock once the owner is known gone. A completed
    result or a deterministic cancel/failure is replayed rather than re-run; only
    a job still mid-flight (``claimed``/``running``) is a genuine takeover.
    """

    state = _read_job_value(job_directory, "state")
    result = _read_job_value(job_directory, "result_gzip_base64")
    if state == "completed" and result:
        return _JobRejoin("replay", encoded_result=result)
    if state == "cancelled":
        return _JobRejoin("replay", was_cancelled=True)
    if state == "failed":
        return _JobRejoin("replay")
    return None


def _rejoin_or_takeover_job(
    job_directory: Path,
    run_id: str,
    request: Mapping[str, object],
) -> _JobRejoin:
    """Rejoin a live owner, replay a settled result, or take over a dead owner.

    A live owner holds the per-job flock, so a non-blocking probe that cannot
    acquire it means "still running -- wait". A probe that *does* acquire it
    means the owner has exited; re-reading the terminal state under the lock
    closes the race against an owner that settled microseconds before releasing
    it. An owner that published its identity and then died without settling
    yields either a recovered terminal result or a true takeover with the lock
    retained. The only unbounded wait remaining is for a *live* owner, which is
    exactly the invocation actually producing the result.
    """

    prepared_digest = str(request.get("prepared_request_digest") or "")
    lock = _open_job_lock(job_directory)
    deadline = time.monotonic() + FANIN_SSM_COMMAND_TIMEOUT_SECONDS
    try:
        while time.monotonic() < deadline:
            terminal = _settled_terminal(job_directory)
            if terminal is not None:
                _release_job_lock(lock)
                return terminal
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                # The owner still holds the lock: it is alive and working.
                time.sleep(JOB_REJOIN_POLL_SECONDS)
                continue
            # The owner has exited. Re-read terminal state under the lock so a
            # graceful owner that settled just before releasing is replayed, not
            # taken over.
            terminal = _settled_terminal(job_directory)
            if terminal is not None:
                _release_job_lock(lock)
                return terminal
            published = _read_job_value(job_directory, "prepared_request_digest")
            if not published or published != prepared_digest:
                # Either the owner died before durably publishing anything, or a
                # different request holds this id. Neither is safe to reclaim.
                _release_job_lock(lock)
                raise RunnerContractError("fanin_job_identity_conflict")
            recovered = _recover_unsettled_terminal(job_directory)
            if recovered is not None:
                _atomic_job_write(job_directory, "settled", "true")
                _release_job_lock(lock)
                return recovered
            # Genuine takeover: the immutable identity is intact and the job is
            # mid-flight. Keep the lock; the caller runs the job as the new owner.
            _atomic_job_write(job_directory, "state", "claimed")
            return _JobRejoin("takeover", lock=lock)
    except BaseException:
        _release_job_lock(lock)
        raise
    _release_job_lock(lock)
    raise RunnerContractError("fanin_job_rejoin_timeout")


def _job_control(request: Mapping[str, object]) -> int:
    job_id = str(request["job_id"])
    job_directory = JOB_ROOT / job_id
    if not job_directory.is_dir() or job_directory.is_symlink():
        if request["action"] in {"cancel_job", "job_result"}:
            raise RunnerContractError("fanin_job_unknown")
        print(
            "JOB_STATUS:"
            + _canonical_json(
                {
                    "protocol": JOB_PROTOCOL,
                    "schema_version": 3,
                    "job_id": job_id,
                    "state": "unknown",
                    "settled": True,
                    "result_available": False,
                    "latest_progress": None,
                }
            ).decode("utf-8"),
            flush=True,
        )
        return 0
    if request["action"] == "cancel_job":
        _atomic_job_write(job_directory, "cancel_requested", "true")
    state = _read_job_value(job_directory, "state") or "unknown"
    settled = _read_job_value(job_directory, "settled") == "true"
    result = _read_job_value(job_directory, "result_gzip_base64")
    result_sha256 = hashlib.sha256(result.encode("ascii")).hexdigest() if result else None
    result_chunks = (
        (len(result) + JOB_RESULT_CHUNK_CHARS - 1) // JOB_RESULT_CHUNK_CHARS if result else 0
    )
    if request["action"] == "job_result":
        if not result:
            raise RunnerContractError("fanin_job_result_unavailable")
        chunk_index = int(request["chunk_index"])
        if chunk_index >= result_chunks:
            raise RunnerContractError("fanin_job_result_chunk_invalid")
        payload = result[
            chunk_index * JOB_RESULT_CHUNK_CHARS : (chunk_index + 1) * JOB_RESULT_CHUNK_CHARS
        ]
        print(
            JOB_RESULT_PREFIX
            + _canonical_json(
                {
                    "protocol": JOB_PROTOCOL,
                    "schema_version": 3,
                    "job_id": job_id,
                    "chunk_index": chunk_index,
                    "chunk_count": result_chunks,
                    "result_sha256": result_sha256,
                    "payload": payload,
                }
            ).decode("utf-8"),
            flush=True,
        )
        return 0
    latest_raw = _read_job_value(job_directory, "latest_progress")
    try:
        latest_progress = json.loads(latest_raw) if latest_raw else None
    except json.JSONDecodeError as exc:
        raise RunnerContractError("fanin_job_progress_invalid") from exc
    print(
        "JOB_STATUS:"
        + _canonical_json(
            {
                "protocol": JOB_PROTOCOL,
                "schema_version": 3,
                "job_id": job_id,
                "state": state,
                "settled": settled,
                "result_available": bool(result),
                "result_sha256": result_sha256,
                "result_size": len(result) if result else 0,
                "result_chunks": result_chunks,
                "latest_progress": latest_progress,
            }
        ).decode("utf-8"),
        flush=True,
    )
    return 0


RESIDENT_CONTROL_PROTOCOL = "round5-resident-control-v3"
RESIDENT_CONTROL_SCHEMA_VERSION = 3
ROUND5_RUNNER_EVENT_TABLE = "anti_demo_coordination.round5_runner_event_v3"
ROUND5_RUNNER_EVENT_DISPOSITION_FUNCTION = (
    "anti_demo_coordination.round5_runner_event_disposition_v1"
)
# The systemd-managed runner attests its loaded harness to a tmpfs file here
# before reading the control DSN. A module constant (rather than an inline
# ``/run`` literal) keeps the path fixed in production while letting tests
# redirect it to a temporary directory.
RESIDENT_ATTESTATION_DIR = Path("/run")

# The exact, lane-bound dispositions the resident precomputes before every
# local transition. They mirror the authoritative slot/outbox/event relations
# the write-side RLS gate consults, so the resident never trusts a persisted
# stage, spawns workers, and only then learns from a rejected insert that a
# newer attempt superseded it.
RESIDENT_DISPOSITION_CURRENT = "current"
RESIDENT_DISPOSITION_CANCELLED = "cancelled"
RESIDENT_DISPOSITION_SUPERSEDED = "superseded"
RESIDENT_DISPOSITION_TERMINAL = "terminal"
RESIDENT_DISPOSITION_UNKNOWN = "unknown"
_RESIDENT_DISPOSITIONS = frozenset(
    {
        RESIDENT_DISPOSITION_CURRENT,
        RESIDENT_DISPOSITION_CANCELLED,
        RESIDENT_DISPOSITION_SUPERSEDED,
        RESIDENT_DISPOSITION_TERMINAL,
        RESIDENT_DISPOSITION_UNKNOWN,
    }
)


class RunnerAttemptSupersededError(RuntimeError):
    """The warm attempt token was revoked between a disposition precheck and an
    RLS-gated insert.

    This is expected cleanup, not a crash: the row-level security ``WITH CHECK``
    denied the event because the durable warm slot no longer names this
    attempt's token (or the job is no longer dispatched under it). The resident
    reclassifies and discards the stale local residue instead of crash-looping.
    """


def _decode_resident_control(body: str) -> dict[str, object]:
    try:
        raw = json.loads(gzip.decompress(base64.urlsafe_b64decode(body)))
    except Exception as exc:
        raise RunnerContractError("resident_control_decode_invalid") from exc
    return _verify_resident_control(raw)


def _verify_resident_control(raw: object) -> dict[str, object]:
    """Reverify a decoded control event's shape, event hash, and request digest.

    Used both for freshly decoded SQS bodies and for control events replayed
    from local stage/active recovery files. Reverifying on recovery means a
    tampered or truncated persisted event is rejected before it drives any local
    transition, rather than being trusted because it once passed on the wire.
    """

    binding = raw.get("binding") if isinstance(raw, dict) else None
    payload = raw.get("payload") if isinstance(raw, dict) else None
    if (
        not isinstance(raw, dict)
        or raw.get("protocol") != RESIDENT_CONTROL_PROTOCOL
        or raw.get("schema_version") != RESIDENT_CONTROL_SCHEMA_VERSION
        or not isinstance(binding, Mapping)
        or not isinstance(payload, Mapping)
        or binding.get("lane_id") not in RUNTIME_LANE_IDS
        or raw.get("kind") not in {"preload", "stage", "release", "cancel"}
        or _SHA256.fullmatch(str(raw.get("event_id") or "")) is None
        or _SHA256.fullmatch(str(binding.get("job_id") or "")) is None
        or _SHA256.fullmatch(str(binding.get("runner_harness_sha256") or "")) is None
        or _SHA256.fullmatch(str(binding.get("request_sha256") or "")) is None
        or isinstance(binding.get("generation"), bool)
        or not isinstance(binding.get("generation"), int)
        or int(binding["generation"]) <= 0
        or isinstance(raw.get("sequence"), bool)
        or not isinstance(raw.get("sequence"), int)
        or int(raw["sequence"]) <= 0
    ):
        raise RunnerContractError("resident_control_contract_invalid")
    for name in (
        "installation_id",
        "warm_attempt_token",
        "runner_boot_id",
        "runner_process_boot_id",
    ):
        value = binding.get(name)
        if not isinstance(value, str) or not value or len(value) > 256:
            raise RunnerContractError("resident_control_contract_invalid")
    claim_bound = (
        all(
            isinstance(binding.get(name), str) and bool(binding.get(name))
            for name in ("claim_id", "bout_id", "bell_id")
        )
        and isinstance(binding.get("fence"), int)
        and not isinstance(binding.get("fence"), bool)
        and int(binding["fence"]) > 0
    )
    if raw["kind"] == "preload":
        if (
            claim_bound
            or any(binding.get(name) is not None for name in ("claim_id", "bout_id", "bell_id"))
            or isinstance(binding.get("fence"), bool)
            or binding.get("fence") != 0
        ):
            raise RunnerContractError("resident_preload_claim_invalid")
    elif not claim_bound:
        raise RunnerContractError("resident_control_claim_missing")
    if raw["kind"] in {"preload", "stage"}:
        request = payload.get("request")
        if not isinstance(request, Mapping):
            raise RunnerContractError("resident_stage_request_invalid")
        request_value = dict(request)
        claimed_digest = request_value.pop("prepared_request_digest", None)
        if (
            claimed_digest != binding["request_sha256"]
            or hashlib.sha256(_canonical_json(request_value)).hexdigest() != claimed_digest
        ):
            raise RunnerContractError("resident_stage_request_digest_invalid")
    identity = {key: value for key, value in raw.items() if key != "event_id"}
    if hashlib.sha256(_canonical_json(identity)).hexdigest() != raw["event_id"]:
        raise RunnerContractError("resident_control_identity_invalid")
    return raw


def _write_resident_event(
    control_dsn: str,
    *,
    binding: Mapping[str, object],
    sequence: int,
    kind: str,
    payload: Mapping[str, object],
) -> None:
    occurred_at = datetime.now(UTC)
    identity = {
        "binding": dict(binding),
        "sequence": sequence,
        "kind": kind,
        "occurred_at": occurred_at.isoformat(),
        "payload": dict(payload),
    }
    event_id = hashlib.sha256(_canonical_json(identity)).hexdigest()
    with psycopg.connect(control_dsn, autocommit=True) as connection:
        with connection.cursor() as cursor:
            try:
                cursor.execute(
                    f"""
                    INSERT INTO {ROUND5_RUNNER_EVENT_TABLE} (
                        event_id, installation_id, lane_id, generation,
                        warm_attempt_token, job_id, sequence, kind, binding,
                        payload, occurred_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s
                    )
                    ON CONFLICT (
                        installation_id, lane_id, generation, warm_attempt_token,
                        job_id, sequence
                    ) DO NOTHING
                    RETURNING event_id
                    """,
                    (
                        event_id,
                        binding["installation_id"],
                        binding["lane_id"],
                        binding["generation"],
                        binding["warm_attempt_token"],
                        binding["job_id"],
                        sequence,
                        kind,
                        json.dumps(dict(binding), sort_keys=True, separators=(",", ":")),
                        json.dumps(dict(payload), sort_keys=True, separators=(",", ":")),
                        occurred_at,
                    ),
                )
            except psycopg.errors.InsufficientPrivilege as exc:
                # The RLS WITH CHECK denied this row: the durable warm slot no
                # longer names this attempt's token, or the job is no longer
                # dispatched under it. Authority rotated between the caller's
                # disposition precheck and this insert. Surface it distinctly so
                # the resident reclassifies and discards the stale residue rather
                # than treating a revoked attempt as a fatal contract failure.
                raise RunnerAttemptSupersededError(kind) from exc
            row = cursor.fetchone()
            if row is not None and str(row[0]) == event_id:
                return
            cursor.execute(
                f"""
                SELECT event_id, binding, payload, kind, occurred_at
                FROM {ROUND5_RUNNER_EVENT_TABLE}
                WHERE installation_id = %s
                  AND lane_id = %s
                  AND generation = %s
                  AND warm_attempt_token = %s
                  AND job_id = %s
                  AND sequence = %s
                """,
                (
                    binding["installation_id"],
                    binding["lane_id"],
                    binding["generation"],
                    binding["warm_attempt_token"],
                    binding["job_id"],
                    sequence,
                ),
            )
            existing = cursor.fetchone()
            if (
                existing is None
                or str(existing[0]) != event_id
                or dict(existing[1]) != dict(binding)
                or dict(existing[2]) != dict(payload)
                or str(existing[3]) != kind
                or existing[4] != occurred_at
            ):
                raise RunnerContractError("resident_runner_event_conflict")


def _last_resident_event_sequence(
    control_dsn: str,
    binding: Mapping[str, object],
) -> int:
    with psycopg.connect(control_dsn, autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT COALESCE(MAX(sequence), 0)
                FROM {ROUND5_RUNNER_EVENT_TABLE}
                WHERE installation_id = %s
                  AND lane_id = %s
                  AND generation = %s
                  AND warm_attempt_token = %s
                  AND job_id = %s
                """,
                (
                    binding["installation_id"],
                    binding["lane_id"],
                    binding["generation"],
                    binding["warm_attempt_token"],
                    binding["job_id"],
                ),
            )
            row = cursor.fetchone()
            return int(row[0]) if row is not None else 0


def _resident_event_disposition(
    control_dsn: str,
    *,
    installation_id: str,
    lane_id: str,
    generation: int,
    warm_attempt_token: str,
    job_id: str,
    event_id: str,
    binding: Mapping[str, object],
) -> str:
    """Classify a control event as current / superseded / terminal / unknown.

    Read-only, via the lane-scoped SECURITY DEFINER disposition function; the
    runner login holds no read grant on the slot/outbox/event relations the
    classifier consults. Called before every local transition so a superseded or
    already-settled event never spawns/stops workers or replaces a stage file.
    """

    with psycopg.connect(control_dsn, autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT {ROUND5_RUNNER_EVENT_DISPOSITION_FUNCTION}"
                "(%s, %s, %s, %s, %s, %s, %s::jsonb)",
                (
                    installation_id,
                    lane_id,
                    generation,
                    warm_attempt_token,
                    job_id,
                    event_id,
                    json.dumps(dict(binding), sort_keys=True, separators=(",", ":")),
                ),
            )
            row = cursor.fetchone()
    disposition = str(row[0]) if row is not None and row[0] is not None else ""
    if disposition not in _RESIDENT_DISPOSITIONS:
        raise RunnerContractError("resident_control_disposition_invalid")
    return disposition


def _resident_control_failure_is_permanent(error: BaseException) -> bool:
    return isinstance(error, (RunnerContractError, ValueError, KeyError, TypeError))


async def _resident_agent(
    *,
    lane_id: str,
    generation: int,
    queue_url: str,
    control_secret_arn: str,
) -> None:
    if lane_id not in RUNTIME_LANE_IDS or generation < 0:
        raise RunnerContractError("resident_agent_identity_invalid")
    process_boot_id = f"process-{secrets.token_hex(16)}"
    runner_boot_id = fanin._runner_boot_id()
    sqs = boto3.client("sqs")
    resident_generation: int | None = generation or None
    resident_installation_id = ""
    resident_attempt_token = ""
    pool: ResidentShardPool | None = None
    JOB_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)  # noqa: ASYNC240
    stage_path = JOB_ROOT / f"resident-{lane_id}-stage.json"
    active_path = JOB_ROOT / f"resident-{lane_id}-active.json"
    if stage_path.is_symlink() or active_path.is_symlink():
        raise RunnerContractError("resident_stage_ownership_invalid")
    staged_binding: dict[str, object] | None = None
    staged_request: dict[str, object] | None = None
    heartbeat_binding: dict[str, object] | None = None
    last_heartbeat_at = 0.0
    event_sequences: dict[str, int] = {}
    consumed_control_events: set[str] = set()

    def persist(path: Path, value: Mapping[str, object]) -> None:
        temporary = path.with_suffix(f".tmp-{os.getpid()}")
        temporary.write_text(
            json.dumps(dict(value), sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )
        temporary.chmod(0o600)
        temporary.replace(path)

    attestation_path = (
        RESIDENT_ATTESTATION_DIR / f"lakebase-anti-demo-round5-{lane_id}.attestation.json"
    )
    persist(
        attestation_path,
        {
            "pid": os.getpid(),
            "runner_boot_id": runner_boot_id,
            "runner_process_boot_id": process_boot_id,
            "runner_harness_sha256": LOADED_RUNNER_HARNESS_SHA256,
        },
    )

    # Read the lane-scoped control DSN only after attesting the loaded harness.
    # Terraform creates the secret container, but its AWSCURRENT value is written
    # by `ensure_coordination`, which during provisioning can run after the runner
    # is configured and started. Attesting first and then waiting for the value --
    # rather than crashing when it is briefly absent -- lets provisioning start
    # this agent and seal the DSN in either order. A genuinely absent secret holds
    # the agent here, attested and healthy under systemd, instead of crash-looping.
    secrets_client = boto3.client("secretsmanager")
    control_dsn = ""
    while True:
        try:
            secret_value = await asyncio.to_thread(
                secrets_client.get_secret_value, SecretId=control_secret_arn
            )
        except secrets_client.exceptions.ResourceNotFoundException:
            await asyncio.sleep(3)
            continue
        control_dsn = str(secret_value.get("SecretString") or "")
        if control_dsn.startswith(("postgresql://", "postgres://")):
            break
        await asyncio.sleep(3)

    def binding_of(event: Mapping[str, object]) -> dict[str, object]:
        value = event.get("binding")
        if not isinstance(value, Mapping):
            raise RunnerContractError("resident_control_binding_invalid")
        return dict(value)

    def require_process_binding(
        binding: Mapping[str, object],
        *,
        allow_unattested: bool,
    ) -> None:
        process_value = str(binding.get("runner_process_boot_id") or "")
        if (
            binding.get("lane_id") != lane_id
            or binding.get("runner_boot_id") != runner_boot_id
            or binding.get("runner_harness_sha256") != LOADED_RUNNER_HARNESS_SHA256
            or process_value
            not in ({"unattested", process_boot_id} if allow_unattested else {process_boot_id})
        ):
            raise RunnerContractError("resident_control_process_binding_invalid")

    def attested_binding(binding: Mapping[str, object]) -> dict[str, object]:
        return {
            **dict(binding),
            "runner_process_boot_id": process_boot_id,
        }

    async def publish(
        binding: Mapping[str, object],
        kind: str,
        payload: Mapping[str, object],
    ) -> None:
        job_id = str(binding["job_id"])
        if job_id not in event_sequences:
            event_sequences[job_id] = await asyncio.to_thread(
                _last_resident_event_sequence,
                control_dsn,
                binding,
            )
        sequence = event_sequences.get(job_id, 0) + 1
        event_sequences[job_id] = sequence
        event_payload = dict(payload)
        if kind == "progress":
            event_payload["sequence"] = sequence
        await asyncio.to_thread(
            _write_resident_event,
            control_dsn,
            binding=binding,
            sequence=sequence,
            kind=kind,
            payload=event_payload,
        )

    async def stop_pool() -> None:
        nonlocal pool
        if pool is None:
            return
        pool.cancel_event.set()
        for process in pool.processes:
            await asyncio.to_thread(process.join, 5)
        for process in pool.processes:
            if process.is_alive():
                process.terminate()
        for process in pool.processes:
            await asyncio.to_thread(process.join, 5)
        pool = None

    async def disposition_of(event: Mapping[str, object]) -> str:
        """Classify an already-verified control event before any local mutation."""

        event_binding = binding_of(event)
        return await asyncio.to_thread(
            _resident_event_disposition,
            control_dsn,
            installation_id=str(event_binding["installation_id"]),
            lane_id=str(event_binding["lane_id"]),
            generation=int(event_binding["generation"]),
            warm_attempt_token=str(event_binding["warm_attempt_token"]),
            job_id=str(event_binding["job_id"]),
            event_id=str(event["event_id"]),
            binding=event_binding,
        )

    async def discard_residue(binding: Mapping[str, object]) -> None:
        """Drop only the local residue that belongs to this exact binding.

        A superseded or terminal event must never disturb another attempt's live
        pool, stage, or active job. This clears the staged file/pool only when
        THIS binding is the one currently staged, and cancels an active job only
        when THIS binding owns it, so a delayed old attempt cannot stop or
        overwrite the attempt that superseded it.
        """

        nonlocal staged_binding, staged_request, heartbeat_binding
        target = dict(binding)
        job_id = str(binding["job_id"])
        current = active.get(job_id)
        if current is not None and current[2] == target:
            current[1].set()
            active.pop(job_id, None)
            prepared_jobs.discard(job_id)
            active_path.unlink(missing_ok=True)
        if staged_binding is not None and dict(staged_binding) == target:
            staged_binding = None
            staged_request = None
            heartbeat_binding = None
            stage_path.unlink(missing_ok=True)
            await stop_pool()

    if active_path.is_file():
        # Active recovery: a process died holding a job. Do not blindly settle it
        # -- classify first. Only a still-current job may be settled failed (its
        # token still matches, so RLS admits the terminal events); a superseded or
        # already-terminal job is discarded without writing under a revoked token
        # or emitting a duplicate terminal event.
        active_event = None
        try:
            active_value = json.loads(active_path.read_text(encoding="utf-8"))
            active_event = _verify_resident_control(active_value["event"])
        except (
            OSError,
            KeyError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
            RunnerContractError,
        ):
            # Unparseable, legacy-format (pre-event schema), or tampered residue,
            # e.g. an active file left by a previous harness version across a
            # refresh. Discard it; never crash-loop the new process on residue it
            # cannot interpret or attest.
            active_event = None
        if active_event is not None:
            active_binding = binding_of(active_event)
            active_disposition = await disposition_of(active_event)
            if active_disposition in {
                RESIDENT_DISPOSITION_CURRENT,
                RESIDENT_DISPOSITION_CANCELLED,
            }:
                restarted_after_cancel = (
                    active_disposition == RESIDENT_DISPOSITION_CANCELLED
                )
                try:
                    if not restarted_after_cancel:
                        await publish(
                            active_binding,
                            "failed",
                            {"code": "resident_process_restarted"},
                        )
                    await publish(
                        active_binding,
                        "settled",
                        (
                            {"state": "cancelled"}
                            if restarted_after_cancel
                            else {
                                "state": "failed",
                                "code": "resident_process_restarted",
                            }
                        ),
                    )
                except RunnerAttemptSupersededError:
                    # Authority rotated between the precheck and the settle: this
                    # is now superseded cleanup, not a failure to record.
                    pass
            elif active_disposition == RESIDENT_DISPOSITION_UNKNOWN:
                print("RESIDENT_CONTROL_QUARANTINED:startup_active_unknown", flush=True)
        active_path.unlink(missing_ok=True)
        stage_path.unlink(missing_ok=True)

    if stage_path.is_file():
        stored_event = None
        stored_binding = None
        stored_request = None
        try:
            stored_stage = json.loads(stage_path.read_text(encoding="utf-8"))
            stored_event = _verify_resident_control(stored_stage["event"])
            stored_binding = binding_of(stored_event)
            stored_request = stored_event["payload"]["request"]
            if not isinstance(stored_binding, Mapping) or not isinstance(
                stored_request,
                Mapping,
            ):
                raise RunnerContractError("resident_stage_invalid")
        except (
            OSError,
            ValueError,
            KeyError,
            TypeError,
            json.JSONDecodeError,
            RunnerContractError,
        ):
            # Unparseable, legacy-format (pre-event schema), or tampered residue,
            # e.g. a stage file left by a previous harness version across a
            # refresh. Discard it; never crash-loop the new process on residue it
            # cannot interpret.
            stage_path.unlink(missing_ok=True)
            stored_event = None
        attestable = False
        if stored_event is not None:
            try:
                require_process_binding(stored_binding, allow_unattested=True)
                attestable = True
            except RunnerContractError:
                # The persisted stage does not belong to this runner build/boot (a
                # redeploy or a foreign file). Discard the local residue only; do
                # not settle a binding we cannot attest, and do not risk an
                # RLS-rejected write under a token that may already be revoked.
                stage_path.unlink(missing_ok=True)
        if attestable:
            # Classify BEFORE spawning workers or emitting readiness. A stale
            # stage from a superseded attempt must be discarded locally -- never
            # replayed as current -- so the resident stops crash-looping on it and
            # is free to consume the current attempt's PRELOAD from the queue.
            stage_disposition = await disposition_of(stored_event)
            if stage_disposition == RESIDENT_DISPOSITION_CURRENT:
                resident_generation = int(stored_binding["generation"])
                resident_installation_id = str(stored_binding["installation_id"])
                resident_attempt_token = str(stored_binding["warm_attempt_token"])
                staged_binding = dict(stored_binding)
                staged_request = dict(stored_request)
                heartbeat_binding = attested_binding(staged_binding)
                pool = ResidentShardPool.start()
                try:
                    await publish(
                        attested_binding(staged_binding),
                        "agent_ready",
                        {
                            "worker_count": fanin.WORKER_COUNT,
                            "worker_ready_indexes": list(pool.worker_ready_indexes),
                            "warm_attempt_token": resident_attempt_token,
                            "runner_boot_id": runner_boot_id,
                            "runner_process_boot_id": process_boot_id,
                            "process_pid": os.getpid(),
                            "runner_harness_sha256": LOADED_RUNNER_HARNESS_SHA256,
                        },
                    )
                except RunnerAttemptSupersededError:
                    # Authority rotated between the precheck and the readiness
                    # insert. Discard the just-started residue; no crash.
                    await stop_pool()
                    stage_path.unlink(missing_ok=True)
                    staged_binding = None
                    staged_request = None
                    heartbeat_binding = None
                    resident_generation = generation or None
                    resident_installation_id = ""
                    resident_attempt_token = ""
            else:
                if stage_disposition == RESIDENT_DISPOSITION_UNKNOWN:
                    print("RESIDENT_CONTROL_QUARANTINED:startup_stage_unknown", flush=True)
                stage_path.unlink(missing_ok=True)

    active: dict[
        str,
        tuple[
            asyncio.Task[dict[str, object]],
            asyncio.Event,
            dict[str, object],
            asyncio.Event,
        ],
    ] = {}
    prepared_jobs: set[str] = set()

    async def execute(
        binding: Mapping[str, object],
        request: Mapping[str, object],
        cancelled: asyncio.Event,
        release_gate: asyncio.Event,
    ) -> dict[str, object]:
        if resident_generation is None or pool is None:
            raise RunnerContractError("resident_generation_unstaged")
        job_id = str(binding["job_id"])
        request = dict(request)
        canonical = dict(request)
        claimed_digest = canonical.pop("prepared_request_digest", None)
        if (
            claimed_digest != binding["request_sha256"]
            or hashlib.sha256(_canonical_json(canonical)).hexdigest() != claimed_digest
        ):
            raise RunnerContractError("resident_stage_request_digest_invalid")
        encoded = base64.urlsafe_b64encode(
            gzip.compress(_canonical_json(request), mtime=0)
        ).decode()
        run_id, targets, _trust, decoded = _decode_fanin_request(encoded)
        progress_values: collections.deque[dict[str, object]] = collections.deque()

        def capture_progress(value: Mapping[str, object]) -> None:
            progress_values.append(dict(value))

        fanin._progress_callback = capture_progress
        RESIDENT_LOCK_PATH.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
        lock_file = RESIDENT_LOCK_PATH.open("a+", encoding="utf-8")  # noqa: ASYNC230

        async def mark_prepared() -> None:
            await publish(
                binding,
                "prepared",
                {
                    "state": "prepared",
                    "worker_ready_count": fanin.WORKER_COUNT,
                    "request_sha256": binding["request_sha256"],
                },
            )
            prepared_jobs.add(job_id)

        try:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RunnerContractError("runner_busy") from exc
            run_task = asyncio.create_task(
                _execute_fanin_request(
                    decoded,
                    targets,
                    cancelled,
                    resident_pool=pool,
                    resident_release_gate=release_gate,
                    on_resident_prepared=mark_prepared,
                )
            )
            while not run_task.done() or progress_values:
                while progress_values:
                    await publish(binding, "progress", progress_values.popleft())
                if not run_task.done():
                    await asyncio.sleep(0.01)
            result = await run_task
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            lock_file.close()
            fanin._progress_callback = None
        if run_id != job_id:
            raise RunnerContractError("resident_job_identity_invalid")
        return result

    try:
        while True:
            for job_id, (
                task,
                _cancelled,
                active_binding,
                _release_gate,
            ) in tuple(active.items()):
                if not task.done():
                    continue
                active.pop(job_id, None)
                prepared_jobs.discard(job_id)
                try:
                    result = task.result()
                except BaseException as exc:
                    code = str(exc)
                    if not code or len(code) > 64 or not code.replace("_", "").isalnum():
                        code = "resident_runner_failed"
                    terminal_kind: str = "failed"
                    terminal_payload: dict[str, object] = {"code": code}
                    settlement: dict[str, object] = {"state": "failed", "code": code}
                else:
                    terminal_kind = "result"
                    terminal_payload = result
                    settlement = {"state": "completed"}
                try:
                    await publish(active_binding, terminal_kind, terminal_payload)
                    await stop_pool()
                    await publish(active_binding, "settled", settlement)
                except RunnerAttemptSupersededError:
                    # The attempt was revoked mid-run; do not settle under the
                    # revoked token. Discard local residue and stop.
                    await stop_pool()
                active_path.unlink(missing_ok=True)
                stage_path.unlink(missing_ok=True)
                return
            response = await asyncio.to_thread(
                sqs.receive_message,
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=1,
                VisibilityTimeout=720,
            )
            for message in response.get("Messages", []):
                binding: dict[str, object] | None = None
                acknowledge = False
                try:
                    event = _decode_resident_control(str(message.get("Body") or ""))
                    binding = binding_of(event)
                    event_generation = int(binding["generation"])
                    job_id = str(binding["job_id"])
                    kind = str(event["kind"])
                    expected_sequence = {
                        "preload": 1,
                        "stage": 1,
                        "release": 2,
                        "cancel": 3,
                    }[kind]
                    if int(event["sequence"]) != expected_sequence:
                        raise RunnerContractError("resident_control_sequence_invalid")
                    event_id = str(event["event_id"])
                    if event_id in consumed_control_events:
                        continue
                    require_process_binding(
                        binding,
                        allow_unattested=kind == "preload",
                    )
                    # Classify BEFORE any local transition (worker spawn/stop,
                    # stage/active file replacement, readiness/settlement emit).
                    # A delayed old attempt arrives on its own FIFO group
                    # (`lane-job`) and can be received after the attempt that
                    # superseded it; without this gate its PRELOAD would stop the
                    # pool and overwrite the current stage. Guarding every
                    # transition -- not just startup -- is what closes that race.
                    disposition = await disposition_of(event)
                    if disposition == RESIDENT_DISPOSITION_UNKNOWN:
                        # No matching dispatched outbox event: tampered, forged, or
                        # never dispatched. Quarantine with NO local state mutation
                        # and NO runner-event write (a 'quarantined' insert would
                        # itself be RLS-denied under an unknown identity). Drop the
                        # forged message so it does not redeliver forever.
                        print(
                            "RESIDENT_CONTROL_QUARANTINED:resident_control_disposition_unknown",
                            flush=True,
                        )
                        consumed_control_events.add(event_id)
                        acknowledge = True
                        continue
                    if disposition == RESIDENT_DISPOSITION_CANCELLED:
                        # The RELEASE crossed the network after its durable
                        # CANCEL committed. ACK only the revoked message: keep
                        # the staged/active job intact so the following FIFO
                        # CANCEL can set its cancellation event and publish the
                        # normal settlement. This is not a stale-attempt cleanup.
                        consumed_control_events.add(event_id)
                        acknowledge = True
                        continue
                    if disposition in {
                        RESIDENT_DISPOSITION_SUPERSEDED,
                        RESIDENT_DISPOSITION_TERMINAL,
                    }:
                        # A valid but revoked (superseded) or already-settled
                        # (terminal) event. Discard only this job's residue -- never
                        # another attempt's live pool/stage -- and ACK/delete the
                        # message without writing any runner event under the
                        # revoked token (no settle, no duplicate terminal).
                        await discard_residue(binding)
                        consumed_control_events.add(event_id)
                        acknowledge = True
                        continue
                    if kind == "preload":
                        if active:
                            raise RunnerContractError("resident_job_active")
                        next_identity = (
                            str(binding["installation_id"]),
                            event_generation,
                            str(binding["warm_attempt_token"]),
                        )
                        current_identity = (
                            resident_installation_id,
                            resident_generation,
                            resident_attempt_token,
                        )
                        if current_identity != ("", None, "") and (
                            current_identity != next_identity
                        ):
                            await stop_pool()
                            stage_path.unlink(missing_ok=True)
                            staged_binding = None
                            staged_request = None
                        resident_installation_id, resident_generation, resident_attempt_token = (
                            next_identity
                        )
                        if pool is None:
                            port_count, _used, remaining = fanin._ephemeral_port_usage()
                            required = (
                                fanin.TARGET_CLIENTS_PER_LANE
                                + fanin.EPHEMERAL_PORT_RESERVE_PER_LANE
                            )
                            if port_count < required or remaining < required:
                                raise RunnerContractError("ephemeral_port_reserve_exhausted")
                            pool = ResidentShardPool.start()
                        request = event["payload"]["request"]
                        assert isinstance(request, Mapping)
                        staged_binding = binding
                        staged_request = dict(request)
                        heartbeat_binding = attested_binding(binding)
                        persist(
                            stage_path,
                            {"event": event, "request": staged_request},
                        )
                        await publish(
                            attested_binding(binding),
                            "agent_ready",
                            {
                                "worker_count": fanin.WORKER_COUNT,
                                "worker_ready_indexes": list(pool.worker_ready_indexes),
                                "warm_attempt_token": resident_attempt_token,
                                "runner_boot_id": runner_boot_id,
                                "runner_process_boot_id": process_boot_id,
                                "process_pid": os.getpid(),
                                "runner_harness_sha256": (LOADED_RUNNER_HARNESS_SHA256),
                            },
                        )
                    elif kind == "stage":
                        if (
                            pool is None
                            or event_generation != resident_generation
                            or binding["installation_id"] != resident_installation_id
                            or binding["warm_attempt_token"] != resident_attempt_token
                        ):
                            raise RunnerContractError("resident_stage_without_preload")
                        request = event["payload"]["request"]
                        assert isinstance(request, Mapping)
                        staged_binding = binding
                        staged_request = dict(request)
                        heartbeat_binding = binding
                        persist(
                            stage_path,
                            {"event": event, "request": staged_request},
                        )
                        cancelled = asyncio.Event()
                        release_gate = asyncio.Event()
                        persist(
                            active_path,
                            {
                                "event": event,
                                "request": staged_request,
                                "state": "staging",
                            },
                        )
                        active[job_id] = (
                            asyncio.create_task(
                                execute(
                                    binding,
                                    staged_request,
                                    cancelled,
                                    release_gate,
                                )
                            ),
                            cancelled,
                            binding,
                            release_gate,
                        )
                    elif kind == "release":
                        if (
                            staged_binding != binding
                            or staged_request is None
                            or job_id not in active
                            or job_id not in prepared_jobs
                        ):
                            raise RunnerContractError("resident_release_without_stage")
                        persist(
                            active_path,
                            {
                                "event": event,
                                "request": staged_request,
                                "state": "released",
                            },
                        )
                        active[job_id][3].set()
                        await publish(
                            binding,
                            "progress",
                            {
                                "protocol": fanin.PROTOCOL,
                                "schema_version": fanin.SCHEMA_VERSION,
                                "lane_id": lane_id,
                                "phase": "ramping",
                                "initiated_clients": 0,
                                "authenticated_clients": 0,
                                "held_clients": 0,
                                "peak_held_clients": 0,
                                "terminal_failures": 0,
                                "sampled_queries_succeeded": 0,
                                "sampled_queries_failed": 0,
                                "elapsed_ms": 0.0,
                                "time_to_target_ms": None,
                                "milestone": "runner_observed_release",
                            },
                        )
                    else:
                        current = active.get(job_id)
                        if current is not None and current[2] == binding:
                            current[1].set()
                        elif staged_binding == binding:
                            await stop_pool()
                            await publish(
                                binding,
                                "settled",
                                {"state": "cancelled"},
                            )
                            active_path.unlink(missing_ok=True)
                            stage_path.unlink(missing_ok=True)
                            return
                        else:
                            raise RunnerContractError("resident_cancel_binding_invalid")
                    consumed_control_events.add(event_id)
                    acknowledge = True
                except asyncio.CancelledError:
                    raise
                except RunnerAttemptSupersededError:
                    # Authority rotated between the disposition precheck above and
                    # the RLS-gated insert this branch attempted. Reclassify: this
                    # is expected superseded cleanup, not a crash. Discard only
                    # this job's residue and ACK/delete the message; never settle
                    # or emit a terminal event under the now-revoked token.
                    if isinstance(binding, Mapping):
                        await discard_residue(binding)
                    acknowledge = True
                except Exception as exc:
                    if _resident_control_failure_is_permanent(exc):
                        code = str(exc) or type(exc).__name__
                        print(
                            "RESIDENT_CONTROL_QUARANTINED:"
                            + re.sub(r"[^A-Za-z0-9_]", "_", code)[:96],
                            flush=True,
                        )
                        try:
                            if isinstance(binding, Mapping):
                                await publish(
                                    binding,
                                    "quarantined",
                                    {"code": code[:96]},
                                )
                            acknowledge = True
                        except Exception:
                            acknowledge = False
                    else:
                        print(
                            "RESIDENT_CONTROL_TRANSIENT:" + type(exc).__name__,
                            flush=True,
                        )
                finally:
                    if acknowledge:
                        await asyncio.to_thread(
                            sqs.delete_message,
                            QueueUrl=queue_url,
                            ReceiptHandle=message["ReceiptHandle"],
                        )
            if (
                pool is not None
                and heartbeat_binding is not None
                and time.monotonic() - last_heartbeat_at >= 2.0
            ):
                try:
                    await publish(
                        heartbeat_binding,
                        "heartbeat",
                        {
                            "worker_ready_indexes": list(pool.worker_ready_indexes),
                            "runner_boot_id": runner_boot_id,
                            "runner_process_boot_id": process_boot_id,
                            "process_pid": os.getpid(),
                            "runner_harness_sha256": LOADED_RUNNER_HARNESS_SHA256,
                        },
                    )
                except RunnerAttemptSupersededError:
                    # This identity's token was revoked. Stop attesting it and
                    # tear down its pool, but keep the process alive: a later
                    # current PRELOAD re-establishes a fresh pool and heartbeat.
                    # Only this attempt's residue is cleared; nothing else.
                    heartbeat_binding = None
                    staged_binding = None
                    staged_request = None
                    resident_installation_id = ""
                    resident_attempt_token = ""
                    resident_generation = generation or None
                    stage_path.unlink(missing_ok=True)
                    await stop_pool()
                else:
                    last_heartbeat_at = time.monotonic()
            await asyncio.sleep(0)
    finally:
        await stop_pool()


def main() -> int:
    if len(sys.argv) == 6 and sys.argv[1] == "--resident-agent":
        asyncio.run(
            _resident_agent(
                lane_id=sys.argv[2],
                generation=int(sys.argv[3]),
                queue_url=sys.argv[4],
                control_secret_arn=sys.argv[5],
            )
        )
        return 0
    run_id = "unknown"
    lock_file: Any | None = None
    lock_acquired = False
    setup_nonce: str | None = None
    setup_cancelled: asyncio.Event | None = None
    job_directory: Path | None = None
    job_owner = False
    job_lock: Any | None = None
    job_taken_over = False
    progress_sequence = 0
    progress_bytes = 0
    exit_code = 1
    try:
        if len(sys.argv) != 2:
            raise RunnerContractError("request_missing")
        envelope = _decode_payload(sys.argv[1])
        protocol = envelope.get("protocol")
        setup_request: dict[str, object] | None = None
        fanin_request: dict[str, object] | None = None
        job_control: dict[str, object] | None = None
        targets: tuple[Target, ...] = ()
        attempts: tuple[Attempt, ...] = ()
        trust_bundle_sha256 = ""
        if protocol == JOB_PROTOCOL:
            job_control = _decode_job_control(sys.argv[1])
            run_id = str(job_control["job_id"])
        elif protocol == SETUP_PROTOCOL:
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
            run_id, targets, trust_bundle_sha256, fanin_request = _decode_fanin_request(sys.argv[1])
        elif protocol == BOUNDED_PROTOCOL:
            run_id, targets, attempts, trust_bundle_sha256 = _decode_request(sys.argv[1])
        else:
            raise RunnerContractError("protocol_invalid")
        if job_control is not None:
            return _job_control(job_control)
        if fanin_request is not None and fanin_request["action"] == "run_lane_v3":
            job_directory, job_owner, job_lock = _claim_job(fanin_request)
            if not job_owner:
                rejoin = _rejoin_or_takeover_job(job_directory, run_id, fanin_request)
                if rejoin.disposition == "takeover":
                    # The first owner died before settling. Adopt its published
                    # identity and per-job lock and run the job to completion, so
                    # a duplicate never waits out the SSM command timeout.
                    job_owner = True
                    job_taken_over = True
                    job_lock = rejoin.lock
                else:
                    if rejoin.encoded_result is not None:
                        print("RESULT_GZIP_BASE64:" + rejoin.encoded_result, flush=True)
                        print(f"CLEANUP_CONFIRMED:{run_id}", flush=True)
                        print(f"RUNNER_FLOCK_RELEASED:{run_id}", flush=True)
                        return 0
                    if rejoin.was_cancelled:
                        print(f"RUNNER_CANCELLED:{run_id}", flush=True)
                    return 1
        LOCK_PATH.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
        lock_file = LOCK_PATH.open("a+", encoding="utf-8")
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RunnerContractError("runner_busy") from exc
        lock_acquired = True
        _validate_runtime()
        if job_directory is not None:
            _atomic_job_write(job_directory, "state", "running")
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

        if fanin_request is not None and fanin_request["action"] == "preflight":
            preflight = asyncio.run(
                fanin.capacity_preflight(str(fanin_request.get("runner_instance_type") or ""))
            )
            shard_preflight = shard_process_preflight()
            harness_assets, harness_sha256 = _runner_harness_evidence()
            preflight["shard_process_preflight"] = shard_preflight
            preflight["runner_asset_sha256s"] = harness_assets
            preflight["runner_harness_sha256"] = harness_sha256
            encoded_preflight = _canonical_json(preflight).decode("utf-8")
            if len(encoded_preflight.encode("utf-8")) > 4096:
                raise RunnerContractError("preflight_result_too_large")
            print("PREFLIGHT_RESULT:" + encoded_preflight, flush=True)
            print(f"CLEANUP_CONFIRMED:{run_id}", flush=True)
            exit_code = 0
            return exit_code

        _validate_trust_bundle(trust_bundle_sha256)
        if job_taken_over:
            # A dead owner may have left its run directory behind; we hold the
            # per-job lock, so reclaiming it here cannot race a live writer.
            _reclaim_stale_run_directory(run_id)
        run_directory = _prepare_run_directory(run_id)
        cancelled = asyncio.Event()

        def stop(*unused: object) -> None:
            cancelled.set()

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        if job_directory is not None:

            def persist_progress(value: Mapping[str, object]) -> None:
                nonlocal progress_bytes, progress_sequence
                payload = {
                    name: value[name] for name in fanin.PROGRESS_WIRE_FIELDS if name in value
                }
                payload["sequence"] = progress_sequence + 1
                line = fanin.PROGRESS_PREFIX + _canonical_json(payload).decode("utf-8")
                encoded_bytes = len(line.encode("utf-8")) + 1
                if progress_bytes + encoded_bytes > fanin.PROGRESS_OUTPUT_BUDGET_BYTES:
                    return
                progress_sequence += 1
                progress_bytes += encoded_bytes
                with (job_directory / "progress.jsonl").open(
                    "a",
                    encoding="utf-8",
                ) as stream:
                    stream.write(line + "\n")
                _atomic_job_write(
                    job_directory,
                    "latest_progress",
                    _canonical_json(payload).decode("utf-8"),
                )
                print(line, flush=True)

            fanin._progress_callback = persist_progress

        async def bounded() -> tuple[dict[str, object] | None, bool]:
            # The fan-in path owns its authoritative deadlines inside
            # _execute_sharded_fanin: a readiness budget before the authoritative
            # release and the release-anchored FANIN_WORKER_RUN_TIMEOUT after T0.
            # A second outer timeout anchored at process entry duplicates that
            # budget and, behind a Proxy exact gate, would clip the scored ramp
            # before it began.  Run the fan-in path under the inner deadline
            # alone; the legacy lifecycle path keeps its own outer bound.
            timeout = None if fanin_request is not None else RUN_TIMEOUT_SECONDS
            async with asyncio.timeout(timeout):
                lifecycle = asyncio.create_task(
                    _execute_fanin_request(fanin_request, targets, cancelled)
                    if fanin_request is not None
                    else _lifecycle(run_id, targets, attempts, cancelled, run_directory)
                )

                async def watch_job_cancel() -> None:
                    if job_directory is None:
                        await asyncio.Event().wait()
                        return
                    while not (  # noqa: ASYNC110 - another process sets the file
                        job_directory / "cancel_requested"
                    ).is_file():
                        await asyncio.sleep(0.1)
                    cancelled.set()

                cancellation = asyncio.create_task(cancelled.wait())
                registry_cancellation = asyncio.create_task(watch_job_cancel())
                try:
                    done, _ = await asyncio.wait(
                        (lifecycle, cancellation, registry_cancellation),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if (
                        cancellation in done or registry_cancellation in done
                    ) and cancelled.is_set():
                        if fanin_request is None:
                            lifecycle.cancel()
                            await asyncio.gather(lifecycle, return_exceptions=True)
                            return None, True
                        return await lifecycle, True
                    completed = await lifecycle
                    if fanin_request is None:
                        # The bounded lifecycle already returns
                        # ``(result, was_cancelled)``. Wrapping it again encoded
                        # the successful result as a JSON array, which the
                        # server correctly rejected as an unexpected shape.
                        return completed
                    return completed, False
                finally:
                    cancellation.cancel()
                    registry_cancellation.cancel()
                    await asyncio.gather(
                        cancellation,
                        registry_cancellation,
                        return_exceptions=True,
                    )
                    if fanin_request is not None:
                        cleanup = asyncio.create_task(
                            asyncio.to_thread(_cleanup_owned, run_id, run_directory)
                        )
                        await asyncio.shield(cleanup)
                        print(f"CLEANUP_CONFIRMED:{run_id}", flush=True)

        result, was_cancelled = asyncio.run(bounded())
        if was_cancelled:
            if result is not None:
                print(
                    "TOWEL_GZIP_BASE64:" + _encode_result(result),
                    flush=True,
                )
            if job_directory is not None:
                _atomic_job_write(job_directory, "state", "cancelled")
            print(f"RUNNER_CANCELLED:{run_id}", flush=True)
        elif result is not None:
            encode_started_ns = time.perf_counter_ns()
            encoded_result = _encode_result(result)
            encode_elapsed_ms = (time.perf_counter_ns() - encode_started_ns) / 1_000_000
            print(
                PARENT_RESULT_PROFILE_PREFIX
                + json.dumps(
                    {
                        "operation": "result_json_gzip_base64",
                        "elapsed_ms": encode_elapsed_ms,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                flush=True,
            )
            print(
                "RESULT_GZIP_BASE64:" + encoded_result,
                flush=True,
            )
            if job_directory is not None:
                _atomic_job_write(
                    job_directory,
                    "result_gzip_base64",
                    encoded_result,
                )
                # This registry stores a raw runner result.  Only the server's
                # contract finalizer may call a lane verified.
                _atomic_job_write(job_directory, "state", "completed")
            exit_code = 0
    except RunnerContractError as exc:
        if job_directory is not None and job_owner:
            _atomic_job_write(job_directory, "state", "failed")
        print(f"RUNNER_ERROR:{exc.args[0] if exc.args else 'contract_failed'}", flush=True)
    except fanin.FanInProtocolError as exc:
        raw_code = str(exc)
        code = (
            raw_code
            if raw_code and len(raw_code) <= 64 and raw_code.replace("_", "").isalnum()
            else "fanin_protocol_failed"
        )
        if job_directory is not None and job_owner:
            _atomic_job_write(job_directory, "state", "failed")
        print(f"RUNNER_ERROR:{code}", flush=True)
    except (TimeoutError, RunnerCancelled, asyncio.CancelledError):
        if job_directory is not None and job_owner:
            _atomic_job_write(job_directory, "state", "cancelled")
        print(f"RUNNER_CANCELLED:{run_id}", flush=True)
    except Exception:
        if job_directory is not None and job_owner:
            _atomic_job_write(job_directory, "state", "failed")
        print("RUNNER_ERROR:operation_failed", flush=True)
    finally:
        fanin._progress_callback = None
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
                if job_directory is not None and job_owner:
                    _atomic_job_write(job_directory, "settled", "true")
                print(f"RUNNER_FLOCK_RELEASED:{run_id}", flush=True)
        if job_directory is not None and job_owner and not lock_acquired:
            # No flock was acquired, so terminal registry state is already
            # settled once this invocation reaches its finalizer.
            _atomic_job_write(job_directory, "settled", "true")
        if job_lock is not None:
            # Release the per-job ownership lock only after the terminal registry
            # state is durably settled above. A rejoining invocation treats a
            # released lock over an unsettled job as owner death and would take
            # over, so this settle-before-release order is what makes takeover a
            # correct recovery rather than a second concurrent run.
            _release_job_lock(job_lock)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
