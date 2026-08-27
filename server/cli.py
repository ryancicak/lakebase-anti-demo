from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from collections.abc import Mapping
from dataclasses import asdict
from pathlib import Path

import psycopg
from botocore.exceptions import BotoCoreError, ClientError

from .generation_lock import (
    GenerationBusyError,
    generation_lock_path,
    hold_generation,
    lock_is_held,
    read_holder,
    transitional_status_recovery,
)
from .lifecycle import (
    DEFAULT_TTL_HOURS,
    Check,
    cleanup,
    deployed_renew_followup,
    doctor,
    ensure_coordination,
    installation_presence_check,
    operator_ingress_check,
    provision,
    refresh_round5_runner,
    renew,
    reset,
    resume_provision,
    setup,
)
from .manifest import apply_manifest_environment, load_manifest, manifest_path
from .process_registry import (
    SERVER_HOST_ENV,
    SERVER_PORT_ENV,
    SUPERVISION_UNKNOWN,
    SUPERVISION_UNSUPERVISED,
    RecordStatus,
    _pid_is_alive,
    inspect_record,
    state_dir_from_environ,
)
from .round_construction import probe_round_construction
from .server_launch import (
    DEFAULT_LOG_KEEP,
    DEFAULT_LOG_MAX_BYTES,
    LOG_KEEP_ENV,
    LOG_MAX_BYTES_ENV,
    RESTART_WINDOW_SECONDS,
    configure_operator_logging,
    default_log_path,
    read_restart_history,
    require_serving_environment,
    restart_record_path,
    serve_command,
    serve_in_background,
)


def _mutating(operation: str):
    """Hold this generation for one mutating operation.

    The lock lives at this one choke point rather than inside each `lifecycle`
    entry point. Every mutation arrives through a subcommand, and `setup` calls
    `reconcile_infrastructure`, `reset` and `resume_provision` in turn, so a lock
    taken here covers all of them without any of them having to know it exists --
    and without the nested calls needing a second, reentrant acquire.

    Deliberately absent from `serve` (except for the one coordination write it
    performs before handing over to uvicorn) and from `status`. A server that
    held this for its whole life would make resealing impossible without taking
    the demo down, which is the failure mode this exists to prevent, not repeat.
    """
    return hold_generation(manifest_path(), operation)


def print_checks(checks: list[Check], as_json: bool) -> None:
    if as_json:
        print(json.dumps({"checks": [asdict(check) for check in checks]}, indent=2))
        return
    for check in checks:
        mark = "PASS" if check.ok else ("WARN" if check.advisory else "FAIL")
        suffix = " (advisory: reported, does not fail this run)" if (
            check.advisory and not check.ok
        ) else ""
        print(f"{mark:4}  {check.name:28} {check.detail}{suffix}")


def checks_passed(checks: list[Check]) -> bool:
    """Aggregate checks, letting advisory findings report without failing the run.

    An advisory check still prints on its own line with a WARN marker; it just
    does not decide the exit code. Keeping the aggregation in one place stops a
    future check from becoming accidentally blocking at one call site only.
    """
    return all(check.ok or check.advisory for check in checks)


def round_construction_checks() -> list[Check]:
    """Ask whether each sealed round can still build its config.

    The gap this closes: doctor's existing round checks validate the seal against
    live AWS and Databricks and none of them asks whether the seal still builds a
    config, so a manifest field that only a config builder reads passed every
    doctor line and still removed the round from the running app.

    Assembled here rather than inside `lifecycle.doctor` only because
    `server/lifecycle.py` is held open by other work. It belongs beside
    `_round4_check`, `_round5_topology_check` and `_round6_check`; it reads the
    manifest and calls no provider, so its position in the list is immaterial.

    Not advisory. A round that cannot be built is a fault rather than advice: the
    installation genuinely cannot serve what it claims to have installed.
    """

    try:
        manifest = load_manifest()
    except Exception:
        # doctor's own `owned_manifest` check already reports this, and saying it
        # twice is not diagnosis.
        return []
    return [
        Check(probe.check_name, probe.ok, probe.detail)
        for probe in probe_round_construction(manifest)
    ]


def round5_principal_notice(manifest: object, *, probe=None) -> str | None:
    """Say so when this process is not the principal Round 5 trusts, and serve anyway.

    The trust policy on the Round 5 control role names one principal and was
    sealed from whoever provisioned the installation. Serving under a different
    one gives an installation where five rounds work and the sixth cannot assume
    its role, and the operator should hear that at launch rather than from a round
    that dies at arm.

    This used to return a refusal and `_serve` used to exit on it, which got the
    priority exactly backwards. A mismatch costs one round; refusing to start
    costs all six. The mismatch is *already* handled correctly downstream and
    without this function's help: the credential probe reports
    `principal_mismatch`, `/readyz` degrades, and `round_availability` takes
    Round 5 off the card with its reason attached -- so the round is never
    advertised and never dies in front of anyone. Losing the whole demo to
    protect a round that was already being withheld is not a safety net, it is
    the larger outage. So the information survives and the exit does not.

    The comparison is not reimplemented. `probe_once` already makes it and already
    knows every way it can be inconclusive, so this speaks on exactly one of its
    answers -- `principal_mismatch`, which `principal_matches` returns only when
    both ARNs parsed and named different principals.

    Everything else is silent. A probe that cannot reach STS, is throttled, has no
    credentials in this process, or cannot compare a federated principal to a role
    is *not* a mismatch, and must not print a scare at launch: the operator would
    learn to ignore the line, and the one time it means something is the one time
    it matters. An installation with no Round 5 has nothing to compare and is not
    asked.
    """
    from .aws_credential_probe import expectations_from_manifest, probe_once

    if manifest is None:
        return None
    try:
        expectations = expectations_from_manifest(manifest)
    except (AttributeError, TypeError, ValueError):
        # An unreadable manifest is somebody else's refusal, with better words.
        return None
    if not expectations.round5_trusted_principal_arn:
        return None
    try:
        import boto3

        verdict = (probe or probe_once)(expectations, session_factory=boto3.Session)
    except Exception:  # noqa: BLE001 - a probe that cannot run is not a mismatch
        return None
    if getattr(verdict, "state", None) != "principal_mismatch":
        return None
    return (
        "!! SERVING WITHOUT ROUND 5: this process is not the principal Round 5 trusts.\n"
        f"        {verdict.detail}\n"
        "        The other five rounds are starting normally. Round 5 will be "
        "reported unavailable in /api/catalog with this reason, so the fight card "
        "will not offer it -- it cannot die at arm. To get it back, serve under the "
        "sealed principal, or reseal the trust policy and the seal together."
    )


def _pipeline_session_notice(manifest: object) -> list[str]:
    """The Round 4 pipeline's bill and its arm precondition, at session start.

    Placed here because `antidemo serve` is the one thing every session passes
    through, and because the foreground path ends in `execvp` --- there is no
    return, so there is no "session over" hook in this process to hang the
    reminder on instead. Session start is the moment the tool reliably owns.

    Swallows everything. This is a reminder printed beside a server launch, and a
    reminder that can stop a demo from starting is worse than a forgotten stop:
    the whole point of the line is that the operator is about to present.
    """

    try:
        from .pipeline_power import session_notice

        return session_notice(manifest)  # type: ignore[arg-type]
    except Exception:  # noqa: BLE001 - see the docstring: never block a serve
        return []


def _serve(host: str, port: int, *, background: bool = False, log: str = "") -> int:
    """Serve this installation, in the foreground or detached from this shell.

    Only returns in `--background` mode, and then with the exit code the caller
    should use. The foreground path ends in `execvp` and never returns at all.
    """
    # First, because nothing downstream will build an environment: the serve
    # command passes `--no-sync` so that a drifted tree cannot trigger a
    # dependency resolve seconds before a demo. The trade is that an environment
    # which was never provisioned has to be refused here by name -- left alone,
    # `uv run --no-sync` creates an empty virtualenv and dies on `No module
    # named uvicorn`, which reads as a bug in this tree rather than as a missing
    # install step.
    require_serving_environment()
    if manifest_path().exists():
        manifest = load_manifest()
        # `ensure_coordination` creates the coordination branch and rewrites the
        # manifest, so it is a mutation and takes the lock like any other. It is
        # released before this function launches anything: the descriptor is
        # close-on-exec, and -- this is what matters for `--background`, since
        # `fork` does not honour close-on-exec -- the block has also already
        # ended, so the descriptor is closed outright before any fork happens.
        # A daemon that inherited a held `flock` would keep it for weeks and
        # deadlock every later mutation.
        with _mutating("antidemo serve (coordination check)"):
            ensure_coordination(manifest)
        apply_manifest_environment(manifest)
        # After the environment is applied, so the probe resolves the credentials
        # this server would actually serve with, and before anything is launched.
        # Printed and then passed over: see `round5_principal_notice` for why this
        # deliberately does not stop the launch.
        notice = round5_principal_notice(manifest)
        if notice is not None:
            print(notice, file=sys.stderr)
        for line in _pipeline_session_notice(manifest):
            print(line, flush=True)
    # The app registers its own pid, so it needs to know the endpoint even when a
    # future entrypoint stops passing --host/--port on the command line.
    os.environ[SERVER_HOST_ENV] = host
    os.environ[SERVER_PORT_ENV] = str(port)
    if background:
        return serve_in_background(
            host,
            port,
            log_path=_serve_log_path(port, log),
            online=_app_is_online,
            readiness=_app_readiness,
        )
    os.execvp("uv", serve_command(host, port))
    # Unreachable: execvp either replaces this process or raises.
    return 0


def _serve_log_path(port: int, requested: str) -> Path:
    """Where a detached server writes. Explicit request first, else the generation.

    Refuses to invent a location. A background server whose log went somewhere
    nobody looks is how the last workaround managed to disable half the demo
    without anyone noticing.
    """
    if requested.strip():
        return Path(requested).expanduser().resolve()
    resolved = default_log_path(port)
    if resolved is not None:
        return resolved
    raise RuntimeError(
        "A background server needs somewhere to write its log. The default is "
        "beside the selected manifest, but ANTI_DEMO_MANIFEST is unset or does "
        "not name an existing generation. Set it, or pass '--log PATH'."
    )


def _status_checks(host: str, port: int, **probes: object) -> list[Check]:
    """Report the launch record without trusting it, and say so when it lies.

    `probes` forwards the liveness and identity lookups to `inspect_record` so a
    test can describe a process it does not have to spawn.
    """
    state_dir = state_dir_from_environ()
    online = _app_is_online(host, port)
    port_detail = (
        f"PORT {port} ANSWERS /api/health" if online else f"PORT {port} IS NOT ANSWERING"
    )
    if state_dir is None:
        return [
            Check("server_launch_record", False, "NO STATE DIRECTORY · SET ANTI_DEMO_MANIFEST"),
            Check("server_port", online, port_detail),
        ]
    status = inspect_record(state_dir, port, **probes)
    checks = [
        Check("server_launch_record", status.state in {"live", "absent"}, status.detail),
        Check("server_port", online, port_detail),
    ]
    # The exact failure this command exists to catch: a served port plus a record
    # that names something else. Trusting the record here kills the wrong process.
    if online and not status.safe_to_signal:
        checks.append(
            Check(
                "server_record_agrees",
                False,
                f"PORT ANSWERS BUT RECORD IS {status.state.upper()} · "
                "FIND THE REAL PID WITH lsof BEFORE SIGNALLING",
            )
        )
    else:
        checks.append(
            Check("server_record_agrees", True, "RECORD AND PORT TELL THE SAME STORY")
        )
    # The same liveness probe the record check used, so a test that describes a
    # process it never spawned describes it to both.
    checks.append(_supervisor_check(status, pid_is_alive=probes.get("pid_is_alive")))
    checks.append(_restart_history_check(port))
    return checks + _generation_checks()


def _supervisor_check(status: RecordStatus, *, pid_is_alive=None) -> Check:
    """Name the supervisor, and never let its absence pass for its presence.

    Worth a line of its own because the answer decides what an operator should
    expect next: a supervised server that dies comes back and says so in the
    restart record, and an unsupervised one just stops. The record used to invite
    the wrong answer -- `launcher_pid` held the `uv` shim, one below the
    supervisor -- so this reads the field that names the supervisor and nothing
    else.

    Four outcomes, because the record can be silent as well as positive or
    negative, and read at the same arm's length as the server pid beside it: the
    record says who was watching at launch, and only a liveness check can say
    whether it still is.
    """
    alive = _pid_is_alive if pid_is_alive is None else pid_is_alive
    record = status.record
    if record is None:
        return Check("server_supervisor", True, "NO RECORD · NO SUPERVISOR TO NAME", advisory=True)
    if record.supervision == SUPERVISION_UNKNOWN:
        return Check(
            "server_supervisor",
            True,
            f"RECORD SCHEMA {record.record_schema} PREDATES SUPERVISOR TRACKING · "
            f"UNKNOWN WHETHER PID {record.pid} IS SUPERVISED",
            advisory=True,
        )
    if record.supervision == SUPERVISION_UNSUPERVISED:
        return Check(
            "server_supervisor",
            True,
            "NO SUPERVISOR · A CRASH WILL NOT BE RESTARTED",
            advisory=True,
        )
    supervisor_pid = int(record.supervisor_pid or 0)
    if not alive(supervisor_pid):
        return Check(
            "server_supervisor",
            False,
            f"RECORDED SUPERVISOR {supervisor_pid} HAS EXITED · "
            f"PID {record.pid} IS RUNNING UNSUPERVISED",
            advisory=True,
        )
    return Check(
        "server_supervisor",
        True,
        f"SUPERVISOR {supervisor_pid} IS ALIVE · "
        f"PARENT OF PID {record.pid} IS THE uv SHIM {record.parent_pid}",
    )


def _restart_history_check(port: int) -> Check:
    """Report that the process answering is a replacement, not an original.

    A supervised server that has died and been replaced looks identical to one
    that has been up all evening -- same port, same record, same answers -- and
    the difference is the whole story when a round has started misbehaving. The
    supervisor already writes this down for exactly that reason; nothing was
    reading it back.

    Advisory: a restart that has been dealt with must not fail `antidemo status`, and
    an operator whose deliberate stop cleared the record gets the same answer as a
    first run, which is the truth. Giving up is the one line that reads as a fault,
    because a supervisor that has stopped trying is not going to fix itself.
    """
    # The record is named for the port, and `status` takes one on the command
    # line without exporting it, so the requested port is what this reads.
    path = restart_record_path({**os.environ, SERVER_PORT_ENV: str(port)})
    if path is None:
        return Check(
            "server_restart_history",
            True,
            "NO STATE DIRECTORY · NO RESTART HISTORY TO READ",
            advisory=True,
        )
    history = read_restart_history(path)
    if history.gave_up:
        return Check(
            "server_restart_history",
            False,
            f"SUPERVISOR GAVE UP AFTER {history.restarts} RESTART(S) · "
            f"LAST {history.last_reason or 'AN UNRECORDED REASON'} "
            f"AT {history.last_at or 'AN UNRECORDED TIME'} · "
            "NOTHING IS WATCHING THIS PORT NOW",
        )
    if history.restarts == 0:
        return Check("server_restart_history", True, "NO RESTARTS RECORDED")
    return Check(
        "server_restart_history",
        not history.flapping,
        f"{history.restarts} RESTART(S) RECORDED, {history.recent} IN THE LAST "
        f"{RESTART_WINDOW_SECONDS / 60:.0f} MINUTES · "
        f"LAST {history.last_reason or 'AN UNRECORDED REASON'} "
        f"AT {history.last_at or 'AN UNRECORDED TIME'}",
        advisory=True,
    )


def _generation_checks() -> list[Check]:
    """Report who is mutating this generation and how to unstick a held status.

    This takes no lock it keeps. `antidemo status` is the command an operator reaches
    for precisely *because* something looks wrong, so it has to answer during a
    mutation instead of queueing behind a fifteen-minute Terraform apply. The
    probe in `lock_is_held` acquires and immediately releases, which disturbs no
    holder and cannot be fooled by a record left behind by a process that has
    since exited.
    """
    try:
        path = manifest_path()
    except RuntimeError:
        return []
    lock_path = generation_lock_path(path)
    if lock_is_held(lock_path):
        holder = read_holder(lock_path)
        detail = (
            f"{holder.operation.upper()} IS MUTATING THIS GENERATION · PID {holder.pid} · "
            f"SINCE {holder.claimed_at or 'AN UNRECORDED TIME'}"
            if holder is not None
            else f"HELD BY AN UNIDENTIFIED PROCESS · FIND IT WITH lsof {lock_path}"
        )
        # A mutation in flight is the lock working, not a fault, so it reports
        # without deciding the exit code.
        checks = [Check("generation_lock", True, detail, advisory=True)]
    else:
        checks = [Check("generation_lock", True, "NO PROCESS IS MUTATING THIS GENERATION")]
    if not path.exists():
        # Not a fault. This is every generation before its first seal, and the
        # commands that care already say so in their own words.
        checks.append(Check("manifest_status", True, "NO MANIFEST SEALED YET"))
        return checks
    try:
        manifest = load_manifest(path)
        status = manifest.status
    except (RuntimeError, OSError, ValueError) as failure:
        # Swallowing this would be the same mistake as the wedge itself: the one
        # command an operator runs when things look wrong would go quiet about
        # the file everything else depends on. `doctor` explains it in full; this
        # only has to say which file and stop the search here.
        checks.append(
            Check(
                "manifest_status",
                False,
                f"MANIFEST AT {path} COULD NOT BE READ ({type(failure).__name__}) · "
                "RUN './antidemo doctor' FOR THE DETAIL",
            )
        )
        return checks
    recovery = transitional_status_recovery(status, manifest_path=path)
    if recovery is not None:
        checks.append(Check("manifest_status", False, f"{status.upper()} · {recovery}"))
    elif status == "ready":
        checks.append(Check("manifest_status", True, status.upper()))
    else:
        # `transitional_status_recovery` returns None for two unlike reasons, and
        # reading that None alone as PASS conflated them: `ready` needs no advice,
        # while `cleanup_failed` has none to give because it cannot be *resumed*
        # at all. So a half-finished cleanup printed as a green line and this
        # command exited 0 -- and it was the only surface saying so. `setup` and
        # `renew` refuse the status by name, `_load_ready_manifest` raises
        # `InvalidStateError` on it, self-heal refuses it with
        # REFUSAL_CLEANUP_FAILED, and bootstrap.sh's own gate asks for `ready`
        # exactly. The three transitional statuses already fail here despite
        # being the recoverable ones, so the severity order was inverted too.
        checks.append(
            Check(
                "manifest_status",
                False,
                f"{status.upper()} · NOT READY AND NOT RESUMABLE · './antidemo setup' "
                "AND './antidemo renew' BOTH REFUSE THIS STATUS BY NAME · SEALED "
                "RESOURCES MAY STILL EXIST AND BILL · RUN './antidemo cleanup "
                "--dry-run' AND READ WHAT IT FINDS",
            )
        )
    # A stale operator address is the failure this command is most likely to be
    # run against without anybody suspecting it: rounds that talk straight to
    # Aurora or RDS stop connecting, and nothing else on screen mentions ingress.
    # Cached and fail-soft, so it costs a probe every few minutes at most and
    # cannot make `antidemo status` fail to answer.
    checks.append(operator_ingress_check(manifest))
    # The failure this command could not report at all: the sandbox reaper sweeps
    # the account on a fortnightly cycle, and until now nothing an operator could
    # run said so. Cached and fail-soft on the same terms as the ingress line, and
    # it distinguishes a confirmed absence from a sweep that could not look --
    # only the first decides the exit code.
    checks.append(installation_presence_check(manifest))
    return checks


def _app_is_online(host: str, port: int) -> bool:
    probe_host = "127.0.0.1" if host in {"0.0.0.0", "::"} else host
    try:
        with urllib.request.urlopen(
            f"http://{probe_host}:{port}/api/health", timeout=1
        ) as response:
            payload = json.loads(response.read(1024))
        return payload == {"status": "ok", "database_connections": "sealed"}
    except (OSError, ValueError):
        return False


def _app_readiness(host: str, port: int) -> dict[str, object] | None:
    """Read `/readyz`, including when it is saying no.

    A degraded server answers 503 here, so the error body *is* the answer and
    discarding it would leave exactly the blind spot this reads to close. `None`
    means the surface could not be read at all, which is not the same as ready and
    is reported as neither.
    """
    probe_host = "127.0.0.1" if host in {"0.0.0.0", "::"} else host
    url = f"http://{probe_host}:{port}/readyz"
    try:
        with urllib.request.urlopen(url, timeout=2) as response:
            body = response.read(64_000)
    except urllib.error.HTTPError as refusal:
        try:
            body = refusal.read(64_000)
        except OSError:
            return None
    except (OSError, ValueError):
        return None
    try:
        payload = json.loads(body)
    except ValueError:
        return None
    return payload if isinstance(payload, dict) else None


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="antidemo",
        description="Lakebase: The Anti-Demo operator",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    setup_parser = subparsers.add_parser(
        "setup",
        help="Make Round 1 stage-ready and open the local UI",
    )
    setup_parser.add_argument(
        "--databricks-profile",
        default=os.environ.get("DATABRICKS_PROFILE", ""),
    )
    setup_parser.add_argument(
        "--aws-profile",
        default=os.environ.get("AWS_PROFILE", ""),
    )
    setup_parser.add_argument(
        "--aws-region",
        # No compiled-in fallback: see _resolved_aws_region. AWS_DEFAULT_REGION is
        # consulted there rather than here so both spellings behave identically.
        default=os.environ.get("AWS_REGION", ""),
    )
    setup_parser.add_argument(
        "--expected-account",
        default=os.environ.get("AWS_EXPECTED_ACCOUNT_ID", ""),
    )
    setup_parser.add_argument("--owner", default="")
    setup_parser.add_argument("--operator-cidr")
    # No default: setup must be able to tell "the operator asked for a TTL" from
    # "the operator said nothing", because on an existing installation the flag is
    # refused with a pointer to `antidemo renew` instead of being silently ignored.
    setup_parser.add_argument(
        "--ttl-hours",
        type=float,
        default=None,
        help=(
            f"Hours until expires-at for a first provision (default "
            f"{DEFAULT_TTL_HOURS:g}). Refused on an existing installation; use "
            f"'antidemo renew --ttl-hours N' there."
        ),
    )
    setup_parser.add_argument("--timeout", type=float, default=900)
    setup_parser.add_argument("--host", default="127.0.0.1")
    setup_parser.add_argument("--port", default=8000, type=int)
    setup_parser.add_argument("--no-serve", action="store_true")

    provision_parser = subparsers.add_parser(
        "provision",
        help="Create one owned Lakebase + AWS Round 1 environment",
    )
    provision_parser.add_argument(
        "--databricks-profile",
        default=os.environ.get("DATABRICKS_PROFILE", ""),
    )
    provision_parser.add_argument(
        "--aws-profile",
        default=os.environ.get("AWS_PROFILE", ""),
    )
    provision_parser.add_argument(
        "--aws-region",
        # No compiled-in fallback: see _resolved_aws_region. AWS_DEFAULT_REGION is
        # consulted there rather than here so both spellings behave identically.
        default=os.environ.get("AWS_REGION", ""),
    )
    provision_parser.add_argument(
        "--expected-account",
        default=os.environ.get("AWS_EXPECTED_ACCOUNT_ID", ""),
    )
    provision_parser.add_argument("--owner", default="")
    provision_parser.add_argument(
        "--operator-cidr",
        help="Explicit public IPv4 /32; defaults to the detected operator address",
    )
    provision_parser.add_argument("--ttl-hours", type=float, default=DEFAULT_TTL_HOURS)
    provision_parser.add_argument("--zero-timeout", type=float, default=900)

    reset_parser = subparsers.add_parser(
        "reset",
        help="Verify schema, clear probes, close clients, and wait for scale zero",
    )
    reset_parser.add_argument("--timeout", type=float, default=900)

    runner_parser = subparsers.add_parser(
        "runner",
        help="Inspect or refresh the sealed Round 5 runner",
    )
    runner_subparsers = runner_parser.add_subparsers(dest="runner_command", required=True)
    runner_refresh_parser = runner_subparsers.add_parser(
        "refresh",
        help="Install and atomically reseal only the current Round 5 runner assets",
    )
    runner_refresh_parser.add_argument("--timeout", type=float, default=300)

    resume_parser = subparsers.add_parser(
        "resume",
        help="Safely continue an interrupted owned provision",
    )
    resume_parser.add_argument("--timeout", type=float, default=900)

    renew_parser = subparsers.add_parser(
        "renew",
        help="Move this installation's expires-at forward without re-provisioning",
    )
    renew_parser.add_argument(
        "--ttl-hours",
        type=float,
        default=DEFAULT_TTL_HOURS,
        help=(
            f"Hours from now for the new expires-at (default {DEFAULT_TTL_HOURS:g}). "
            "Retags AWS and the Round 5 IAM conditions, then moves the manifest and "
            "re-seals the Round 5 ownership tags. Refused while a bout holds the ring "
            "or while any Round 5 per-bout resource still exists."
        ),
    )
    renew_parser.add_argument("--timeout", type=float, default=900)

    doctor_parser = subparsers.add_parser(
        "doctor",
        help="Local and cloud preflight; restores only the demo-owned Round 4 baseline",
    )
    doctor_parser.add_argument("--json", action="store_true", dest="as_json")
    doctor_parser.add_argument(
        "--competitor",
        choices=("aurora", "rds"),
        default="aurora",
        help="Validate the selected Round 1 opponent",
    )

    serve_parser = subparsers.add_parser("serve", help="Run the local orchestrator and UI")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", default=8000, type=int)
    serve_parser.add_argument(
        "--background",
        action="store_true",
        help=(
            "Detach into a session of its own and return. Survives closing the "
            "terminal or ending the shell session, which 'nohup' does not: nohup "
            "only ignores SIGHUP, and a process-group teardown still kills the "
            "server. This is the supported way to run a long-lived server."
        ),
    )
    serve_parser.add_argument(
        "--log",
        default="",
        help=(
            "Where a --background server writes. Defaults to server-<port>.log "
            "beside the selected manifest. Rolled at "
            f"{DEFAULT_LOG_MAX_BYTES // (1024 * 1024)} MiB, "
            f"{DEFAULT_LOG_KEEP} kept; override with "
            f"{LOG_MAX_BYTES_ENV} and {LOG_KEEP_ENV}."
        ),
    )

    status_parser = subparsers.add_parser(
        "status",
        help="Verify the launch record against the process that is really serving",
    )
    status_parser.add_argument("--host", default="127.0.0.1")
    status_parser.add_argument("--port", default=8000, type=int)
    status_parser.add_argument("--json", action="store_true", dest="as_json")

    # The one standing cost in this installation that a session can switch off.
    # Deliberately three explicit verbs rather than a toggle: "stop" and "start"
    # must be typed, so neither can be reached by a repeated command recalled
    # from history against the wrong intent.
    pipeline_parser = subparsers.add_parser(
        "pipeline",
        help="Stop, start, or price the Round 4 Managed Sync pipeline",
    )
    pipeline_parser.add_argument(
        "action",
        choices=("stop", "start", "status"),
        help=(
            "stop: end the continuous update and stop the meter. start: resume "
            "it without a full refresh. status: say which state it is in and "
            "what that costs per day. None of these touch the synced table."
        ),
    )

    cleanup_parser = subparsers.add_parser(
        "cleanup",
        help="Inventory or permanently delete only manifest-owned resources",
    )
    cleanup_mode = cleanup_parser.add_mutually_exclusive_group(required=True)
    cleanup_mode.add_argument("--dry-run", action="store_true")
    cleanup_mode.add_argument("--yes", action="store_true")
    # Deliberately a value, not a switch, and deliberately read only from argv.
    # `store_true` plus an env-var fallback would let a CI variable set months
    # ago silently disarm a destructive-operation gate; requiring the operator
    # to type the environment's own token means the flag cannot be inherited,
    # defaulted, or recalled from history against the wrong environment.
    cleanup_parser.add_argument(
        "--force-round6",
        metavar="RUN_ID_OR_BRANCH_UID",
        default="",
        help=(
            "Override ONLY the Round 6 seal check for an unrepairably drifted "
            "environment. Requires this environment's run ID or sealed branch UID "
            "as a confirmation token. Prints everything it will destroy and writes "
            "an audit record first. Does not bypass the unexpected-tables check, "
            "the source-schema ownership and contents checks, or any identity check."
        ),
    )
    return parser


def _resolved_aws_region(region: str) -> str:
    """The region to provision into, or a refusal naming how to supply one.

    This used to fall back to a compiled-in `us-west-2`, which is the author's
    sandbox region. For anybody else that default is silently wrong rather than
    absent: it provisions real resources in a region they did not choose and were
    not asked about, and the mistake only shows up as an empty console.
    """

    resolved = (region or os.environ.get("AWS_DEFAULT_REGION", "")).strip()
    if resolved:
        return resolved
    raise RuntimeError(
        "No AWS region was given and neither AWS_REGION nor AWS_DEFAULT_REGION is set. "
        "Pass --aws-region, or export AWS_REGION, or set `region` on the AWS profile "
        "this install uses. There is deliberately no default: provisioning into a "
        "guessed region spends money somewhere nobody chose."
    )


def _pipeline_power(action: str) -> int:
    """Run one Round 4 pipeline power verb and print what it cost or saved."""

    from .lifecycle import _databricks_api, _round4_get_pipeline
    from .manifest import load_manifest
    from .pipeline_power import RESTART_SECONDS_ESTIMATE, power_state, start, stop

    manifest = load_manifest()
    if action == "status":
        power = power_state(
            manifest,
            lambda identifier: _round4_get_pipeline(manifest, identifier),
        )
        print(f"PIPELINE {power.pipeline_id} · {power.summary()}")
        return 0
    # `stop` and `start` both mutate a shared cloud resource, so they take the
    # same claim every other mutating verb takes. A stop landing in the middle of
    # a bout would fail the round in a way that looks like a product defect.
    if action == "stop":
        with _mutating("antidemo pipeline stop"):
            power = stop(manifest, _databricks_api)
        print(f"STOPPED {power.pipeline_id} · {power.summary()}")
        return 0
    with _mutating("antidemo pipeline start"):
        power = start(manifest, _databricks_api)
    # Two measurements now stand behind RESTART_SECONDS_ESTIMATE -- 19.2 s and
    # 25.1 s to a fully healthy continuous sync, taken 2026-08-24 -- and this
    # still offers scale rather than promising a number. Two samples on one
    # installation are a scale, not a distribution, and the error that costs
    # something is the optimistic one: an operator told "19s" who waits two
    # minutes for a cold start would reasonably conclude the start had failed.
    # So the estimate stays deliberately above what was observed, the cold case
    # is still named, and the reading is still where the answer comes from.
    print(
        f"STARTED {power.pipeline_id} · resuming without a full refresh · roughly "
        f"{RESTART_SECONDS_ESTIMATE}s, and a cold start can take minutes · "
        f"'antidemo pipeline status' says when it reads RUNNING"
    )
    return 0


def _invocation(args: argparse.Namespace) -> str:
    """The command line the operator typed, as this program understood it."""
    label = f"antidemo {args.command}"
    if args.command == "cleanup":
        label += " --dry-run" if args.dry_run else " --yes"
    return label


#: Said in full, and said on `cleanup` above all. AWS refuses individual calls,
#: not whole commands, so a denial lands in the middle of a command that has
#: already printed things -- and `cleanup --dry-run` prints its orphan inventory
#: and its cost lines before it reaches the Round 5 baseline reads. An operator
#: who has just read plausible cost lines and then meets a failure has every
#: reason to take the lines for a finished report unless something says
#: otherwise. Omitting the category silently would be worse still: a health
#: surface here does not get to report without saying what it actually checked.
_AWS_REPORT_IS_PARTIAL = (
    "WHATEVER THIS COMMAND ALREADY PRINTED IS A PARTIAL REPORT AND NOT AN "
    "ALL-CLEAR. The call that failed is one of the reads that finds owned and "
    "orphaned resources, so whatever it would have found is INCOMPLETE in the "
    "lines above and may still be billing."
)

#: The AWS error codes that mean "this principal may not", as against a fault to
#: wait out. The two have opposite remedies -- a grant and a retry -- and only
#: the first is worth naming `docs/iam/` over. `AccessDenied` and
#: `AccessDeniedException` are how IAM, Secrets Manager and RDS say it; EC2 says
#: the same thing as `UnauthorizedOperation` and `AuthFailure`.
_AWS_DENIAL_CODES = frozenset(
    {"AccessDenied", "AccessDeniedException", "UnauthorizedOperation", "AuthFailure"}
)


def _is_access_denial(error: BaseException) -> bool:
    response = getattr(error, "response", None)
    if not isinstance(response, Mapping):
        return False
    details = response.get("Error")
    if not isinstance(details, Mapping):
        return False
    return str(details.get("Code") or "") in _AWS_DENIAL_CODES


def _aws_refusal(args: argparse.Namespace, error: BaseException) -> str:
    """What an operator reads when an AWS call this command needed did not answer.

    `manager.operator_diagnosis` renders the cause rather than `str(error)`,
    which is the same bargain the arm and cost-window refusals already make and
    it matters more here than usual: an `AccessDenied` message quotes the
    calling principal's ARN verbatim, and `manager._message_is_ours_to_quote`
    withholds a third party's words for exactly that reason. What survives the
    redaction is the actionable half -- the AWS error code and the API
    operation -- and for a denial the missing IAM action carries the operation's
    own name, so naming the operation names the permission.

    A denial and a fault split here rather than sharing one sentence. Pointing
    an operator whose credentials expired at `docs/iam/` would send them to
    grant a permission they already hold.
    """

    from .manager import operator_diagnosis

    if _is_access_denial(error):
        return (
            f"REFUSED {_invocation(args)} — AWS refused a call this command "
            f"depends on: {operator_diagnosis(error)}. {_AWS_REPORT_IS_PARTIAL} "
            f"The missing IAM action is named after the refused operation; "
            f"docs/iam/ holds the operator policies this repository expects "
            f"an operator principal to carry. Grant it and run this again."
        )
    return (
        f"ERROR {_invocation(args)} — an AWS call this command depends on did "
        f"not complete: {operator_diagnosis(error)}. {_AWS_REPORT_IS_PARTIAL} "
        f"Whatever stopped the call is the thing to fix, then run this again."
    )


#: The Postgres counterpart of `_AWS_REPORT_IS_PARTIAL`, and deliberately not the
#: same sentence. The AWS reads that fail late on `cleanup --dry-run` find owned
#: and orphaned *AWS* resources; the Postgres read that fails there is
#: `_round5_active_journal_addons`, and it is the only thing on that path that can
#: see a per-bout add-on Terraform never created -- so an unanswered one leaves
#: the inventory above silent about exactly the resources it cannot otherwise
#: account for. That case is named rather than generalised away because it is the
#: expensive one, but the sentence has to hold on `setup`, `reset` and `renew`
#: too, which reach Postgres for schema and grant work rather than for an
#: inventory. Hence "a question this command needed answered" and not a claim
#: about which read it was.
_POSTGRES_REPORT_IS_PARTIAL = (
    "WHATEVER THIS COMMAND ALREADY PRINTED IS A PARTIAL REPORT AND NOT AN "
    "ALL-CLEAR. The read that failed asked the coordination database a question "
    "this command needed answered — on cleanup that is which per-bout Round 5 "
    "add-ons are still unresolved, the resources Terraform never created and "
    "cannot destroy — so whatever is still alive and billing under this run is "
    "INCOMPLETE in the lines above."
)


def _postgres_refusal(args: argparse.Namespace, error: BaseException) -> str:
    """What an operator reads when a Postgres read this command needed did not answer.

    Routed through `manager.operator_diagnosis` for the same reason the AWS
    refusal above is, and with more at stake: a psycopg message routinely carries
    the endpoint hostname, the address it resolved to and the login role, and
    `manager._message_is_ours_to_quote` withholds a third party's words for
    exactly that. What survives is the actionable half -- the exception class and
    its `SQLSTATE` -- which is the same trade `lifecycle._connect` already makes
    when it reports a refused login as `... (SQLSTATE 28P01)` rather than quoting
    the server.

    The denial branch is `coordination.privilege_refusal`, not a fresh
    `isinstance`. That function is the established answer to this question here,
    it walks the cause chain rather than the head -- which matters, because
    `_require_round5_clean_baseline` reaches this read through frames that
    re-raise -- and its scope is already argued: `InsufficientPrivilege` alone,
    never its `ProgrammingError` ancestor, which a syntax error also is.

    A denial and a fault split for the reason the AWS refusal splits, and the
    boundary lands somewhere worth naming: a class-28 login failure is a *fault*
    here, not a denial. Its remedy is a credential, not a `GRANT`, so sending that
    operator to the grant set would be sending them to award themselves something
    they already hold.
    """

    from .coordination import privilege_refusal
    from .manager import operator_diagnosis

    if privilege_refusal(error) is not None:
        return (
            f"REFUSED {_invocation(args)} — Postgres refused a read this command "
            f"depends on, and this is not a fault to wait out: "
            f"{operator_diagnosis(error)}. {_POSTGRES_REPORT_IS_PARTIAL} "
            f"THIS IS A LAKEBASE GRANT, NOT AWS IAM; docs/DEPLOY.md holds the "
            f"complete coordination-database grant set this identity is expected "
            f"to carry. Grant it and run this again."
        )
    return (
        f"ERROR {_invocation(args)} — a Postgres read this command depends on did "
        f"not complete: {operator_diagnosis(error)}. {_POSTGRES_REPORT_IS_PARTIAL} "
        f"Whatever stopped the read is the thing to fix, then run this again."
    )


def main() -> int:
    # `doctor`, `cleanup` and `reap` run the same `server/*` code the server
    # does, in this process, so they reach the same warnings -- an operator
    # running `antidemo cleanup` should see an `ORPHAN RISK` line labelled the
    # same way it is labelled in the server log, not as a bare sentence adrift
    # in the command's own output.
    configure_operator_logging()
    args = _parser().parse_args()
    try:
        if args.command == "setup":
            with _mutating("antidemo setup"):
                manifest = setup(
                    databricks_profile=args.databricks_profile,
                    aws_profile=args.aws_profile,
                    aws_region=_resolved_aws_region(args.aws_region),
                    expected_account=args.expected_account,
                    owner=args.owner,
                    operator_cidr=args.operator_cidr,
                    ttl_hours=args.ttl_hours,
                    timeout_seconds=args.timeout,
                )
            # Serving happens outside the lock on purpose. Setup is finished by
            # here, and the server that follows must not inherit a claim it would
            # then hold for hours.
            url = f"http://{args.host}:{args.port}/"
            print(f"READY TO RING — {manifest.run_id} — {url}", flush=True)
            if args.no_serve or _app_is_online(args.host, args.port):
                return 0
            return _serve(args.host, args.port)
        if args.command == "provision":
            with _mutating("antidemo provision"):
                manifest = provision(
                    databricks_profile=args.databricks_profile,
                    aws_profile=args.aws_profile,
                    aws_region=_resolved_aws_region(args.aws_region),
                    expected_account=args.expected_account,
                    owner=args.owner,
                    operator_cidr=args.operator_cidr,
                    ttl_hours=args.ttl_hours,
                    zero_timeout_seconds=args.zero_timeout,
                )
            print(f"READY {manifest.run_id} — both eligible lanes verified at scale zero")
            return 0
        if args.command == "reset":
            with _mutating("antidemo reset"):
                manifest = reset(args.timeout)
            print(f"READY {manifest.run_id} — ring the bell within the armed UI window")
            return 0
        if args.command == "runner" and args.runner_command == "refresh":
            with _mutating("antidemo runner refresh"):
                manifest = refresh_round5_runner(timeout=args.timeout)
            print(
                f"REFRESHED {manifest.run_id} — Round 5 runner source, EC2, and seal align"
            )
            print(
                "NEXT ./bootstrap.sh --deploy-only --yes — publish the refreshed seal "
                "and matching app source"
            )
            return 0
        if args.command == "resume":
            with _mutating("antidemo resume"):
                manifest = resume_provision(args.timeout)
            print(f"READY {manifest.run_id} — interrupted provision recovered safely")
            return 0
        if args.command == "renew":
            with _mutating("antidemo renew"):
                manifest = renew(ttl_hours=args.ttl_hours, timeout_seconds=args.timeout)
            print(
                f"RENEWED {manifest.run_id} — expires-at is now "
                f"{manifest.expires_at.isoformat()}"
            )
            for line in deployed_renew_followup(manifest):
                print(line)
            return 0
        if args.command == "doctor":
            # Doctor writes no manifest, but it does restore the owned Round 4
            # baseline and reap orphans, and it runs Terraform against the shared
            # `infra/aws` working directory. Running it beside a setup would race
            # both, and its findings would describe a moving target anyway.
            with _mutating("antidemo doctor"):
                checks = doctor(args.competitor) + round_construction_checks()
            print_checks(checks, args.as_json)
            return 0 if checks_passed(checks) else 1
        if args.command == "cleanup":
            # Both modes, including `--dry-run`. The inventory does not rewrite
            # the manifest, but it does run `terraform init` and `plan` in the
            # same working directory a concurrent apply is using.
            with _mutating(f"antidemo cleanup {'--dry-run' if args.dry_run else '--yes'}"):
                manifest = cleanup(dry_run=args.dry_run, force_round6=args.force_round6)
            if args.dry_run:
                print(f"DRY RUN ONLY — no resources changed for {manifest.run_id}")
            else:
                print(f"CLEAN {manifest.run_id} — owned AWS and Lakebase resources removed")
                if args.force_round6.strip():
                    print(
                        "FORCE Round 6 seal verification was overridden for this cleanup; "
                        "see round6-force.jsonl beside the manifest"
                    )
            return 0
        if args.command == "pipeline":
            return _pipeline_power(args.action)
        if args.command == "status":
            checks = _status_checks(args.host, args.port)
            print_checks(checks, args.as_json)
            return 0 if checks_passed(checks) else 1
        if args.command == "serve":
            return _serve(
                args.host,
                args.port,
                background=args.background,
                log=args.log,
            )
    except GenerationBusyError as exc:
        # Listed before RuntimeError, which it subclasses, so the refusal keeps
        # its own wording instead of being flattened into a generic ERROR line.
        print(f"REFUSED {exc}", file=sys.stderr)
        return 1
    except (ClientError, BotoCoreError) as exc:
        # Neither is a `RuntimeError`, an `OSError` or a `ValueError` -- botocore
        # hangs both off `Exception` directly -- so both used to walk past the
        # handler below and end as a traceback. `cleanup --dry-run` is the one
        # that made that intolerable: it is the command README's "Cost and
        # safety" section points a stranger at when they want to know what they
        # are being charged for and how to stop it, and it reaches
        # `_require_round5_clean_baseline`, whose Secrets Manager, EC2, RDS and
        # IAM reads are the only unwrapped AWS calls left on the path.
        # `reconcile_live` and `_aws_ownership` each catch their own.
        #
        # `BotoCoreError` rides along on purpose. `NoCredentialsError` and
        # `EndpointConnectionError` are the same shape of defect wearing a
        # different cause, and fixing one denied action while three siblings
        # stayed bare would not be fixing this.
        print(_aws_refusal(args, exc), file=sys.stderr)
        return 1
    except psycopg.Error as exc:
        # The same defect one vendor over, and it escapes by the same route on the
        # same line: `psycopg.Error` hangs off `Exception` directly too, so it is
        # no more a `RuntimeError` than a `ClientError` is, and the handler above
        # covers botocore only. `_round5_active_journal_addons` names two psycopg
        # classes -- `InvalidSchemaName` and `UndefinedTable`, the pair that means
        # "no journal here" -- and lets every other one through, and on
        # `cleanup --dry-run` it is the last read before the Round 5 baseline
        # verdict, reached after the whole orphan inventory and its cost lines
        # have printed. An `InsufficientPrivilege` on the journal table used to
        # end that command in a traceback under a plausible report.
        #
        # Deliberately the whole `psycopg.Error` tree rather than the classes
        # observed failing. `_connect` already turns a non-retryable connect
        # failure into a `RuntimeError`, so what reaches here is the statement-
        # level remainder -- an ACL refusal, a drifted column, a cancelled query,
        # an admin shutdown mid-read -- and enumerating that set would be
        # enumerating the ways a database can say no, which is not a list this
        # file gets to be right about.
        print(_postgres_refusal(args, exc), file=sys.stderr)
        return 1
    except (RuntimeError, OSError, ValueError) as exc:
        print(f"ERROR {exc}", file=sys.stderr)
        return 1
    return 2


if __name__ == "__main__":
    sys.exit(main())
