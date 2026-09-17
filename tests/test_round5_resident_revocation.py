"""Deterministic revocation/recovery tests for the Round 5 resident runner.

The resident recovery state machine must be *revocation-aware*: it must never
trust a persisted stage, mutate/spawn workers, and only then discover via a
rejected insert that a newer warm attempt superseded it and crash-loop. Every
local transition (startup stage/active recovery, incoming PRELOAD/STAGE, worker
spawn/stop, heartbeat, file replacement) is gated on an exact, lane-bound
disposition -- current / superseded / terminal / unknown -- computed against the
same authoritative slot/outbox/event relations the write-side RLS gate consults.

These tests drive the real ``_resident_agent`` loop against an in-memory control
plane that emulates the disposition classifier and the RLS ``WITH CHECK`` insert
gate, plus focused tests of the extracted primitives and the SECURITY DEFINER
disposition function's structure.

Scenario map (from the review):
1. restart on stale A while B is current -> no A insert/spawn, A removed, B ready
2. A still current on recovery -> resume works
3. delayed PRELOAD A after B -> A ACKed without touching B
4. token rotates between precheck and readiness insert -> superseded, no crash
5. stale heartbeat only clears matching A and the process stays alive
6. terminal-before-unlink crash -> no duplicate terminal event
7. unknown/tampered -> no state mutation, quarantined and ACKed
8. disposition/RLS identity is exact (event_id, lane, gen, token, job, binding)
9. both lane residents must be current for READY
"""

from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from runner import connection_spike_runner as runner
from server import lifecycle
from server.connection_spike_live import LiveConnectionSpikeEngine
from server.round5_control import (
    Round5ControlBinding,
    Round5ControlEvent,
    Round5ControlKind,
)

BOOT_ID = runner.fanin._runner_boot_id()
HARNESS = runner.LOADED_RUNNER_HARNESS_SHA256
INSTALLATION = "install-one"
GENERATION = 9


class _StopLoop(Exception):
    """Sentinel used to break the resident's otherwise-infinite receive loop."""


def _canonical_request() -> dict[str, object]:
    request: dict[str, object] = {"protocol": runner.fanin.PROTOCOL, "action": "run_lane_v3"}
    request["prepared_request_digest"] = runner.hashlib.sha256(
        json.dumps(request, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return request


def _binding(
    *,
    lane_id: str,
    token: str,
    job_id: str,
    process_boot_id: str,
    request_sha256: str,
) -> Round5ControlBinding:
    return Round5ControlBinding(
        installation_id=INSTALLATION,
        lane_id=lane_id,
        generation=GENERATION,
        warm_attempt_token=token,
        claim_id=None,
        bout_id=None,
        bell_id=None,
        fence=0,
        job_id=job_id,
        runner_boot_id=BOOT_ID,
        runner_process_boot_id=process_boot_id,
        runner_harness_sha256=HARNESS,
        request_sha256=request_sha256,
    )


def _preload_event(*, lane_id: str, token: str, job_id: str) -> tuple[Round5ControlEvent, dict]:
    request = _canonical_request()
    request_sha256 = request["prepared_request_digest"]
    binding = _binding(
        lane_id=lane_id,
        token=token,
        job_id=job_id,
        process_boot_id="unattested",
        request_sha256=str(request_sha256),
    )
    event = Round5ControlEvent.create(
        binding=binding,
        sequence=1,
        kind=Round5ControlKind.PRELOAD,
        payload={"request": request},
    )
    return event, request


class _FakeControlPlane:
    """Emulates the disposition classifier and the RLS-gated event insert."""

    def __init__(self) -> None:
        # (installation, generation) -> current warm_attempt_token
        self.current_token: dict[tuple[str, int], str] = {}
        # dispatched control events: keyed identity -> stripped binding
        self.outbox: dict[tuple, dict] = {}
        self.events: list[dict] = []

    def dispatch(self, event: Round5ControlEvent) -> None:
        b = event.binding
        stripped = {k: v for k, v in b.wire_value().items() if k != "runner_process_boot_id"}
        self.outbox[
            (
                b.installation_id,
                b.lane_id,
                b.generation,
                b.warm_attempt_token,
                b.job_id,
                event.event_id,
            )
        ] = stripped

    def make_current(self, token: str) -> None:
        self.current_token[(INSTALLATION, GENERATION)] = token

    # -- emulated SQL surface ------------------------------------------------
    def disposition(self, installation, lane, gen, token, job, event_id, binding_json) -> str:
        binding = json.loads(binding_json)
        stripped = {k: v for k, v in binding.items() if k != "runner_process_boot_id"}
        key = (installation, lane, gen, token, job, event_id)
        outbox = self.outbox.get(key)
        if outbox is None or outbox != stripped:
            return "unknown"
        if any(
            ev["installation"] == installation
            and ev["lane"] == lane
            and ev["generation"] == gen
            and ev["token"] == token
            and ev["job"] == job
            and ev["kind"] in {"settled", "quarantined"}
            for ev in self.events
        ):
            return "terminal"
        if self.current_token.get((installation, gen)) == token:
            return "current"
        return "superseded"

    def rls_authorized(self, installation, lane, gen, token, job, binding) -> bool:
        if self.current_token.get((installation, gen)) != token:
            return False
        stripped = {k: v for k, v in binding.items() if k != "runner_process_boot_id"}
        return any(
            k[:5] == (installation, lane, gen, token, job) and v == stripped
            for k, v in self.outbox.items()
        )

    def max_sequence(self, installation, lane, gen, token, job) -> int:
        return max(
            (
                ev["sequence"]
                for ev in self.events
                if (ev["installation"], ev["lane"], ev["generation"], ev["token"], ev["job"])
                == (installation, lane, gen, token, job)
            ),
            default=0,
        )


class _FakeCursor:
    def __init__(self, plane: _FakeControlPlane) -> None:
        self._plane = plane
        self._result: list[tuple] = []

    def execute(self, statement: str, params=None) -> None:
        params = params or ()
        if "round5_runner_event_disposition_v1" in statement:
            disp = self._plane.disposition(*params)
            self._result = [(disp,)]
        elif "COALESCE(MAX(sequence)" in statement:
            installation, lane, gen, token, job = params
            self._result = [(self._plane.max_sequence(installation, lane, gen, token, job),)]
        elif statement.strip().startswith(
            "INSERT INTO anti_demo_coordination.round5_runner_event_v3"
        ):
            (
                event_id,
                installation,
                lane,
                gen,
                token,
                job,
                sequence,
                kind,
                binding_json,
                payload_json,
                _occurred,
            ) = params
            binding = json.loads(binding_json)
            if not self._plane.rls_authorized(installation, lane, gen, token, job, binding):
                raise runner.psycopg.errors.InsufficientPrivilege(
                    "new row violates row-level security policy"
                )
            self._plane.events.append(
                {
                    "event_id": event_id,
                    "installation": installation,
                    "lane": lane,
                    "generation": gen,
                    "token": token,
                    "job": job,
                    "sequence": sequence,
                    "kind": kind,
                    "binding": binding,
                    "payload": json.loads(payload_json),
                }
            )
            self._result = [(event_id,)]
        else:  # pragma: no cover - defensive
            self._result = []

    def fetchone(self):
        return self._result[0] if self._result else None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeConnection:
    def __init__(self, plane: _FakeControlPlane) -> None:
        self._plane = plane

    def cursor(self):
        return _FakeCursor(self._plane)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakePool:
    worker_ready_indexes = (0, 1, 2, 3)

    def __init__(self) -> None:
        self.processes: list = []

        class _Event:
            def set(self_inner) -> None:
                return None

        self.cancel_event = _Event()

    @classmethod
    def start(cls) -> _FakePool:
        return cls()


class _FakeSqs:
    def __init__(self, batches: list[list[dict]], hooks: list | None = None) -> None:
        self._batches = list(batches)
        self._hooks = list(hooks) if hooks is not None else [None] * len(batches)
        self.deleted: list[str] = []

    def receive_message(self, **_kwargs) -> dict:
        if self._batches:
            hook = self._hooks.pop(0) if self._hooks else None
            if hook is not None:
                hook()
            return {"Messages": self._batches.pop(0)}
        raise _StopLoop

    def delete_message(self, *, QueueUrl, ReceiptHandle) -> None:  # noqa: N803
        del QueueUrl
        self.deleted.append(ReceiptHandle)


class _ResourceNotFound(Exception):
    pass


class _FakeSecrets:
    class exceptions:  # noqa: N801
        ResourceNotFoundException = _ResourceNotFound

    def get_secret_value(self, *, SecretId):  # noqa: N803
        del SecretId
        return {"SecretString": "postgresql://resident@coord/anti_demo"}


def _install_fakes(monkeypatch, tmp_path: Path, plane: _FakeControlPlane, sqs: _FakeSqs) -> None:
    monkeypatch.setattr(runner, "JOB_ROOT", tmp_path / "jobs")
    monkeypatch.setattr(runner, "RESIDENT_ATTESTATION_DIR", tmp_path / "run")
    (tmp_path / "run").mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(runner, "ResidentShardPool", _FakePool)

    def fake_boto_client(name: str):
        if name == "sqs":
            return sqs
        if name == "secretsmanager":
            return _FakeSecrets()
        raise AssertionError(f"unexpected client {name}")

    monkeypatch.setattr(runner.boto3, "client", fake_boto_client)
    monkeypatch.setattr(
        runner.psycopg,
        "connect",
        lambda *a, **k: _FakeConnection(plane),
    )
    monkeypatch.setattr(runner.fanin, "_ephemeral_port_usage", lambda: (100_000, 0, 100_000))

    # A monotonic clock that leaps 5s per read so the >=2s heartbeat interval
    # fires on every loop iteration in these fast tests.
    ticks = {"value": 0.0}

    def clock() -> float:
        ticks["value"] += 5.0
        return ticks["value"]

    monkeypatch.setattr(runner.time, "monotonic", clock)


async def _drive(monkeypatch, tmp_path, plane, batches, hooks=None):
    sqs = _FakeSqs(batches, hooks=hooks)
    _install_fakes(monkeypatch, tmp_path, plane, sqs)
    try:
        await runner._resident_agent(
            lane_id="lakebase",
            generation=GENERATION,
            queue_url="q",
            control_secret_arn="arn",
        )
    except _StopLoop:
        pass
    return sqs


def _agent_ready_tokens(plane: _FakeControlPlane) -> list[str]:
    return [ev["token"] for ev in plane.events if ev["kind"] == "agent_ready"]


def _write_stage_file(tmp_path: Path, event: Round5ControlEvent, request: dict) -> Path:
    jobs = tmp_path / "jobs"
    jobs.mkdir(parents=True, exist_ok=True)
    path = jobs / "resident-lakebase-stage.json"
    path.write_text(
        json.dumps({"event": event.wire_value(), "request": request}),
        encoding="utf-8",
    )
    return path


# --------------------------------------------------------------------------- #
# Scenario 1: restart on a stale stage A while B is current.
# --------------------------------------------------------------------------- #
async def test_startup_discards_superseded_stage_and_does_not_emit_or_crash(
    monkeypatch, tmp_path
) -> None:
    plane = _FakeControlPlane()
    event_a, request_a = _preload_event(lane_id="lakebase", token="attempt-A", job_id="a" * 64)
    plane.dispatch(event_a)
    # Attempt B is what the durable warm slot now names; A's token is revoked.
    plane.make_current("attempt-B")
    stage_path = _write_stage_file(tmp_path, event_a, request_a)

    await _drive(monkeypatch, tmp_path, plane, batches=[])

    # No agent_ready was emitted for the stale attempt, and the stale stage was
    # discarded rather than replayed (which is what previously crash-looped).
    assert _agent_ready_tokens(plane) == []
    assert not stage_path.exists()


# --------------------------------------------------------------------------- #
# Scenario 2: a still-current stage recovers and reaches agent_ready.
# --------------------------------------------------------------------------- #
async def test_startup_current_stage_resumes_to_agent_ready(monkeypatch, tmp_path) -> None:
    plane = _FakeControlPlane()
    event_a, request_a = _preload_event(lane_id="lakebase", token="attempt-A", job_id="a" * 64)
    plane.dispatch(event_a)
    plane.make_current("attempt-A")
    _write_stage_file(tmp_path, event_a, request_a)

    await _drive(monkeypatch, tmp_path, plane, batches=[])

    assert _agent_ready_tokens(plane) == ["attempt-A"]


# --------------------------------------------------------------------------- #
# Scenario 3: a delayed PRELOAD A arrives after B is current and staged.
# --------------------------------------------------------------------------- #
async def test_delayed_preload_after_supersession_is_acked_without_touching_current(
    monkeypatch, tmp_path
) -> None:
    plane = _FakeControlPlane()
    event_b, _ = _preload_event(lane_id="lakebase", token="attempt-B", job_id="b" * 64)
    event_a, _ = _preload_event(lane_id="lakebase", token="attempt-A", job_id="a" * 64)
    plane.dispatch(event_b)
    plane.dispatch(event_a)
    plane.make_current("attempt-B")

    sqs = await _drive(
        monkeypatch,
        tmp_path,
        plane,
        batches=[
            [{"Body": event_b.encoded_body(), "ReceiptHandle": "rb"}],
            [{"Body": event_a.encoded_body(), "ReceiptHandle": "ra"}],
        ],
    )

    # B reached readiness; the delayed, superseded A was ACKed/deleted and never
    # produced its own agent_ready or displaced B's stage.
    assert "attempt-B" in _agent_ready_tokens(plane)
    assert "attempt-A" not in _agent_ready_tokens(plane)
    assert "ra" in sqs.deleted
    stage = (tmp_path / "jobs" / "resident-lakebase-stage.json").read_text()
    assert "attempt-B" in stage


# --------------------------------------------------------------------------- #
# Scenario 4: token rotates between the precheck and the readiness insert.
# --------------------------------------------------------------------------- #
async def test_rotation_between_precheck_and_insert_is_superseded_not_crash(
    monkeypatch, tmp_path
) -> None:
    plane = _FakeControlPlane()
    event_a, _ = _preload_event(lane_id="lakebase", token="attempt-A", job_id="a" * 64)
    plane.dispatch(event_a)
    plane.make_current("attempt-A")

    # The disposition precheck sees 'current', then the token rotates before the
    # agent_ready insert, which the RLS gate rejects.
    original = plane.disposition

    def rotate_after_precheck(*args, **kwargs):
        verdict = original(*args, **kwargs)
        plane.make_current("attempt-B")  # authority rotates immediately after
        return verdict

    monkeypatch.setattr(plane, "disposition", rotate_after_precheck)

    sqs = await _drive(
        monkeypatch,
        tmp_path,
        plane,
        batches=[[{"Body": event_a.encoded_body(), "ReceiptHandle": "ra"}]],
    )

    # No crash, no agent_ready recorded under the revoked token, message ACKed.
    assert _agent_ready_tokens(plane) == []
    assert "ra" in sqs.deleted


# --------------------------------------------------------------------------- #
# Deploy safety: a legacy/foreign stage or active file must be discarded, not
# crash-loop the freshly refreshed harness.
# --------------------------------------------------------------------------- #
async def test_legacy_format_recovery_files_are_discarded_without_crash(
    monkeypatch, tmp_path
) -> None:
    plane = _FakeControlPlane()
    plane.make_current("attempt-A")
    jobs = tmp_path / "jobs"
    jobs.mkdir(parents=True, exist_ok=True)
    # Pre-event-schema residue left by an older harness version (no "event" key).
    (jobs / "resident-lakebase-stage.json").write_text(
        json.dumps({"binding": {"lane_id": "lakebase"}, "request": {}}),
        encoding="utf-8",
    )
    (jobs / "resident-lakebase-active.json").write_text(
        json.dumps({"binding": {"lane_id": "lakebase"}, "request": {}, "state": "released"}),
        encoding="utf-8",
    )

    await _drive(monkeypatch, tmp_path, plane, batches=[])

    # No crash, no events written, both residue files discarded.
    assert plane.events == []
    assert not (jobs / "resident-lakebase-stage.json").exists()
    assert not (jobs / "resident-lakebase-active.json").exists()


# --------------------------------------------------------------------------- #
# Scenario 5: a stale heartbeat only clears its own identity, process survives.
# --------------------------------------------------------------------------- #
async def test_stale_heartbeat_clears_only_matching_identity_and_survives(
    monkeypatch, tmp_path
) -> None:
    plane = _FakeControlPlane()
    event_b, _ = _preload_event(lane_id="lakebase", token="attempt-B", job_id="b" * 64)
    event_a, _ = _preload_event(lane_id="lakebase", token="attempt-A", job_id="a" * 64)
    plane.dispatch(event_b)
    plane.dispatch(event_a)
    plane.make_current("attempt-B")

    # Batch 1 stages B (current) and heartbeats it. Before batch 2, authority
    # rotates to C, so the heartbeat that fires after batch 2 is RLS-denied.
    def rotate_to_c() -> None:
        plane.make_current("attempt-C")

    sqs = await _drive(
        monkeypatch,
        tmp_path,
        plane,
        batches=[
            [{"Body": event_b.encoded_body(), "ReceiptHandle": "rb"}],
            [{"Body": event_a.encoded_body(), "ReceiptHandle": "ra"}],
        ],
        hooks=[None, rotate_to_c],
    )

    # The process did not crash (the loop ran to the _StopLoop sentinel), B did
    # reach readiness, and the stale heartbeat cleared only B's own stage residue.
    assert "attempt-B" in _agent_ready_tokens(plane)
    assert "ra" in sqs.deleted
    assert not (tmp_path / "jobs" / "resident-lakebase-stage.json").exists()


# --------------------------------------------------------------------------- #
# Scenario 6: a terminal (already-settled) job recovered from active.json.
# --------------------------------------------------------------------------- #
async def test_startup_terminal_active_emits_no_duplicate_settlement(monkeypatch, tmp_path) -> None:
    plane = _FakeControlPlane()
    event_a, request_a = _preload_event(lane_id="lakebase", token="attempt-A", job_id="a" * 64)
    plane.dispatch(event_a)
    plane.make_current("attempt-A")
    # The job already settled before the crash.
    plane.events.append(
        {
            "installation": INSTALLATION,
            "lane": "lakebase",
            "generation": GENERATION,
            "token": "attempt-A",
            "job": "a" * 64,
            "sequence": 5,
            "kind": "settled",
            "binding": {},
            "payload": {},
        }
    )
    jobs = tmp_path / "jobs"
    jobs.mkdir(parents=True, exist_ok=True)
    active_path = jobs / "resident-lakebase-active.json"
    active_path.write_text(
        json.dumps({"event": event_a.wire_value(), "request": request_a, "state": "released"}),
        encoding="utf-8",
    )

    await _drive(monkeypatch, tmp_path, plane, batches=[])

    settled = [ev for ev in plane.events if ev["kind"] == "settled"]
    assert len(settled) == 1  # no duplicate terminal event
    assert not active_path.exists()


# --------------------------------------------------------------------------- #
# Scenario 7: an unknown/tampered incoming control event.
# --------------------------------------------------------------------------- #
async def test_unknown_incoming_event_quarantines_without_state_mutation(
    monkeypatch, tmp_path
) -> None:
    plane = _FakeControlPlane()
    # Never dispatched to the outbox -> disposition 'unknown' (tamper/forgery).
    event_a, _ = _preload_event(lane_id="lakebase", token="attempt-A", job_id="a" * 64)
    plane.make_current("attempt-A")

    sqs = await _drive(
        monkeypatch,
        tmp_path,
        plane,
        batches=[[{"Body": event_a.encoded_body(), "ReceiptHandle": "ra"}]],
    )

    # No worker events written, and no stage file created (no local mutation).
    assert plane.events == []
    assert not (tmp_path / "jobs" / "resident-lakebase-stage.json").exists()
    assert "ra" in sqs.deleted


# --------------------------------------------------------------------------- #
# Scenario 4/8 primitive: the RLS reject surfaces as a superseded signal.
# --------------------------------------------------------------------------- #
def test_write_resident_event_maps_rls_denial_to_superseded(monkeypatch) -> None:
    plane = _FakeControlPlane()
    monkeypatch.setattr(runner.psycopg, "connect", lambda *a, **k: _FakeConnection(plane))
    binding = {
        "installation_id": INSTALLATION,
        "lane_id": "lakebase",
        "generation": GENERATION,
        "warm_attempt_token": "attempt-A",
        "job_id": "a" * 64,
        "runner_process_boot_id": "process-x",
    }
    # Not authorized (no current token / no outbox) -> RLS WITH CHECK denies it.
    with pytest.raises(runner.RunnerAttemptSupersededError):
        runner._write_resident_event(
            "postgresql://x",
            binding=binding,
            sequence=1,
            kind="agent_ready",
            payload={},
        )


def test_resident_event_disposition_maps_each_classification(monkeypatch) -> None:
    plane = _FakeControlPlane()
    monkeypatch.setattr(runner.psycopg, "connect", lambda *a, **k: _FakeConnection(plane))
    event_a, _ = _preload_event(lane_id="lakebase", token="attempt-A", job_id="a" * 64)
    binding = event_a.binding.wire_value()

    def classify() -> str:
        return runner._resident_event_disposition(
            "postgresql://x",
            installation_id=INSTALLATION,
            lane_id="lakebase",
            generation=GENERATION,
            warm_attempt_token="attempt-A",
            job_id="a" * 64,
            event_id=event_a.event_id,
            binding=binding,
        )

    assert classify() == "unknown"  # not dispatched
    plane.dispatch(event_a)
    plane.make_current("attempt-A")
    assert classify() == "current"
    plane.make_current("attempt-B")
    assert classify() == "superseded"
    plane.make_current("attempt-A")
    plane.events.append(
        {
            "installation": INSTALLATION,
            "lane": "lakebase",
            "generation": GENERATION,
            "token": "attempt-A",
            "job": "a" * 64,
            "sequence": 9,
            "kind": "settled",
            "binding": {},
            "payload": {},
        }
    )
    assert classify() == "terminal"


def test_resident_event_disposition_rejects_wrong_binding(monkeypatch) -> None:
    plane = _FakeControlPlane()
    monkeypatch.setattr(runner.psycopg, "connect", lambda *a, **k: _FakeConnection(plane))
    event_a, _ = _preload_event(lane_id="lakebase", token="attempt-A", job_id="a" * 64)
    plane.dispatch(event_a)
    plane.make_current("attempt-A")
    tampered = dict(event_a.binding.wire_value())
    tampered["fence"] = 999  # binding differs -> exact identity fails -> unknown
    result = runner._resident_event_disposition(
        "postgresql://x",
        installation_id=INSTALLATION,
        lane_id="lakebase",
        generation=GENERATION,
        warm_attempt_token="attempt-A",
        job_id="a" * 64,
        event_id=event_a.event_id,
        binding=tampered,
    )
    assert result == "unknown"


# --------------------------------------------------------------------------- #
# Structural guarantees on the state machine and the disposition SQL.
# --------------------------------------------------------------------------- #
def test_state_machine_checks_disposition_before_every_transition() -> None:
    source = inspect.getsource(runner._resident_agent)
    # The disposition gate precedes the PRELOAD branch (worker spawn / stage).
    gate = source.index("disposition = await disposition_of(event)")
    preload_branch = source.index('if kind == "preload":', gate)
    spawn = source.index("ResidentShardPool.start()", preload_branch)
    assert gate < preload_branch < spawn
    # Startup recovery reverifies the persisted event and classifies before spawn.
    assert "_verify_resident_control(stored_stage[\"event\"])" in source
    assert "_verify_resident_control(active_value[\"event\"])" in source
    stage_recovery = source.index("stage_disposition = await disposition_of(stored_event)")
    stage_spawn = source.index("pool = ResidentShardPool.start()", stage_recovery)
    assert stage_recovery < stage_spawn
    # Superseded/terminal discard residue and ACK without settling.
    assert "await discard_residue(binding)" in source
    # RLS rejection during a current publish is reclassified, not fatal.
    assert "except RunnerAttemptSupersededError:" in source


def test_disposition_function_binds_exact_identity_and_is_locked_down() -> None:
    source = inspect.getsource(lifecycle._rotate_round5_resident_login)
    assert "round5_runner_event_disposition_v1" in source
    # Exact identity: event_id + lane + generation + token + job + binding.
    assert "control.event_id = p_event_id" in source
    assert "control.lane_id = p_lane_id" in source
    assert "control.generation = p_generation" in source
    assert "control.warm_attempt_token = p_warm_attempt_token" in source
    assert "control.job_id = p_job_id" in source
    assert "(control.payload -> 'binding') - 'runner_process_boot_id'::text" in source
    # Classification order: unknown -> terminal -> current -> superseded.
    unknown = source.index("THEN 'unknown'")
    terminal = source.index("THEN 'terminal'")
    current = source.index("THEN 'current'")
    superseded = source.index("ELSE 'superseded'")
    assert unknown < terminal < current < superseded
    # Locked down like the write-side authorizer.
    assert "SECURITY DEFINER" in source
    assert "SET search_path = pg_catalog" in source
    assert "REVOKE ALL ON FUNCTION {} FROM PUBLIC" in source
    assert "GRANT EXECUTE ON FUNCTION {} TO {}" in source


# --------------------------------------------------------------------------- #
# Scenario 9: both lane residents must be current for READY.
# --------------------------------------------------------------------------- #
def test_ready_provenance_requires_all_lanes_current() -> None:
    source = inspect.getsource(LiveConnectionSpikeEngine.validate_ready_provenance)
    assert "resident_is_current" in source
    assert "all(await asyncio.gather(*checks))" in source


# --------------------------------------------------------------------------- #
# Requirement 3: coordination native login is persisted in code.
# --------------------------------------------------------------------------- #
def test_ensure_coordination_persists_native_login() -> None:
    ensure = inspect.getsource(lifecycle.ensure_coordination)
    assert "_enable_coordination_lakebase_native_login(manifest)" in ensure
    helper = inspect.getsource(lifecycle._enable_coordination_lakebase_native_login)
    assert "enable_pg_native_login" in helper
    assert "_coordination_lakebase_binding(manifest).project_id" in helper
    # Idempotent: returns early when already enabled.
    assert 'get("enable_pg_native_login") is True' in helper
