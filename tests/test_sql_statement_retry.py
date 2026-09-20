"""Regression tests for `_sql_statement` transient-failure handling.

A clean end-to-end install once aborted after ~20 minutes because a single
Databricks serverless SQL statement returned a terminal ``FAILED`` for a
transient reason -- a warehouse node signed an S3 request to the workspace root
bucket with the wrong region ("the authorization header is malformed; the
region 'us-east-1' is wrong"). Every statement the installer runs is idempotent,
so `_sql_statement` now retries transient failures (whether they surface on the
initial POST or during polling) and, either way, surfaces the control plane's
own error code and message instead of a bare state name.
"""

from __future__ import annotations

import pytest

import server.lifecycle as lifecycle

_S3_REGION_FLAKE = (
    "Remote exception occurred: AWSBadRequestException: doesBucketExist on "
    "cicaktest-rootbucket-ecb88b29: The authorization header is malformed; the "
    "region 'us-east-1' is wrong; expecting 'us-west-2'"
)


def _running(statement_id: str = "stmt-1") -> dict:
    return {"status": {"state": "RUNNING"}, "statement_id": statement_id}


def _terminal(state: str, message: str = "", code: str = "", statement_id: str = "stmt-1") -> dict:
    status: dict = {"state": state}
    if message or code:
        status["error"] = {"message": message, "error_code": code}
    return {
        "status": status,
        "statement_id": statement_id,
        "manifest": {"schema": {"columns": []}},
        "result": {"data_array": []},
    }


def _succeeded(statement_id: str = "stmt-1") -> dict:
    return _terminal("SUCCEEDED", statement_id=statement_id)


class _FakeApi:
    """Returns queued responses in order, recording the method of every call."""

    def __init__(self, responses: list[dict]) -> None:
        self._responses = list(responses)
        self.calls: list[str] = []

    def __call__(self, profile, method, path, *, body=None, timeout=600):
        self.calls.append(method)
        assert self._responses, "unexpected extra Databricks API call"
        return self._responses.pop(0)

    @property
    def posts(self) -> int:
        return self.calls.count("post")


@pytest.fixture()
def no_real_sleep(monkeypatch):
    slept: list[float] = []
    monkeypatch.setattr(lifecycle.time, "sleep", lambda seconds: slept.append(seconds))
    return slept


def test_transient_failure_on_post_is_retried_then_succeeds(monkeypatch, no_real_sleep):
    fake = _FakeApi([_terminal("FAILED", _S3_REGION_FLAKE), _succeeded()])
    monkeypatch.setattr(lifecycle, "_databricks_api", fake)

    payload = lifecycle._sql_statement("prof", "wh", "CREATE SCHEMA IF NOT EXISTS x")

    assert payload["status"]["state"] == "SUCCEEDED"
    assert fake.calls == ["post", "post"], "statement re-issued exactly once"
    assert no_real_sleep == [3.0], "one transient retry backs off before re-issuing"


def test_persistent_transient_exhausts_and_reports(monkeypatch, no_real_sleep):
    fake = _FakeApi([_terminal("FAILED", _S3_REGION_FLAKE) for _ in range(4)])
    monkeypatch.setattr(lifecycle, "_databricks_api", fake)

    with pytest.raises(RuntimeError) as excinfo:
        lifecycle._sql_statement("prof", "wh", "CREATE SCHEMA IF NOT EXISTS x", max_attempts=4)

    assert fake.posts == 4, "tries exactly max_attempts times"
    assert no_real_sleep == [3.0, 6.0, 12.0], "bounded exponential backoff between attempts"
    assert "the region 'us-east-1' is wrong" in str(excinfo.value)
    assert "warehouse=wh" in str(excinfo.value), "final error carries operational context"


def test_permanent_failure_is_not_retried_and_surfaces_message(monkeypatch, no_real_sleep):
    fake = _FakeApi([
        _terminal("FAILED", "Catalog 'nope' does not exist.", code="CATALOG_DOES_NOT_EXIST")
    ])
    monkeypatch.setattr(lifecycle, "_databricks_api", fake)

    with pytest.raises(RuntimeError) as excinfo:
        lifecycle._sql_statement("prof", "wh", "SELECT 1", max_attempts=4)

    message = str(excinfo.value)
    assert fake.posts == 1, "a permanent error must not be retried"
    assert no_real_sleep == [], "no backoff for a permanent error"
    assert "CATALOG_DOES_NOT_EXIST" in message
    assert "does not exist" in message


@pytest.mark.parametrize("state", ["CANCELED", "CLOSED"])
def test_non_failed_terminal_states_are_not_retried(monkeypatch, no_real_sleep, state):
    fake = _FakeApi([_terminal(state, "cancelled by user")])
    monkeypatch.setattr(lifecycle, "_databricks_api", fake)

    with pytest.raises(RuntimeError) as excinfo:
        lifecycle._sql_statement("prof", "wh", "SELECT 1", max_attempts=4)

    assert fake.posts == 1, f"{state} must not be retried"
    assert state in str(excinfo.value)


@pytest.mark.parametrize("state", ["CANCELED", "CLOSED"])
def test_transient_message_on_non_failed_state_not_retried(monkeypatch, no_real_sleep, state):
    # Pins the `state == "FAILED"` guard: a CANCELED/CLOSED carrying a transient
    # message must NOT be retried, even though the message would classify transient.
    fake = _FakeApi([_terminal(state, _S3_REGION_FLAKE)])
    monkeypatch.setattr(lifecycle, "_databricks_api", fake)

    with pytest.raises(RuntimeError) as excinfo:
        lifecycle._sql_statement("prof", "wh", "SELECT 1", max_attempts=4)

    assert fake.posts == 1, f"{state} with a transient message must not be retried"
    assert no_real_sleep == []
    assert state in str(excinfo.value)


def test_polling_path_reaches_success_via_get(monkeypatch, no_real_sleep):
    fake = _FakeApi([_running(), _succeeded()])
    monkeypatch.setattr(lifecycle, "_databricks_api", fake)

    payload = lifecycle._sql_statement("prof", "wh", "SELECT 1")

    assert payload["status"]["state"] == "SUCCEEDED"
    assert fake.calls == ["post", "get"], "non-terminal POST is polled with GET"


def test_transient_failure_during_polling_is_retried(monkeypatch, no_real_sleep):
    # POST -> RUNNING, GET -> transient FAILED (retry), POST -> SUCCEEDED.
    fake = _FakeApi([_running(), _terminal("FAILED", _S3_REGION_FLAKE), _succeeded()])
    monkeypatch.setattr(lifecycle, "_databricks_api", fake)

    payload = lifecycle._sql_statement("prof", "wh", "CREATE SCHEMA IF NOT EXISTS x")

    assert payload["status"]["state"] == "SUCCEEDED"
    assert fake.calls == ["post", "get", "post"], "a transient FAILED seen while polling re-issues"
    assert 3.0 in no_real_sleep, "the retry backoff fired"


def test_failed_without_error_object_is_not_retried(monkeypatch, no_real_sleep):
    fake = _FakeApi([_terminal("FAILED")])  # no error message/code -> undiagnosable
    monkeypatch.setattr(lifecycle, "_databricks_api", fake)

    with pytest.raises(RuntimeError) as excinfo:
        lifecycle._sql_statement("prof", "wh", "SELECT 1", max_attempts=4)

    assert fake.posts == 1, "a FAILED with no classifiable error must not loop"
    assert no_real_sleep == []
    assert "FAILED" in str(excinfo.value)


def test_transient_signalled_only_by_error_code_is_retried(monkeypatch, no_real_sleep):
    # Empty message, transient signal in the structured code.
    fake = _FakeApi([_terminal("FAILED", message="", code="TEMPORARILY_UNAVAILABLE"), _succeeded()])
    monkeypatch.setattr(lifecycle, "_databricks_api", fake)

    payload = lifecycle._sql_statement("prof", "wh", "CREATE SCHEMA IF NOT EXISTS x")

    assert payload["status"]["state"] == "SUCCEEDED"
    assert fake.posts == 2, "error_code alone can classify a transient failure"
    assert no_real_sleep == [3.0]


def test_transient_classifier_matches_known_flakes_and_ignores_real_errors():
    assert lifecycle._sql_statement_error_is_transient(_S3_REGION_FLAKE)
    assert lifecycle._sql_statement_error_is_transient("Request throttled, please try again")
    assert lifecycle._sql_statement_error_is_transient("TEMPORARILY_UNAVAILABLE ")
    assert lifecycle._sql_statement_error_is_transient("INTERNAL_ERROR ")
    assert not lifecycle._sql_statement_error_is_transient("Catalog 'x' does not exist")
    assert not lifecycle._sql_statement_error_is_transient("")
