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
    """Returns queued responses in order, recording *every argument* of every call.

    Recording the method alone was enough to prove the retry re-issues, and not
    enough to prove it re-issues *the same statement to the same warehouse*. A
    retry that silently dropped the warehouse id, sent an empty statement, or
    switched endpoints would still produce ``["post", "post"]`` and pass. So the
    whole call is captured -- profile, method, endpoint path, request body and
    per-attempt timeout -- and `test_a_retry_re_issues_the_identical_call`
    asserts against it across the retry boundary.
    """

    def __init__(self, responses: list[dict]) -> None:
        self._responses = list(responses)
        self.records: list[dict] = []

    def __call__(self, profile, method, path, *, body=None, timeout=600):
        self.records.append(
            {
                "profile": profile,
                "method": method,
                "path": path,
                "body": body,
                "timeout": timeout,
            }
        )
        assert self._responses, "unexpected extra Databricks API call"
        return self._responses.pop(0)

    @property
    def calls(self) -> list[str]:
        return [record["method"] for record in self.records]

    @property
    def posts(self) -> int:
        return self.calls.count("post")

    @property
    def post_bodies(self) -> list[dict]:
        return [record["body"] for record in self.records if record["method"] == "post"]


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


def test_a_retry_re_issues_the_identical_call_to_the_same_warehouse(monkeypatch, no_real_sleep):
    """A retry must resend the same statement to the same warehouse, not just resend.

    ``["post", "post"]`` proves the statement was re-issued and nothing about
    *what* was re-issued. A retry that dropped the warehouse id, truncated the
    statement, changed the profile or hit a different endpoint would satisfy the
    method-count assertions elsewhere in this file and still be a bug -- one that
    only shows up as a wrong-warehouse or empty-statement failure in front of an
    audience. So this pins the whole request across the retry boundary.
    """
    fake = _FakeApi([_terminal("FAILED", _S3_REGION_FLAKE), _succeeded()])
    monkeypatch.setattr(lifecycle, "_databricks_api", fake)

    lifecycle._sql_statement("anti-demo-x", "wh-42", "CREATE SCHEMA IF NOT EXISTS proof")

    posts = [record for record in fake.records if record["method"] == "post"]
    assert len(posts) == 2, "the transient failure was re-issued exactly once"
    for attempt, record in enumerate(posts, start=1):
        assert record["profile"] == "anti-demo-x", f"attempt {attempt} used the wrong profile"
        assert record["path"] == "/api/2.0/sql/statements", (
            f"attempt {attempt} hit {record['path']!r}, not the statement-execution endpoint"
        )
        assert record["body"]["warehouse_id"] == "wh-42", (
            f"attempt {attempt} lost the warehouse id"
        )
        assert record["body"]["statement"] == "CREATE SCHEMA IF NOT EXISTS proof", (
            f"attempt {attempt} did not re-issue the identical statement text"
        )
        # Each POST carries the same bounded per-request budget (min(timeout,120)),
        # so a retry does not silently send a different or unbounded timeout. (This
        # pins the POST argument; it does not by itself prove the polling deadline
        # is reset per attempt.)
        assert record["timeout"] == 120, f"attempt {attempt} sent {record['timeout']}, not 120"

    # The retried POST is byte-for-byte the first: same warehouse, same SQL, same
    # everything an idempotent re-issue must not vary.
    assert posts[0]["body"] == posts[1]["body"]


def test_the_polling_get_targets_the_returned_statement_id(monkeypatch, no_real_sleep):
    """The GET half of the round trip is captured too, so a wrong poll URL fails here.

    A retry seen while polling re-POSTs; the poll before it is a GET to the exact
    statement id the POST returned. Pinning the GET endpoint keeps a future edit
    from polling the collection instead of the statement, which would never
    terminate.
    """
    fake = _FakeApi([_running("stmt-77"), _terminal("FAILED", _S3_REGION_FLAKE), _succeeded()])
    monkeypatch.setattr(lifecycle, "_databricks_api", fake)

    lifecycle._sql_statement("prof", "wh", "CREATE SCHEMA IF NOT EXISTS x")

    assert fake.calls == ["post", "get", "post"]
    get_record = next(record for record in fake.records if record["method"] == "get")
    assert get_record["path"] == "/api/2.0/sql/statements/stmt-77"
    assert get_record["body"] is None, "a poll carries no body"


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
