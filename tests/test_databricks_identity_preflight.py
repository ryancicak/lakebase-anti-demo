"""Regression tests for Databricks identity/workspace preflight classification.

A virgin install once died confusingly after the workspace reaper deleted the
Databricks workspace: ``_verify_databricks_identity`` raised the control plane's
bare last line, so a deleted workspace, a forbidden principal, and a rejected
credential were indistinguishable -- and the operator, told nothing specific,
re-issued credentials that had been correct all along. These tests pin the three
properties that fix demands: the cause is classified, the workspace host is
named, and no secret is ever echoed. The bootstrap.sh side of the same taxonomy
is covered by tests/bootstrap_stub_harness.sh::case_databricks_identity_failures.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import server.lifecycle as lifecycle

# Error strings shaped like the ones the real Databricks CLI / SDK emit, so a
# passing test means the classifier handles reality rather than its own fixtures.
_DNS_GONE = (
    "Error: failed during request visitor: inner token failed: oauth-m2m: "
    "dial tcp: lookup dbc-stub-0000.cloud.databricks.com: no such host"
)
_HTTP_404 = "Error: io.jsonwebtoken ... 404 Not Found: the workspace does not exist"
_FORBIDDEN = (
    "Error: 403 Forbidden: this service principal is not authorized to access this workspace"
)
_BAD_TOKEN = (
    "Error: default auth: oauth-m2m: token request failed: "
    "401 Unauthorized (error code: invalid_client)"
)
_EXPIRED = "Error: the access token has expired and must be refreshed"
_MYSTERY = "Error: something the classifier has never seen"


class TestClassifier:
    @pytest.mark.parametrize("raw", [_DNS_GONE, _HTTP_404])
    def test_workspace_gone(self, raw):
        assert lifecycle._classify_databricks_identity_error(raw) == "workspace-gone"

    def test_forbidden_is_no_access(self):
        assert lifecycle._classify_databricks_identity_error(_FORBIDDEN) == "no-access"

    @pytest.mark.parametrize("raw", [_BAD_TOKEN, _EXPIRED])
    def test_rejected_credentials_is_bad_token(self, raw):
        assert lifecycle._classify_databricks_identity_error(raw) == "bad-token"

    def test_unrecognised_is_unknown(self):
        assert lifecycle._classify_databricks_identity_error(_MYSTERY) == "unknown"

    def test_dns_signal_wins_over_a_coincident_401(self):
        # A deleted workspace whose OAuth endpoint still 401s must not be reported
        # as a bad credential: the fix would be re-issuing a working secret.
        both = "RESOURCE_DOES_NOT_EXIST while handling 401 Unauthorized re-auth"
        assert lifecycle._classify_databricks_identity_error(both) == "workspace-gone"

    def test_403_wins_over_a_coincident_401(self):
        both = "403 Forbidden after a 401 retry"
        assert lifecycle._classify_databricks_identity_error(both) == "no-access"

    @pytest.mark.parametrize(
        "raw",
        [
            "401 Unauthorized invalid_client (request-id: a7f404e2-1c3d)",
            "invalid_client: credential rejected at offset 40412",
            "oauth-m2m token request failed (trace 55404123)",
        ],
    )
    def test_http_code_inside_a_trace_id_or_offset_is_not_workspace_gone(self, raw):
        # The whole point of the change: a bad-token error whose text merely
        # *contains* the digits 404/403 must not be reported as a deleted
        # workspace. Bare-substring matching regressed exactly this.
        assert lifecycle._classify_databricks_identity_error(raw) == "bad-token"

    def test_a_real_bare_http_code_is_still_classified(self):
        # \b-anchored, so a genuine standalone code still resolves.
        classify = lifecycle._classify_databricks_identity_error
        assert classify("server returned 404") == "workspace-gone"
        assert classify("got a bare 403 here") == "no-access"
        assert classify("bare 401 only") == "bad-token"

    @pytest.mark.parametrize(
        "raw",
        [
            "request-id a7f404e2 only",  # 404 inside hex
            "byte offset 55404123 in stream",  # 404 inside a digit run
            "correlation 4030201 recorded",  # 403 inside a digit run
            "trace 4012345 seen",  # 401 inside a digit run
        ],
    )
    def test_a_code_inside_a_longer_run_without_a_text_marker_is_unknown(self, raw):
        # This is the assertion that makes the \b anchoring matter: with bare
        # substring matching each of these would classify as a code (and the
        # reaper misdirection would return). Bare-substring -> this test goes red.
        assert lifecycle._classify_databricks_identity_error(raw) == "unknown"


class TestRedaction:
    def test_key_value_secret_is_redacted(self):
        out = lifecycle._redact_databricks_secrets("client_secret=dose_abc123DEF at host")
        assert "dose_abc123DEF" not in out
        assert "[redacted]" in out

    @pytest.mark.parametrize(
        "raw,secret",
        [
            ('{"client_secret":"dose_JSON1abc"}', "dose_JSON1abc"),
            ('{"client_secret": "dose_JSON2abc"}', "dose_JSON2abc"),
            ('{"access_token":"eyJvSTOLEN0000aa"}', "eyJvSTOLEN0000aa"),
        ],
    )
    def test_json_quoted_secret_is_redacted(self, raw, secret):
        # The shape a "future CLI prints the config it was handed" would emit;
        # key=value redaction alone missed it.
        out = lifecycle._redact_databricks_secrets(raw)
        assert secret not in out
        assert "[redacted]" in out

    @pytest.mark.parametrize(
        "raw,secret",
        [
            ("{'client_secret': 'dose_SQdict9'}", "dose_SQdict9"),
            ("client_secret='dose_SQkv7'", "dose_SQkv7"),
            ("token: 'dose_SQtok3'", "dose_SQtok3"),
        ],
    )
    def test_single_quoted_secret_is_redacted(self, raw, secret):
        # Python dict-repr / single-quote shapes; the double-quote-only pass missed them.
        out = lifecycle._redact_databricks_secrets(raw)
        assert secret not in out
        assert "[redacted]" in out

    @pytest.mark.parametrize(
        "raw,secret",
        [
            ("clientSecret=dose_CAMEL9", "dose_CAMEL9"),  # camelCase
            ('Config{ClientSecret:"dose_PASCAL9"}', "dose_PASCAL9"),  # Go %#v PascalCase
            ("{'client-secret': 'dose_HYPHEN9'}", "dose_HYPHEN9"),  # hyphenated key
            ("aws_secret_access_key=wJalrLEAK1234567890", "wJalrLEAK1234567890"),  # AWS
        ],
    )
    def test_cased_and_hyphenated_and_aws_keys_are_redacted(self, raw, secret):
        # Locks the _DB_SECRET_KEY breadth: narrowing it back to snake-only leaves
        # these shapes leaking while the rest of the suite stays green.
        out = lifecycle._redact_databricks_secrets(raw)
        assert secret not in out
        assert "[redacted]" in out

    def test_basic_auth_credential_is_redacted(self):
        out = lifecycle._redact_databricks_secrets("Authorization: Basic ZG9zZTpzZWtyZXQxMjM=")
        assert "ZG9zZTpzZWtyZXQxMjM" not in out
        assert "[redacted]" in out

    def test_bare_valueless_token_is_a_documented_limit(self):
        # A high-entropy secret with NO key, quote, or Bearer/Basic prefix cannot be
        # redacted by shape (the Python side never holds the literal). This pins the
        # known boundary; the bash preflight, which DOES hold the literal secret,
        # redacts it there. If this ever changes, update the guarantee deliberately.
        bare = "oauth failed\ndose_BARE_NO_KEY_TOKEN_9"
        assert "dose_BARE_NO_KEY_TOKEN_9" in lifecycle._redact_databricks_secrets(bare)

    def test_the_english_word_basic_is_not_overredacted(self):
        # "basic" as prose (short following word) must stay readable.
        out = lifecycle._redact_databricks_secrets("basic authentication is required")
        assert out == "basic authentication is required"

    def test_colon_token_is_redacted(self):
        out = lifecycle._redact_databricks_secrets("token: sekrit_value_1234")
        assert "sekrit_value_1234" not in out
        assert "[redacted]" in out

    def test_bearer_token_is_redacted(self):
        out = lifecycle._redact_databricks_secrets(
            "Authorization: Bearer eyJhbGciOiJERT111aaaaaaaaaaaaaaaa"
        )
        assert "eyJhbGciOiJERT111aaaaaaaaaaaaaaaa" not in out
        assert "[redacted]" in out

    def test_free_text_token_word_is_left_readable(self):
        # "token request failed" is a message, not a secret; over-redacting it
        # would strip the very words that explain the failure.
        out = lifecycle._redact_databricks_secrets("oauth-m2m: token request failed")
        assert out == "oauth-m2m: token request failed"

    def test_non_secret_key_is_not_redacted(self):
        out = lifecycle._redact_databricks_secrets("client_id=abc123 host=https://x")
        assert "abc123" in out


class TestFailureMessage:
    def test_names_host_and_cause_for_workspace_gone(self):
        msg = lifecycle._databricks_identity_failure_message(
            host="https://dbc-stub-0000.cloud.databricks.com",
            profile="anti-demo-x",
            raw=_DNS_GONE,
        )
        assert "could not be reached or no longer exists" in msg
        assert "https://dbc-stub-0000.cloud.databricks.com" in msg
        # Mutual exclusivity: a concat-all-leads or wrong-branch bug must fail.
        assert "rejected these credentials" not in msg
        assert "is not authorized to use the workspace" not in msg

    def test_bad_token_message_never_leaks_the_secret(self):
        raw = (
            "oauth-m2m: token request failed for client_secret=dose_TOPSECRET9 "
            "at https://example.cloud.databricks.com: 401 Unauthorized invalid_client"
        )
        msg = lifecycle._databricks_identity_failure_message(
            host="https://example.cloud.databricks.com", profile="p", raw=raw
        )
        assert "rejected these credentials" in msg
        assert "https://example.cloud.databricks.com" in msg
        assert "dose_TOPSECRET9" not in msg
        assert "[redacted]" in msg
        assert "could not be reached or no longer exists" not in msg

    def test_no_access_message(self):
        msg = lifecycle._databricks_identity_failure_message(
            host="https://example.cloud.databricks.com", profile="p", raw=_FORBIDDEN
        )
        assert "is not authorized to use the workspace" in msg
        assert "rejected these credentials" not in msg
        assert "could not be reached or no longer exists" not in msg

    def test_unknown_message_lists_all_three_causes(self):
        msg = lifecycle._databricks_identity_failure_message(
            host="https://example.cloud.databricks.com", profile="p", raw=_MYSTERY
        )
        assert "Could not establish a Databricks identity" in msg
        assert "https://example.cloud.databricks.com" in msg

    def test_falls_back_to_profile_when_host_unknown(self):
        msg = lifecycle._databricks_identity_failure_message(
            host="", profile="anti-demo-x", raw=_DNS_GONE
        )
        assert "anti-demo-x" in msg


class TestProfileHost:
    def test_reads_host_for_the_named_profile(self, tmp_path, monkeypatch):
        cfg = tmp_path / "databrickscfg"
        cfg.write_text(
            "[anti-demo-x]\n"
            "host = https://dbc-stub-0000.cloud.databricks.com\n"
            "client_id = abc\n",
            encoding="utf-8",
        )
        monkeypatch.setenv("DATABRICKS_CONFIG_FILE", str(cfg))
        assert (
            lifecycle._databricks_profile_host("anti-demo-x")
            == "https://dbc-stub-0000.cloud.databricks.com"
        )

    def test_missing_profile_returns_empty_not_error(self, tmp_path, monkeypatch):
        cfg = tmp_path / "databrickscfg"
        cfg.write_text("[other]\nhost = https://x\n", encoding="utf-8")
        monkeypatch.setenv("DATABRICKS_CONFIG_FILE", str(cfg))
        assert lifecycle._databricks_profile_host("anti-demo-x") == ""

    def test_percent_in_host_does_not_raise_interpolation_error(self, tmp_path, monkeypatch):
        # RawConfigParser (not ConfigParser): a '%' in the host must be returned
        # verbatim, not raise InterpolationError and drop the host from the message.
        cfg = tmp_path / "databrickscfg"
        cfg.write_text(
            "[anti-demo-x]\nhost = https://h-%-weird.cloud.databricks.com\n", encoding="utf-8"
        )
        monkeypatch.setenv("DATABRICKS_CONFIG_FILE", str(cfg))
        assert (
            lifecycle._databricks_profile_host("anti-demo-x")
            == "https://h-%-weird.cloud.databricks.com"
        )

    def test_unreadable_config_returns_empty_not_error(self, tmp_path, monkeypatch):
        # A directory where a file is expected: read() must not become a second failure.
        monkeypatch.setenv("DATABRICKS_CONFIG_FILE", str(tmp_path))
        assert lifecycle._databricks_profile_host("anti-demo-x") == ""


class TestVerifyIdentity:
    """The end-to-end contract callers depend on: fail closed, classified, no leak."""

    def _host(self, monkeypatch, host="https://dbc-stub-0000.cloud.databricks.com"):
        monkeypatch.setattr(lifecycle, "_databricks_profile_host", lambda _profile: host)

    def test_success_returns_user(self, monkeypatch):
        monkeypatch.setattr(
            lifecycle, "_databricks_json", lambda *a, **k: {"userName": "sp@acme.com"}
        )
        monkeypatch.setattr(lifecycle, "_run", lambda *a, **k: None)
        assert lifecycle._verify_databricks_identity("anti-demo-x") == "sp@acme.com"

    def test_deleted_workspace_is_named_and_does_not_return(self, monkeypatch):
        self._host(monkeypatch)

        def boom(*_a, **_k):
            raise RuntimeError(_DNS_GONE)

        monkeypatch.setattr(lifecycle, "_databricks_json", boom)
        with pytest.raises(RuntimeError) as excinfo:
            lifecycle._verify_databricks_identity("anti-demo-x")
        message = str(excinfo.value)
        assert "could not be reached or no longer exists" in message
        assert "dbc-stub-0000.cloud.databricks.com" in message

    def test_does_not_proceed_past_a_401_and_does_not_leak_the_secret(self, monkeypatch):
        self._host(monkeypatch, "https://example.cloud.databricks.com")
        reached_capability_check = []
        monkeypatch.setattr(
            lifecycle, "_run", lambda *a, **k: reached_capability_check.append(True)
        )

        def boom(*_a, **_k):
            raise RuntimeError(
                "oauth-m2m: token request failed for client_secret=dose_TOPSECRET9: "
                "401 Unauthorized invalid_client"
            )

        monkeypatch.setattr(lifecycle, "_databricks_json", boom)
        with pytest.raises(RuntimeError) as excinfo:
            lifecycle._verify_databricks_identity("anti-demo-x")
        message = str(excinfo.value)
        assert "rejected these credentials" in message
        assert "dose_TOPSECRET9" not in message
        # A 401 must stop the probe: the Lakebase capability check must never run.
        assert reached_capability_check == []

    def test_empty_username_is_refused_with_the_host(self, monkeypatch):
        self._host(monkeypatch)
        monkeypatch.setattr(lifecycle, "_databricks_json", lambda *a, **k: {"userName": ""})
        monkeypatch.setattr(lifecycle, "_run", lambda *a, **k: None)
        with pytest.raises(RuntimeError) as excinfo:
            lifecycle._verify_databricks_identity("anti-demo-x")
        message = str(excinfo.value)
        assert "no workspace userName" in message
        assert "dbc-stub-0000.cloud.databricks.com" in message

    def test_lakebase_capability_failure_is_distinct_and_names_the_user(self, monkeypatch):
        self._host(monkeypatch)
        monkeypatch.setattr(
            lifecycle, "_databricks_json", lambda *a, **k: {"userName": "sp@acme.com"}
        )

        def boom(*_a, **_k):
            raise RuntimeError("PERMISSION_DENIED: Lakebase is not enabled")

        monkeypatch.setattr(lifecycle, "_run", boom)
        with pytest.raises(RuntimeError) as excinfo:
            lifecycle._verify_databricks_identity("anti-demo-x")
        message = str(excinfo.value)
        assert "Lakebase (Postgres) API is not usable" in message
        assert "sp@acme.com" in message


# A shared corpus of realistic control-plane errors. Each must classify the same
# way through the Python classifier AND the bash classifier -- a behavioral check,
# not a marker-presence one, so recategorisation, if/elif reordering, or a
# boundary-anchoring divergence (the bug that let a hex "404" trace id be called
# workspace-gone) is caught. Includes the tricky cross-bucket and boundary cases.
_TAXONOMY_CORPUS: tuple[tuple[str, str], ...] = (
    ("dial tcp: lookup example.cloud.databricks.com: no such host", "workspace-gone"),
    ("Could not resolve host example.cloud.databricks.com", "workspace-gone"),
    ("404 Not Found: the workspace does not exist", "workspace-gone"),
    ("RESOURCE_DOES_NOT_EXIST", "workspace-gone"),
    ("x509: certificate signed by unknown authority", "workspace-gone"),
    ("server returned a bare 404", "workspace-gone"),
    ("407 Proxy Authentication Required", "workspace-gone"),
    ("403 Forbidden: not authorized to access this workspace", "no-access"),
    ("PERMISSION_DENIED: access denied", "no-access"),
    ("got a bare 403 back", "no-access"),
    ("401 Unauthorized invalid_client", "bad-token"),
    ("default auth: oauth-m2m: token request failed", "bad-token"),
    ("the access token has expired", "bad-token"),
    ("401 Unauthorized invalid_client (request-id: a7f404e2-1c3d)", "bad-token"),
    ("invalid_client rejected at offset 40412", "bad-token"),
    # Cross-bucket: a text signal must beat a co-occurring HTTP code, identically
    # on both surfaces (this is where inline-code vs backstop precedence drifted).
    ("invalid_client error occurred, http status 404", "bad-token"),
    ("403 Forbidden; correlation id 404aa", "no-access"),
    ("403 Forbidden after a 401 retry", "no-access"),
    # A code's digits INSIDE a longer run, with NO text marker, must NOT classify
    # as that code -- this is what makes the \b anchoring load-bearing: with bare
    # substring matching these become workspace-gone/no-access on BOTH surfaces.
    ("request-id a7f404e2 only", "unknown"),
    ("byte offset 55404123 in stream", "unknown"),
    ("correlation 4030201 recorded", "unknown"),
    # Prose "deleted" is a marker on neither side -> honest three-way, agreed.
    ("the workspace was deleted by an administrator", "unknown"),
    ("kaboom, an unmapped control-plane condition", "unknown"),
)


def _bash_classify(raw: str) -> str:
    """Run bootstrap.sh's real `databricks_failure_category` on `raw`."""
    import subprocess

    bootstrap = Path(__file__).resolve().parent.parent / "bootstrap.sh"
    # Extract the function verbatim and invoke it, so the test drives the exact
    # shell the installer runs, not a Python transliteration of it.
    script = (
        f'''eval "$(awk '/^databricks_failure_category\\(\\) {{/,/^}}/' '{bootstrap}')"'''
        '\ndatabricks_failure_category "$1"\n'
    )
    result = subprocess.run(
        ["bash", "-c", script, "_", raw],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


@pytest.mark.parametrize("raw,expected", _TAXONOMY_CORPUS)
def test_python_classifier_matches_the_corpus(raw, expected):
    assert lifecycle._classify_databricks_identity_error(raw) == expected


@pytest.mark.skipif(__import__("shutil").which("bash") is None, reason="bash not on PATH")
@pytest.mark.parametrize("raw,expected", _TAXONOMY_CORPUS)
def test_python_and_bootstrap_classifiers_agree_behaviorally(raw, expected):
    """The two preflight surfaces must return the SAME category for the SAME error.

    Behavioral, not marker-presence: this would have caught the bare-substring
    "404" divergence, an if/elif reorder, or a recategorised marker -- none of
    which a grep-for-markers test can see.
    """
    assert lifecycle._classify_databricks_identity_error(raw) == expected
    assert _bash_classify(raw) == expected
