"""`scripts/attach-operator-policies.sh` must update a drifted policy, not reuse it.

The trap this covers had already been set by this very change set: the operator
policies grew `sqs:CreateQueue` and `iam:SimulatePrincipalPolicy`, and an operator
who had run the attach script once and re-ran it after pulling would, under the
old script, keep the *old* customer-managed policy unchanged -- `get-policy`
succeeded, so the document was never looked at -- and then fail the very apply the
new grants were for. So the script now compares the live default version against
the rendered document and creates a new default version on drift, pruning the
oldest non-default versions if IAM's five-version limit is in the way.

Every AWS call is a stub. Nothing here touches real IAM: the stub records the
mutating calls to a log and answers reads from a per-policy file it owns, so the
assertions are about *which* IAM writes the script decided to make.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
SCRIPT = REPO / "scripts" / "attach-operator-policies.sh"

# A stateful `aws` stub. It stores each policy's current default document under
# $STUB_STATE/<name>.doc and its version count under <name>.versions, and appends
# every mutating iam call to $STUB_STATE/calls.log. That statefulness is the point:
# it lets a first run "create" a policy whose document a second run then reads
# back, so "reuse when unchanged" and "new version on drift" are both exercised
# against a store that behaves like IAM rather than a fixed canned answer.
_AWS_STUB = r"""#!/usr/bin/env bash
set -u
S="$STUB_STATE"
args="$*"
# --- read the flags this script actually passes ---
arn=""; version_id=""; policy_name=""; doc_file=""; query=""
prev=""
for a in "$@"; do
  case "$prev" in
    --policy-arn) arn="$a" ;;
    --version-id) version_id="$a" ;;
    --policy-name) policy_name="$a" ;;
    --policy-document) doc_file="${a#file://}" ;;
    --query) query="$a" ;;
  esac
  prev="$a"
done
name_from_arn() { printf '%s' "${1##*/}"; }
[[ -n "$arn" ]] && policy_name="$(name_from_arn "$arn")"

case "$args" in
  *"sts get-caller-identity"*)
    echo "${STUB_ACCOUNT:-111122223333}" ;;

  *"iam get-policy "*)
    # Existence is "a .doc file is present". DefaultVersionId is a fixed handle;
    # the stub returns the current doc regardless, which is what matters here.
    [[ -f "$S/$policy_name.doc" ]] || { echo "NoSuchEntity" >&2; exit 255; }
    if [[ "$query" == *"DefaultVersionId"* ]]; then echo "v-default"; else echo "$arn"; fi ;;

  *"iam get-policy-version"*)
    # STUB_CLI_V1 reproduces AWS CLI v1, which returns Document URL-encoded (a JSON
    # string literal) rather than the decoded object v2 returns. Otherwise return
    # the stored doc with its top-level keys reversed and compacted, so the "already
    # matches" outcome depends on the script's `jq -cS` canonicalization rather than
    # on byte-identity -- deleting that canonicalization would flip this to drift.
    if [[ "${STUB_CLI_V1:-0}" == "1" ]]; then
      echo '"%7B%22Version%22%3A%222012-10-17%22%7D"'
    else
      jq -c 'to_entries|reverse|from_entries' "$S/$policy_name.doc" 2>/dev/null || echo '{}'
    fi ;;

  *"iam list-policy-versions"*)
    count="$(cat "$S/$policy_name.versions" 2>/dev/null || echo 1)"
    if [[ "$query" == *"length(Versions)"* ]]; then
      echo "$count"
    else
      # The oldest non-default version id. The stub keeps them as v2..vN with vN
      # the newest, so the oldest non-default is v2 while any remain.
      if (( count >= 2 )); then echo "v2"; else echo "None"; fi
    fi ;;

  *"iam create-policy "*)
    printf '%s' "create-policy $policy_name" >>"$S/calls.log"; printf '\n' >>"$S/calls.log"
    cp "$doc_file" "$S/$policy_name.doc"
    echo "1" >"$S/$policy_name.versions"
    echo "arn:aws:iam::${STUB_ACCOUNT:-111122223333}:policy/$policy_name" ;;

  *"iam create-policy-version"*)
    printf '%s' "create-policy-version $policy_name" >>"$S/calls.log"; printf '\n' >>"$S/calls.log"
    cp "$doc_file" "$S/$policy_name.doc"
    count="$(cat "$S/$policy_name.versions" 2>/dev/null || echo 1)"
    echo "$((count + 1))" >"$S/$policy_name.versions"
    echo "v-new" ;;

  *"iam delete-policy-version"*)
    printf '%s' "delete-policy-version $policy_name $version_id" >>"$S/calls.log"
    printf '\n' >>"$S/calls.log"
    count="$(cat "$S/$policy_name.versions" 2>/dev/null || echo 1)"
    echo "$((count - 1))" >"$S/$policy_name.versions"
    echo "{}" ;;

  *"iam list-attached-user-policies"*) echo "0" ;;
  *"iam list-attached-role-policies"*) echo "0" ;;
  *"iam attach-user-policy"*)
    printf '%s' "attach-user-policy $policy_name" >>"$S/calls.log"; printf '\n' >>"$S/calls.log" ;;
  *"iam attach-role-policy"*)
    printf '%s' "attach-role-policy $policy_name" >>"$S/calls.log"; printf '\n' >>"$S/calls.log" ;;

  *) echo "{}" ;;
esac
exit 0
"""

POLICIES = ("AntiDemoOperatorNetwork", "AntiDemoOperatorDatabases", "AntiDemoOperatorIdentity")


def _run(
    tmp_path: Path, state: Path, extra_env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    stub = bin_dir / "aws"
    stub.write_text(_AWS_STUB, encoding="utf-8")
    stub.chmod(0o755)
    # Real jq is required: the script canonicalises documents with it.
    real_jq = shutil.which("jq")
    assert real_jq, "jq is required for this test"
    jq_link = bin_dir / "jq"
    if not (jq_link.exists() or jq_link.is_symlink()):
        jq_link.symlink_to(real_jq)
    env = {
        "PATH": f"{bin_dir}:/usr/bin:/bin",
        "HOME": str(tmp_path),
        "STUB_STATE": str(state),
        "STUB_ACCOUNT": "111122223333",
        **(extra_env or {}),
    }
    return subprocess.run(
        ["bash", str(SCRIPT), "--user", "operator", "--region", "us-west-2"],
        cwd=REPO,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )


def _calls(state: Path) -> list[str]:
    log = state / "calls.log"
    return log.read_text(encoding="utf-8").splitlines() if log.exists() else []


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash is not on PATH")
@pytest.mark.skipif(shutil.which("jq") is None, reason="jq is not on PATH")
def test_first_run_creates_every_operator_policy(tmp_path):
    state = tmp_path / "state"
    state.mkdir()
    result = _run(tmp_path, state)
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
    created = [line for line in _calls(state) if line.startswith("create-policy ")]
    assert sorted(line.split()[1] for line in created) == sorted(POLICIES)
    # A brand-new policy is created, never "version"-ed.
    assert not any(line.startswith("create-policy-version") for line in _calls(state))


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash is not on PATH")
@pytest.mark.skipif(shutil.which("jq") is None, reason="jq is not on PATH")
def test_unchanged_policies_are_reused_not_reversioned(tmp_path):
    """The idempotency the old script had, which the new one must keep."""

    state = tmp_path / "state"
    state.mkdir()
    assert _run(tmp_path, state).returncode == 0  # populates the store from the real docs
    (state / "calls.log").write_text("", encoding="utf-8")  # forget the creates

    result = _run(tmp_path, state)
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
    assert "already matches the rendered document" in result.stdout
    # Nothing mutated: no new version, no create, no delete.
    assert not any(
        line.startswith(("create-policy", "create-policy-version", "delete-policy-version"))
        for line in _calls(state)
    ), _calls(state)


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash is not on PATH")
@pytest.mark.skipif(shutil.which("jq") is None, reason="jq is not on PATH")
def test_aws_cli_v1_url_encoded_document_reuses_rather_than_churning(tmp_path):
    """Under AWS CLI v1 the live document is URL-encoded and cannot be compared.

    The unsafe default would be to read "cannot compare" as "drifted" and create a
    new version -- plus a prune -- on every run, silently walking toward the
    5-version limit and rotating DefaultVersionId each time. The script must
    instead reuse the policy (the historical behaviour) and say why.
    """

    state = tmp_path / "state"
    state.mkdir()
    assert _run(tmp_path, state).returncode == 0
    (state / "calls.log").write_text("", encoding="utf-8")

    result = _run(tmp_path, state, extra_env={"STUB_CLI_V1": "1"})
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
    assert "URL-encoded (v1)" in result.stdout
    assert not any(
        line.startswith(("create-policy-version", "delete-policy-version"))
        for line in _calls(state)
    ), f"v1 must not churn versions: {_calls(state)}"


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash is not on PATH")
@pytest.mark.skipif(shutil.which("jq") is None, reason="jq is not on PATH")
def test_a_drifted_policy_gets_a_new_default_version(tmp_path):
    """The gap this whole change closes: a stale document must be updated, not kept."""

    state = tmp_path / "state"
    state.mkdir()
    assert _run(tmp_path, state).returncode == 0
    # Simulate the live policy having an older, different document than docs/iam.
    (state / "AntiDemoOperatorIdentity.doc").write_text(
        '{"Version":"2012-10-17","Statement":[{"Sid":"Old","Effect":"Allow",'
        '"Action":"sts:GetCallerIdentity","Resource":"*"}]}',
        encoding="utf-8",
    )
    (state / "calls.log").write_text("", encoding="utf-8")

    result = _run(tmp_path, state)
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
    assert "has drifted from docs/iam" in result.stdout
    versioned = [
        line.split()[1] for line in _calls(state) if line.startswith("create-policy-version")
    ]
    # Only the drifted policy is re-versioned; the two that still match are left.
    assert versioned == ["AntiDemoOperatorIdentity"], _calls(state)
    assert not any(line.startswith("delete-policy-version") for line in _calls(state)), (
        "nothing to prune below the five-version limit"
    )


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash is not on PATH")
@pytest.mark.skipif(shutil.which("jq") is None, reason="jq is not on PATH")
def test_the_five_version_limit_is_pruned_before_a_new_version_is_added(tmp_path):
    """A policy updated enough times hits IAM's cap; the oldest non-default is freed."""

    state = tmp_path / "state"
    state.mkdir()
    assert _run(tmp_path, state).returncode == 0
    # Drift the document AND stand it at the five-version limit.
    (state / "AntiDemoOperatorDatabases.doc").write_text('{"Version":"old"}', encoding="utf-8")
    (state / "AntiDemoOperatorDatabases.versions").write_text("5", encoding="utf-8")
    (state / "calls.log").write_text("", encoding="utf-8")

    result = _run(tmp_path, state)
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
    assert "pruned old version" in result.stdout
    calls = _calls(state)
    prunes = [c for c in calls if c.startswith("delete-policy-version AntiDemoOperatorDatabases")]
    versions = [c for c in calls if c.startswith("create-policy-version AntiDemoOperatorDatabases")]
    assert prunes, f"a policy at the limit was not pruned: {calls}"
    assert versions, f"a drifted policy was not re-versioned: {calls}"
    # Prune happens before the create, or the create is the call that hits the cap.
    assert calls.index(prunes[0]) < calls.index(versions[0]), calls
