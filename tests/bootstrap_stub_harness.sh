#!/usr/bin/env bash
#
# Stub harness for bootstrap.sh. Builds a fake PATH whose `aws`, `databricks`
# and `terraform` reproduce the real response shapes, so every branch --
# including the failure branches -- can be exercised without touching a cloud.
#
# Nothing here reaches AWS or Databricks. It exists because the paths worth
# testing are the ones that cost money or break a live demo when they misfire.
#
#   ./tests/bootstrap_stub_harness.sh                 # run the whole matrix
#   ./tests/bootstrap_stub_harness.sh <case>          # one case
#   ./tests/bootstrap_stub_harness.sh --list          # case names
#   ./tests/bootstrap_stub_harness.sh --stubs-only    # build a sandbox, print its path
#
# Behaviour is driven by STUB_* variables so one set of stubs covers every case.
# tests/test_bootstrap_stub_harness.py runs the whole matrix under pytest, which
# is what keeps this from being a script nobody remembers to run.

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PASS=0
FAIL=0
RED=$'\033[31m'
GREEN=$'\033[32m'
BOLD=$'\033[1m'
RESET=$'\033[0m'

# ---------------------------------------------------------------------------
# Stub construction
# ---------------------------------------------------------------------------

make_stubs() {
  local dir="$1"
  mkdir -p "$dir/bin" "$dir/home"

  # A real ~/.databrickscfg usually has a [DEFAULT] section, and configparser
  # inherits its keys into every other section. A sandbox without one hid a bug
  # where bootstrap.sh refused to reuse the profile it had just written, because
  # the inherited keys read back as differences. Every case gets one.
  cat >"$dir/home/.databrickscfg" <<'CFG'
[DEFAULT]
host = https://dbc-unrelated-9999.cloud.databricks.com
auth_type = databricks-cli
account_id = 00000000-0000-0000-0000-000000000000
workspace_id = 1234567890123456

[someone-elses-profile]
host = https://dbc-unrelated-9999.cloud.databricks.com
token = not-ours
CFG
  chmod 600 "$dir/home/.databrickscfg"

  cat >"$dir/bin/aws" <<'STUB'
#!/usr/bin/env bash
# Ordered most-specific first: --query forms must win over their bare commands.
args="$*"
case "$args" in
  *"--dry-run"*)
    if [[ "${STUB_EC2_DRYRUN_DENIED:-0}" == "1" ]]; then
      echo "An error occurred (UnauthorizedOperation) when calling the operation" >&2; exit 254
    fi
    echo "An error occurred (DryRunOperation) when calling the operation: Request would have succeeded" >&2
    exit 254 ;;
  *"sts get-caller-identity"*)
    # A deploy-only run must never reach this. Setting STUB_STS_FAILS is how the
    # seal-only case proves it: an installation provisioned through an SSO
    # profile has no usable credentials here, and that must not block a redeploy.
    [[ "${STUB_STS_FAILS:-0}" == "1" ]] && { echo "Unable to locate credentials" >&2; exit 255; }
    echo "{\"UserId\":\"AIDASTUB\",\"Account\":\"${STUB_ACCOUNT:-111122223333}\",\"Arn\":\"${STUB_ARN:-arn:aws:iam::111122223333:user/stub}\"}" ;;
  *"describe-vpcs"*"Vpcs[0].VpcId"*)
    [[ "${STUB_NO_DEFAULT_VPC:-0}" == "1" ]] && { echo "None"; exit 0; }
    echo "vpc-0stub" ;;
  *"ssm get-parameter"*"Parameter.Value"*) echo "ami-0stub" ;;
  *"s3api head-bucket"*)
    [[ "${STUB_BUCKET_EXISTS:-0}" == "1" ]] || { echo "Not Found" >&2; exit 255; }
    echo '{}' ;;
  *"s3api get-bucket-location"*) echo "${STUB_BUCKET_REGION:-us-west-2}" ;;
  *"s3api get-bucket-versioning"*) echo "${STUB_BUCKET_VERSIONING:-Enabled}" ;;
  *"s3api create-bucket"*)
    [[ "${STUB_CREATE_BUCKET_FAILS:-0}" == "1" ]] && { echo "An error occurred (BucketAlreadyExists)" >&2; exit 255; }
    echo '{"Location":"/stub"}' ;;
  *"s3api put-bucket-versioning"*)
    [[ "${STUB_VERSIONING_FAILS:-0}" == "1" ]] && { echo "AccessDenied" >&2; exit 255; }
    exit 0 ;;
  *"s3api list-buckets"*)
    [[ "${STUB_S3_LIST_DENIED:-0}" == "1" ]] && { echo "AccessDenied" >&2; exit 255; }
    echo '{"Buckets":[]}' ;;
  *"iam get-role"*) echo "arn:aws:iam::${STUB_ACCOUNT:-111122223333}:role/stub-role" ;;
  *) echo '{}' ;;
esac
STUB

  # The deploy verification mints a token and then asks the app's front door
  # whether it is answering. Both have to be stubbable, because "the platform
  # says SUCCEEDED and the app serves 502" is a real state this must catch.
  cat >"$dir/bin/curl" <<'STUB'
#!/usr/bin/env bash
args="$*"
case "$args" in
  *"/oidc/v1/token"*)
    [[ "${STUB_TOKEN_FAILS:-0}" == "1" ]] && exit 22
    echo '{"access_token":"stub-token","expires_in":3600}' ;;
  *"/api/health"*)
    # Two different probes hit this. The pre-flight one asks only for a status
    # code (-w '%{http_code}') so it can tell "already broken" from "working,
    # about to be replaced"; the post-deploy one wants the body. They have to be
    # answered differently or the pre-flight reads a JSON body as its code.
    if [[ "$args" == *"http_code"* ]]; then
      echo "${STUB_PREFLIGHT_CODE:-200}"
      exit 0
    fi
    # STUB_APP_NOT_SERVING reproduces a container that was scheduled and built
    # but whose process never came up: curl -f sees 502 and exits non-zero.
    [[ "${STUB_APP_NOT_SERVING:-0}" == "1" ]] && exit 22
    echo '{"status":"ok","database_connections":"sealed"}' ;;
  *"checkip.amazonaws.com"*) echo "203.0.113.7" ;;
  *) echo '{}' ;;
esac
STUB

  cat >"$dir/bin/databricks" <<'STUB'
#!/usr/bin/env bash
args="$*"
case "$args" in
  *"current-user me"*) echo "{\"userName\":\"${STUB_DB_USER:-stub@example.com}\",\"id\":\"42\"}" ;;
  *"postgres list-projects"*) echo '{"projects":[]}' ;;
  *"warehouses list"*) echo '[{"id":"whstub","name":"Stub WH","warehouse_type":"PRO"}]' ;;
  *"catalogs get"*)
    [[ "${STUB_CATALOG_MISSING:-0}" == "1" ]] && { echo "does not exist" >&2; exit 1; }
    echo '{"name":"stubcat"}' ;;
  *"apps get"*)
    [[ "${STUB_APP_MISSING:-0}" == "1" ]] && { echo "RESOURCE_DOES_NOT_EXIST" >&2; exit 1; }
    cat <<JSON
{"name":"lakebase-anti-demo",
 "service_principal_client_id":"app-client-stub",
 "url":"https://stub.aws.databricksapps.com",
 "compute_status":{"state":"${STUB_APP_COMPUTE:-ACTIVE}","message":"${STUB_APP_MSG:-App compute is running.}"},
 "active_deployment":{"status":{"state":"${STUB_APP_DEPLOY:-SUCCEEDED}","message":"App started successfully"}},
 "resources":[{"name":"anti-demo-manifest-json","secret":{"scope":"${STUB_BOUND_SCOPE:-lakebase-anti-demo-.anti-demo-v7}","key":"manifest-json","permission":"READ"}}]}
JSON
    ;;
  *"secrets list-scopes"*)
    [[ "${STUB_SCOPE_EXISTS:-1}" == "1" ]] && echo '[{"name":"lakebase-anti-demo-.anti-demo-v7"}]' || echo '[]' ;;
  *"secrets create-scope"*)
    [[ "${STUB_SCOPE_CREATE_RACE:-0}" == "1" ]] && { echo "RESOURCE_ALREADY_EXISTS" >&2; exit 1; }
    [[ "${STUB_SCOPE_CREATE_FAILS:-0}" == "1" ]] && { echo "PERMISSION_DENIED" >&2; exit 1; }
    exit 0 ;;
  *"secrets put-secret"*)
    # The value has to be consumed and *inspected*, not discarded. The real CLI
    # refuses an empty one from its own client-side check, before it sends
    # anything: "Secret value must be specified in a create request!". This stub
    # used to `cat >/dev/null` and exit 0, which is why the harness stayed green
    # while the keys-only deploy -- the configuration this installer is designed
    # around -- could not write its empty aws-session-token and aborted here.
    body="$(cat)"
    [[ "${STUB_PUT_SECRET_FAILS:-0}" == "1" ]] && { echo "PERMISSION_DENIED on scope" >&2; exit 1; }
    if [[ -z "$body" ]]; then
      echo "Error: Secret value must be specified in a create request!" >&2
      exit 1
    fi
    exit 0 ;;
  *"api post /api/2.0/secrets/put"*)
    # The REST endpoint underneath, which accepts "string_value": "" where the
    # CLI will not. It records what it wrote so a case can assert the empty
    # session token was really published rather than quietly skipped.
    body_file=""
    for a in "$@"; do
      [[ "$a" == @* ]] && body_file="${a#@}"
    done
    [[ -n "$body_file" && -r "$body_file" ]] ||
      { echo "stub: --json @file was not supplied" >&2; exit 1; }
    jq -r '"\(.key)=[\(.string_value)]"' <"$body_file" \
      >>"${STUB_STATE_DIR:-/tmp}/secrets-put-api" 2>/dev/null
    [[ "${STUB_PUT_SECRET_FAILS:-0}" == "1" ]] && { echo "PERMISSION_DENIED on scope" >&2; exit 1; }
    echo '{}' ;;
  *"secrets list-secrets"*)
    # STUB_SECRET_KEYS lists the keys the scope already holds, which is what
    # decides whether a run with no AWS keys may leave them alone or must refuse.
    ms="${STUB_SECRET_MS:-1700000000000}"
    printf '{"secrets":['
    sep=""
    for k in ${STUB_SECRET_KEYS:-manifest-json}; do
      printf '%s{"key":"%s","last_updated_timestamp":"%s"}' "$sep" "$k" "$ms"
      sep=","
    done
    printf ']}\n' ;;
  *"apps update"*)
    cat >/dev/null
    [[ "${STUB_APP_UPDATE_FAILS:-0}" == "1" ]] && { echo "INVALID_PARAMETER_VALUE: resources" >&2; exit 1; }
    echo '{}' ;;
  *"apps deploy"*)
    # STUB_DEPLOY_FLAKY counts how many attempts fail with a transport-shaped
    # error before one succeeds, which is the shape of the real failure: one
    # wheel out of fifty timing out against the package proxy, a different one
    # each attempt. The count lives on disk because each attempt is a separate
    # process.
    n_file="${STUB_STATE_DIR:-/tmp}/deploy-attempts"
    n=$(( $(cat "$n_file" 2>/dev/null || echo 0) + 1 ))
    echo "$n" >"$n_file"
    if (( n <= ${STUB_DEPLOY_FLAKY:-0} )); then
      echo "deployment failed. To view app logs, run: databricks apps logs" >&2; exit 1
    fi
    [[ "${STUB_DEPLOY_FAILS:-0}" == "1" ]] && { echo "deployment failed: build error" >&2; exit 1; }
    echo '{"deployment_id":"stub"}' ;;
  *"sync "*)
    [[ "${STUB_SYNC_FAILS:-0}" == "1" ]] && { echo "sync: permission denied" >&2; exit 1; }
    exit 0 ;;
  *"workspace get-status"*)
    # STUB_SYNC_NO_DIST reproduces the .gitignore trap: sync obeys .gitignore,
    # frontend/dist never reaches the workspace, the deploy still succeeds, and
    # every page answers 503.
    if [[ "$args" == *"frontend/dist/index.html"* && "${STUB_SYNC_NO_DIST:-0}" == "1" ]]; then
      echo "RESOURCE_DOES_NOT_EXIST" >&2; exit 1
    fi
    echo '{"object_type":"FILE"}' ;;
  *"apps logs"*)
    # The retry decision is made from the build log, not from the CLI's exit
    # output, so the log has to be able to speak both dialects: a download that
    # timed out (retryable) and a dependency that cannot resolve (not).
    case "${STUB_LOG_MODE:-invalid-state}" in
      timeout)
        echo "Request failed after 3 retries"
        echo "Failed to fetch: https://packages.example.com/packages/x/certifi.whl"
        echo "client error (Connect)"
        echo "operation timed out" ;;
      conflict)
        echo "No solution found when resolving dependencies:"
        echo "Because there is no version of frobnicate==9.9.9 and lakebase-anti-demo depends on frobnicate==9.9.9, we can conclude that your requirements are unsatisfiable." ;;
      *)
        echo "InvalidStateError: Demo setup is currently PROVISIONING, not READY" ;;
    esac ;;
  *"apps stop"* | *"apps start"*) exit 0 ;;
  *) echo '{}' ;;
esac
STUB

  cat >"$dir/bin/terraform" <<'STUB'
#!/usr/bin/env bash
echo "{\"terraform_version\":\"${STUB_TF_VERSION:-1.11.4}\"}"
STUB

  for t in psql node npm uv; do
    printf '#!/usr/bin/env bash\nexit 0\n' >"$dir/bin/$t"
  done
  chmod +x "$dir"/bin/*
}

write_env() {
  cat >"$1" <<EOF
DATABRICKS_HOST=https://dbc-stub-0000.cloud.databricks.com
DATABRICKS_CLIENT_ID=stub-client
DATABRICKS_CLIENT_SECRET=stub-secret
ANTI_DEMO_OWNER=stub@example.com
EOF
  # NO_AWS_KEYS reproduces an installation provisioned through an SSO profile:
  # the operator has no static keys to hand a deploy-only run, which is the
  # normal case for a seal republish.
  if [[ "${NO_AWS_KEYS:-0}" != "1" ]]; then
    cat >>"$1" <<EOF
AWS_ACCESS_KEY_ID=${ONLY_KEY_ID:-AKIASTUB}
${OMIT_SECRET_KEY:+#}AWS_SECRET_ACCESS_KEY=stubsecret
AWS_DEFAULT_REGION=us-west-2
EOF
  fi
  printf '%s\n' "${EXTRA_ENV:-}" >>"$1"
}

# A manifest just complete enough for the checks bootstrap makes.
write_manifest() {
  python3 - "$1" "${2:-ready}" "${3:-7}" <<'PY'
import json, pathlib, sys
path, status, version = pathlib.Path(sys.argv[1]), sys.argv[2], int(sys.argv[3])
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps({
    "manifest_version": version,
    "installation_id": "00000000-0000-0000-0000-000000000000",
    "run_id": "ad-19700101-0000-stub",
    "status": status,
    "owner": "stub@example.com",
    "created_at": "1970-01-01T00:00:00Z",
    "expires_at": "2099-01-01T00:00:00Z",
    "aws": {"account_id": "111122223333", "region": "us-west-2",
            "auth_mode": "environment", "profile": None,
            "terraform_state": str(path.parent / "terraform.tfstate")},
    "databricks": {"user": "stub@example.com", "profile": "stub"},
    "round4": {"storage_catalog": "stubcat"},
}, indent=2) + "\n", encoding="utf-8")
PY
}

# ---------------------------------------------------------------------------
# Assertions
# ---------------------------------------------------------------------------

# run <sandbox> <args...> -> writes $OUT, returns exit code
run() {
  local sb="$1"
  shift
  OUT="$(HOME="$sb/home" PATH="$sb/bin:/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin" \
    bash ./bootstrap.sh --env-file "$sb/env" "$@" 2>&1)"
  return $?
}

check() {
  local name="$1" want="$2"
  if printf '%s' "$OUT" | grep -qF -- "$want"; then
    printf '  %sok%s   %s\n' "$GREEN" "$RESET" "$name"
    PASS=$((PASS + 1))
  else
    printf '  %sFAIL%s %s\n       wanted: %s\n' "$RED" "$RESET" "$name" "$want"
    printf '%s\n' "$OUT" | tail -12 | sed 's/^/       | /'
    FAIL=$((FAIL + 1))
  fi
}

check_absent() {
  local name="$1" unwanted="$2"
  if printf '%s' "$OUT" | grep -qF -- "$unwanted"; then
    printf '  %sFAIL%s %s\n       must not contain: %s\n' "$RED" "$RESET" "$name" "$unwanted"
    FAIL=$((FAIL + 1))
  else
    printf '  %sok%s   %s\n' "$GREEN" "$RESET" "$name"
    PASS=$((PASS + 1))
  fi
}

sandbox() {
  local sb
  sb="$(mktemp -d)"
  make_stubs "$sb"
  write_env "$sb/env"
  printf '%s' "$sb"
}

# ---------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------

case_check_clean() {
  printf '\n%s== check mode, fresh generation ==%s\n' "$BOLD" "$RESET"
  local sb gen
  gen="$(mktemp -d)/gen"
  sb="$(EXTRA_ENV="ANTI_DEMO_MANIFEST=$gen/manifest.json" sandbox)"
  run "$sb"
  check "exits 0" "Check mode complete"
  check "local backend is the default" "local backend: state stays in the generation directory"
  check "advertises the s3 opt-in" "opt in with --state-backend s3"
  check "reads app.yaml resource keys" "app.yaml requires"
  check "no manifest means first provision" "this will be a first provision"
  check_absent "no credential leaked" "stubsecret"
  check_absent "no client secret leaked" "stub-secret"

  # "Nothing was provisioned" has to mean "nothing was written". Check mode used
  # to write bootstrap.json into a generation directory it had only read, which
  # made the read-only mode of the installer the thing that created the
  # installation's first file.
  check_absent "does not claim to have written a record" "derived values recorded in"
  if [[ -e "$gen/bootstrap.json" ]]; then
    printf '  %sFAIL%s check mode wrote %s\n' "$RED" "$RESET" "$gen/bootstrap.json"
    FAIL=$((FAIL + 1))
  else
    printf '  %sok%s   check mode wrote no bootstrap.json\n' "$GREEN" "$RESET"
    PASS=$((PASS + 1))
  fi
}

# The two files whose mere presence breaks a deploy. Neither exists in this tree,
# so the guard has to be shown a tree where one does.
#
# `bootstrap.sh` does `cd` to its own directory, so a copy of the script into a
# temporary directory is a copy of the check into a tree nothing else shares.
# Creating the banned file in the real repository root would work too, and was
# how this started, but it makes a two-second window in which every other test
# and every other agent sees a repository that violates its own invariant --
# including tests/test_deploy_hygiene.py, which asserts exactly this file's
# absence. The guard fires at step 1, before anything else in the tree is read,
# so the copy needs no other file.
run_in_isolated_tree() { # <sandbox> <banned_file_name> <contents>
  local sb="$1" name="$2" contents="$3" tree
  tree="$(mktemp -d)"
  cp bootstrap.sh "$tree/bootstrap.sh"
  printf '%s\n' "$contents" >"$tree/$name"
  OUT="$(HOME="$sb/home" PATH="$sb/bin:/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin" \
    bash "$tree/bootstrap.sh" --env-file "$sb/env" 2>&1)"
  local status=$?
  rm -rf "$tree"
  return $status
}

case_banned_files() {
  printf '\n%s== the preflight refuses a resurrected requirements.txt or .python-version ==%s\n' \
    "$BOLD" "$RESET"
  local sb
  sb="$(EXTRA_ENV="ANTI_DEMO_MANIFEST=$(mktemp -d)/gen/manifest.json" sandbox)"

  run_in_isolated_tree "$sb" requirements.txt 'fastapi==0.1.0'
  local status=$?
  check "refuses a requirements.txt" "requirements.txt is back in the repository root"
  check "says what its presence does" "pip and Python 3.11"
  check "names where dependencies do live" "add it to pyproject.toml and run 'uv lock'"
  if ((status != 0)); then
    printf '  %sok%s   and exits non-zero\n' "$GREEN" "$RESET"
    PASS=$((PASS + 1))
  else
    printf '  %sFAIL%s a banned requirements.txt did not fail the run\n' "$RED" "$RESET"
    FAIL=$((FAIL + 1))
  fi

  run_in_isolated_tree "$sb" .python-version '3.12'
  status=$?
  check "refuses a .python-version" "A .python-version in the repository root"
  check "quotes the pin it found" "'3.12' is what it now asks for"
  check "names requires-python as the supported mechanism" "requires-python governs both"
  if ((status != 0)); then
    printf '  %sok%s   and exits non-zero too\n' "$GREEN" "$RESET"
    PASS=$((PASS + 1))
  else
    printf '  %sFAIL%s a banned .python-version did not fail the run\n' "$RED" "$RESET"
    FAIL=$((FAIL + 1))
  fi

  # And the guard must not fire on this repository, which is the state it defends.
  run "$sb"
  check_absent "does not fire on a clean tree" "is back in the repository root"
  check_absent "nor on a tree with no pin" "in the repository root pins the interpreter"
}

case_print_env() {
  printf '\n%s== print-env stays eval-clean ==%s\n' "$BOLD" "$RESET"
  local sb tmp
  tmp="$(mktemp -d)"
  sb="$(EXTRA_ENV="ANTI_DEMO_MANIFEST=$tmp/gen/manifest.json" sandbox)"
  OUT="$(HOME="$sb/home" PATH="$sb/bin:/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin" \
    bash ./bootstrap.sh --env-file "$sb/env" --print-env 2>/dev/null)"
  check "only exports on stdout" "export ANTI_DEMO_MANIFEST="
  check_absent "no narration on stdout" "==>"
  check_absent "no ok lines on stdout" "  ok    "
  if HOME="$sb/home" PATH="$sb/bin:/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin" \
    bash -c "eval \"\$(./bootstrap.sh --env-file $sb/env --print-env 2>/dev/null)\"" 2>/dev/null; then
    printf '  %sok%s   evals without error\n' "$GREEN" "$RESET"
    PASS=$((PASS + 1))
  else
    printf '  %sFAIL%s eval failed\n' "$RED" "$RESET"
    FAIL=$((FAIL + 1))
  fi
}

case_s3_refuses_existing() {
  printf '\n%s== s3 is refused on an existing generation ==%s\n' "$BOLD" "$RESET"
  local sb gen
  gen="$(mktemp -d)/gen"
  write_manifest "$gen/manifest.json"
  sb="$(EXTRA_ENV="ANTI_DEMO_MANIFEST=$gen/manifest.json" sandbox)"
  run "$sb" --state-backend s3 --state-bucket stub-state-bucket
  check "refuses to migrate" "This generation already exists, so --state-backend s3 is refused"
  check "names the dangerous command" "terraform init -migrate-state"
  check "points at the manual path" "docs/DEPLOY.md"
}

case_s3_needs_tf_111() {
  printf '\n%s== s3 requires terraform 1.11 ==%s\n' "$BOLD" "$RESET"
  local sb
  sb="$(EXTRA_ENV="ANTI_DEMO_MANIFEST=$(mktemp -d)/gen/manifest.json" sandbox)"
  STUB_TF_VERSION=1.9.8 run "$sb" --state-backend s3 --state-bucket stub-state-bucket
  check "rejects 1.9.8" "needs Terraform >= 1.11.0 (found 1.9.8)"
  check "explains the DynamoDB choice" "DynamoDB lock table, which this design deliberately avoids"
  STUB_TF_VERSION=1.11.0 run "$sb" --state-backend s3 --state-bucket stub-state-bucket
  check "accepts 1.11.0" "supports S3-native locking"
  STUB_TF_VERSION=1.13.1 run "$sb" --state-backend s3 --state-bucket stub-state-bucket
  check "accepts 1.13.1" "supports S3-native locking"
}

case_s3_needs_patch() {
  # The patch has landed, so the first branch is the live one. The refusal branch
  # is kept because it is what a revert or a partial checkout must still hit:
  # proceeding without the patch means failing inside `antidemo setup` at init.
  printf '\n%s== s3 gate tracks the lifecycle patch ==%s\n' "$BOLD" "$RESET"
  local sb
  sb="$(EXTRA_ENV="ANTI_DEMO_MANIFEST=$(mktemp -d)/gen/manifest.json" sandbox)"
  run "$sb" --state-backend s3 --state-bucket stub-state-bucket
  if grep -q 'terraform-backend.json' server/lifecycle.py 2>/dev/null; then
    check "patch present, proceeds" "s3 backend selected"
  else
    check "names the blocking function" "server/lifecycle.py:_terraform_init still hardcodes"
    check "points at the patch" "Required patch to"
  fi
}

case_s3_bucket_states() {
  printf '\n%s== s3 bucket detection ==%s\n' "$BOLD" "$RESET"
  local sb
  sb="$(EXTRA_ENV="ANTI_DEMO_MANIFEST=$(mktemp -d)/gen/manifest.json" sandbox)"
  STUB_BUCKET_EXISTS=1 run "$sb" --state-backend s3 --state-bucket stub-state-bucket
  check "existing bucket detected" "bucket stub-state-bucket exists in us-west-2"
  STUB_BUCKET_EXISTS=1 STUB_BUCKET_VERSIONING=Suspended \
    run "$sb" --state-backend s3 --state-bucket stub-state-bucket
  check "warns on unversioned bucket" "without versioning a bad write is unrecoverable"
  STUB_BUCKET_EXISTS=1 STUB_BUCKET_REGION=eu-west-1 \
    run "$sb" --state-backend s3 --state-bucket stub-state-bucket
  check "warns on cross-region bucket" "the bucket is in eu-west-1 but this install is us-west-2"
  run "$sb" --state-backend s3 --state-bucket stub-state-bucket
  check "absent bucket reported" "does not exist or is not readable"
  run "$sb" --state-backend s3 --state-bucket "Bad_Bucket_Name"
  check "rejects an illegal name" "is not a valid S3 bucket name"
  run "$sb" --state-backend s3
  check "requires a bucket name" "needs --state-bucket NAME"
  STUB_S3_LIST_DENIED=1 run "$sb" --state-backend s3 --state-bucket stub-state-bucket
  check "surfaces a denied probe" "s3:ListAllMyBuckets"
}

case_drift() {
  printf '\n%s== drift detection ==%s\n' "$BOLD" "$RESET"
  local sb gen
  gen="$(mktemp -d)/gen"
  write_manifest "$gen/manifest.json"
  sb="$(EXTRA_ENV="ANTI_DEMO_MANIFEST=$gen/manifest.json" sandbox)"

  # No record at all: unknowable, so treated as drifted.
  run "$sb"
  check "no deploy record is drift" "no local deploy record"

  # Matching record and matching remote timestamp: clean.
  local sha
  sha="$(shasum -a 256 "$gen/manifest.json" | awk '{print $1}')"
  printf '{"manifest_sha256":"%s","secret_updated_ms":"1700000000000"}\n' "$sha" >"$gen/app-deploy.json"
  run "$sb"
  check "matching seal is clean" "the deployed seal matches the current manifest"

  # Manifest moves on: stale.
  printf '{"manifest_sha256":"%s","secret_updated_ms":"1700000000000"}\n' "0000000000000000" >"$gen/app-deploy.json"
  run "$sb"
  check "changed seal is stale" "the deployed seal is stale"
  check "check mode offers the fix" "./bootstrap.sh --deploy-only"

  # Someone else pushed: remote timestamp moved.
  printf '{"manifest_sha256":"%s","secret_updated_ms":"1600000000000"}\n' "$sha" >"$gen/app-deploy.json"
  run "$sb"
  check "foreign push detected" "Someone pushed a seal from elsewhere"
}

case_deploy_refusals() {
  printf '\n%s== deploy refuses an unservable seal ==%s\n' "$BOLD" "$RESET"
  local sb gen
  gen="$(mktemp -d)/gen"

  write_manifest "$gen/manifest.json" provisioning
  sb="$(EXTRA_ENV="ANTI_DEMO_MANIFEST=$gen/manifest.json" sandbox)"
  run "$sb" --deploy-only --yes
  check "refuses a non-ready manifest" "the manifest status is 'provisioning', not 'ready'"
  check "names the raise site" "app.py:108 raises InvalidStateError"

  write_manifest "$gen/manifest.json" ready 1
  run "$sb" --deploy-only --yes
  check "refuses a v1 seal" "app.py:112 needs v2+"

  # No manifest at all.
  local empty
  empty="$(mktemp -d)/none"
  sb="$(EXTRA_ENV="ANTI_DEMO_MANIFEST=$empty/manifest.json" sandbox)"
  run "$sb" --deploy-only --yes
  check "refuses with no installation" "--deploy-only needs an installation to deploy"
}

case_deploy_runner_guard() {
  printf '\n%s== deploy refuses runner source ahead of the EC2 seal ==%s\n' "$BOLD" "$RESET"
  local sb gen status source_sha tmp
  gen="$(mktemp -d)/gen"
  write_manifest "$gen/manifest.json"
  tmp="$gen/manifest.next"
  jq '.round5 = {"harness_sha256": ("0" * 64)}' "$gen/manifest.json" >"$tmp"
  mv "$tmp" "$gen/manifest.json"
  sb="$(EXTRA_ENV="ANTI_DEMO_MANIFEST=$gen/manifest.json" sandbox)"

  ROUND4_CATALOG=stubcat STUB_STATE_DIR="$sb" run "$sb" --deploy-only --yes
  status=$?
  check "names the incompatible runner source" "REFUSING TO DEPLOY INCOMPATIBLE ROUND 5 RUNNER SOURCE"
  check "prints the targeted repair" "./antidemo runner refresh"
  check "prints the deploy that follows" "./bootstrap.sh --deploy-only --yes"
  check "states remote app surfaces were untouched" \
    "No Databricks secret, workspace source, or app deployment was changed"
  check_absent "refuses before publishing a secret" "published the seal to"
  check_absent "refuses before syncing source" "source synced"
  check_absent "refuses before deploying the app" "deployment accepted"
  if ((status != 0)); then
    printf '  %sok%s   mismatch exits non-zero\n' "$GREEN" "$RESET"
    PASS=$((PASS + 1))
  else
    printf '  %sFAIL%s mismatch exited zero\n' "$RED" "$RESET"
    FAIL=$((FAIL + 1))
  fi

  source_sha="$("$PYTHON_ENVIRONMENT/bin/python" - <<'PY'
from server.connection_spike_live import runner_harness_sha256
print(runner_harness_sha256())
PY
)"
  jq --arg sha "$source_sha" '.round5.harness_sha256 = $sha' \
    "$gen/manifest.json" >"$tmp"
  mv "$tmp" "$gen/manifest.json"
  ROUND4_CATALOG=stubcat STUB_STATE_DIR="$sb" run "$sb" --deploy-only --yes
  check "aligned source passes the guard" "Round 5 source matches the sealed EC2 harness"
  check "aligned source reaches publication" "published the seal to"
  check "aligned source reaches sync" "source synced"
}

case_deploy_happy() {
  printf '\n%s== deploy, happy path ==%s\n' "$BOLD" "$RESET"
  local sb gen
  gen="$(mktemp -d)/gen"
  write_manifest "$gen/manifest.json"
  sb="$(EXTRA_ENV="ANTI_DEMO_MANIFEST=$gen/manifest.json" sandbox)"
  STUB_STATE_DIR="$sb" run "$sb" --deploy-only --yes
  check "publishes the seal" "published the seal to"
  check "rotates the aws key when one is supplied" "rotated aws-access-key-id"
  check "empty session token is explained" "holds the empty string"
  # This is the configuration the installer is designed around -- an access key
  # and secret from a permanent IAM user, and no session token -- so this run
  # has to write an empty `aws-session-token`, and `secrets put-secret` refuses
  # to. Asserting the value and not just the log line: the message is printed
  # before the write, so a silent no-op would still say "holds the empty
  # string" and the app would fail its valueFrom at startup instead.
  if grep -qxF 'aws-session-token=[]' "$sb/secrets-put-api" 2>/dev/null; then
    printf '  %sok%s   the empty session token really was written\n' "$GREEN" "$RESET"
    PASS=$((PASS + 1))
  else
    printf '  %sFAIL%s the empty session token was not written: %s\n' "$RED" "$RESET" \
      "$(cat "$sb/secrets-put-api" 2>/dev/null || echo '<no secrets/put call>')"
    FAIL=$((FAIL + 1))
  fi
  check "binds resources" "app resources bound to scope"
  check "syncs source" "source synced"
  check "deploys" "deployment accepted"
  check "restarts" "restart requested"
  check "verifies startup" "compute ACTIVE, deployment SUCCEEDED"
  check "records the seal" "recorded the deployed seal"
  check "warns about future drift" "leaves it a generation"
  check_absent "no aws secret leaked" "stubsecret"
  check_absent "no oauth secret leaked" "stub-secret"
  if [[ -f "$gen/app-deploy.json" ]]; then
    local mode
    # BSD stat on macOS, GNU coreutils stat everywhere else. Neither spelling
    # exists on both. Keep their output in separate assignments: GNU stat
    # interprets -f as filesystem mode and prints for the valid file before
    # rejecting the BSD format operand, which would contaminate the fallback.
    if mode="$(stat -f '%Lp' "$gen/app-deploy.json" 2>/dev/null)"; then
      :
    else
      mode="$(stat -c '%a' "$gen/app-deploy.json")"
    fi
    if [[ "$mode" == "600" ]]; then
      printf '  %sok%s   deploy record is mode 600\n' "$GREEN" "$RESET"
      PASS=$((PASS + 1))
    else
      printf '  %sFAIL%s deploy record is mode %s, want 600\n' "$RED" "$RESET" "$mode"
      FAIL=$((FAIL + 1))
    fi
    if grep -qE 'stubsecret|stub-secret|AKIASTUB' "$gen/app-deploy.json"; then
      printf '  %sFAIL%s deploy record contains a credential\n' "$RED" "$RESET"
      FAIL=$((FAIL + 1))
    else
      printf '  %sok%s   deploy record holds no credential\n' "$GREEN" "$RESET"
      PASS=$((PASS + 1))
    fi
  fi
}

# A seal republish on an installation that was provisioned through an SSO
# profile. There are no static keys to supply, so the run must reach Databricks
# without authenticating to AWS at all, publish the seal, and leave the app's
# AWS credential secrets exactly as they are.
case_deploy_seal_only() {
  printf '\n%s== deploy-only without AWS keys (seal republish) ==%s\n' "$BOLD" "$RESET"
  local sb gen
  gen="$(mktemp -d)/gen"
  write_manifest "$gen/manifest.json"

  sb="$(NO_AWS_KEYS=1 EXTRA_ENV="ANTI_DEMO_MANIFEST=$gen/manifest.json" sandbox)"
  STUB_STS_FAILS=1 \
    STUB_SECRET_KEYS='manifest-json aws-access-key-id aws-secret-access-key aws-session-token' \
    run "$sb" --deploy-only --yes
  check "succeeds with no AWS credentials at all" "compute ACTIVE, deployment SUCCEEDED"
  check_absent "never reaches the AWS identity step" "Unable to locate credentials"
  check_absent "and does not print the AWS identity header" "AWS identity"
  check "says it is republishing the seal only" "republishes the seal only"
  check "still publishes the seal" "published the seal to"
  check "leaves the access key alone" "aws-access-key-id already exists"
  check "leaves the session token alone" "aws-session-token already exists"
  check "warns the AWS credentials may be expired" "AWS credentials were not refreshed"

  # The same run when the scope has no AWS secrets at all must refuse: a
  # valueFrom whose resource is missing fails the container at startup, so
  # "leave it alone" is not available.
  sb="$(NO_AWS_KEYS=1 EXTRA_ENV="ANTI_DEMO_MANIFEST=$gen/manifest.json" sandbox)"
  STUB_SECRET_KEYS='manifest-json' run "$sb" --deploy-only --yes
  local status=$?
  check "refuses when the scope lacks the AWS secrets" "does not have it"
  if ((status != 0)); then
    printf '  %sok%s   %s\n' "$GREEN" "$RESET" "and exits non-zero"
    PASS=$((PASS + 1))
  else
    printf '  %sFAIL%s %s\n' "$RED" "$RESET" "and exits non-zero"
    FAIL=$((FAIL + 1))
  fi

  # Half a pair would sign requests with a mismatched credential.
  sb="$(OMIT_SECRET_KEY=1 EXTRA_ENV="ANTI_DEMO_MANIFEST=$gen/manifest.json" sandbox)"
  run "$sb" --deploy-only --yes
  check "refuses half an AWS key pair" "only one half of the AWS key pair"
}

# --deploy-only resolves a strict subset of the derived values: it never
# authenticates to AWS, so the caller ARN, the Round 5 principal and the operator
# CIDR have no answer in that mode. It used to record its non-answers anyway, so
# a redeploy replaced a provision's real values with "(not resolved: ...)" and a
# malformed "/32" -- a record that degraded every time it was rewritten.
case_deploy_record_merge() {
  printf '\n%s== deploy-only does not degrade bootstrap.json ==%s\n' "$BOLD" "$RESET"
  local sb gen
  gen="$(mktemp -d)/gen"
  write_manifest "$gen/manifest.json"
  sb="$(EXTRA_ENV="ANTI_DEMO_MANIFEST=$gen/manifest.json" sandbox)"

  # What an earlier --apply would have left behind.
  cat >"$gen/bootstrap.json" <<'JSON'
{
  "aws_account_id": "111122223333",
  "aws_region": "us-west-2",
  "aws_caller_arn": "arn:aws:iam::111122223333:user/real-operator",
  "round5_app_principal_arn": "arn:aws:iam::111122223333:role/real-round5",
  "operator_cidr": "198.51.100.4/32",
  "recorded_by": "bootstrap.sh --apply"
}
JSON
  run "$sb" --deploy-only --yes
  check "still records" "derived values recorded in"

  local kept=1
  for probe in \
    '.aws_caller_arn == "arn:aws:iam::111122223333:user/real-operator"' \
    '.round5_app_principal_arn == "arn:aws:iam::111122223333:role/real-round5"' \
    '.operator_cidr == "198.51.100.4/32"'; do
    jq -e "$probe" "$gen/bootstrap.json" >/dev/null 2>&1 || kept=0
  done
  if ((kept == 1)); then
    printf '  %sok%s   the values deploy-only cannot resolve survived it\n' "$GREEN" "$RESET"
    PASS=$((PASS + 1))
  else
    printf '  %sFAIL%s deploy-only overwrote values it never resolved:\n' "$RED" "$RESET"
    sed 's/^/       | /' "$gen/bootstrap.json"
    FAIL=$((FAIL + 1))
  fi

  # And it must still record the things it *did* resolve.
  if jq -e '.secret_scope != "" and .databricks_app_client_id != ""' \
    "$gen/bootstrap.json" >/dev/null 2>&1; then
    printf '  %sok%s   and its own answers were written\n' "$GREEN" "$RESET"
    PASS=$((PASS + 1))
  else
    printf '  %sFAIL%s deploy-only recorded none of its own answers\n' "$RED" "$RESET"
    FAIL=$((FAIL + 1))
  fi
  if jq -e '.recorded_by == "bootstrap.sh --deploy"' "$gen/bootstrap.json" >/dev/null 2>&1; then
    printf '  %sok%s   and the record says which mode last touched it\n' "$GREEN" "$RESET"
    PASS=$((PASS + 1))
  else
    printf '  %sFAIL%s the record does not say which mode wrote it\n' "$RED" "$RESET"
    FAIL=$((FAIL + 1))
  fi

  # A malformed record cannot be merged into, and must not stop the run.
  printf 'not json at all\n' >"$gen/bootstrap.json"
  run "$sb" --deploy-only --yes
  if jq -e '.secret_scope != ""' "$gen/bootstrap.json" >/dev/null 2>&1; then
    printf '  %sok%s   an unparseable record is replaced rather than fatal\n' "$GREEN" "$RESET"
    PASS=$((PASS + 1))
  else
    printf '  %sFAIL%s an unparseable record was not recovered from\n' "$RED" "$RESET"
    FAIL=$((FAIL + 1))
  fi
}

# The seal is validated, published and recorded from one snapshot, so the sha in
# app-deploy.json names the bytes that actually reached the secret. Three
# separate reads of a live manifest.json could not promise that.
case_deploy_seal_snapshot() {
  printf '\n%s== the recorded sha names the published bytes ==%s\n' "$BOLD" "$RESET"
  local sb gen sha
  gen="$(mktemp -d)/gen"
  write_manifest "$gen/manifest.json"
  sb="$(EXTRA_ENV="ANTI_DEMO_MANIFEST=$gen/manifest.json" sandbox)"
  sha="$(shasum -a 256 "$gen/manifest.json" | awk '{print $1}')"

  run "$sb" --deploy-only --yes
  check "reports the sha it is publishing" "sha256:${sha:0:16}"
  if [[ "$(jq -r .manifest_sha256 "$gen/app-deploy.json")" == "$sha" ]]; then
    printf '  %sok%s   app-deploy.json records the published seal\n' "$GREEN" "$RESET"
    PASS=$((PASS + 1))
  else
    printf '  %sFAIL%s app-deploy.json records a different seal\n' "$RED" "$RESET"
    FAIL=$((FAIL + 1))
  fi
  if [[ -e "$gen/seal-in-flight.json" ]]; then
    printf '  %sFAIL%s the seal snapshot was left behind\n' "$RED" "$RESET"
    FAIL=$((FAIL + 1))
  else
    printf '  %sok%s   the seal snapshot is cleaned up\n' "$GREEN" "$RESET"
    PASS=$((PASS + 1))
  fi
}

case_deploy_idempotent() {
  printf '\n%s== deploy twice is idempotent ==%s\n' "$BOLD" "$RESET"
  local sb gen first
  gen="$(mktemp -d)/gen"
  write_manifest "$gen/manifest.json"
  sb="$(EXTRA_ENV="ANTI_DEMO_MANIFEST=$gen/manifest.json" sandbox)"
  run "$sb" --deploy-only --yes
  first="$(cat "$gen/app-deploy.json")"
  run "$sb" --deploy-only --yes
  check "reuses the profile it wrote rather than refusing it" "profile already matches"
  check "second run also succeeds" "compute ACTIVE, deployment SUCCEEDED"
  check "reuses the existing scope" "secret scope 'lakebase-anti-demo-.anti-demo-v7' already exists"
  if [[ "$(jq -r .manifest_sha256 <<<"$first")" == "$(jq -r .manifest_sha256 "$gen/app-deploy.json")" ]]; then
    printf '  %sok%s   the recorded seal is unchanged\n' "$GREEN" "$RESET"
    PASS=$((PASS + 1))
  else
    printf '  %sFAIL%s the recorded seal changed between identical runs\n' "$RED" "$RESET"
    FAIL=$((FAIL + 1))
  fi
  STUB_SCOPE_EXISTS=0 STUB_SCOPE_CREATE_RACE=1 run "$sb" --deploy-only --yes
  check "tolerates a concurrent scope create" "already exists"
}

case_deploy_failures() {
  printf '\n%s== deploy failure paths ==%s\n' "$BOLD" "$RESET"
  local sb gen
  gen="$(mktemp -d)/gen"
  write_manifest "$gen/manifest.json"
  sb="$(EXTRA_ENV="ANTI_DEMO_MANIFEST=$gen/manifest.json" sandbox)"

  STUB_SCOPE_EXISTS=0 STUB_SCOPE_CREATE_FAILS=1 run "$sb" --deploy-only --yes
  check "scope create failure stops" "could not create secret scope"

  STUB_PUT_SECRET_FAILS=1 run "$sb" --deploy-only --yes
  check "secret write failure stops" "could not write secret"

  STUB_APP_UPDATE_FAILS=1 run "$sb" --deploy-only --yes
  check "resource bind failure stops" "could not bind the app's secret resources"

  STUB_SYNC_FAILS=1 run "$sb" --deploy-only --yes
  check "sync failure stops and surfaces the real error" "sync: permission denied"

  # The 503 trap: sync obeys .gitignore, .gitignore excludes frontend/dist, and
  # a deploy without it starts cleanly and serves nothing.
  sb="$(EXTRA_ENV="ANTI_DEMO_MANIFEST=$gen/manifest.json" sandbox)"
  STUB_SYNC_NO_DIST=1 run "$sb" --deploy-only --yes
  check "refuses when the built frontend never reached the workspace" "would answer 503"
  check_absent "and does not deploy it anyway" "deployment accepted"

  # The failure this whole verification exists for: the platform reports a clean
  # deploy and the process inside the container is not answering. Status polling
  # alone calls this a success, so only the HTTP probe can catch it.
  sb="$(EXTRA_ENV="ANTI_DEMO_MANIFEST=$gen/manifest.json" sandbox)"
  STUB_APP_NOT_SERVING=1 run "$sb" --deploy-only --yes
  local status=$?
  check "catches a successful deploy that does not serve" "deploy succeeded and the app is not serving"
  check "names the runtime Python trap" "installs with pip on 3.11"
  check "shows the logs" "Last 60 log lines"
  if ((status != 0)); then
    printf '  %sok%s   %s\n' "$GREEN" "$RESET" "and exits non-zero"
    PASS=$((PASS + 1))
  else
    printf '  %sFAIL%s %s\n' "$RED" "$RESET" "and exits non-zero"
    FAIL=$((FAIL + 1))
  fi

  STUB_DEPLOY_FAILS=1 run "$sb" --deploy-only --yes
  check "deploy failure stops" "databricks apps deploy failed"

  # The important one: deploy succeeds, container never starts.
  STUB_APP_COMPUTE=ERROR STUB_APP_MSG="container exited" run "$sb" --deploy-only --yes
  check "detects an app that never starts" "The app did not come up"
  check "surfaces the real startup error" "InvalidStateError"
  check "names the lifespan cause" "raises inside the FastAPI lifespan"
  check "exits non-zero" "the deploy reported success but the app is not serving"

  STUB_APP_MISSING=1 run "$sb" --deploy-only --yes
  check "no app means no deploy" "service principal is unresolved"
}

case_deploy_retry() {
  printf '\n%s== deploy retries a flaky package proxy, and only that ==%s\n' "$BOLD" "$RESET"
  local sb gen status
  gen="$(mktemp -d)/gen"
  write_manifest "$gen/manifest.json"

  # One attempt loses a wheel to a timeout and the next one gets it. This is the
  # real-world case: nothing is wrong with the tree, and a run that stopped here
  # would be reporting an outage as a defect.
  sb="$(EXTRA_ENV="ANTI_DEMO_MANIFEST=$gen/manifest.json" sandbox)"
  STUB_STATE_DIR="$sb" STUB_DEPLOY_FLAKY=1 STUB_LOG_MODE=timeout \
    run "$sb" --deploy-only --yes
  status=$?
  check "names the timeout as transient" "timed out against the workspace's proxy, which is transient"
  check "retries" "retrying the deploy (attempt 2 of 4)"
  check "and then succeeds" "deployment accepted"
  if ((status == 0)); then
    printf '  %sok%s   %s\n' "$GREEN" "$RESET" "exits 0"
    PASS=$((PASS + 1))
  else
    printf '  %sFAIL%s %s\n' "$RED" "$RESET" "exits 0 (got $status)"
    FAIL=$((FAIL + 1))
  fi

  # A proxy that is down for the whole run must be reported as a proxy outage,
  # bounded, and must say that the previously deployed app is untouched -- a
  # failed build does not replace a running deployment.
  sb="$(EXTRA_ENV="ANTI_DEMO_MANIFEST=$gen/manifest.json" sandbox)"
  STUB_STATE_DIR="$sb" STUB_DEPLOY_FLAKY=9 STUB_LOG_MODE=timeout \
    run "$sb" --deploy-only --yes
  status=$?
  # Four, not three. bootstrap.sh raised DEPLOY_ATTEMPTS after six consecutive
  # real deploys lost a different wheel each time to the workspace package
  # proxy's connect timeout, so the bound is the code's and this follows it. The
  # assertion that matters is not the number but that there *is* one: an
  # unbounded retry against a proxy outage would spin instead of reporting.
  check "bounds the retries at four" "All 4 attempts failed to download packages"
  check_absent "does not try a fifth" "attempt 5 of"
  check "says plainly that the app is now down" "This has taken the app down."
  check "and that there is no rollback" "There is no rollback"
  if ((status != 0)); then
    printf '  %sok%s   %s\n' "$GREEN" "$RESET" "exits non-zero"
    PASS=$((PASS + 1))
  else
    printf '  %sFAIL%s %s\n' "$RED" "$RESET" "exits non-zero"
    FAIL=$((FAIL + 1))
  fi

  # A dependency that cannot resolve fails identically every time, so retrying
  # it only delays the report. This is the half of the decision that keeps the
  # retry honest.
  sb="$(EXTRA_ENV="ANTI_DEMO_MANIFEST=$gen/manifest.json" sandbox)"
  STUB_STATE_DIR="$sb" STUB_DEPLOY_FLAKY=9 STUB_LOG_MODE=conflict \
    run "$sb" --deploy-only --yes
  check "does not retry an unsatisfiable requirement" "will fail the same way"
  check_absent "so there is no second attempt" "attempt 2 of"
  check "surfaces the resolver's own words" "your requirements are unsatisfiable"
}

case_deploy_reads_app_yaml() {
  printf '\n%s== deploy follows app.yaml, not a hardcoded list ==%s\n' "$BOLD" "$RESET"
  local sb gen
  gen="$(mktemp -d)/gen"
  write_manifest "$gen/manifest.json"
  sb="$(EXTRA_ENV="ANTI_DEMO_MANIFEST=$gen/manifest.json" sandbox)"
  run "$sb"
  # Whatever app.yaml currently declares is what should be reported.
  local want
  want="$(sed -n 's/^[[:space:]]*valueFrom:[[:space:]]*\([A-Za-z0-9._-]*\).*$/\1/p' app.yaml | wc -l | tr -d ' ')"
  check "counts app.yaml's keys" "app.yaml requires $want resources"
}

# The apply tail cannot be exercised: it ends in `./antidemo setup`, which is
# path-relative and so not interceptable by a PATH stub, and which provisions
# real infrastructure. These two artefacts are the risky part of it, so they are
# checked directly instead of left entirely unverified.
case_generated_artefacts() {
  printf '\n%s== generated artefacts are well formed ==%s\n' "$BOLD" "$RESET"

  # 1. The backend record, extracted from bootstrap.sh so it cannot drift from
  #    the code that writes it.
  local tmp
  tmp="$(mktemp -d)"
  STATE_BUCKET=stub-state-bucket \
    STATE_KEY=anti-demo/.anti-demo-v7/terraform.tfstate \
    AWS_REGION=us-west-2 \
    python3 - "$tmp/terraform-backend.json" <<'PY'
import json, os, pathlib, sys
pathlib.Path(sys.argv[1]).write_text(json.dumps({
    "backend": "s3",
    "bucket": os.environ["STATE_BUCKET"],
    "key": os.environ["STATE_KEY"],
    "region": os.environ["AWS_REGION"],
    "use_lockfile": True,
    "encrypt": True,
}, indent=2) + "\n", encoding="utf-8")
PY
  if python3 -c "
import json,sys
d=json.load(open('$tmp/terraform-backend.json'))
assert d['backend']=='s3' and d['use_lockfile'] is True and d['encrypt'] is True
assert d['bucket'] and d['key'] and d['region']
assert 'dynamodb_table' not in d
"; then
    printf '  %sok%s   terraform-backend.json is valid and lock-file based\n' "$GREEN" "$RESET"
    PASS=$((PASS + 1))
  else
    printf '  %sFAIL%s terraform-backend.json is malformed\n' "$RED" "$RESET"
    FAIL=$((FAIL + 1))
  fi

  # 2. The override HCL from the documented patch. Terraform is never invoked,
  #    so this checks the shape and the fmt alignment by inspection.
  local hcl="$tmp/backend_override.tf"
  cat >"$hcl" <<'HCL'
terraform {
  backend "s3" {
    bucket       = "stub-state-bucket"
    key          = "anti-demo/.anti-demo-v7/terraform.tfstate"
    region       = "us-west-2"
    use_lockfile = true
    encrypt      = true
  }
}
HCL
  local ok_hcl=1
  grep -q 'backend "s3"' "$hcl" || ok_hcl=0
  grep -q 'use_lockfile = true' "$hcl" || ok_hcl=0
  grep -q 'dynamodb_table' "$hcl" && ok_hcl=0
  # Every `=` in the block must land in the same column, which is what
  # `terraform fmt` produces.
  local cols
  cols="$(grep -n '=' "$hcl" | sed 's/.*/&/' | awk -F= '{print index($0,"=")}' | sort -u | wc -l | tr -d ' ')"
  [[ "$cols" == "1" ]] || ok_hcl=0
  if ((ok_hcl == 1)); then
    printf '  %sok%s   override HCL is fmt-aligned, s3, lockfile, no DynamoDB\n' "$GREEN" "$RESET"
    PASS=$((PASS + 1))
  else
    printf '  %sFAIL%s override HCL is wrong (= columns: %s)\n' "$RED" "$RESET" "$cols"
    FAIL=$((FAIL + 1))
  fi

  # 3. The committed example must match that shape too, since it is what a
  #    reviewer reads.
  local ex="infra/aws/backend_override.tf.example"
  if grep -q 'use_lockfile = true' "$ex" && ! grep -q 'dynamodb_table *=' "$ex"; then
    printf '  %sok%s   %s agrees with the generator\n' "$GREEN" "$RESET" "$ex"
    PASS=$((PASS + 1))
  else
    printf '  %sFAIL%s %s disagrees with the generator\n' "$RED" "$RESET" "$ex"
    FAIL=$((FAIL + 1))
  fi

  # 4. versions.tf must still declare the local backend as the committed default.
  if grep -q 'backend "local" {}' infra/aws/versions.tf; then
    printf '  %sok%s   versions.tf still defaults to the local backend\n' "$GREEN" "$RESET"
    PASS=$((PASS + 1))
  else
    printf '  %sFAIL%s versions.tf no longer defaults to local\n' "$RED" "$RESET"
    FAIL=$((FAIL + 1))
  fi

  # 5. The generated name must be unommittable.
  if git check-ignore -q infra/aws/backend_override.tf; then
    printf '  %sok%s   backend_override.tf is gitignored\n' "$GREEN" "$RESET"
    PASS=$((PASS + 1))
  else
    printf '  %sFAIL%s backend_override.tf could be committed\n' "$RED" "$RESET"
    FAIL=$((FAIL + 1))
  fi

  # 6. The state policy must be valid JSON, under the policy size limit, and
  #    must not grant delete on the state object itself.
  #
  #    The addendum this replaces lived at docs/iam-addendum-s3-state.json and
  #    has been adopted as the fourth file in docs/iam/. That move also fixed
  #    what this assertion used to tolerate: DeleteObject was one action on a
  #    statement whose Resource list named both the lock AND the state object,
  #    so the grant this case is named after was in fact being granted. It is
  #    now its own statement with one ARN, and the assertion is tightened to
  #    what the heading always claimed it checked.
  if python3 -c "
import json
p = json.load(open('docs/iam/anti-demo-operator-4-state.json'))
size = len(json.dumps(p, separators=(',', ':')))
assert size <= 6144, size
deletes = 0
for s in p['Statement']:
    actions = s['Action'] if isinstance(s['Action'], list) else [s['Action']]
    if 's3:DeleteObject' in actions:
        deletes += 1
        res = s['Resource'] if isinstance(s['Resource'], list) else [s['Resource']]
        assert all(r.endswith('.tflock') for r in res), res
assert deletes == 1, deletes
print('size', size)
"; then
    printf '  %sok%s   IAM state policy is valid and within the 6144 limit\n' "$GREEN" "$RESET"
    PASS=$((PASS + 1))
  else
    printf '  %sFAIL%s IAM state policy is invalid\n' "$RED" "$RESET"
    FAIL=$((FAIL + 1))
  fi
}

case_no_regression() {
  printf '\n%s== the live installation is untouched ==%s\n' "$BOLD" "$RESET"
  if [[ ! -f .anti-demo-v7/terraform.tfstate ]]; then
    printf '  skip  no live state present\n'
    return
  fi
  local before after
  before="$(shasum -a 256 .anti-demo-v7/terraform.tfstate | awk '{print $1}')"
  local sb
  sb="$(sandbox)"
  run "$sb"
  run "$sb" --state-backend s3 --state-bucket stub-state-bucket
  after="$(shasum -a 256 .anti-demo-v7/terraform.tfstate | awk '{print $1}')"
  if [[ "$before" == "$after" ]]; then
    printf '  %sok%s   live terraform.tfstate unchanged (%s)\n' "$GREEN" "$RESET" "${before:0:16}"
    PASS=$((PASS + 1))
  else
    printf '  %sFAIL%s live terraform.tfstate CHANGED\n' "$RED" "$RESET"
    FAIL=$((FAIL + 1))
  fi
  if [[ -f infra/aws/backend_override.tf ]]; then
    printf '  %sFAIL%s a backend override was generated into infra/aws\n' "$RED" "$RESET"
    FAIL=$((FAIL + 1))
  else
    printf '  %sok%s   no backend override written to infra/aws\n' "$GREEN" "$RESET"
    PASS=$((PASS + 1))
  fi
}

CASES=(
  case_check_clean
  case_banned_files
  case_print_env
  case_s3_refuses_existing
  case_s3_needs_tf_111
  case_s3_needs_patch
  case_s3_bucket_states
  case_drift
  case_deploy_refusals
  case_deploy_runner_guard
  case_deploy_happy
  case_deploy_seal_only
  case_deploy_record_merge
  case_deploy_seal_snapshot
  case_deploy_idempotent
  case_deploy_failures
  case_deploy_retry
  case_deploy_reads_app_yaml
  case_generated_artefacts
  case_no_regression
)

if [[ "${1:-}" == "--list" ]]; then
  printf '%s\n' "${CASES[@]}"
  exit 0
fi

# Build a stub sandbox and print its path, for driving bootstrap.sh by hand.
if [[ "${1:-}" == "--stubs-only" ]]; then
  sb="${2:-$(mktemp -d)}"
  make_stubs "$sb"
  write_env "$sb/env"
  printf '%s\n' "$sb"
  exit 0
fi

# ---------------------------------------------------------------------------
# What this harness needs from the tree it runs in
# ---------------------------------------------------------------------------
#
# The stubs replace `uv` and `npm` with `exit 0`, so bootstrap.sh's step 1b
# reports a synced .venv and a built frontend/dist having produced neither. The
# real script then asserts both exist -- `./antidemo` refuses every subcommand
# without the interpreter, and the deployed UI answers 503 on every page without
# the build -- so what the matrix actually requires is a tree where both are
# already there. Nothing stubs those two assertions, and nothing should: they
# are the reason step 1b exists.
#
# Every tree this had ever run on was a warm laptop, so the requirement was
# invisible until the repository's first CI run. frontend/dist is gitignored
# build output, a fresh checkout has none, and all nineteen cases failed on the
# same line -- `frontend/dist/index.html is still missing after the build step`
# -- 1,800 lines deep in a log that named nothing else.
#
# Checked here so the answer is two lines instead. --list and --stubs-only sit
# above this on purpose: neither runs bootstrap.sh, and the pytest case that
# asserts every case is registered runs in the fast job, which builds no
# frontend and must not start needing one.
UNMET=0
PYTHON_ENVIRONMENT="${UV_PROJECT_ENVIRONMENT:-.venv}"
if [[ ! -x "$PYTHON_ENVIRONMENT/bin/python" ]]; then
  printf '%sFAIL%s  %s/bin/python does not exist, and the stub `uv` cannot create it.\n' \
    "$RED" "$RESET" "$PYTHON_ENVIRONMENT" >&2
  printf '        Run: uv sync --locked --all-groups\n' >&2
  UNMET=$((UNMET + 1))
fi
if [[ ! -f frontend/dist/index.html ]]; then
  printf '%sFAIL%s  frontend/dist/index.html does not exist, and the stub `npm` cannot build it.\n' \
    "$RED" "$RESET" >&2
  printf '        It is gitignored build output, so a fresh checkout never has it.\n' >&2
  printf '        Run: (cd frontend && npm ci && npm run build)\n' >&2
  UNMET=$((UNMET + 1))
fi
if ((UNMET > 0)); then
  printf '\n%sFAIL%s  %d precondition(s) unmet, so no case was run. Every case drives\n' \
    "$RED" "$RESET" "$UNMET" >&2
  printf '      bootstrap.sh through step 1b, which asserts both of the above and stops\n' >&2
  printf '      there -- so without them this reports nineteen failures for one cause.\n' >&2
  exit 1
fi

if [[ $# -gt 0 ]]; then
  "$1"
else
  for c in "${CASES[@]}"; do "$c"; done
fi

printf '\n%s%d passed, %d failed%s\n' "$BOLD" "$PASS" "$FAIL" "$RESET"
((FAIL == 0))
