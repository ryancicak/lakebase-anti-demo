#!/usr/bin/env bash
#
# One entry point from five credentials to a stage-ready installation.
#
# Nothing before the confirmation prompt costs money and nothing before it
# touches AWS: every AWS call up to that point is a describe, a list or an
# explicit --dry-run. It is not, however, read-only, and the whole list is here
# because a stranger reading this line is deciding whether it is safe to run
# this script merely to *look* at something. Before the prompt it:
#
#   - writes an OAuth M2M profile into ~/.databrickscfg (step "Databricks
#     service principal profile"; mode 600, and it refuses to overwrite a
#     profile of the same name that differs unless given --force-profile);
#   - creates the generation directory and takes a lock file inside it, in
#     every mode including the default check;
#   - writes bootstrap.json into that directory in every mode except check;
#   - under --apply, may create the Databricks App, because Round 4 seals its
#     service principal client ID and the app has to exist to have one.
#
# The single billed step is `./antidemo setup`, which runs Terraform and
# provisions real infrastructure; it happens only under --apply, only after an
# itemised cost summary, and only after the operator types the confirmation.
#
# `--apply` against an installation that already reads `ready` is refused at
# step 1a, before any of the above -- see the comment there.
#
# Run `./bootstrap.sh --help` for the input contract.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

ENV_FILE=".env.bootstrap"
MODE="check"
FORCE_PROFILE=0
ASSUME_YES=0
DEPLOY_APP=0
DEPLOY_ONLY=0

# Publishing an STS session into the deployed app is refused, because the app
# cannot be handed a replacement while it runs and so simply dies when the
# session expires. This is the escape hatch for the operator who means it.
ALLOW_TEMPORARY_CREDENTIAL=0

# --apply against an installation that is already `ready` is not a resume: it
# runs reconcile_infrastructure, which resets both database lanes and clears the
# Round 3 anchors. That is the right behaviour for the command; it is the wrong
# thing to reach for when what you wanted was a redeploy, and --yes used to skip
# the only sentence that said so. This flag is the explicit opt-in, and it is
# required under --yes as well as interactively.
RESET_READY=0

# The local environment -- .venv and frontend/dist -- is provisioned by this
# script rather than by two manual commands in a document. Both steps are
# idempotent and cheap to repeat, so the skip exists for the operator who
# already ran them and wants the validation alone, not as an optimisation
# anybody needs to think about.
SKIP_INSTALL="${ANTI_DEMO_SKIP_INSTALL:-0}"

# Force a brand-new generation directory instead of adopting the newest
# existing one. See the manifest step for why this is not the default.
NEW_GENERATION=0

# Terraform state location. "local" is the historical behaviour and stays the
# default: state lives beside the manifest, one generation per directory.
# "s3" is opt-in for NEW installations only -- switching an existing one needs
# `terraform init -migrate-state`, which this script deliberately never runs.
STATE_BACKEND=""
STATE_BUCKET=""
STATE_KEY=""

# S3-native state locking (use_lockfile) landed in Terraform 1.10 and went GA in
# 1.11. Requiring 1.11 for the opt-in path buys locking with no DynamoDB table,
# no second billed resource, and no extra IAM surface. versions.tf keeps its
# `>= 1.9.0` floor so local-backend installs are untouched by this choice.
TF_S3_BACKEND_MIN="1.11.0"

# Region-scoped list prices, copied from server/cost_model.py so the summary
# below cannot drift from the model the app itself bills against.
RATE_RDS_T4G_MEDIUM_HOUR="0.065"
RATE_EC2_M6I_LARGE_HOUR="0.096"
RATE_PUBLIC_IPV4_HOUR="0.005"
RATE_RDS_GP3_GB_MONTH="0.115"
RATE_EBS_GP3_GB_MONTH="0.08"
RATE_SECRET_MONTH="0.40"
RATE_AURORA_ACU_HOUR="0.12"
RATE_LAKEBASE_DBU="0.26"
RATE_LAKEBASE_DBU_PER_CU_HOUR="0.213"

# Terraform declares these counts for a fresh v7 installation. See
# infra/aws/locals.tf (v7_round_keys, v7_rds_round_keys), aurora.tf, rds.tf,
# round5_runner.tf and round5_secrets.tf.
COUNT_AURORA_CLUSTERS=4
COUNT_RDS_INSTANCES=3
COUNT_RUNNERS=1
COUNT_TF_SECRETS=2
COUNT_MANAGED_MASTER_SECRETS=7
COUNT_LAKEBASE_PROJECTS=7

RED=''
BOLD=''
DIM=''
RESET=''
if [[ -t 1 ]]; then
  RED=$'\033[31m'
  BOLD=$'\033[1m'
  DIM=$'\033[2m'
  RESET=$'\033[0m'
fi

say() { printf '%s\n' "$*"; }
STEP_N=0
STEP_TOTAL=8
banner() { printf '\n%s==> %s%s\n' "$BOLD" "$*" "$RESET"; }
step() {
  STEP_N=$((STEP_N + 1))
  printf '\n%s==> %d/%d  %s%s\n' "$BOLD" "$STEP_N" "$STEP_TOTAL" "$*" "$RESET"
}
ok() { printf '  ok    %s\n' "$*"; }
info() { printf '  info  %s\n' "$*"; }
warn() { printf '  warn  %s\n' "$*"; }
die() {
  printf '\n%sFAIL%s  %s\n' "$RED" "$RESET" "$*" >&2
  exit 1
}

# ---------------------------------------------------------------------------
# Collected failures
# ---------------------------------------------------------------------------
#
# `die` on the first bad input is what turns one wrong value into four runs:
# fix the region, discover the warehouse is ambiguous, fix that, discover the
# catalog does not exist. Every check between here and the preflight gate
# records instead of exiting, so one run names everything that is wrong.
#
# `die` is kept for the cases where continuing would be dishonest rather than
# merely noisy: an unwritable Databricks profile means every Databricks check
# after it would report a failure it cannot attribute, and the state-backend
# guards are refusals to act rather than findings about the environment.
PREFLIGHT_FAILURES=()
fail() {
  printf '  %sFAIL%s  %s\n' "$RED" "$RESET" "$*" >&2
  PREFLIGHT_FAILURES+=("$*")
}

# Everything a failure would have blocked is skipped rather than guessed at, so
# the summary never mixes real findings with consequences of earlier ones.
skipped() { printf '  %s----%s  %s\n' "$DIM" "$RESET" "not checked: $*"; }

preflight_gate() {
  ((${#PREFLIGHT_FAILURES[@]} > 0)) || return 0
  printf '\n%sFAIL%s  %d preflight %s. Nothing was provisioned and nothing was written\n' \
    "$RED" "$RESET" "${#PREFLIGHT_FAILURES[@]}" \
    "$( ((${#PREFLIGHT_FAILURES[@]} == 1)) && echo check failed || echo checks failed)" >&2
  printf '      into the generation directory. In full:\n\n' >&2
  local n=0
  for entry in "${PREFLIGHT_FAILURES[@]}"; do
    n=$((n + 1))
    printf '  %s%d.%s %s\n\n' "$BOLD" "$n" "$RESET" "$entry" >&2
  done
  exit 1
}

usage() {
  cat <<'USAGE'
bootstrap.sh — five credentials to a stage-ready Lakebase Anti-Demo.

  ./bootstrap.sh                 validate everything, change nothing (default)
  ./bootstrap.sh --apply         validate, show the bill, confirm, then provision
  ./bootstrap.sh --print-env     emit the derived environment for a manual ./antidemo run
  ./bootstrap.sh --deploy-only   publish the seal and (re)deploy the Databricks App

Options
  --env-file PATH     read inputs from PATH (default .env.bootstrap)
  --skip-install      do not sync .venv or build frontend/dist; assume both exist
                      (ANTI_DEMO_SKIP_INSTALL=1 does the same)
  --new-generation    provision into a NEW .anti-demo-v<N+1> instead of adopting
                      the newest existing installation
  --force-profile     overwrite an existing ~/.databrickscfg profile that differs
  --yes               skip the interactive spend confirmation (still prints it).
                      Does NOT authorise resetting a ready install: see
                      --reset-ready
  --reset-ready       allow --apply against an installation whose status is
                      already 'ready'. That run resets both database lanes and
                      clears the Round 3 anchors. To redeploy the app instead,
                      use --deploy-only
  --deploy-app        with --apply, also deploy the Databricks App afterwards
  --deploy-only       deploy the app against an already-provisioned install;
                      does not touch AWS and never runs Terraform
  --i-know-this-expires
                      publish a temporary STS credential into the app anyway.
                      Refused by default: Databricks Apps injects secrets once
                      at container start, so the app dies when the session
                      expires and cannot be given a replacement while running
  --state-backend M   Terraform state location: local (default) or s3.
                      s3 is accepted for NEW installations only.
  --state-bucket NAME S3 bucket for state when --state-backend=s3
  -h, --help          this text

The five inputs, from --env-file or interactive prompts:

  DATABRICKS_HOST            https://<workspace-host>
  DATABRICKS_CLIENT_ID       service principal OAuth (M2M) client ID
  DATABRICKS_CLIENT_SECRET   service principal OAuth (M2M) secret
  AWS_ACCESS_KEY_ID
  AWS_SECRET_ACCESS_KEY

Everything else is derived: the AWS region (from `aws configure get region` when
it is not supplied), the AWS account ID, the Round 5 app principal ARN, the
manifest path, the Databricks CLI profile, the SQL warehouse, the Databricks App
and its service principal client ID. It also syncs .venv from uv.lock and builds
frontend/dist, because ./antidemo refuses to run without the first and the UI answers
503 without the second.

  AWS_DEFAULT_REGION         only when the AWS CLI has no configured region
                             (AWS_REGION is accepted as a synonym)

Optional overrides, for when derivation cannot be unambiguous:

  ANTI_DEMO_OWNER            ownership tag on every AWS resource
  DATABRICKS_WAREHOUSE_ID    required if the workspace has more than one warehouse
  DATABRICKS_APP_NAME        Databricks App to create or adopt (default lakebase-anti-demo)
  DATABRICKS_APP_CLIENT_ID   skip app derivation and use this client ID
  ROUND5_APP_PRINCIPAL_ARN   required if the AWS identity is an assumed role
  DATABRICKS_CDF_CATALOG     Round 6 destination catalog (default main)
  ROUND4_CATALOG             Round 4 source catalog; an existing installation's
                             sealed catalog wins and this must agree with it
  ANTI_DEMO_MANIFEST         override the derived manifest path
  AWS_SESSION_TOKEN          only for temporary credentials
  ANTI_DEMO_TTL_HOURS        first-provision expiry, default 72
  ANTI_DEMO_TF_BACKEND       local (default) or s3
  ANTI_DEMO_TF_STATE_BUCKET  S3 bucket for Terraform state
  ANTI_DEMO_TF_STATE_KEY     object key, default anti-demo/<installation>/terraform.tfstate
  ANTI_DEMO_SECRET_SCOPE     Databricks secret scope for the app's seal

Start from docs/bootstrap.env.example. Never commit .env.bootstrap — .gitignore
already excludes .env.* . Full walkthrough in docs/BOOTSTRAP.md.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --apply) MODE="apply" ;;
    --print-env) MODE="print-env" ;;
    --env-file)
      shift
      [[ $# -gt 0 ]] || die "--env-file needs a path"
      ENV_FILE="$1"
      ;;
    --env-file=*) ENV_FILE="${1#*=}" ;;
    --skip-install) SKIP_INSTALL=1 ;;
    --new-generation) NEW_GENERATION=1 ;;
    --force-profile) FORCE_PROFILE=1 ;;
    --yes) ASSUME_YES=1 ;;
    --reset-ready) RESET_READY=1 ;;
    --deploy-app) DEPLOY_APP=1 ;;
    --i-know-this-expires) ALLOW_TEMPORARY_CREDENTIAL=1 ;;
    --deploy-only)
      MODE="deploy"
      DEPLOY_APP=1
      DEPLOY_ONLY=1
      ;;
    --state-backend)
      shift
      [[ $# -gt 0 ]] || die "--state-backend needs local or s3"
      STATE_BACKEND="$1"
      ;;
    --state-backend=*) STATE_BACKEND="${1#*=}" ;;
    --state-bucket)
      shift
      [[ $# -gt 0 ]] || die "--state-bucket needs a bucket name"
      STATE_BUCKET="$1"
      ;;
    --state-bucket=*) STATE_BUCKET="${1#*=}" ;;
    -h | --help)
      usage
      exit 0
      ;;
    *) die "unknown argument: $1 (try --help)" ;;
  esac
  shift
done

# --print-env exists to be consumed by `eval`, so its only stdout must be the
# export block. Everything narrated moves to stderr, and the block is written to
# fd 3, which is stdout as it was before this swap.
if [[ "$MODE" == "print-env" ]]; then
  exec 3>&1 1>&2
fi

# --deploy-only touches only Databricks, so it skips the AWS preflight, the
# state-backend decision and the bill entirely.
RUN_AWS_SECTIONS=1
[[ "$MODE" == "deploy" ]] && RUN_AWS_SECTIONS=0

# --print-env is consumed by eval and must not provision a Python environment or
# a frontend build as a side effect of being asked what it derived.
RUN_INSTALL=1
[[ "$SKIP_INSTALL" == "1" ]] && RUN_INSTALL=0
[[ "$MODE" == "print-env" ]] && RUN_INSTALL=0

# Unconditional: tools, Databricks profile, Databricks resources, manifest,
# app state. AWS adds identity, the permissions preflight, the state backend and
# the bill.
STEP_TOTAL=5
((RUN_INSTALL == 1)) && STEP_TOTAL=$((STEP_TOTAL + 1))
((RUN_AWS_SECTIONS == 1)) && STEP_TOTAL=$((STEP_TOTAL + 4))
[[ "$MODE" == "apply" ]] && STEP_TOTAL=$((STEP_TOTAL + 1))
((DEPLOY_APP == 1)) && STEP_TOTAL=$((STEP_TOTAL + 1))

# ---------------------------------------------------------------------------
# 1. Tools
# ---------------------------------------------------------------------------
#
# Deliberately the first thing that runs, and deliberately before the Inputs
# section below. This gate used to sit after the five credential prompts, which
# meant an operator on a laptop missing `terraform` and holding no
# .env.bootstrap was asked for DATABRICKS_HOST, refused for not having it, and
# never told about the missing binary at all -- so the whole point of naming
# every absent tool in one run only ever reached people who already had
# credentials in place.
#
# Nothing here reads an input. It needs only the step counter above (so the
# "1/N" it prints is the final N), the message helpers, and the filesystem.
# Keep it that way: the region below prompts, and a prompt must never be able
# to run before this refusal.
#
# It also has to precede the AWS region derivation further down, which shells
# out to `aws configure get region` behind a `|| true` -- a missing `aws` would
# be swallowed there and surface later as a mystery.

step "Prerequisite tools"

# The first seven are exactly what server/lifecycle.py:doctor checks for.
#
# Every absence is collected and reported together. Dying on the first one turns
# a laptop that is short three tools into three runs of this script, each ending
# on a different single name -- and each of the intervening installs is a
# download and a shell restart. `die` rather than `fail` is still right, because
# a run without `jq` or `python3` would report failures in every later step that
# are really this one; what was wrong was reporting one name when nine were
# checked.
MISSING_TOOLS=()
for tool in uv node npm databricks aws terraform psql python3 jq; do
  if command -v "$tool" >/dev/null 2>&1; then
    ok "$tool"
  else
    printf '  %sFAIL%s  %s is not on PATH\n' "$RED" "$RESET" "$tool" >&2
    MISSING_TOOLS+=("$tool")
  fi
done
if ((${#MISSING_TOOLS[@]} > 0)); then
  MISSING_LIST="$(printf ', %s' "${MISSING_TOOLS[@]}")"
  MISSING_LIST="${MISSING_LIST#, }"
  if ((${#MISSING_TOOLS[@]} == 1)); then
    die "$MISSING_LIST is not on PATH. Install it before bootstrapping; 'antidemo doctor'
       requires the same set."
  fi
  die "${#MISSING_TOOLS[@]} required tools are not on PATH: $MISSING_LIST.
       Install all of them before bootstrapping; 'antidemo doctor' requires the same set.
       Nothing after this step was checked, because every one of those checks runs
       one of these."
fi

TF_VERSION="$(terraform version -json 2>/dev/null | jq -r '.terraform_version' 2>/dev/null || echo unknown)"
[[ -n "$TF_VERSION" ]] || TF_VERSION=unknown
[[ "$TF_VERSION" == unknown ]] && warn "could not read the Terraform version; infra/aws requires >= 1.9.0"
[[ "$TF_VERSION" != unknown ]] && ok "terraform $TF_VERSION (infra/aws/versions.tf requires >= 1.9.0)"

# Compare dotted versions without assuming sort -V is available everywhere.
version_at_least() {
  printf '%s\n%s\n' "$2" "$1" | awk -F. '
    NR == 1 { for (i = 1; i <= 3; i++) need[i] = ($i == "" ? 0 : $i + 0); next }
    { for (i = 1; i <= 3; i++) have[i] = ($i == "" ? 0 : $i + 0) }
    END {
      for (i = 1; i <= 3; i++) {
        if (have[i] > need[i]) exit 0
        if (have[i] < need[i]) exit 1
      }
      exit 0
    }'
}

# Two files this tree must not contain. Both are absent by design, both break
# something silently when they come back, and both are the kind of file a tool
# regenerates without being asked -- so their absence is asserted here rather
# than assumed. Both are pure filesystem checks, which is why they belong beside
# the tool gate and ahead of every input.
#
# requirements.txt: Databricks Apps picks its installer from which files are
# present, and requirements.txt wins unconditionally, pinning the app to pip on
# Python 3.11. This source does not parse there (a PEP 695 `type` alias in
# server/connection_spike_journal.py), so the container dies on a SyntaxError
# before the first request while the platform still reports SUCCEEDED. The
# dependency set lives in pyproject.toml and uv.lock, which is what the runtime
# reads when this file is absent.
if [[ -f requirements.txt ]]; then
  die "requirements.txt is back in the repository root, and its mere presence puts the
       deployed app on pip and Python 3.11 -- where this source does not parse. Nothing
       installs from it: 'uv sync' reads pyproject.toml and uv.lock, and so does the
       Apps runtime once this file is gone. Delete it.
       If a dependency needs adding, add it to pyproject.toml and run 'uv lock'."
fi

# .python-version: uv reads it and will discard a project environment built on
# any other interpreter. A stray one pinning 3.12 already deleted this tree's
# provisioned 3.14 environment and began rebuilding it mid-incident, which from
# the outside is indistinguishable from a hang. The deployed tree needs no pin
# either -- requires-python >= 3.12 in pyproject.toml is what uv honours there.
if [[ -f .python-version ]]; then
  die "A .python-version in the repository root pins the interpreter uv builds .venv on,
       and '$(cat .python-version 2>/dev/null | tr -d '\n')' is what it now asks for. If that
       is not what .venv was built on, the next 'antidemo serve' spends its first minutes
       silently rebuilding the environment instead of serving. Nothing needs this file:
       pyproject.toml's requires-python governs both local and deployed resolution.
       Delete it."
fi

# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------
#
# Below the tool gate on purpose: see the note above it. Nothing between here
# and the end of this section may be depended on by that gate.

if [[ -f "$ENV_FILE" ]]; then
  # Sourced rather than parsed so an operator can use shell quoting, but read
  # with `set -a` off so only explicit exports leak; every value is re-read
  # from the shell below.
  set +u
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set -u
  say "Read inputs from $ENV_FILE"
else
  say "No $ENV_FILE (start from docs/bootstrap.env.example); missing inputs will be prompted for."
fi

# ---------------------------------------------------------------------------
# 1a. Is this --apply a reset in disguise?
# ---------------------------------------------------------------------------
#
# This refusal used to fire only at the provision step, where the manifest has
# been read properly -- but by then the run had already prompted for five
# credentials, written a profile into ~/.databrickscfg, created the generation
# directory, and under --apply possibly created the Databricks App. None of that
# costs money and none of it touches AWS, so the refusal was never unsafe; it
# just was not the "nothing has happened yet" it claimed to be.
#
# So the question is asked here as well, from two flags and one `jq` read of one
# file. The condition and the message are each written once, below; this call
# site and the one at the provision step differ only in which manifest path they
# hand to them, and the provision step keeps its copy because it is the only
# place that knows the answer for certain rather than by prediction.

# Highest generation by NUMBER, not by glob order. `.anti-demo-v10/` sorts
# before `.anti-demo-v7/` lexically, so a last-wins loop would adopt v9 while
# v10 was the live installation -- and then reconcile a generation nobody was
# using while the real one kept billing. Sets LATEST_GENERATION and HIGHEST_N;
# reads the filesystem and nothing else.
scan_generations() {
  local candidate candidate_n
  LATEST_GENERATION=""
  HIGHEST_N=-1
  for candidate in .anti-demo-v*/; do
    [[ -d "$candidate" ]] || continue
    candidate_n="${candidate%/}"
    candidate_n="${candidate_n#.anti-demo-v}"
    [[ "$candidate_n" =~ ^[0-9]+$ ]] || continue
    if ((10#$candidate_n > HIGHEST_N)); then
      HIGHEST_N=$((10#$candidate_n))
      LATEST_GENERATION="${candidate%/}"
    fi
  done
}

# The manifest this run would adopt, or empty when it would create a new one.
# --new-generation always provisions into .anti-demo-v<N+1>, which cannot be
# `ready` because it does not exist yet.
manifest_this_run_would_adopt() {
  if [[ -n "${ANTI_DEMO_MANIFEST:-}" ]]; then
    printf '%s' "$ANTI_DEMO_MANIFEST"
    return
  fi
  ((NEW_GENERATION == 0)) || return 0
  scan_generations
  [[ -n "$LATEST_GENERATION" ]] || return 0
  printf '%s' "$ROOT/$LATEST_GENERATION/manifest.json"
}

# True when this run is an --apply, without --reset-ready, against a manifest
# that already reads `ready`. Any other status still falls through: that is the
# interrupted provision --apply genuinely does resume.
apply_would_reset_a_ready_install() { # <manifest path, possibly empty>
  [[ "$MODE" == "apply" ]] || return 1
  ((RESET_READY == 0)) || return 1
  [[ -n "$1" && -f "$1" ]] || return 1
  [[ "$(jq -r '.status // empty' "$1" 2>/dev/null || true)" == "ready" ]]
}

# --apply is not a resume of a finished install.
#
# `antidemo setup` decides from the manifest: it provisions, resumes an
# interrupted provision, or -- on a manifest that already says `ready` --
# reconciles and RESETS. The reset drops and reseeds both database lanes and
# clears the Round 3 anchors, so a bout mid-round dies and any Round 3 recovery
# point taken since the last reset is gone. The warning below the confirmation
# prompt said so, but only under `--yes == 0`, and only after the operator had
# already committed to this path; `--apply --yes` against a ready install said
# nothing at all.
#
# Refused rather than confirmed, because the overwhelmingly common reason to be
# here on a ready install is wanting the app redeployed -- which is
# --deploy-only, touches no database and runs no Terraform.
refuse_ready_install() { # <run id, possibly empty>
  die "installation ${1:-} is already 'ready', and --apply is not a resume of it.

       './antidemo setup' would reconcile it: 'terraform plan' and 'terraform apply'
       for any pending diff in infra/aws, and then a RESET of both database lanes
       and a clear of the Round 3 anchors. A bout running now would die, and Round 3
       recovery points taken since the last reset would be gone.

       What you probably want:

         ./bootstrap.sh --deploy-only   republish the seal and redeploy the app.
                                        Touches no database, runs no Terraform.
                                        This is the resume path for a ready install.

       If a reset is genuinely what you mean -- an infra diff to apply, or lanes to
       return to a known state -- say so explicitly:

         ./bootstrap.sh --apply --reset-ready

       Nothing was changed."
}

EARLY_MANIFEST="$(manifest_this_run_would_adopt)"
if apply_would_reset_a_ready_install "$EARLY_MANIFEST"; then
  refuse_ready_install "$(jq -r '.run_id // empty' "$EARLY_MANIFEST" 2>/dev/null || true)"
fi
unset EARLY_MANIFEST

# How long a prompt waits before it becomes a refusal. A `read` from /dev/tty
# does not fail when nobody is there to answer -- it blocks, for ever -- so an
# automated or supervised run presents a missing input as a HANG with no output
# and no clue which value is wanted. That is what `--apply` with no
# .env.bootstrap did, and it cost an install window. Bounded so the same run
# ends in an actionable error instead. Generous enough that an operator reading
# the prompt and reaching for a password manager is not cut off.
PROMPT_TIMEOUT_SECONDS="${ANTI_DEMO_PROMPT_TIMEOUT_SECONDS:-120}"

# Both prompts refuse through this, so the message cannot differ between them.
# Every input is also readable from the environment -- the call sites pass
# `${VAR:-}` as the current value -- which is what makes the remedy below a real
# remedy rather than "run it again by hand".
no_input_die() {
  local name="$1" reason="$2"
  die "$name is not set, and $reason.

       Give it one of these three ways, then re-run the same command:

         1. Put it in $ENV_FILE beside this script (start from
            docs/bootstrap.env.example). That is the supported route.
         2. Export it: $name=... ./bootstrap.sh ...
         3. Run this in a terminal you can type into.

       Nothing was changed. This refusal is deliberate: waiting on a prompt
       nobody is going to answer looks exactly like a wedged install."
}

prompt_value() {
  local name="$1" prompt="$2" current="${3:-}"
  if [[ -n "$current" ]]; then
    printf '%s' "$current"
    return
  fi
  [[ -t 0 && -r /dev/tty ]] ||
    no_input_die "$name" "this run has no terminal to ask on"
  local reply=""
  read -r -t "$PROMPT_TIMEOUT_SECONDS" -p "  $prompt: " reply </dev/tty ||
    no_input_die "$name" "nothing was typed within ${PROMPT_TIMEOUT_SECONDS}s"
  [[ -n "$reply" ]] || no_input_die "$name" "the answer at the prompt was empty"
  printf '%s' "$reply"
}

prompt_secret() {
  local name="$1" prompt="$2" current="${3:-}"
  if [[ -n "$current" ]]; then
    printf '%s' "$current"
    return
  fi
  [[ -t 0 && -r /dev/tty ]] ||
    no_input_die "$name" "this run has no terminal to ask on"
  local reply=""
  read -r -s -t "$PROMPT_TIMEOUT_SECONDS" -p "  $prompt: " reply </dev/tty || {
    printf '\n' >&2
    no_input_die "$name" "nothing was typed within ${PROMPT_TIMEOUT_SECONDS}s"
  }
  printf '\n' >&2
  [[ -n "$reply" ]] || no_input_die "$name" "the answer at the prompt was empty"
  printf '%s' "$reply"
}

DATABRICKS_HOST="$(prompt_value DATABRICKS_HOST 'Databricks workspace URL' "${DATABRICKS_HOST:-}")"
DATABRICKS_CLIENT_ID="$(prompt_value DATABRICKS_CLIENT_ID 'Databricks service principal client ID' "${DATABRICKS_CLIENT_ID:-}")"
DATABRICKS_CLIENT_SECRET="$(prompt_secret DATABRICKS_CLIENT_SECRET 'Databricks service principal secret' "${DATABRICKS_CLIENT_SECRET:-}")"

REQUIRED=(
  "DATABRICKS_HOST:$DATABRICKS_HOST"
  "DATABRICKS_CLIENT_ID:$DATABRICKS_CLIENT_ID"
  "DATABRICKS_CLIENT_SECRET:$DATABRICKS_CLIENT_SECRET"
)

# Provisioning needs AWS credentials because Terraform runs. A deploy-only run
# does not: it talks to Databricks alone, and the account and region it would
# report are already sealed in the manifest. Demanding keys there made the whole
# path unusable for an installation provisioned through an SSO profile, which
# has no static keys to give.
#
# The keys stay *optional* rather than absent in deploy mode, because the app's
# aws-access-key-id and aws-secret-access-key secrets do need refreshing
# whenever they expire, and that is a deploy, not a provision. Supplying them
# rotates them; omitting them leaves whatever the scope already holds.
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}"
AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-${AWS_REGION:-}}"
if ((RUN_AWS_SECTIONS == 1)); then
  AWS_ACCESS_KEY_ID="$(prompt_value AWS_ACCESS_KEY_ID 'AWS_ACCESS_KEY_ID' "$AWS_ACCESS_KEY_ID")"
  AWS_SECRET_ACCESS_KEY="$(prompt_secret AWS_SECRET_ACCESS_KEY 'AWS_SECRET_ACCESS_KEY' "$AWS_SECRET_ACCESS_KEY")"
  # A region the AWS CLI already has configured is not an input, it is a fact
  # about this laptop, and asking for it is what made a five-input install a
  # six-input one. `aws configure get region` reads ~/.aws/config only; it does
  # not authenticate, does not consult the environment keys above, and prints
  # nothing when there is no answer -- in which case it is still prompted for.
  if [[ -z "$AWS_DEFAULT_REGION" ]]; then
    CONFIGURED_REGION="$(env -u AWS_PROFILE -u AWS_DEFAULT_PROFILE \
      aws configure get region 2>/dev/null | tr -d '[:space:]' || true)"
    # Shape-checked before it is adopted: anything else is a stub, an error
    # message or a stray line, and a bad region here becomes a confusing
    # endpoint-resolution failure several calls later.
    if [[ "$CONFIGURED_REGION" =~ ^[a-z][a-z-]+-[0-9]$ ]]; then
      AWS_DEFAULT_REGION="$CONFIGURED_REGION"
      info "region $AWS_DEFAULT_REGION taken from the AWS CLI configuration (aws configure get region)"
    fi
  fi
  AWS_DEFAULT_REGION="$(prompt_value AWS_DEFAULT_REGION 'AWS_DEFAULT_REGION' "$AWS_DEFAULT_REGION")"
  REQUIRED+=(
    "AWS_ACCESS_KEY_ID:$AWS_ACCESS_KEY_ID"
    "AWS_SECRET_ACCESS_KEY:$AWS_SECRET_ACCESS_KEY"
    "AWS_DEFAULT_REGION:$AWS_DEFAULT_REGION"
  )
elif [[ -n "$AWS_ACCESS_KEY_ID" || -n "$AWS_SECRET_ACCESS_KEY" ]]; then
  # Half a key pair is worse than none: it would publish one new secret beside
  # one stale one and the app would sign requests with a mismatched pair.
  [[ -n "$AWS_ACCESS_KEY_ID" && -n "$AWS_SECRET_ACCESS_KEY" ]] ||
    die "deploy-only was given only one half of the AWS key pair. Supply both
         AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to rotate the app's credentials,
         or neither to leave the ones already in the scope untouched."
  info "AWS keys were supplied, so this run will rotate the app's AWS credential secrets"
else
  info "no AWS keys supplied; this run republishes the seal only and leaves the
        app's AWS credential secrets as they are"
fi

for pair in "${REQUIRED[@]}"; do
  [[ -n "${pair#*:}" ]] || die "${pair%%:*} is empty"
done

# Flags win over the env file, which wins over the built-in default.
STATE_BACKEND="${STATE_BACKEND:-${ANTI_DEMO_TF_BACKEND:-local}}"
STATE_BUCKET="${STATE_BUCKET:-${ANTI_DEMO_TF_STATE_BUCKET:-}}"
STATE_KEY="${STATE_KEY:-${ANTI_DEMO_TF_STATE_KEY:-}}"
case "$STATE_BACKEND" in
  local | s3) ;;
  *) die "--state-backend must be local or s3, not '$STATE_BACKEND'" ;;
esac

# server/cli.py reads AWS_REGION, not AWS_DEFAULT_REGION. Terraform and boto3
# read either. Bind both to the one value so no layer disagrees.
AWS_REGION="$AWS_DEFAULT_REGION"
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_REGION AWS_DEFAULT_REGION

# A named profile and ambient keys together are refused outright by
# server/aws_auth.py:select_setup_auth. Keys win here, so the profile
# variables must not survive into any child process.
if [[ -n "${AWS_PROFILE:-}" || -n "${AWS_DEFAULT_PROFILE:-}" ]]; then
  info "unsetting inherited AWS_PROFILE/AWS_DEFAULT_PROFILE; this run is keys-only"
fi
unset AWS_PROFILE AWS_DEFAULT_PROFILE

if [[ -n "${AWS_SESSION_TOKEN:-}" ]]; then
  export AWS_SESSION_TOKEN
  info "using the supplied AWS_SESSION_TOKEN (temporary credentials)"
else
  # A stale token inherited from an SSO session signs requests with the wrong
  # identity and fails in a way that looks like a bad access key.
  unset AWS_SESSION_TOKEN
fi

if [[ "$DATABRICKS_HOST" != https://* ]]; then
  DATABRICKS_HOST="https://${DATABRICKS_HOST#http://}"
fi
DATABRICKS_HOST="${DATABRICKS_HOST%/}"
WORKSPACE_FQDN="${DATABRICKS_HOST#https://}"

# The SDK and CLI both resolve a profile from ~/.databrickscfg. Nothing else in
# this repository accepts DATABRICKS_CLIENT_ID directly, so the profile is the
# integration point. Name it after the workspace so two workspaces cannot
# silently share one entry.
DATABRICKS_PROFILE="${DATABRICKS_PROFILE:-anti-demo-${WORKSPACE_FQDN%%.*}}"

APP_NAME="${DATABRICKS_APP_NAME:-lakebase-anti-demo}"
CDF_CATALOG="${DATABRICKS_CDF_CATALOG:-main}"
TTL_HOURS="${ANTI_DEMO_TTL_HOURS:-72}"

# UV_PROJECT_ENVIRONMENT is read here rather than beside the tool gate above,
# because $ENV_FILE is allowed to set it and the gate deliberately runs before
# that file is sourced.
PYTHON_ENVIRONMENT="${UV_PROJECT_ENVIRONMENT:-.venv}"
case "$PYTHON_ENVIRONMENT" in
  /*) ;;
  *) PYTHON_ENVIRONMENT="$ROOT/$PYTHON_ENVIRONMENT" ;;
esac

# ---------------------------------------------------------------------------
# 1b. The local environment: .venv and frontend/dist
# ---------------------------------------------------------------------------
#
# These were two manual commands in docs/BOOTSTRAP.md, and an installer whose
# documented first step is "run two other commands first" is not an installer.
# `./antidemo` refuses outright without .venv, and the UI answers 503 without
# frontend/dist, so a run that provisioned the fleet without them would bill for
# something nobody could start.
#
# Both are idempotent. `uv sync --locked` is a no-op against an environment that
# already matches, and the frontend build is skipped when dist is newer than
# every input to it.
#
# --locked and --no-sync, never a plain `uv run` or a bare `uv sync`.
# Installing is not resolving: uv downloads from the per-file URLs already in
# uv.lock and consults an index only when it has to re-resolve. That difference
# is why this works on a laptop where pypi.org is blackholed and
# files.pythonhosted.org is not -- and a re-resolve here is what wrote an
# unreachable internal proxy hostname into all 775 lockfile URLs and cost 23
# consecutive App deploys. tests/test_deploy_hygiene.py is the standing guard.

if ((RUN_INSTALL == 1)); then
step "Local environment"

if [[ ! -f pyproject.toml ]]; then
  warn "no pyproject.toml here, so there is no environment to sync"
elif uv sync --locked; then
  ok "uv sync --locked: .venv matches uv.lock exactly"
else
  # --locked fails rather than re-resolving when the lockfile disagrees with
  # pyproject.toml, which is the whole point: the alternative is a silent
  # re-resolve against an index this machine may not be able to reach, and a
  # lockfile rewritten with hostnames a build container cannot resolve.
  fail "'uv sync --locked' could not build the Python environment from uv.lock.
      If it reported the lockfile is out of date, run 'uv lock' on a machine with an
      index it can reach and commit the result -- do not let this step re-resolve.
      If it reported a network failure, note that installing from uv.lock needs only
      files.pythonhosted.org; a connection refused to pypi.org means something asked
      it to resolve. './antidemo' refuses to run at all without $PYTHON_ENVIRONMENT."
fi

if [[ ! -f frontend/package.json ]]; then
  warn "no frontend/package.json here, so there is nothing to build"
else
  # `npm ci` is the expensive half and is only needed when the lockfile has
  # moved under node_modules. `npm run build` is re-run whenever any source is
  # newer than the built index.html, which is what makes a second bootstrap run
  # cost a second rather than a minute.
  NEED_NPM_CI=0
  [[ -d frontend/node_modules ]] || NEED_NPM_CI=1
  [[ -f frontend/package-lock.json && frontend/package-lock.json -nt frontend/node_modules ]] &&
    NEED_NPM_CI=1
  if ((NEED_NPM_CI == 1)); then
    info "installing frontend dependencies (npm ci); this takes a minute or two"
    if (cd frontend && npm ci); then
      ok "npm ci"
    else
      fail "'npm ci' failed in frontend/, so frontend/dist cannot be built and the
      deployed UI would answer 503. package-lock.json is committed, so this is not a
      resolution problem: it is the network, the Node version, or the registry this
      machine is pointed at. Check 'npm config get registry' -- a laptop configured
      for an internal proxy in ~/.npmrc fails here the moment that proxy is
      unreachable, and package-lock.json still names registry.npmjs.org."
    fi
  else
    ok "frontend/node_modules is current with package-lock.json"
  fi

  NEED_BUILD=0
  if [[ ! -f frontend/dist/index.html ]]; then
    NEED_BUILD=1
  else
    while IFS= read -r newer; do
      [[ -n "$newer" ]] && NEED_BUILD=1 && break
    done < <(find frontend/src frontend/index.html frontend/package.json frontend/vite.config.ts \
      -newer frontend/dist/index.html 2>/dev/null)
  fi
  if ((NEED_BUILD == 1)); then
    info "building the frontend (npm run build)"
    if (cd frontend && npm run build); then
      ok "frontend/dist built"
    else
      fail "'npm run build' failed, so frontend/dist is missing or stale. The app serves
      503 for every UI request without it (frontend/dist is deliberately not committed)."
    fi
  else
    ok "frontend/dist is newer than every source that feeds it"
  fi
fi

# Asserted rather than assumed, because a build tool that exits 0 without
# producing its output is a thing that happens, and the next thing to notice
# would be a 503 on stage.
if [[ -x "$PYTHON_ENVIRONMENT/bin/python" ]]; then
  ok "$PYTHON_ENVIRONMENT is runnable"
  # Which interpreter uv built on is reported, not enforced. Nothing pins it:
  # requires-python in pyproject.toml is a floor (>= 3.12) and there is
  # deliberately no .python-version, so uv takes whatever `python3` resolves to
  # on this machine. A cold clone therefore builds .venv on one minor version
  # while the tree that has been serving all along runs another -- which is fine
  # until it is not, and then it is a day of bisecting a difference nobody could
  # see. Printing it makes the divergence a line in the install log. Choosing a
  # pin is a decision for the owner, and the wrong pin would delete a working
  # environment and rebuild it mid-install.
  VENV_PYTHON_VERSION="$("$PYTHON_ENVIRONMENT/bin/python" -c \
    'import platform; print(platform.python_implementation(), platform.python_version())' 2>/dev/null || true)"
  VENV_PYTHON_BASE="$("$PYTHON_ENVIRONMENT/bin/python" -c \
    'import sys; print(sys.base_prefix)' 2>/dev/null || true)"
  if [[ -n "$VENV_PYTHON_VERSION" ]]; then
    info "built on $VENV_PYTHON_VERSION (from ${VENV_PYTHON_BASE:-an unknown prefix}).
        Nothing pins this: pyproject.toml asks only for >= 3.12 and there is no
        .python-version, so another machine can build the same commit on another minor
        version. If a serve behaves differently from this install, compare this line first."
  else
    warn "could not ask $PYTHON_ENVIRONMENT/bin/python which version it is"
  fi
else
  fail "there is still no interpreter at $PYTHON_ENVIRONMENT/bin/python, so './antidemo'
      will refuse every subcommand including the provision this script is about to run."
fi
[[ -f frontend/dist/index.html ]] || fail "frontend/dist/index.html is still missing after the build step"
else
# Deliberately not a numbered step: STEP_TOTAL does not count a step that is
# not run, and a counter that skips a number is a counter nobody trusts.
banner "Local environment (skipped)"
info "not syncing .venv or building frontend/dist; assuming both already exist"
[[ -x "$PYTHON_ENVIRONMENT/bin/python" ]] ||
  warn "there is no interpreter at $PYTHON_ENVIRONMENT/bin/python and this run was told not to
        create one; './antidemo' will refuse to start. Drop --skip-install, or run 'uv sync --locked'."
[[ -d frontend/dist ]] ||
  warn "frontend/dist is missing and this run was told not to build it; the UI answers 503."
fi

# ---------------------------------------------------------------------------
# 2. AWS identity
# ---------------------------------------------------------------------------

# Gated, because STEP_TOTAL already excludes this step in deploy mode and a
# deploy-only run may legitimately have no AWS credentials at all. Calling STS
# here unconditionally is what made --deploy-only unusable on an
# SSO-provisioned installation: it died before reaching Databricks, on
# credentials it was never going to use.
AWS_IDENTITY_OK=0
if ((RUN_AWS_SECTIONS == 1)); then
step "AWS identity"

AWS_ACCOUNT_ID=""
CALLER_ARN=""
if CALLER_JSON="$(aws sts get-caller-identity --output json 2>&1)"; then
  AWS_ACCOUNT_ID="$(printf '%s' "$CALLER_JSON" | jq -r '.Account // empty' 2>/dev/null || true)"
  CALLER_ARN="$(printf '%s' "$CALLER_JSON" | jq -r '.Arn // empty' 2>/dev/null || true)"
  if [[ "$AWS_ACCOUNT_ID" =~ ^[0-9]{12}$ ]]; then
    AWS_IDENTITY_OK=1
    ok "account $AWS_ACCOUNT_ID, region $AWS_REGION"
    ok "principal $CALLER_ARN"
  else
    fail "sts:GetCallerIdentity returned account '$AWS_ACCOUNT_ID', which is not 12 digits."
  fi
else
  fail "AWS credentials rejected by sts:GetCallerIdentity. The error was: ${CALLER_JSON}
      Check AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, and note that this run is
      deliberately keys-only: any AWS_PROFILE you had set has been unset."
fi
else
  # Placeholders so the recorded-state writer and the manifest cross-checks have
  # defined values under `set -u`. The manifest fills them in at step 6, where
  # it is read; until then nothing in this mode consults them.
  AWS_ACCOUNT_ID=""
  CALLER_ARN=""
fi

# ROUND5_APP_PRINCIPAL_ARN is the stable IAM principal that the deployed app
# uses before sts:AssumeRole. server/lifecycle.py:_terraform_variables refuses
# anything that is not an exact iam role or user ARN in this account, and it
# does so before Terraform starts, so deriving it here is the difference
# between a clear message and a confusing one ten minutes in.
#
# --deploy-only never reaches Terraform, and the value it would need is already
# sealed in the manifest, so an ambiguous identity must not block a redeploy.
if ((RUN_AWS_SECTIONS == 0)); then
  ROUND5_APP_PRINCIPAL_ARN="${ROUND5_APP_PRINCIPAL_ARN:-}"
  OPERATOR_IP=""
  info "deploy-only: skipping the Round 5 principal derivation and the ingress probe"
elif ((AWS_IDENTITY_OK == 0)); then
  ROUND5_APP_PRINCIPAL_ARN="${ROUND5_APP_PRINCIPAL_ARN:-}"
  OPERATOR_IP=""
  skipped "the Round 5 principal and the ingress probe need a working AWS identity"
elif [[ -z "${ROUND5_APP_PRINCIPAL_ARN:-}" ]]; then
  case "$CALLER_ARN" in
    arn:aws:iam::*:user/* | arn:aws:iam::*:role/*)
      # The common case, and the one that keeps this off the input list: a
      # long-lived IAM user or role has a stable ARN and STS returns it
      # verbatim. Round 5's control-role trust policy is sealed from this value
      # at first provision, so a wrong answer here does not fail now -- it fails
      # at click time, on stage. That is why nothing is inferred, reformatted or
      # defaulted: this branch adopts the exact string STS returned, and every
      # other shape is refused below rather than guessed at.
      ROUND5_APP_PRINCIPAL_ARN="$CALLER_ARN"
      ok "derived ROUND5_APP_PRINCIPAL_ARN from the caller identity: $CALLER_ARN"
      info "Round 5's control role trusts exactly this principal, and the trust policy is
        sealed at first provision. If the app will run as something else, set
        ROUND5_APP_PRINCIPAL_ARN now -- after the seal it cannot be changed without a
        cleanup and re-provision."
      ;;
    arn:aws:sts::*:assumed-role/*)
      ROLE_NAME="$(printf '%s' "$CALLER_ARN" | awk -F/ '{print $2}')"
      ROLE_ARN="$(aws iam get-role --role-name "$ROLE_NAME" --query 'Role.Arn' --output text 2>/dev/null || true)"
      if [[ "$ROLE_ARN" == arn:aws:iam::* ]]; then
        ROUND5_APP_PRINCIPAL_ARN="$ROLE_ARN"
        ok "resolved the assumed role to its IAM ARN via iam:GetRole: $ROLE_ARN"
      else
        ROUND5_APP_PRINCIPAL_ARN=""
        fail "ROUND5_APP_PRINCIPAL_ARN cannot be derived and must be supplied.
      This identity is a temporary assumed role and iam:GetRole could not resolve it to a
      stable IAM ARN:
          caller     $CALLER_ARN
          role name  $ROLE_NAME
      The STS 'assumed-role' form is not an IAM ARN and it drops the role's path, so it
      cannot be converted by string surgery. Round 5's control role is created with a trust
      policy naming this principal exactly, and that policy is sealed into the manifest at
      first provision -- a wrong value provisions cleanly and then fails Round 5 in front of
      an audience. So this is required rather than guessed.
      Find it with:
          aws iam list-roles --query \"Roles[?RoleName=='$ROLE_NAME'].Arn\" --output text
      and set ROUND5_APP_PRINCIPAL_ARN in $ENV_FILE. For an SSO role it is the
      arn:aws:iam::${AWS_ACCOUNT_ID}:role/aws-reserved/sso.amazonaws.com/<region>/<name>
      form, never the assumed-role form."
      fi
      ;;
    *)
      ROUND5_APP_PRINCIPAL_ARN=""
      fail "ROUND5_APP_PRINCIPAL_ARN cannot be derived from '$CALLER_ARN', which is neither an
      IAM user, an IAM role, nor an assumed role. Set it explicitly in $ENV_FILE to the IAM
      role or user ARN the deployed app authenticates as."
      ;;
  esac
else
  ok "using the supplied ROUND5_APP_PRINCIPAL_ARN"
fi

if ((RUN_AWS_SECTIONS == 1 && AWS_IDENTITY_OK == 1)); then
  case "$ROUND5_APP_PRINCIPAL_ARN" in
    "" ) ;; # already reported above; do not report the consequence twice
    "arn:aws:iam::${AWS_ACCOUNT_ID}:role/"* | "arn:aws:iam::${AWS_ACCOUNT_ID}:user/"*) ;;
    *) fail "ROUND5_APP_PRINCIPAL_ARN must be an IAM role or user ARN in account $AWS_ACCOUNT_ID,
      and it is '$ROUND5_APP_PRINCIPAL_ARN'. server/lifecycle.py:_terraform_variables refuses
      the same value before Terraform starts." ;;
  esac

  OPERATOR_IP="$(curl -fsS --max-time 10 https://checkip.amazonaws.com | tr -d '[:space:]' || true)"
  if [[ "$OPERATOR_IP" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    ok "operator ingress will be locked to ${OPERATOR_IP}/32"
  else
    fail "Could not detect a public IPv4 address from checkip.amazonaws.com.
      server/lifecycle.py:detect_operator_cidr needs one and rejects IPv6, and the database
      security groups allow exactly one /32. An IPv6-only network cannot provision this."
  fi
fi

# ---------------------------------------------------------------------------
# 3. AWS permissions
# ---------------------------------------------------------------------------

if ((RUN_AWS_SECTIONS == 1)); then
step "AWS permissions preflight"

if ((AWS_IDENTITY_OK == 0)); then
skipped "every AWS authorisation probe, because the credentials above did not authenticate"
DEFAULT_VPC=""
else

probe() {
  local label="$1"
  shift
  local output
  if output="$("$@" 2>&1)"; then
    ok "$label"
    return 0
  fi
  if printf '%s' "$output" | grep -q 'DryRunOperation'; then
    ok "$label (dry run authorised)"
    return 0
  fi
  # An `aws` usage error is a bug in this script, not a missing permission.
  # Reporting it as "denied" would send an operator to their cloud team over a
  # typo, so it stops the run with the command that was wrong.
  if printf '%s' "$output" | grep -q '^usage: aws'; then
    die "bootstrap.sh built an invalid AWS CLI call for '$label': $*"
  fi
  printf '  %sdenied%s %s\n' "$RED" "$RESET" "$label"
  PERMISSION_FAILURES+=("$label")
  return 0
}

PERMISSION_FAILURES=()

probe "rds:DescribeDBInstances" aws rds describe-db-instances --max-records 20
probe "rds:DescribeDBClusters" aws rds describe-db-clusters --max-records 20
probe "rds:DescribeDBSubnetGroups" aws rds describe-db-subnet-groups --max-records 20
probe "rds:DescribeDBProxies" aws rds describe-db-proxies
probe "rds:DescribeOrderableDBInstanceOptions (aurora-postgresql serverless)" \
  aws rds describe-orderable-db-instance-options --engine aurora-postgresql \
  --db-instance-class db.serverless --max-records 20
probe "ec2:DescribeVpcs" aws ec2 describe-vpcs --filters Name=isDefault,Values=true
probe "ec2:DescribeSubnets" aws ec2 describe-subnets --max-results 5
probe "ec2:DescribeSecurityGroups" aws ec2 describe-security-groups --max-results 5
probe "ec2:DescribeRouteTables" aws ec2 describe-route-tables --max-results 5
probe "secretsmanager:ListSecrets" aws secretsmanager list-secrets --max-results 1
probe "ssm:GetParameter (Amazon Linux 2023 AMI pointer)" \
  aws ssm get-parameter --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64
probe "ssm:DescribeInstanceInformation" aws ssm describe-instance-information --max-results 5
probe "cloudwatch:ListMetrics" aws cloudwatch list-metrics --namespace AWS/RDS --max-items 1
probe "iam:ListRoles" aws iam list-roles --max-items 1

# The mutating half. EC2 supports a real authorisation-only dry run, so these
# prove the permission without creating anything. RDS and IAM have no
# equivalent, which is why the summary below says what it cannot prove.
DEFAULT_VPC="$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true \
  --query 'Vpcs[0].VpcId' --output text 2>/dev/null || echo None)"
if [[ "$DEFAULT_VPC" == vpc-* ]]; then
  ok "default VPC $DEFAULT_VPC will be used (infra/aws/locals.tf: use_default_network)"
  probe "ec2:CreateSecurityGroup" aws ec2 create-security-group --dry-run \
    --group-name "anti-demo-preflight-$$" --description "authorisation probe" --vpc-id "$DEFAULT_VPC"
  AMI_ID="$(aws ssm get-parameter --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
    --query 'Parameter.Value' --output text 2>/dev/null || true)"
  if [[ "$AMI_ID" == ami-* ]]; then
    probe "ec2:RunInstances (m6i.large)" aws ec2 run-instances --dry-run \
      --image-id "$AMI_ID" --instance-type m6i.large --count 1
  fi
else
  fail "No default VPC in $AWS_REGION, and this is not a permissions problem — the demo has
      no way to bring its own network. infra/aws/locals.tf:network_input_mode requires
      vpc_id, subnet_ids and runner_subnet_id together, and
      server/lifecycle.py:_terraform_variables passes none of the three, so default-VPC
      discovery is the only supported network mode. Either pick a region that still has
      its default VPC, or land a Terraform change that plumbs those variables through."
fi

if [[ "$STATE_BACKEND" == "s3" ]]; then
  probe "s3:ListAllMyBuckets" aws s3api list-buckets --max-items 1
fi

if ((${#PERMISSION_FAILURES[@]} > 0)); then
  # Recorded as one finding rather than one per probe: they share a single
  # remedy, and a list of fourteen denials reads as fourteen problems.
  DENIED_LIST=""
  for denied in "${PERMISSION_FAILURES[@]}"; do
    DENIED_LIST="${DENIED_LIST}
        - ${denied}"
  done
  # Named rather than counted. docs/iam/ holds five documents -- four operator
  # policies and the app runtime's own -- so "the three policies in docs/iam/"
  # asked a reader who is already blocked to guess which three. File 4 covers
  # only the opt-in S3 state backend, so it is listed when, and only when, this
  # run asked for it.
  ATTACH_FILES=(anti-demo-operator-1-network.json anti-demo-operator-2-databases.json
    anti-demo-operator-3-identity.json)
  if [[ "$STATE_BACKEND" == "s3" ]]; then
    ATTACH_FILES+=(anti-demo-operator-4-state.json)
  fi
  ATTACH_LIST=""
  for policy in "${ATTACH_FILES[@]}"; do
    ATTACH_LIST="${ATTACH_LIST}
        - docs/iam/${policy}"
  done
  PREFLIGHT_FAILURES+=("These ${#PERMISSION_FAILURES[@]} AWS authorisation probes failed:${DENIED_LIST}
      Attach these ${#ATTACH_FILES[@]} policies to this principal and re-run:${ATTACH_LIST}
      They carry <AWS_ACCOUNT_ID> and <AWS_REGION> placeholders that have to be
      replaced first; docs/iam/README.md has that loop and says what each one
      grants.")
  printf '  %sFAIL%s  %d AWS authorisation probes were denied (listed above)\n' "$RED" "$RESET" \
    "${#PERMISSION_FAILURES[@]}" >&2
else
  ok "all read probes and every available dry run passed"
fi
warn "rds:Create*, iam:CreateRole and secretsmanager:CreateSecret have no dry run.
        They are covered by docs/iam/ but are first exercised by the real apply."
fi # AWS_IDENTITY_OK
fi # RUN_AWS_SECTIONS

# ---------------------------------------------------------------------------
# 3b. Terraform state backend
# ---------------------------------------------------------------------------
#
# The default is unchanged and unconditional: state stays beside the manifest,
# one generation per directory, exactly as server/lifecycle.py:_terraform_init
# has always arranged it. Nothing below runs unless the operator asks for s3.

STATE_BACKEND_FILE=""

if ((RUN_AWS_SECTIONS == 1)); then
step "Terraform state backend"

if [[ "$STATE_BACKEND" == "local" ]]; then
  ok "local backend: state stays in the generation directory beside the manifest"
  info "opt in with --state-backend s3 --state-bucket NAME on a NEW installation"
else
  # Guard one: an existing installation must not be migrated by this script.
  # Moving live state between backends is `terraform init -migrate-state`, a
  # real mutation on the one file that stands between the operator and
  # unfindable billed resources. It is a deliberate, supervised, one-way
  # operation and it is not bootstrap's to perform.
  if [[ -f "${ANTI_DEMO_MANIFEST:-/nonexistent}" ]]; then
    die "This generation already exists, so --state-backend s3 is refused.
         Switching backends on a live installation requires
         'terraform init -migrate-state', which this script never runs: it
         rewrites the only record of which resources you own. The S3 backend is
         opt-in for NEW installations. docs/DEPLOY.md, 'Migrating an existing
         installation', has the supervised manual procedure."
  fi

  # Guard two: the inputs. Pure local validation, so it comes before anything
  # environmental -- a typo should be reported as a typo, not hidden behind a
  # Terraform version or a missing patch.
  [[ -n "$STATE_BUCKET" ]] ||
    die "--state-backend s3 needs --state-bucket NAME (or ANTI_DEMO_TF_STATE_BUCKET)."
  [[ "$STATE_BUCKET" =~ ^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$ ]] ||
    die "'$STATE_BUCKET' is not a valid S3 bucket name (3-63 chars, lowercase letters,
         digits, dots and hyphens, starting and ending alphanumeric)."
  case "$STATE_BUCKET" in
    *..* | *.-* | *-.*) die "'$STATE_BUCKET' has an adjacent dot/hyphen sequence S3 rejects." ;;
  esac
  ok "bucket name '$STATE_BUCKET' is well formed"

  # Guard three: S3-native locking, and therefore this whole mode, needs 1.11.
  if [[ "$TF_VERSION" == unknown ]]; then
    die "--state-backend s3 needs Terraform >= $TF_S3_BACKEND_MIN for S3-native state
         locking, and the installed version could not be read."
  fi
  version_at_least "$TF_VERSION" "$TF_S3_BACKEND_MIN" ||
    die "--state-backend s3 needs Terraform >= $TF_S3_BACKEND_MIN (found $TF_VERSION).
         S3-native state locking (use_lockfile) went GA in 1.11; below that the only
         option is a DynamoDB lock table, which this design deliberately avoids.
         Upgrade Terraform, or leave the default local backend in place."
  ok "terraform $TF_VERSION supports S3-native locking (use_lockfile)"

  BUCKET_EXISTS=0
  if aws s3api head-bucket --bucket "$STATE_BUCKET" >/dev/null 2>&1; then
    BUCKET_EXISTS=1
    BUCKET_REGION="$(aws s3api get-bucket-location --bucket "$STATE_BUCKET" \
      --query 'LocationConstraint' --output text 2>/dev/null || echo unknown)"
    # us-east-1 is reported as the literal "None"/"null" by this API.
    [[ "$BUCKET_REGION" == "None" || "$BUCKET_REGION" == "null" ]] && BUCKET_REGION="us-east-1"
    ok "bucket $STATE_BUCKET exists in $BUCKET_REGION"
    if [[ "$BUCKET_REGION" != "$AWS_REGION" && "$BUCKET_REGION" != "unknown" ]]; then
      warn "the bucket is in $BUCKET_REGION but this install is $AWS_REGION.
        That works -- the backend takes its own region -- but a cross-region state
        bucket adds a failure mode you did not ask for."
    fi
    VERSIONING="$(aws s3api get-bucket-versioning --bucket "$STATE_BUCKET" \
      --query 'Status' --output text 2>/dev/null || echo unknown)"
    if [[ "$VERSIONING" == "Enabled" ]]; then
      ok "bucket versioning is enabled"
    else
      warn "bucket versioning is '$VERSIONING'. This file is the only record of which
        billed resources you own; without versioning a bad write is unrecoverable.
        Enable it: aws s3api put-bucket-versioning --bucket $STATE_BUCKET
        --versioning-configuration Status=Enabled"
    fi
  else
    info "bucket $STATE_BUCKET does not exist or is not readable by this principal"
    if [[ "$MODE" == "apply" ]]; then
      info "it will be created with versioning, SSE and public access blocked, after confirmation"
    else
      info "run with --apply to create it (versioning + SSE + public access blocked)"
    fi
  fi

  # Guard four, deliberately last. It is a hard blocker, but reporting the
  # bucket facts first means one run tells the operator everything that is
  # wrong instead of one thing per run.
  #
  # Without the patch, _terraform_init still passes `-backend-config=path=...`,
  # a local-backend argument that the S3 backend rejects outright. Failing here
  # beats failing at init inside `antidemo setup`.
  if grep -q 'terraform-backend.json' server/lifecycle.py 2>/dev/null; then
    ok "server/lifecycle.py carries the backend-aware _terraform_init"
  else
    die "server/lifecycle.py:_terraform_init still hardcodes '-backend-config=path=...',
         which is a local-backend argument that the S3 backend rejects. The one-function
         patch that makes this work is in docs/DEPLOY.md, 'Required patch to
         server/lifecycle.py'. Until it lands, --state-backend s3 cannot work, so this
         stops here rather than failing inside 'antidemo setup'."
  fi

  ok "s3 backend selected: bucket $STATE_BUCKET, S3-native locking, no DynamoDB table"
fi
fi # RUN_AWS_SECTIONS

# ---------------------------------------------------------------------------
# 4. Databricks profile
# ---------------------------------------------------------------------------

step "Databricks service principal profile"

CFG="${DATABRICKS_CONFIG_FILE:-$HOME/.databrickscfg}"

if [[ "$MODE" == "print-env" ]]; then
  info "print-env mode: not touching $CFG"
else
  DB_PROFILE_NAME="$DATABRICKS_PROFILE" \
    DB_PROFILE_HOST="$DATABRICKS_HOST" \
    DB_PROFILE_CLIENT_ID="$DATABRICKS_CLIENT_ID" \
    DB_PROFILE_CLIENT_SECRET="$DATABRICKS_CLIENT_SECRET" \
    DB_PROFILE_PATH="$CFG" \
    DB_PROFILE_FORCE="$FORCE_PROFILE" \
    python3 - <<'PY' || die "could not write the Databricks profile"
import configparser
import os
import pathlib
import sys

path = pathlib.Path(os.environ["DB_PROFILE_PATH"]).expanduser()
name = os.environ["DB_PROFILE_NAME"]
desired = {
    "host": os.environ["DB_PROFILE_HOST"],
    "client_id": os.environ["DB_PROFILE_CLIENT_ID"],
    "client_secret": os.environ["DB_PROFILE_CLIENT_SECRET"],
}
force = os.environ["DB_PROFILE_FORCE"] == "1"

# A literal [DEFAULT] section in ~/.databrickscfg is inherited by every other
# section under configparser's normal rules, so `items(name)` would report keys
# this script never wrote and the comparison below would find a difference on
# every re-run -- refusing to reuse the very profile it had just created.
# Renaming the default section means [DEFAULT] is read and written back as an
# ordinary section, inherited by nothing.
parser = configparser.ConfigParser(default_section="__anti_demo_no_defaults__")
parser.optionxform = str
if path.exists():
    parser.read(path)

if parser.has_section(name):
    existing = dict(parser.items(name))
    if existing == desired:
        print("  ok    profile already matches; left untouched")
        sys.exit(0)
    if not force:
        differing = sorted(
            key
            for key in set(existing) | set(desired)
            if existing.get(key) != desired.get(key)
        )
        print(
            f"  FAIL  [{name}] exists in {path} and differs in: {', '.join(differing)}.\n"
            "        Re-run with --force-profile to overwrite it, or set DATABRICKS_PROFILE\n"
            "        to a name you own. Values are never printed.",
            file=sys.stderr,
        )
        sys.exit(1)
    parser.remove_section(name)

parser.add_section(name)
for key, value in desired.items():
    parser.set(name, key, value)

path.parent.mkdir(parents=True, exist_ok=True)
tmp = path.with_suffix(path.suffix + ".anti-demo-tmp")
with tmp.open("w", encoding="utf-8") as handle:
    parser.write(handle)
tmp.chmod(0o600)
tmp.replace(path)
print(f"  ok    wrote OAuth M2M profile [{name}] to {path} (mode 600)")
PY
fi

DATABRICKS_ARGS=(-p "$DATABRICKS_PROFILE" -o json)

DATABRICKS_OK=0
DATABRICKS_PRINCIPAL=""
if ME_JSON="$(databricks current-user me "${DATABRICKS_ARGS[@]}" 2>&1)"; then
  DATABRICKS_PRINCIPAL="$(printf '%s' "$ME_JSON" | jq -r '.userName // empty' 2>/dev/null || true)"
  if [[ -n "$DATABRICKS_PRINCIPAL" ]]; then
    DATABRICKS_OK=1
    ok "authenticated as $DATABRICKS_PRINCIPAL"
  else
    fail "'databricks current-user me' returned no userName.
      server/lifecycle.py:_verify_databricks_identity requires one and refuses to provision
      without it."
  fi
else
  fail "The service principal could not authenticate to $DATABRICKS_HOST.
      'databricks current-user me -p $DATABRICKS_PROFILE' said:
        $(printf '%s' "$ME_JSON" | tail -2)
      Check DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET. A personal access token is
      not accepted here: the mechanism is a workspace service principal's OAuth (M2M) secret."
fi

# _verify_databricks_identity treats this as a capability check as well as an
# authentication check, so a workspace without Lakebase fails here rather than
# after the AWS side has been built.
if ((DATABRICKS_OK == 0)); then
  skipped "Lakebase, the warehouse and both catalogs, because nothing authenticated"
elif LAKEBASE_PROBE="$(databricks postgres list-projects "${DATABRICKS_ARGS[@]}" 2>&1)"; then
  ok "Lakebase API reachable"
else
  fail "The Lakebase (Databricks Postgres) API is not usable by this principal.
      'databricks postgres list-projects' said:
        $(printf '%s' "$LAKEBASE_PROBE" | tail -2)
      server/lifecycle.py:_verify_databricks_identity runs the same call and fails the
      provision on it. Either Lakebase is not enabled on this workspace, or this principal
      cannot see it."
fi

OWNER="${ANTI_DEMO_OWNER:-${DATABRICKS_PRINCIPAL:-unknown-owner}}"
if [[ "$OWNER" != *@* ]]; then
  warn "owner '$OWNER' has no @, so the UI will show no operator email
        (server/manifest.py:apply_manifest_environment keys that off an @).
        Set ANTI_DEMO_OWNER to a human email to fix the ringside identity."
fi
[[ ${#OWNER} -ge 3 ]] ||
  fail "ANTI_DEMO_OWNER must be at least 3 characters (infra/aws/variables.tf:owner)"

# ---------------------------------------------------------------------------
# 5. Databricks resources
# ---------------------------------------------------------------------------

step "Databricks resources"

DATABRICKS_WAREHOUSE_ID="${DATABRICKS_WAREHOUSE_ID:-}"
if ((DATABRICKS_OK == 0)); then
  :
elif [[ -n "$DATABRICKS_WAREHOUSE_ID" ]]; then
  ok "using the supplied DATABRICKS_WAREHOUSE_ID $DATABRICKS_WAREHOUSE_ID"
elif ! WAREHOUSES="$(databricks warehouses list "${DATABRICKS_ARGS[@]}" 2>&1)"; then
  # Distinguished from "no warehouses exist", which it used to be indistinguishable
  # from: the call was `|| echo '[]'`, so an API error and an empty workspace both
  # produced "no SQL warehouse is visible" and sent the operator to create a second one.
  fail "'databricks warehouses list' failed, so the SQL warehouse could not be resolved:
        $(printf '%s' "$WAREHOUSES" | tail -2)
      This is an API or permission failure rather than an empty workspace. Set
      DATABRICKS_WAREHOUSE_ID in $ENV_FILE to skip the lookup."
else
  WAREHOUSE_COUNT="$(printf '%s' "$WAREHOUSES" | jq 'length' 2>/dev/null || echo 0)"
  if [[ "$WAREHOUSE_COUNT" == "1" ]]; then
    DATABRICKS_WAREHOUSE_ID="$(printf '%s' "$WAREHOUSES" | jq -r '.[0].id')"
    WAREHOUSE_NAME="$(printf '%s' "$WAREHOUSES" | jq -r '.[0].name // "-"')"
    ok "derived the only SQL warehouse: $DATABRICKS_WAREHOUSE_ID ($WAREHOUSE_NAME)"
    info "Round 4 seals this choice, so a second warehouse appearing later changes nothing"
  elif [[ "$WAREHOUSE_COUNT" == "0" ]]; then
    fail "No SQL warehouse is visible to this principal, and Round 4 requires one
      (server/lifecycle.py:3713). Create one, grant the service principal CAN_USE on it,
      and either set DATABRICKS_WAREHOUSE_ID or re-run once it is the only one."
  else
    # Named as well as identified. An operator who is shown four bare hex IDs
    # cannot tell which is the serverless one they meant, and the ID is not what
    # anyone recognises a warehouse by.
    WAREHOUSE_TABLE="$(printf '%s' "$WAREHOUSES" |
      jq -r '.[] | "        \(.id)  \(.name)  \(.warehouse_type // "-")  \(.state // "-")"')"
    fail "This workspace has $WAREHOUSE_COUNT SQL warehouses and Round 4 seals exactly one into
      its contract, so guessing would bind the demo to an arbitrary warehouse. Pick one:
$WAREHOUSE_TABLE
      Then set it in $ENV_FILE:
          DATABRICKS_WAREHOUSE_ID=$(printf '%s' "$WAREHOUSES" | jq -r '.[0].id')"
  fi
fi

# Round 4's catalog cannot be checked yet: an existing installation's sealed
# value wins over anything supplied here, and the manifest is resolved later.
if ((DATABRICKS_OK == 0)); then
  :
elif databricks catalogs get "$CDF_CATALOG" "${DATABRICKS_ARGS[@]}" >/dev/null 2>&1; then
  ok "Round 6 catalog '$CDF_CATALOG' exists"
  # Existing is not the same as chosen. Round 4 already says this about its own
  # default and Round 6 did not, which is the worse half of the asymmetry: `main`
  # exists in virtually every Unity Catalog workspace, so the check above passes
  # for a stranger who never picked a catalog and Round 6 then creates a schema
  # in it. Writing into somebody's default catalog unasked is the one outcome
  # here that cannot be taken back by a cleanup.
  if [[ -z "${DATABRICKS_CDF_CATALOG:-}" ]]; then
    info "nobody chose catalog '$CDF_CATALOG' for Round 6 -- it is the compiled-in default,
        and it exists here as it does in most Unity Catalog workspaces. Round 6 will
        create a schema in it and a cleanup will delete that schema. Set
        DATABRICKS_CDF_CATALOG in $ENV_FILE to write somewhere you picked."
  fi
else
  fail "Round 6 needs Unity Catalog '$CDF_CATALOG' and this principal cannot see it.
      '$CDF_CATALOG' is the default; set DATABRICKS_CDF_CATALOG in $ENV_FILE to a catalog
      this principal can write to (server/round6_lifecycle.py:723). List them with:
          databricks catalogs list -p $DATABRICKS_PROFILE"
fi

# The app's own service principal client ID is sealed into the Round 4 contract,
# so the app has to exist before setup runs. Creating it is cheap, reversible,
# and the only way this stays a derived value rather than a sixth input.
if ((DATABRICKS_OK == 0)); then
  DATABRICKS_APP_CLIENT_ID="${DATABRICKS_APP_CLIENT_ID:-}"
  skipped "the Databricks App and its service principal"
elif [[ -z "${DATABRICKS_APP_CLIENT_ID:-}" ]]; then
  if APP_JSON="$(databricks apps get "$APP_NAME" "${DATABRICKS_ARGS[@]}" 2>/dev/null)"; then
    DATABRICKS_APP_CLIENT_ID="$(printf '%s' "$APP_JSON" | jq -r '.service_principal_client_id // empty')"
    [[ -n "$DATABRICKS_APP_CLIENT_ID" ]] ||
      die "App '$APP_NAME' exists but exposes no service_principal_client_id.
           Set DATABRICKS_APP_CLIENT_ID explicitly."
    ok "adopted the existing app '$APP_NAME'"
  elif [[ "$MODE" == "apply" ]]; then
    info "creating Databricks App '$APP_NAME' to obtain its service principal"
    APP_JSON="$(databricks apps create "$APP_NAME" "${DATABRICKS_ARGS[@]}" 2>&1)" ||
      die "Could not create the Databricks App '$APP_NAME': $(printf '%s' "$APP_JSON" | tail -2)"
    DATABRICKS_APP_CLIENT_ID="$(printf '%s' "$APP_JSON" | jq -r '.service_principal_client_id // empty')"
    [[ -n "$DATABRICKS_APP_CLIENT_ID" ]] ||
      die "The created app returned no service_principal_client_id"
    ok "created '$APP_NAME'"
  else
    warn "app '$APP_NAME' does not exist yet. --apply would create it and read its
        service principal; check mode cannot, so DATABRICKS_APP_CLIENT_ID stays unresolved."
    DATABRICKS_APP_CLIENT_ID=""
  fi
else
  ok "using the supplied DATABRICKS_APP_CLIENT_ID"
fi

# ---------------------------------------------------------------------------
# 6. Manifest generation
# ---------------------------------------------------------------------------

step "Manifest generation"

# server/manifest.py:manifest_path raises unless ANTI_DEMO_MANIFEST is set, and
# there is deliberately no default, because a default once pointed at a dead
# generation while a live one billed beside it. Choosing it here keeps that
# property — the choice is explicit and printed — without making a human know
# the variable exists.
if [[ -z "${ANTI_DEMO_MANIFEST:-}" ]]; then
  # Same scan the early --apply-against-ready probe used, so the generation this
  # step adopts and the generation that probe inspected cannot be different ones.
  scan_generations

  if ((NEW_GENERATION == 1)); then
    NEXT_N=$((HIGHEST_N + 1))
    ((NEXT_N > 0)) || NEXT_N=7
    ANTI_DEMO_MANIFEST="$ROOT/.anti-demo-v${NEXT_N}/manifest.json"
    LATEST_GENERATION=""
    info "--new-generation: provisioning a fresh .anti-demo-v${NEXT_N}, leaving any existing
        generation and everything it owns exactly as it is"
    info "a second generation is a second full fleet and a second full bill. The first one
        keeps billing until './antidemo cleanup --yes' is run against ITS manifest."
  elif [[ -n "$LATEST_GENERATION" && -f "$LATEST_GENERATION/manifest.json" ]]; then
    ANTI_DEMO_MANIFEST="$ROOT/$LATEST_GENERATION/manifest.json"
    info "adopting the existing generation $LATEST_GENERATION"
    # Said here, before any credential is spent on it, because "install it
    # again" and "adopt and reconcile the running one" are the same command.
    warn "this is NOT a fresh install. --apply against this generation runs 'terraform
        plan' and 'terraform apply' over the live installation, resets both database
        lanes and clears Round 3 anchors. Pass --new-generation to build a separate
        installation alongside it instead."
  elif [[ -n "$LATEST_GENERATION" ]]; then
    ANTI_DEMO_MANIFEST="$ROOT/$LATEST_GENERATION/manifest.json"
    info "empty generation directory $LATEST_GENERATION will receive the new manifest"
  else
    ANTI_DEMO_MANIFEST="$ROOT/.anti-demo-v7/manifest.json"
    # Nothing is created here -- only chosen. The directory appears at the first
    # write into it, which is after the preflight gate and, under --apply, after
    # the confirmation; a run that fails before then leaves no .anti-demo-v7
    # behind, and saying "creating" made that look like a half-made install.
    info "no generation directory exists; this run would use .anti-demo-v7"
  fi
fi
MANIFEST_DIR="$(dirname "$ANTI_DEMO_MANIFEST")"
ok "ANTI_DEMO_MANIFEST=$ANTI_DEMO_MANIFEST"

EXISTING_INSTALL=0
if [[ -f "$ANTI_DEMO_MANIFEST" ]]; then
  EXISTING_INSTALL=1
  MANIFEST_RUN="$(jq -r '.run_id' "$ANTI_DEMO_MANIFEST")"
  MANIFEST_STATUS="$(jq -r '.status' "$ANTI_DEMO_MANIFEST")"
  MANIFEST_ACCOUNT="$(jq -r '.aws.account_id' "$ANTI_DEMO_MANIFEST")"
  MANIFEST_REGION="$(jq -r '.aws.region' "$ANTI_DEMO_MANIFEST")"
  MANIFEST_USER="$(jq -r '.databricks.user' "$ANTI_DEMO_MANIFEST")"
  MANIFEST_EXPIRY="$(jq -r '.expires_at' "$ANTI_DEMO_MANIFEST")"
  ok "existing installation $MANIFEST_RUN, status $MANIFEST_STATUS, expires $MANIFEST_EXPIRY"
  if ((RUN_AWS_SECTIONS == 1 && AWS_IDENTITY_OK == 1)); then
    [[ "$MANIFEST_ACCOUNT" == "$AWS_ACCOUNT_ID" ]] ||
      fail "This manifest owns resources in AWS account $MANIFEST_ACCOUNT but the supplied
      keys resolve to $AWS_ACCOUNT_ID. Point ANTI_DEMO_MANIFEST at the right generation
      or supply the right keys; setup would refuse this anyway
      (server/lifecycle.py:_verify_aws_identity)."
    [[ "$MANIFEST_REGION" == "$AWS_REGION" ]] ||
      fail "This manifest is bound to region $MANIFEST_REGION, not $AWS_REGION. Set
      AWS_DEFAULT_REGION=$MANIFEST_REGION, or point ANTI_DEMO_MANIFEST at another generation."
  elif ((RUN_AWS_SECTIONS == 1)); then
    skipped "the manifest's AWS account and region, because the credentials did not authenticate"
  else
    # Nothing was authenticated against AWS in this mode, so the seal is the
    # only source for these. They are recorded, not verified, and the recorded
    # state says so.
    AWS_ACCOUNT_ID="$MANIFEST_ACCOUNT"
    AWS_REGION="$MANIFEST_REGION"
    AWS_DEFAULT_REGION="$MANIFEST_REGION"
    CALLER_ARN="(not resolved: deploy-only does not authenticate to AWS)"
    info "account and region taken from the seal: $AWS_ACCOUNT_ID / $AWS_REGION"
  fi
  if ((DATABRICKS_OK == 1)) && [[ "$MANIFEST_USER" != "$DATABRICKS_PRINCIPAL" ]]; then
    fail "This manifest was provisioned by Databricks principal $MANIFEST_USER, and the
      supplied service principal is $DATABRICKS_PRINCIPAL. reconcile_infrastructure
      (server/lifecycle.py:5148) refuses on exactly this mismatch. Use the original
      principal, or clean up and re-provision."
  fi
  # Advisory only, and now structurally so: server/lifecycle.py:_warn_if_expired
  # and _expiry_check both report a passed TTL without failing, and the method
  # that used to refuse on one no longer exists on DemoManifest to be called.
  info "a passed expires_at is advisory here and does not block setup"
else
  ok "no manifest yet; this will be a first provision (TTL ${TTL_HOURS}h)"
  if [[ "$MODE" == "deploy" ]]; then
    die "--deploy-only needs an installation to deploy. There is no manifest at
         $ANTI_DEMO_MANIFEST, so there is no seal to publish. Run './bootstrap.sh --apply'
         first."
  fi
fi

# Round 4's catalog resolves exactly as server/lifecycle.py:_round4_catalog
# resolves it: a sealed installation's value wins, ROUND4_CATALOG selects one for
# a first provision, and the module default is `main`, which a Unity
# Catalog-enabled workspace usually already has but is not guaranteed to. It is
# checked here rather than in 5/8 because the seal outranks anything supplied,
# and the manifest is not resolved until this step.
ROUND4_DEFAULT_CATALOG="$(sed -n 's/^ROUND4_DEFAULT_CATALOG = "\(.*\)"$/\1/p' server/lifecycle.py)"
[[ -n "$ROUND4_DEFAULT_CATALOG" ]] ||
  die "could not read ROUND4_DEFAULT_CATALOG from server/lifecycle.py"
SEALED_ROUND4_CATALOG=""
if ((EXISTING_INSTALL == 1)); then
  SEALED_ROUND4_CATALOG="$(jq -r '.round4.storage_catalog // empty' "$ANTI_DEMO_MANIFEST")"
fi
ROUND4_FROM_DEFAULT=0
if [[ -n "$SEALED_ROUND4_CATALOG" ]]; then
  if [[ -n "${ROUND4_CATALOG:-}" && "$ROUND4_CATALOG" != "$SEALED_ROUND4_CATALOG" ]]; then
    fail "This installation sealed Round 4 into Unity Catalog '$SEALED_ROUND4_CATALOG' and
      ROUND4_CATALOG says '$ROUND4_CATALOG'. server/lifecycle.py:_round4_catalog refuses
      exactly this mismatch. Unset it, or clean up and re-provision to move Round 4."
  fi
  ROUND4_CATALOG="$SEALED_ROUND4_CATALOG"
  info "Round 4 is sealed into catalog '$ROUND4_CATALOG'"
elif [[ -n "${ROUND4_CATALOG:-}" ]]; then
  info "Round 4 will provision into catalog '$ROUND4_CATALOG' (from ROUND4_CATALOG)"
else
  ROUND4_CATALOG="$ROUND4_DEFAULT_CATALOG"
  ROUND4_FROM_DEFAULT=1
  # Named as a default rather than presented as a decision. `main` is only a
  # likely catalog, not a certain one, and it is sealed on first provision -- so
  # an operator who did not choose it should find that out here and not from a
  # Round 4 failure later.
  info "Round 4 will provision into catalog '$ROUND4_CATALOG', which is the compiled-in
        default from server/lifecycle.py:ROUND4_DEFAULT_CATALOG -- nobody chose it for this
        workspace. It is sealed on first provision and cannot be changed afterwards without
        a cleanup. Set ROUND4_CATALOG in $ENV_FILE to override it."
fi
if ((DATABRICKS_OK == 0)); then
  :
elif databricks catalogs get "$ROUND4_CATALOG" "${DATABRICKS_ARGS[@]}" >/dev/null 2>&1; then
  ok "Round 4 catalog '$ROUND4_CATALOG' exists"
elif ((ROUND4_FROM_DEFAULT == 1)); then
  fail "Round 4 needs a Unity Catalog it can create a schema in, and the compiled-in default
      '$ROUND4_DEFAULT_CATALOG' is not visible to this principal. That default is only the
      catalog Databricks creates for a Unity Catalog-enabled workspace, so it is a likely
      name rather than a guaranteed one -- nobody chose it here, and ROUND4_CATALOG is
      required. Set it in $ENV_FILE to a catalog this principal can CREATE SCHEMA in:
          databricks catalogs list -p $DATABRICKS_PROFILE
      It is read on a first provision only and then sealed into the manifest."
else
  fail "Round 4 needs Unity Catalog '$ROUND4_CATALOG' and this principal cannot see it.
      Set ROUND4_CATALOG to a catalog it can create schemas in:
          databricks catalogs list -p $DATABRICKS_PROFILE"
fi

# The state key is per-installation so two generations cannot collide in one
# bucket. installation_id is stable across resets; run_id is not.
if [[ "$STATE_BACKEND" == "s3" && -z "$STATE_KEY" ]]; then
  STATE_KEY="anti-demo/$(basename "$MANIFEST_DIR")/terraform.tfstate"
fi

# ---------------------------------------------------------------------------
# 6a. The preflight gate
# ---------------------------------------------------------------------------
#
# Everything above is a read. This is the line the run does not cross with a
# known-bad input, and it is deliberately before the first write into the
# generation directory -- before the mkdir, before the lock file, before
# bootstrap.json -- so a failed preflight leaves the filesystem exactly as it
# found it, and the operator fixes everything that is wrong in one pass instead
# of one thing per run.
#
# It is also before the cost summary. Printing a bill for a provision that
# cannot start is how an operator learns to skim the bill.
preflight_gate
ok "preflight passed: every input resolved and every read probe answered"

# ---------------------------------------------------------------------------
# 6b. Databricks App: publish state and drift
# ---------------------------------------------------------------------------
#
# app.yaml binds ANTI_DEMO_MANIFEST_JSON to the *contents* of a secret, not to a
# file. So the deployed app keeps running whatever seal was last pushed, and
# every `antidemo setup`, `antidemo renew` or resume that rewrites the manifest silently
# leaves it a generation behind. Detecting that is the point of this section; it
# is read-only and runs in every mode.

step "Databricks App state"

# Which resource keys the app actually needs is a property of app.yaml, not of
# this script. Reading them means a change there -- for instance dropping the
# AWS session token binding -- needs no change here.
APP_RESOURCE_KEYS=()
if [[ -f app.yaml ]]; then
  while IFS= read -r key; do
    [[ -n "$key" ]] && APP_RESOURCE_KEYS+=("$key")
  done < <(sed -n 's/^[[:space:]]*valueFrom:[[:space:]]*\([A-Za-z0-9._-]*\).*$/\1/p' app.yaml)
fi
if ((${#APP_RESOURCE_KEYS[@]} == 0)); then
  warn "could not read any 'valueFrom:' resource key from app.yaml; app deploy is unavailable"
else
  ok "app.yaml requires ${#APP_RESOURCE_KEYS[@]} resources: ${APP_RESOURCE_KEYS[*]}"
fi

SECRET_SCOPE="${ANTI_DEMO_SECRET_SCOPE:-}"
if [[ -z "$SECRET_SCOPE" ]]; then
  # Keyed on the generation directory, not on run_id. The existing convention
  # embeds run_id, which changes on every reset and forces the app's resource
  # bindings to be rewritten alongside the secret. A stable scope means only the
  # secret value moves.
  SECRET_SCOPE="lakebase-anti-demo-$(basename "$MANIFEST_DIR" | tr -cd 'a-zA-Z0-9._-')"
fi
SECRET_MANIFEST_KEY="manifest-json"
DEPLOY_RECORD="$MANIFEST_DIR/app-deploy.json"

MANIFEST_SHA=""
SEAL_DEPLOYABLE=0
SEAL_BLOCKER=""
if ((EXISTING_INSTALL == 1)); then
  MANIFEST_SHA="$(shasum -a 256 "$ANTI_DEMO_MANIFEST" | awk '{print $1}')"
  MANIFEST_VERSION="$(jq -r '.manifest_version // 0' "$ANTI_DEMO_MANIFEST")"
  # app.py:_load_ready_manifest raises InvalidStateError unless status is
  # exactly "ready" and, for anything the UI can actually drive, the seal is v2
  # or newer. That raise happens inside the FastAPI lifespan, so a deploy made
  # against a non-ready manifest does not degrade -- the container never starts.
  if [[ "$MANIFEST_STATUS" != "ready" ]]; then
    # ${var^^} is bash 4, and macOS still ships 3.2 as /bin/bash.
    SEAL_BLOCKER="the manifest status is '$MANIFEST_STATUS', not 'ready'. app.py:108 raises
        InvalidStateError(\"Demo setup is currently $(printf '%s' "$MANIFEST_STATUS" | tr '[:lower:]' '[:upper:]'), not READY\")
        from inside the FastAPI lifespan, so the deployed container would fail to start."
  elif [[ "$MANIFEST_VERSION" -lt 2 ]]; then
    SEAL_BLOCKER="the seal is manifest v$MANIFEST_VERSION and app.py:112 requires v2 or newer."
  else
    SEAL_DEPLOYABLE=1
    ok "seal is deployable: status ready, manifest v$MANIFEST_VERSION"
    for r in 5 6; do
      if [[ "$(jq -r ".round$r // empty" "$ANTI_DEMO_MANIFEST")" == "" ]]; then
        info "round$r is unsealed, so that round will be unavailable in the deployed app"
      fi
    done
  fi
  [[ -n "$SEAL_BLOCKER" ]] && warn "not deployable: $SEAL_BLOCKER"
fi

# Drift. The remote secret value cannot be read back: `secrets get-secret` is an
# undocumented DBUtils-only API that refuses ordinary callers outside a
# notebook. So the comparison is between the local record of what was last
# pushed and the seal as it is now, cross-checked against the remote
# last_updated_timestamp to catch a push from somewhere else.
APP_LIVE=0
APP_EXISTS=0
SECRET_DRIFTED=0
DEPLOYED_SHA=""
if [[ -f "$DEPLOY_RECORD" ]]; then
  DEPLOYED_SHA="$(jq -r '.manifest_sha256 // empty' "$DEPLOY_RECORD" 2>/dev/null || true)"
fi

if [[ -n "${DATABRICKS_APP_CLIENT_ID:-}" ]] &&
  APP_STATE_JSON="$(databricks apps get "$APP_NAME" "${DATABRICKS_ARGS[@]}" 2>/dev/null)"; then
  APP_EXISTS=1
  APP_COMPUTE="$(printf '%s' "$APP_STATE_JSON" | jq -r '.compute_status.state // "UNKNOWN"')"
  APP_DEPLOY_STATE="$(printf '%s' "$APP_STATE_JSON" | jq -r '.active_deployment.status.state // "NONE"')"
  APP_URL="$(printf '%s' "$APP_STATE_JSON" | jq -r '.url // empty')"
  APP_STATUS_STATE="$(printf '%s' "$APP_STATE_JSON" | jq -r '.app_status.state // "UNKNOWN"')"
  ok "app '$APP_NAME' exists: compute $APP_COMPUTE, deployment $APP_DEPLOY_STATE, app $APP_STATUS_STATE"
  [[ "$APP_COMPUTE" == "ACTIVE" && "$APP_DEPLOY_STATE" == "SUCCEEDED" ]] && APP_LIVE=1
  [[ -n "$APP_URL" ]] && info "$APP_URL"

  # Whether the app is answering *now*, before this run changes anything.
  #
  # Reporting "deployment SUCCEEDED" and stopping there is how this script came
  # to describe an app that was serving 502 as healthy: a container that dies on
  # an import error still leaves the deployment SUCCEEDED and app_status RUNNING,
  # because the platform is reporting that it scheduled and started the process,
  # not that the process lived. The same trust in status is what the post-deploy
  # HTTP probe exists to correct; there is no reason to be more credulous before
  # a deploy than after one.
  #
  # It matters for a second reason. A failed deployment replaces the active one,
  # so a run that fails takes the app down. Whether that is a regression or a
  # continuation depends entirely on whether it was serving beforehand, and after
  # the fact nobody can tell. Recording it here is what makes that answerable.
  APP_SERVING="unknown"
  if [[ -n "$APP_URL" ]]; then
    PROBE_TOKEN="$(curl -fsS --max-time 20 \
      -u "$DATABRICKS_CLIENT_ID:$DATABRICKS_CLIENT_SECRET" \
      -d 'grant_type=client_credentials&scope=all-apis' \
      "$DATABRICKS_HOST/oidc/v1/token" 2>/dev/null | jq -r '.access_token // empty')"
    if [[ -n "$PROBE_TOKEN" ]]; then
      PROBE_CODE="$(curl -s -o /dev/null -w '%{http_code}' --max-time 20 \
        -H "Authorization: Bearer $PROBE_TOKEN" "$APP_URL/api/health" 2>/dev/null || true)"
      unset PROBE_TOKEN
      case "$PROBE_CODE" in
        200)
          APP_SERVING="yes"
          ok "and it is serving now: GET /api/health -> 200"
          ;;
        "" | 000)
          warn "could not reach $APP_URL to find out whether it is serving"
          ;;
        *)
          APP_SERVING="no"
          # Deliberately loud even in check mode. "The platform says the app is
          # fine and the app is not" is the single most misleading state this
          # tool can be run against.
          warn "the deployed app is NOT serving: GET /api/health -> $PROBE_CODE, despite
                deployment $APP_DEPLOY_STATE. It is already broken, so a deploy from here
                can only improve matters -- but check the logs for why:
                    databricks apps logs $APP_NAME --tail-lines 100 ${DATABRICKS_ARGS[*]}"
          ;;
      esac
    fi
  fi

  BOUND_SCOPE="$(printf '%s' "$APP_STATE_JSON" |
    jq -r --arg n "anti-demo-manifest-json" '.resources[]? | select(.name==$n) | .secret.scope // empty')"
  if [[ -z "$BOUND_SCOPE" ]]; then
    warn "the app has no 'anti-demo-manifest-json' resource, so it has no seal at all"
  else
    ok "the app reads its seal from scope '$BOUND_SCOPE'"
    if [[ "$BOUND_SCOPE" != "$SECRET_SCOPE" ]]; then
      info "this run would bind scope '$SECRET_SCOPE'; the app's resources will be updated"
      SECRET_SCOPE="$BOUND_SCOPE"
      info "adopting the scope the app is already bound to: $SECRET_SCOPE"
    fi
  fi
fi

if ((EXISTING_INSTALL == 1 && APP_EXISTS == 1)); then
  if [[ -z "$DEPLOYED_SHA" ]]; then
    warn "no local deploy record at $DEPLOY_RECORD, so the seal the app is serving cannot
        be compared. If this app was deployed by hand, treat it as drifted and republish
        with './bootstrap.sh --deploy-only'."
    SECRET_DRIFTED=1
  elif [[ "$DEPLOYED_SHA" != "$MANIFEST_SHA" ]]; then
    printf '  %sdrift %s the deployed seal is stale\n' "$RED" "$RESET"
    warn "the manifest has changed since the secret was last pushed.
        pushed  sha256:${DEPLOYED_SHA:0:16}
        current sha256:${MANIFEST_SHA:0:16}
        The deployed app is running an older seal. Republish and restart with
        './bootstrap.sh --deploy-only'."
    SECRET_DRIFTED=1
  else
    ok "the deployed seal matches the current manifest (sha256:${MANIFEST_SHA:0:16})"
    # A matching local record still misses a push made from another machine, so
    # confirm the remote timestamp is the one this record wrote.
    RECORDED_MS="$(jq -r '.secret_updated_ms // empty' "$DEPLOY_RECORD" 2>/dev/null || true)"
    LIVE_MS="$(databricks secrets list-secrets "$SECRET_SCOPE" "${DATABRICKS_ARGS[@]}" 2>/dev/null |
      jq -r --arg k "$SECRET_MANIFEST_KEY" \
        'if type=="array" then .[] else (.secrets // [])[] end | select(.key==$k) | .last_updated_timestamp // empty' 2>/dev/null || true)"
    if [[ -n "$RECORDED_MS" && -n "$LIVE_MS" && "$RECORDED_MS" != "$LIVE_MS" ]]; then
      warn "the secret was last written at $LIVE_MS but this record says $RECORDED_MS.
        Someone pushed a seal from elsewhere; its contents cannot be read back, so
        republish to be certain what the app is serving."
      SECRET_DRIFTED=1
    fi
  fi
elif ((EXISTING_INSTALL == 1)); then
  info "no deployed app to compare against"
fi

if ((DEPLOY_APP == 0 && SECRET_DRIFTED == 1)); then
  info "re-run with --deploy-only to republish the seal and restart the app"
fi

# ---------------------------------------------------------------------------
# 7. The bill
# ---------------------------------------------------------------------------

if ((RUN_AWS_SECTIONS == 1)); then
step "What this will cost"

fixed_daily() {
  awk -v rds="$RATE_RDS_T4G_MEDIUM_HOUR" -v n_rds="$COUNT_RDS_INSTANCES" \
    -v ec2="$RATE_EC2_M6I_LARGE_HOUR" -v n_ec2="$COUNT_RUNNERS" \
    -v ip="$RATE_PUBLIC_IPV4_HOUR" \
    'BEGIN { printf "%.2f", 24 * (rds * n_rds + ec2 * n_ec2 + ip * n_ec2) }'
}
metered_daily() {
  awk -v rdsgb="$RATE_RDS_GP3_GB_MONTH" -v n_rds="$COUNT_RDS_INSTANCES" \
    -v ebsgb="$RATE_EBS_GP3_GB_MONTH" -v sec="$RATE_SECRET_MONTH" \
    -v n_sec="$((COUNT_TF_SECRETS + COUNT_MANAGED_MASTER_SECRETS))" \
    'BEGIN { printf "%.2f", (rdsgb * 20 * n_rds + ebsgb * 20 + sec * n_sec) / 30.0 }'
}
aurora_floor_daily() {
  awk -v acu="$RATE_AURORA_ACU_HOUR" -v n="$COUNT_AURORA_CLUSTERS" \
    'BEGIN { printf "%.2f", 24 * acu * 0.5 * n }'
}

cat <<SUMMARY
  This provisions real, billed infrastructure. Nothing reaps it on a timer:
  the expires-at tag is an ownership label, and only 'antidemo cleanup --yes' stops
  the spend. Counts come from infra/aws (locals.tf v7_round_keys and
  v7_rds_round_keys); rates are the us-west-2 list prices in
  server/cost_model.py, which is authoritative for the app's own accounting.

  AWS, always on
    ${COUNT_RDS_INSTANCES} x RDS PostgreSQL db.t4g.medium, 20 GiB gp3   \$${RATE_RDS_T4G_MEDIUM_HOUR}/h each
    ${COUNT_RUNNERS} x EC2 m6i.large Round 5 runner, 20 GiB gp3      \$${RATE_EC2_M6I_LARGE_HOUR}/h
    ${COUNT_RUNNERS} x public IPv4 address on that runner            \$${RATE_PUBLIC_IPV4_HOUR}/h
                                                     -> ~\$$(fixed_daily)/day fixed
    storage and $((COUNT_TF_SECRETS + COUNT_MANAGED_MASTER_SECRETS)) Secrets Manager secrets       -> ~\$$(metered_daily)/day

  AWS, usage-shaped and not estimated here
    ${COUNT_AURORA_CLUSTERS} x Aurora Serverless v2 PostgreSQL 17, 0-2 ACU, 300 s auto-pause.
      \$${RATE_AURORA_ACU_HOUR}/ACU-hour. A cluster that never pauses costs \$$(awk -v a="$RATE_AURORA_ACU_HOUR" 'BEGIN{printf "%.2f", a*0.5*24}')/day at its
      0.5 ACU floor, so all four idling awake is ~\$$(aurora_floor_daily)/day on top of the above.
    Aurora cluster storage, both engines' automated backups, and the per-bout
      PITR restores and copy-on-write clones that Rounds 2 and 3 create.
    RDS Proxy while a Round 5 bout runs, \$0.015/capacity-hour.

  Databricks, usage-shaped
    ${COUNT_LAKEBASE_PROJECTS} x Lakebase project (six measured rounds plus coordination).
      \$${RATE_LAKEBASE_DBU_PER_CU_HOUR} DBU/CU-hour at \$${RATE_LAKEBASE_DBU}/DBU, plus \$0.023/DSU storage.
    SQL warehouse time for Round 4 and Round 6.

  IAM this creates: 3 roles (Round 5 control, runner, proxy service),
  1 customer-managed permissions boundary, 1 instance profile, 3 inline role
  policies, 1 AWS-managed policy attachment. Networking reuses the default VPC
  and its subnets; it creates 8 security groups, 1 egress rule and 4 DB subnet
  groups.

  Expect the standing total in the tens of dollars per day. The app's own
  standing-cost panel is the number to trust once it is running.
SUMMARY

if [[ "$STATE_BACKEND" == "s3" ]]; then
  cat <<'S3COST'

  Terraform state in S3 adds a few cents a month: one small versioned object per
  apply, plus one lock object per operation. No DynamoDB table is created.
S3COST
fi
fi # RUN_AWS_SECTIONS

if [[ "$MODE" == "print-env" ]]; then
  banner "Derived environment"
  cat >&3 <<ENVOUT
# eval "\$(./bootstrap.sh --print-env)" then run ./antidemo setup yourself.
# No secret is printed. Export the two AWS keys in your own shell.
export ANTI_DEMO_MANIFEST='$ANTI_DEMO_MANIFEST'
export DATABRICKS_PROFILE='$DATABRICKS_PROFILE'
export AWS_REGION='$AWS_REGION'
export AWS_DEFAULT_REGION='$AWS_DEFAULT_REGION'
export AWS_EXPECTED_ACCOUNT_ID='$AWS_ACCOUNT_ID'
export ROUND5_APP_PRINCIPAL_ARN='$ROUND5_APP_PRINCIPAL_ARN'
export DATABRICKS_WAREHOUSE_ID='$DATABRICKS_WAREHOUSE_ID'
export DATABRICKS_APP_CLIENT_ID='${DATABRICKS_APP_CLIENT_ID:-}'
export DATABRICKS_CDF_CATALOG='$CDF_CATALOG'
export ROUND4_CATALOG='$ROUND4_CATALOG'
unset AWS_PROFILE AWS_DEFAULT_PROFILE
ENVOUT
  exit 0
fi

mkdir -p -m 700 "$MANIFEST_DIR"

# ---------------------------------------------------------------------------
# 7b. Claim the generation before writing anything into it
# ---------------------------------------------------------------------------
#
# Everything past this point writes into the generation directory, and --apply
# runs `./antidemo setup`, which rewrites the manifest through a long sequence of
# individually-atomic-but-collectively-not steps. Two operators doing that at
# once cycled one manifest ready -> seeding -> ready -> seeding for forty minutes
# and left it stuck at seeding with nothing running; app.py refuses to start on a
# status that is not ready, so the demo was down. Refusing to write into a
# generation somebody else is mutating is the fix.
#
# macOS ships no flock(1), and a lock held by a helper process that then exits is
# not a lock. One mechanism solves both: this shell opens the lock file on
# descriptor 9, server/generation_lock.py locks *that inherited descriptor*, and
# the lock then lives on this shell's open file description. The kernel releases
# it when this shell exits, however it exits -- SIGKILL, panic, closed laptop --
# so there is no stale lock file to find and nothing to delete by hand.
GENERATION_LOCK="$MANIFEST_DIR/mutation.lock"
GENERATION_LOCK_HELD=0
release_generation_lock() {
  ((GENERATION_LOCK_HELD == 1)) || return 0
  GENERATION_LOCK_HELD=0
  # Clears the record only. The lock itself went when the shell closed fd 9.
  python3 -m server.generation_lock release --manifest "$ANTI_DEMO_MANIFEST" \
    >/dev/null 2>&1 || true
}
trap release_generation_lock EXIT
# `<>` and not `>>`: append mode would make the in-place record rewrite append
# instead, and `>` would truncate the file every reader identifies the lock by.
exec 9<>"$GENERATION_LOCK"
if LOCK_ENV="$(python3 -m server.generation_lock acquire --fd 9 --pid $$ \
  --manifest "$ANTI_DEMO_MANIFEST" --operation "bootstrap.sh --$MODE" 2>&1)"; then
  # Exports ANTI_DEMO_GENERATION_LOCK_TOKEN, which is what lets the `./antidemo
  # setup` below recognise this shell's lock as its own instead of deadlocking
  # against its own parent.
  eval "$LOCK_ENV"
  GENERATION_LOCK_HELD=1
  ok "holding the generation lock ($GENERATION_LOCK)"
elif [[ "$MODE" == "check" ]]; then
  # Check mode provisions nothing and writes no state either way, so a busy
  # generation costs it nothing beyond the directory and this lock file it has
  # already made. Reported rather than passed over: the holder is worth knowing about,
  # because it means the state every check above just read is being rewritten.
  printf '%s\n' "$LOCK_ENV" | sed 's/^/  warn  /'
  warn "this generation is being mutated, so everything above describes a moving target"
else
  die "$LOCK_ENV"
fi

STATE_FILE="$MANIFEST_DIR/bootstrap.json"

# Check mode does not write this. Its closing line says "Nothing was
# provisioned", and an operator who runs the read-only mode of an installer is
# entitled to read that as "nothing was written" -- a mode-600 file appearing in
# the generation directory contradicts it. Nothing consumes bootstrap.json
# either: it is a record for a human, produced by the modes that actually did
# something. docs/BOOTSTRAP.md says so too.
if ((GENERATION_LOCK_HELD == 1)) && [[ "$MODE" != "check" ]]; then
  # Merged, not overwritten, and every value arrives through the environment.
  #
  # Merged because --deploy-only resolves a strict subset of these. It never
  # authenticates to AWS, so CALLER_ARN, ROUND5_APP_PRINCIPAL_ARN and OPERATOR_IP
  # are placeholders or empty in that mode -- and the previous version wrote them
  # anyway, so a redeploy quietly replaced a provision's real values with
  # `"aws_caller_arn": "(not resolved: ...)"` and a malformed `"operator_cidr":
  # "/32"`. A record that degrades every time it is rewritten is worse than one
  # that is occasionally stale, because nothing announces the degradation.
  #
  # Through the environment because these values come from `aws` and `databricks`
  # output. Interpolated into the heredoc as Python source, as they were, one
  # quotation mark anywhere in an ARN or a profile name is a SyntaxError in a
  # generated script -- and this runs after the lock is taken, so it fails in the
  # middle of an operation rather than before one.
  STATE_FILE="$STATE_FILE" \
    AWS_ACCOUNT_ID="$AWS_ACCOUNT_ID" \
    AWS_REGION="$AWS_REGION" \
    AWS_CALLER_ARN="$CALLER_ARN" \
    ROUND5_APP_PRINCIPAL_ARN="$ROUND5_APP_PRINCIPAL_ARN" \
    OPERATOR_CIDR="${OPERATOR_IP:+$OPERATOR_IP/32}" \
    DATABRICKS_HOST="$DATABRICKS_HOST" \
    DATABRICKS_PROFILE="$DATABRICKS_PROFILE" \
    DATABRICKS_PRINCIPAL="$DATABRICKS_PRINCIPAL" \
    DATABRICKS_WAREHOUSE_ID="$DATABRICKS_WAREHOUSE_ID" \
    DATABRICKS_APP_NAME="$APP_NAME" \
    DATABRICKS_APP_CLIENT_ID="${DATABRICKS_APP_CLIENT_ID:-}" \
    ROUND4_CATALOG="$ROUND4_CATALOG" \
    CDF_CATALOG="$CDF_CATALOG" \
    MANIFEST_PATH="$ANTI_DEMO_MANIFEST" \
    OWNER="$OWNER" \
    TERRAFORM_BACKEND="$STATE_BACKEND" \
    TERRAFORM_STATE_BUCKET="$STATE_BUCKET" \
    TERRAFORM_STATE_KEY="$STATE_KEY" \
    SECRET_SCOPE="$SECRET_SCOPE" \
    RUN_MODE="$MODE" \
    python3 - <<'PY' || die "could not record derived values in $STATE_FILE"
import json, os, pathlib

path = pathlib.Path(os.environ["STATE_FILE"])

# record key -> environment variable holding this run's answer
FIELDS = {
    "aws_account_id": "AWS_ACCOUNT_ID",
    "aws_region": "AWS_REGION",
    "aws_caller_arn": "AWS_CALLER_ARN",
    "round5_app_principal_arn": "ROUND5_APP_PRINCIPAL_ARN",
    "operator_cidr": "OPERATOR_CIDR",
    "databricks_host": "DATABRICKS_HOST",
    "databricks_profile": "DATABRICKS_PROFILE",
    "databricks_principal": "DATABRICKS_PRINCIPAL",
    "databricks_warehouse_id": "DATABRICKS_WAREHOUSE_ID",
    "databricks_app_name": "DATABRICKS_APP_NAME",
    "databricks_app_client_id": "DATABRICKS_APP_CLIENT_ID",
    "round4_catalog": "ROUND4_CATALOG",
    "cdf_catalog": "CDF_CATALOG",
    "manifest": "MANIFEST_PATH",
    "owner": "OWNER",
    "terraform_backend": "TERRAFORM_BACKEND",
    "terraform_state_bucket": "TERRAFORM_STATE_BUCKET",
    "terraform_state_key": "TERRAFORM_STATE_KEY",
    "secret_scope": "SECRET_SCOPE",
}

# A mode that did not resolve a value must not record its non-answer over a
# value some earlier run did resolve. Empty covers "not asked for"; the parens
# form is what the deploy-only path substitutes for an unauthenticated AWS
# identity, and is prose rather than a value.
def resolved(raw: str) -> bool:
    text = raw.strip()
    return bool(text) and not text.startswith("(not resolved")

existing = {}
if path.exists():
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except ValueError:
        # An unreadable record is replaced rather than merged into. It cannot be
        # what a previous run wrote, so there is nothing in it worth keeping.
        loaded = None
    if isinstance(loaded, dict):
        existing = loaded

record = dict(existing)
for key, variable in FIELDS.items():
    value = os.environ.get(variable, "")
    if resolved(value):
        record[key] = value
    elif key not in record:
        # First write, and this mode has no answer. Recorded empty so the shape
        # of the file does not depend on which mode wrote it first.
        record[key] = ""
record["recorded_by"] = f"bootstrap.sh --{os.environ['RUN_MODE']}"

path.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
path.chmod(0o600)
PY
  ok "derived values recorded in $STATE_FILE (no credentials)"
fi

if [[ "$MODE" == "check" ]]; then
  banner "Check mode complete"
  say "  Everything validated. Nothing was provisioned."
  if [[ -z "${DATABRICKS_APP_CLIENT_ID:-}" ]]; then
    say "  Re-run with --apply to create the Databricks App and provision."
  elif ((SECRET_DRIFTED == 1)); then
    say "  ${BOLD}The deployed app's seal is stale.${RESET} Republish it with:"
    say "      ./bootstrap.sh --deploy-only"
  else
    say "  Re-run with --apply to provision, or --deploy-only to refresh the app."
  fi
  exit 0
fi

# ---------------------------------------------------------------------------
# 8. Provision
# ---------------------------------------------------------------------------

if [[ "$MODE" == "apply" ]]; then
step "Provision"

[[ -n "${DATABRICKS_APP_CLIENT_ID:-}" ]] ||
  die "DATABRICKS_APP_CLIENT_ID is still unresolved; Round 4 cannot be sealed without it"

# The same refusal as step 1a, from the same two functions, on the manifest this
# run actually resolved rather than the one it predicted it would. Normally
# unreachable, because 1a already refused -- it stays because 1a predicts the
# generation from flags and the filesystem, and this is the only place that knows
# for certain. If the two ever disagree, the last word should be the one that
# read the real manifest, not the one that guessed.
if ((EXISTING_INSTALL == 1)) && apply_would_reset_a_ready_install "$ANTI_DEMO_MANIFEST"; then
  refuse_ready_install "${MANIFEST_RUN:-}"
fi

if ((ASSUME_YES == 0)); then
  [[ -t 0 ]] || die "--apply needs a terminal to confirm on, or pass --yes"
  if ((EXISTING_INSTALL == 1)); then
    say ""
    say "  ${BOLD}This installation already exists.${RESET} 'antidemo setup' will run"
    say "  'terraform plan' and 'terraform apply' against it through"
    say "  reconcile_infrastructure (server/lifecycle.py:5176), so any pending"
    say "  diff in infra/aws will be applied now. It will also reset both"
    say "  database lanes and clear Round 3 anchors."
    if [[ "${MANIFEST_STATUS:-}" == "ready" ]]; then
      say ""
      say "  ${BOLD}This install is 'ready', so that reset is not a no-op.${RESET} You passed"
      say "  --reset-ready, which is what got you here. A bout running now will die."
      say "  ${BOLD}--deploy-only${RESET} is the path that redeploys without touching a database."
    fi
  fi
  if [[ "$STATE_BACKEND" == "s3" ]] && ((BUCKET_EXISTS == 0)); then
    say ""
    say "  S3 bucket ${BOLD}${STATE_BUCKET}${RESET} will be created in $AWS_REGION with"
    say "  versioning on, SSE-S3 encryption, and all public access blocked."
  fi
  say ""
  say "  ${BOLD}Interrupting this is the expensive mistake.${RESET} Ctrl-C part way through"
  say "  leaves a half-created fleet that is fully billing, and the only record of"
  say "  what exists is the local Terraform state in"
  say "      $MANIFEST_DIR"
  say "  Nothing reaps it on a timer. If you do interrupt, re-run this command --"
  say "  'antidemo setup' resumes rather than duplicating -- or run"
  say "  './antidemo cleanup --yes' against this same generation to destroy it. Losing"
  say "  that directory means the resources still bill and only their AWS tags"
  say "  identify them."
  say ""
  printf '  Type %sPROVISION%s to continue: ' "$BOLD" "$RESET"
  read -r CONFIRM </dev/tty
  [[ "$CONFIRM" == "PROVISION" ]] || die "not confirmed; nothing was changed"
fi

# The bucket has to exist before `terraform init`, and init happens inside
# `antidemo setup`, so this is the last moment to create it.
if [[ "$STATE_BACKEND" == "s3" ]]; then
  if ((BUCKET_EXISTS == 0)); then
    info "creating s3://$STATE_BUCKET"
    CREATE_ARGS=(s3api create-bucket --bucket "$STATE_BUCKET")
    # us-east-1 is the one region that rejects an explicit LocationConstraint.
    [[ "$AWS_REGION" != "us-east-1" ]] &&
      CREATE_ARGS+=(--create-bucket-configuration "LocationConstraint=$AWS_REGION")
    OUT="$(aws "${CREATE_ARGS[@]}" 2>&1)" ||
      die "could not create s3://$STATE_BUCKET: $(printf '%s' "$OUT" | tail -2)"
    ok "bucket created"
    aws s3api put-bucket-versioning --bucket "$STATE_BUCKET" \
      --versioning-configuration Status=Enabled ||
      die "the bucket exists but versioning could not be enabled. Fix that before applying:
           an unversioned state file cannot be recovered from a bad write, and it is the
           only record of which billed resources you own."
    ok "versioning enabled"
    aws s3api put-bucket-encryption --bucket "$STATE_BUCKET" \
      --server-side-encryption-configuration \
      '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"},"BucketKeyEnabled":true}]}' ||
      warn "default encryption could not be set; state is still encrypted in transit"
    aws s3api put-public-access-block --bucket "$STATE_BUCKET" \
      --public-access-block-configuration \
      'BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true' ||
      warn "public access block could not be set; set it by hand before applying"
    ok "SSE-S3 and public access block applied"
  fi

  # The generation-scoped record is what makes the choice durable. A later
  # `antidemo setup` re-derives the backend from it, so a forgotten flag cannot
  # silently re-init against an empty local state and plan a second copy of
  # every billed resource.
  STATE_BACKEND_FILE="$MANIFEST_DIR/terraform-backend.json"
  python3 - "$STATE_BACKEND_FILE" <<PY
import json, pathlib, sys
pathlib.Path(sys.argv[1]).write_text(json.dumps({
    "backend": "s3",
    "bucket": "$STATE_BUCKET",
    "key": "$STATE_KEY",
    "region": "$AWS_REGION",
    "use_lockfile": True,
    "encrypt": True,
}, indent=2) + "\n", encoding="utf-8")
PY
  ok "recorded the backend choice in $STATE_BACKEND_FILE"
  info "s3://$STATE_BUCKET/$STATE_KEY"
fi

export ANTI_DEMO_MANIFEST DATABRICKS_PROFILE ROUND5_APP_PRINCIPAL_ARN
export DATABRICKS_WAREHOUSE_ID DATABRICKS_APP_CLIENT_ID
export DATABRICKS_CDF_CATALOG="$CDF_CATALOG"
export ROUND4_CATALOG
export AWS_EXPECTED_ACCOUNT_ID="$AWS_ACCOUNT_ID"

SETUP_ARGS=(setup --owner "$OWNER" --no-serve)
if ((EXISTING_INSTALL == 0)); then
  SETUP_ARGS+=(--ttl-hours "$TTL_HOURS")
fi

say ""
# A first provision is roughly half an hour of mostly-silent waiting, and an
# operator with no sense of the expected duration cannot tell a hang from
# Aurora behaving normally. These are shapes, not promises: the two database
# creation phases dominate and everything else is minutes.
cat <<'DURATION'
  Expected duration of './antidemo setup' on a first provision, roughly:

    terraform init and plan                          1-2 min
    IAM roles, security groups, subnet groups        1-2 min
    4 x Aurora Serverless v2 cluster creation       10-20 min   <- the long one
    3 x RDS PostgreSQL instance creation             8-15 min   <- and this one
    Round 5 runner EC2 instance and bootstrap        3-5 min
    7 x Lakebase project creation                    3-8 min
    schema seeding, grants, manifest seal            2-5 min
                                                    ----------
                                                     30-55 min typical

  Long silences during the two database phases are normal: AWS reports nothing
  while a cluster is creating. Terraform prints an elapsed counter every 10s,
  so a line that has not moved in several minutes is the thing to worry about.
  Re-running after an interruption resumes; it does not duplicate.

DURATION
say "  ./antidemo ${SETUP_ARGS[*]}"
say ""
# `antidemo setup` is itself idempotent and resumable: it provisions, resumes an
# interrupted provision, or reconciles and resets a ready one, deciding from
# the manifest. Re-running bootstrap.sh after a failure therefore continues
# rather than duplicating.
#
# The exit code is caught rather than left to `set -e` because of what has
# already happened by the time it is non-zero. Terraform creates the fleet
# before anything downstream of it can fail, so a setup that dies at minute 25
# has usually left billed resources behind -- and `set -e` would end this script
# on whatever line `antidemo setup` last printed, with no mention of them. The
# warning that says so is thirty minutes and several thousand lines of Terraform
# output further up, and it is skipped entirely under --yes. This is the moment
# it is needed.
SETUP_STATUS=0
./antidemo "${SETUP_ARGS[@]}" || SETUP_STATUS=$?
if ((SETUP_STATUS != 0)); then
  die "'./antidemo setup' exited $SETUP_STATUS, and it does not undo what it created.
       Anything Terraform applied before the failure exists now and is billing now.
       Nothing reaps it on a timer.

       Find out what is running:   ./antidemo cleanup --dry-run
       Continue where it stopped:  ./bootstrap.sh --apply   (resumes; does not duplicate)
       Stop the spend:             ./antidemo cleanup --yes

       All three need this generation, so keep ANTI_DEMO_MANIFEST pointed at
       $ANTI_DEMO_MANIFEST
       or let bootstrap.sh re-derive it. Losing $MANIFEST_DIR
       leaves the resources billing with only their AWS tags to identify them."
fi

# setup rewrites the manifest, so everything read before it is now stale. The
# deploy stage re-reads rather than trusting the earlier values.
if [[ -f "$ANTI_DEMO_MANIFEST" ]]; then
  MANIFEST_SHA="$(shasum -a 256 "$ANTI_DEMO_MANIFEST" | awk '{print $1}')"
fi
fi # MODE == apply

# ---------------------------------------------------------------------------
# 9. Deploy the Databricks App
# ---------------------------------------------------------------------------

if ((DEPLOY_APP == 1)); then
step "Deploy the Databricks App"

[[ -f "$ANTI_DEMO_MANIFEST" ]] || die "there is no manifest to publish at $ANTI_DEMO_MANIFEST"
[[ -n "${DATABRICKS_APP_CLIENT_ID:-}" ]] || die "the app's service principal is unresolved"
((${#APP_RESOURCE_KEYS[@]} > 0)) || die "app.yaml declared no resource keys to bind"

# A Round 5 source change is a two-surface deployment: the sealed EC2 runner
# first, then the Databricks App. Publishing in the opposite order leaves the
# app carrying code that its own manifest correctly refuses. This gate runs
# before the seal snapshot, every put-secret, resource update, workspace sync,
# and app deploy. A warning is not enough because the resulting app is healthy
# everywhere except the round the operator is trying to repair.
SEALED_RUNNER_SHA="$(jq -r '.round5.harness_sha256 // empty' "$ANTI_DEMO_MANIFEST")"
if [[ -n "$SEALED_RUNNER_SHA" ]]; then
  SOURCE_RUNNER_SHA="$("$PYTHON_ENVIRONMENT/bin/python" - <<'PY'
from server.connection_spike_live import runner_harness_sha256
print(runner_harness_sha256())
PY
)" || die "could not compute the Round 5 source harness digest"
  if [[ "$SOURCE_RUNNER_SHA" != "$SEALED_RUNNER_SHA" ]]; then
    die "REFUSING TO DEPLOY INCOMPATIBLE ROUND 5 RUNNER SOURCE.

       source  sha256:$SOURCE_RUNNER_SHA
       sealed  sha256:$SEALED_RUNNER_SHA

       No Databricks secret, workspace source, or app deployment was changed.
       Refresh and verify the sealed EC2 runner first:

         ./antidemo runner refresh

       Then publish the now-matching seal and source:

         env -u AWS_PROFILE -u AWS_DEFAULT_PROFILE \\
           ANTI_DEMO_MANIFEST=\"\$PWD/$(basename "$MANIFEST_DIR")/manifest.json\" \\
           ./bootstrap.sh --deploy-only --yes"
  fi
  ok "Round 5 source matches the sealed EC2 harness (sha256:${SOURCE_RUNNER_SHA:0:16})"
fi

# The seal has to be servable before anything is pushed. app.py raises inside
# the FastAPI lifespan, so an unservable seal is not a degraded app -- it is a
# container that never starts, while the deploy still reports success.
#
# Validated, published and recorded from one snapshot rather than three separate
# reads of a live file. Every read of manifest.json is a separate answer: the
# status was checked here, the bytes were sent to put-secret further down, and
# the sha recorded in app-deploy.json came from the preflight hundreds of lines
# earlier. Nothing forced those three to describe the same manifest, so the
# recorded sha could name a seal that was never published -- and the next run
# compares against that record to decide whether the app has drifted, so a wrong
# record does not stay a cosmetic problem.
#
# The generation lock makes a mutation here unlikely rather than impossible: it
# excludes every cooperating writer (all of them arrive through a `./antidemo`
# subcommand, and `tests/test_api.py` keeps the server itself from ever becoming
# one), but it cannot exclude a hand-edit. A snapshot is what makes the
# guarantee structural instead of conditional.
SEAL_SNAPSHOT="$MANIFEST_DIR/seal-in-flight.json"
remove_seal_snapshot() { rm -f "$SEAL_SNAPSHOT"; }
cp "$ANTI_DEMO_MANIFEST" "$SEAL_SNAPSHOT" ||
  die "could not snapshot the seal to $SEAL_SNAPSHOT"
chmod 600 "$SEAL_SNAPSHOT"
trap 'remove_seal_snapshot; release_generation_lock' EXIT

SEAL_SHA="$(shasum -a 256 "$SEAL_SNAPSHOT" | awk '{print $1}')"
if [[ -n "$MANIFEST_SHA" && "$SEAL_SHA" != "$MANIFEST_SHA" ]]; then
  # Not fatal: what gets published is the snapshot, and the snapshot is what the
  # two checks below examine, so an unservable seal still cannot reach the
  # secret. It is reported because the drift verdict printed in step 6b was about
  # the earlier manifest and is now describing something that no longer exists.
  warn "the manifest changed between this run's preflight (sha256:${MANIFEST_SHA:0:16})
        and this publish (sha256:${SEAL_SHA:0:16}). The snapshot being published is the
        newer one, and the drift report earlier in this run described the older one.
        Nothing that holds the generation lock can have done this, so something wrote
        manifest.json without taking it."
fi
MANIFEST_SHA="$SEAL_SHA"
MANIFEST_STATUS="$(jq -r '.status' "$SEAL_SNAPSHOT")"
MANIFEST_VERSION="$(jq -r '.manifest_version // 0' "$SEAL_SNAPSHOT")"
if [[ "$MANIFEST_STATUS" != "ready" ]]; then
  die "Refusing to deploy: the manifest status is '$MANIFEST_STATUS', not 'ready'.
       app.py:108 raises InvalidStateError on exactly this from inside the FastAPI
       lifespan, so the container would fail to start and the deploy would still look
       like it worked. Finish or repair setup first."
fi
[[ "$MANIFEST_VERSION" -ge 2 ]] ||
  die "Refusing to deploy: the seal is manifest v$MANIFEST_VERSION and app.py:112 needs v2+."
ok "seal is servable: status ready, manifest v$MANIFEST_VERSION (sha256:${SEAL_SHA:0:16})"

if [[ ! -d frontend/dist ]]; then
  die "frontend/dist is missing, and the app answers 503 on every page without it.
       Build it first: cd frontend && npm ci && npm run build
       This script will not build for you, because 'npm run build' overwrites the
       directory a locally running server is serving from."
fi
ok "frontend/dist present"

if ((ASSUME_YES == 0)); then
  [[ -t 0 ]] || die "the deploy needs a terminal to confirm on, or pass --yes"
  say ""
  say "  This writes ${#APP_RESOURCE_KEYS[@]} Databricks secrets in scope"
  say "  ${BOLD}${SECRET_SCOPE}${RESET}, syncs this working tree to the workspace,"
  say "  deploys app ${BOLD}${APP_NAME}${RESET} and restarts it."
  say "  Two of those secrets are your AWS keys."
  say ""
  printf '  Type %sDEPLOY%s to continue: ' "$BOLD" "$RESET"
  read -r CONFIRM </dev/tty
  [[ "$CONFIRM" == "DEPLOY" ]] || die "not confirmed; nothing was changed"
fi

if databricks secrets list-scopes "${DATABRICKS_ARGS[@]}" -o json 2>/dev/null |
  jq -e --arg s "$SECRET_SCOPE" 'if type=="array" then . else (.scopes // []) end
    | any(.name == $s)' >/dev/null; then
  ok "secret scope '$SECRET_SCOPE' already exists"
else
  if OUT="$(databricks secrets create-scope "$SECRET_SCOPE" "${DATABRICKS_ARGS[@]}" 2>&1)"; then
    ok "created secret scope '$SECRET_SCOPE'"
  elif printf '%s' "$OUT" | grep -qi 'already exists\|RESOURCE_ALREADY_EXISTS'; then
    ok "secret scope '$SECRET_SCOPE' already exists"
  else
    die "could not create secret scope '$SECRET_SCOPE': $(printf '%s' "$OUT" | tail -2)"
  fi
fi

# put-secret overwrites in place, which is what makes the whole deploy
# idempotent. Values arrive on stdin so no credential is ever an argv entry
# visible to `ps`.
# DATABRICKS_ARGS carries -o json, so `apps logs` emits one JSON event per line
# whose .message is a fragment of the original stream -- often a few characters,
# with the real newlines inside the strings. Printed raw it is unreadable
# exactly when it matters most. jq -j reassembles the stream as it was written.
app_logs() {
  local lines="${1:-40}" raw
  raw="$(databricks apps logs "$APP_NAME" --tail-lines "$lines" "${DATABRICKS_ARGS[@]}" 2>&1)"
  if printf '%s' "$raw" | jq -se 'length > 0 and (.[0] | type) == "object"' >/dev/null 2>&1; then
    printf '%s' "$raw" | jq -rj '.message // ""'
    printf '\n'
  else
    printf '%s\n' "$raw"
  fi
}

put_secret_stdin() {
  local key="$1" out
  if ! out="$(databricks secrets put-secret "$SECRET_SCOPE" "$key" "${DATABRICKS_ARGS[@]}" 2>&1)"; then
    die "could not write secret '$key' to scope '$SECRET_SCOPE': $(printf '%s' "$out" | tail -2)"
  fi
}

# The empty string is a legitimate secret value here and the CLI will not write
# one: `databricks secrets put-secret` fails its own client-side check with
# "Secret value must be specified in a create request!" before anything is sent.
# That refusal aborted the deploy in exactly the configuration this installer is
# built around -- a permanent IAM user, so an access key and secret and no
# session token -- because `aws-session-token` still has to exist as a resource.
# The REST endpoint underneath accepts `"string_value": ""` without complaint,
# so this goes straight to it.
#
# Deliberately a second function rather than a branch inside put_secret_stdin:
# that one streams the seal snapshot from a file descriptor, and $SEAL_SHA names
# those exact bytes. Reading them into a shell variable to measure their length
# is how a trailing newline goes missing and the recorded sha stops describing
# what was actually published.
put_empty_secret() {
  local key="$1" payload out
  payload="$(mktemp)" || die "could not create a request body for secret '$key'"
  chmod 600 "$payload"
  if ! jq -n --arg scope "$SECRET_SCOPE" --arg key "$key" \
    '{scope: $scope, key: $key, string_value: ""}' >"$payload"; then
    rm -f "$payload"
    die "could not build the request body for secret '$key'"
  fi
  out="$(databricks api post /api/2.0/secrets/put --json "@$payload" \
    "${DATABRICKS_ARGS[@]}" 2>&1)" || {
    rm -f "$payload"
    die "could not write the empty secret '$key' to scope '$SECRET_SCOPE': $(printf '%s' "$out" | tail -2)"
  }
  rm -f "$payload"
}

# Which keys the scope already holds. A republish of the seal must not overwrite
# a working AWS credential with an empty string just because this run had no
# keys to give, and a missing resource must not be left missing, because
# Databricks Apps fails a valueFrom whose resource does not exist. Knowing which
# case applies is the difference between the two.
EXISTING_KEYS=""
EXISTING_KEYS="$(databricks secrets list-secrets "$SECRET_SCOPE" "${DATABRICKS_ARGS[@]}" -o json 2>/dev/null |
  jq -r 'if type=="array" then .[] else (.secrets // [])[] end | .key' 2>/dev/null | tr '\n' ' ' || true)"
scope_has_key() { [[ " $EXISTING_KEYS " == *" $1 "* ]]; }

# ---------------------------------------------------------------------------
# The app's AWS credential: the one the operator supplied, published as given
# ---------------------------------------------------------------------------
#
# The contract is deliberately flat. The operator supplies AWS_ACCESS_KEY_ID,
# AWS_SECRET_ACCESS_KEY and AWS_DEFAULT_REGION once, in $ENV_FILE, alongside the
# three Databricks values, and those same AWS values are what the deployed app
# authenticates as. Nothing is minted here, nothing is vended, nothing rotates
# on a schedule. Five inputs in, one working installation out.
#
# WHAT THAT MAKES THE OPERATOR RESPONSIBLE FOR, stated here because it is the
# one thing the design does not decide for them:
#
#   * PERMANENT KEYS NEVER EXPIRE; SESSION CREDENTIALS DO. Databricks Apps
#     resolves a `valueFrom` secret once, at container start, and the process
#     holds that value for its whole life -- there is no path by which a rotated
#     secret reaches a running container. So a permanent IAM key pair (AKIA...,
#     no session token) is the configuration in which the app simply keeps
#     working, and an STS session (ASIA..., with a token) is the configuration
#     in which it works this afternoon and is dead by morning. That is not a
#     prediction: it is what happened on 2026-08-24, when the app was found
#     serving as an expiring SSO session:
#     `assumed-role/AWSReservedSSO_<permission-set>/<operator>@<employer>`.
#     The refusal below is what stops it happening silently again.
#   * THE APP HOLDS WHATEVER THE SUPPLIED KEY HOLDS. Provisioning needs broad
#     actions -- it creates IAM roles and launches an EC2 instance -- and the
#     app needs a much smaller set. docs/iam/anti-demo-app-runtime.json is that
#     smaller set, and `python -m server.aws_permissions` prints it from the
#     round modules' own call sites. An operator who wants the deployed app to
#     hold less than the installer does can supply a key on that narrower policy
#     here; see docs/BOOTSTRAP.md, "What the deployed app does with the key".
ROTATE_AWS=0
[[ -n "$AWS_ACCESS_KEY_ID" && -n "$AWS_SECRET_ACCESS_KEY" ]] && ROTATE_AWS=1

# Does the credential this run is about to publish carry an expiry? Two
# independent tells, because either alone is enough to be sure: AWS prefixes
# temporary access keys `ASIA` and permanent ones `AKIA`, and a session token
# only exists for temporary credentials.
CREDENTIAL_IS_TEMPORARY=0
if ((ROTATE_AWS == 1)); then
  case "$AWS_ACCESS_KEY_ID" in ASIA*) CREDENTIAL_IS_TEMPORARY=1 ;; esac
  [[ -n "${AWS_SESSION_TOKEN:-}" ]] && CREDENTIAL_IS_TEMPORARY=1
fi

# Refused rather than warned about, and this is the one place in the deploy
# where that is the right severity. A warning scrolls past in a wall of `ok`
# lines and the operator learns the truth hours later, from an audience looking
# at four rounds that will not arm. --i-know-this-expires is the escape hatch
# for the case where a short-lived credential really is what was wanted, so
# nothing is made impossible -- only impossible to do by accident.
if ((CREDENTIAL_IS_TEMPORARY == 1 && ALLOW_TEMPORARY_CREDENTIAL == 0)); then
  die "REFUSING TO PUBLISH A CREDENTIAL THAT EXPIRES INTO THE DEPLOYED APP.

       The AWS key this run would publish is '$AWS_ACCESS_KEY_ID', which is a temporary
       STS session, not a permanent IAM key pair. Databricks Apps injects secrets into
       the container once, at start, so the app would hold this session for its whole
       life and there is no way to hand a running container a replacement. It would
       serve correctly today and answer 'credentials_state: rejected' tomorrow, with
       Rounds 1, 2, 3 and 5 off the card and Rounds 4 and 6 still working -- which
       looks like a broken demo rather than an expired key.

       This is almost always an 'aws sso login' session that leaked in through the
       environment. What to supply instead, in $ENV_FILE:

         AWS_ACCESS_KEY_ID=AKIA...            a permanent IAM user access key
         AWS_SECRET_ACCESS_KEY=...            its secret half
         # AWS_SESSION_TOKEN stays unset      permanent keys have none

       The app does not need your installer privileges to run its rounds. If you would
       rather it held less, docs/iam/anti-demo-app-runtime.json is the least-privilege
       policy for exactly what it calls, and 'python -m server.aws_permissions' prints
       that set from the code. Attach it to a dedicated IAM user and supply that user's
       key here instead.

       If a credential with an expiry is genuinely what you want, re-run with
       --i-know-this-expires and this becomes a warning."
fi

# Publishing a credential this run does not own would be worse than leaving a
# stale one: an empty access key breaks every AWS call the app makes, whereas a
# stale one at least still works until it expires.
publish_or_keep() {
  local key="$1" value="$2"
  if ((ROTATE_AWS == 1)); then
    printf '%s' "$value" | put_secret_stdin "$key"
    ok "rotated $key"
  elif scope_has_key "$key"; then
    info "$key already exists and no replacement was supplied, so it is left as it is"
  else
    die "The app needs secret '$key' and the scope '$SECRET_SCOPE' does not have it.
         Databricks Apps fails a valueFrom whose resource is missing, so the container
         would not start. Supply AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in $ENV_FILE
         and re-run, which will create all three AWS secrets."
  fi
}

# What the scope already holds, by shape only. The access key id is not a
# secret -- it is what sts:GetCallerIdentity echoes back -- and its first four
# characters are the whole diagnosis. Read so that a run which publishes nothing
# can still say whether what is already there will outlive the demo, which is
# the question the old "cannot be read back, so cannot say" warning gave up on.
published_app_key_id() {
  scope_has_key aws-access-key-id || return 0
  databricks secrets get-secret "$SECRET_SCOPE" aws-access-key-id \
    "${DATABRICKS_ARGS[@]}" -o json 2>/dev/null |
    jq -r '.value // empty' 2>/dev/null | base64 -d 2>/dev/null || true
}

for key in "${APP_RESOURCE_KEYS[@]}"; do
  case "$key" in
    anti-demo-manifest-json)
      # The snapshot, not the live file: these are the exact bytes the two
      # servability checks above passed and the exact bytes $SEAL_SHA names.
      put_secret_stdin "$SECRET_MANIFEST_KEY" <"$SEAL_SNAPSHOT"
      ok "published the seal to $SECRET_SCOPE/$SECRET_MANIFEST_KEY ($(wc -c <"$SEAL_SNAPSHOT" | tr -d ' ') bytes)"
      ;;
    aws-access-key-id)
      publish_or_keep "$key" "$AWS_ACCESS_KEY_ID"
      ;;
    aws-secret-access-key)
      publish_or_keep "$key" "$AWS_SECRET_ACCESS_KEY"
      ;;
    aws-session-token)
      if ((ROTATE_AWS == 0)) && scope_has_key "$key"; then
        info "$key already exists and the AWS keys were not rotated, so it is left as it is"
      elif [[ -n "${AWS_SESSION_TOKEN:-}" ]]; then
        printf '%s' "$AWS_SESSION_TOKEN" | put_secret_stdin "$key"
        ok "published $key"
      else
        # Databricks Apps has no optional binding, and a valueFrom whose
        # resource is missing fails the app at startup. So the resource has to
        # exist even for a permanent IAM user that has no token. Empty, not a
        # space: botocore reads the token with `if token:` and
        # server/aws_auth.py with bool(), so empty is absent to both, while a
        # space is truthy and would be signed into every request. See the
        # comment on AWS_SESSION_TOKEN in app.yaml.
        #
        # Not `put_secret_stdin`: the CLI refuses an empty value outright, so
        # dropping the binding is not the alternative -- the app would not
        # start. See put_empty_secret.
        put_empty_secret "$key"
        info "no AWS_SESSION_TOKEN, so $key holds the empty string, which both
        botocore and server/aws_auth.py read as absent"
      fi
      ;;
    *)
      warn "app.yaml wants resource '$key' and this script cannot fill it.
        Create it by hand or the app will not start."
      ;;
  esac
done

# WHAT THE APP WILL ACTUALLY WAKE UP HOLDING, said out loud either way.
#
# This used to be a single warning fired whenever no keys were supplied, whose
# text was "secret values cannot be read back, so this script cannot tell"
# whether what is in the scope has expired. That premise was wrong -- a
# Databricks-backed scope reads back to whoever can write it -- and the cost of
# believing it was that the one fact worth reporting was never looked up. The
# access key id is not a secret, and its prefix settles the question outright.
#
# Whether this run touched the credential at all is separate from what the
# credential now is, and both are worth saying. A `--deploy-only` with the AWS
# fields left empty deliberately leaves the published pair alone -- that is the
# supported way to republish a seal without disturbing a working app -- but it
# means "the deploy succeeded" carries no information about the credential the
# app will start with, which is the exact gap that let an expiring session sit
# in the scope across several apparently-clean deploys.
#
# Keyed on whether the secret EXISTS, which `scope_has_key` settles, rather than
# on whether its value read back -- those are two different questions and
# conflating them reports "no AWS access key is published" at a scope that has
# one, whenever the read fails for a reason of its own.
APP_KEY_PRESENT=0
scope_has_key aws-access-key-id && APP_KEY_PRESENT=1
if ((ROTATE_AWS == 0 && APP_KEY_PRESENT == 1)); then
  warn "the AWS credentials were not refreshed by this run: no AWS_ACCESS_KEY_ID and
        AWS_SECRET_ACCESS_KEY were supplied in $ENV_FILE, so the app keeps whatever
        pair was already in '$SECRET_SCOPE'."
fi

if ((APP_KEY_PRESENT == 0)); then
  warn "no AWS access key is published in '$SECRET_SCOPE', so the deployed app will
        start without AWS credentials and offer Rounds 4 and 6 only. Supply
        AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in $ENV_FILE and re-run."
  PUBLISHED_KEY_ID="__absent__"
else
  PUBLISHED_KEY_ID="$(published_app_key_id)"
fi
case "$PUBLISHED_KEY_ID" in
  __absent__) ;;
  AKIA*)
    ok "the app's AWS credential is the permanent key $PUBLISHED_KEY_ID"
    info "permanent keys carry no expiry, so this app keeps working without anyone
        re-authenticating. Nothing here needs revisiting until you rotate it yourself."
    ;;
  ASIA*)
    warn "THE APP'S AWS CREDENTIAL IS THE TEMPORARY SESSION $PUBLISHED_KEY_ID AND WILL
        STOP WORKING WHEN IT EXPIRES -- typically within hours. Databricks Apps injects
        secrets at container start, so a running app cannot be handed a replacement:
        when this dies, Rounds 1, 2, 3 and 5 leave the card and Rounds 4 and 6 keep
        working, which reads as a broken demo rather than an expired key. Put a
        permanent IAM key pair (AKIA..., no session token) in $ENV_FILE and re-run
        './bootstrap.sh --deploy-only' to replace it."
    ;;
  "")
    # The secret is there and its value did not read back. Reported as the
    # unknown it is, rather than as an absence, because the two call for
    # opposite responses and the app is probably fine.
    info "secret 'aws-access-key-id' exists in '$SECRET_SCOPE' but its value could not be
        read back, so this script cannot say whether the app's credential is permanent
        or an expiring session. Confirm from the running app:
            curl -s \"\$APP_URL/readyz\" | jq .credentials_principal"
    ;;
  *)
    warn "the app's AWS access key '$PUBLISHED_KEY_ID' has an unfamiliar shape; expected
        a permanent key beginning AKIA. Confirm it with 'aws sts get-caller-identity'."
    ;;
esac

# `apps update` accepts resources only through --json, and replaces the whole
# list, so the payload is rebuilt from app.yaml's keys rather than patched.
RESOURCES_JSON="$(printf '%s\n' "${APP_RESOURCE_KEYS[@]}" | jq -R . | jq -s \
  --arg scope "$SECRET_SCOPE" --arg mkey "$SECRET_MANIFEST_KEY" \
  '[ .[] | { name: ., secret: { scope: $scope,
             key: (if . == "anti-demo-manifest-json" then $mkey else . end),
             permission: "READ" } } ]')"

if OUT="$(jq -n --arg name "$APP_NAME" --argjson res "$RESOURCES_JSON" \
  '{name: $name, resources: $res}' |
  databricks apps update "$APP_NAME" --json @/dev/stdin "${DATABRICKS_ARGS[@]}" 2>&1)"; then
  ok "app resources bound to scope '$SECRET_SCOPE'"
else
  die "could not bind the app's secret resources: $(printf '%s' "$OUT" | tail -3)"
fi

WORKSPACE_SRC="/Workspace/Users/$DATABRICKS_PRINCIPAL/$APP_NAME"

# `databricks sync` honours .gitignore, and .gitignore excludes frontend/dist
# deliberately -- a fingerprinted bundle has no business in git. The consequence
# is that the built frontend, the one thing the app cannot serve a page without,
# is invisible to sync unless it is named back in with --include. Without this
# the deploy succeeds, the container starts, and every page answers 503.
SYNC_ARGS=(
  --exclude '.anti-demo*' --exclude '.venv' --exclude 'node_modules'
  --exclude '.git' --exclude '.env*'
  --include 'frontend/dist'
)

# Databricks Apps picks its installer, and with it the Python version, from
# which files are present. requirements.txt wins unconditionally and pins the
# app to pip on Python 3.11; only when it is absent, and pyproject.toml and
# uv.lock are both present, does the runtime use uv and honour requires-python.
#
# This repository ships pyproject.toml and uv.lock and no requirements.txt,
# which is what puts the deployed app on uv. The preflight refuses to run at all
# if requirements.txt reappears, so there is nothing to withhold from the sync
# here -- but uv.lock is what makes requires-python binding, so its absence is
# still worth naming precisely.
REQUIRED_PY="$(sed -n 's/^requires-python[[:space:]]*=[[:space:]]*"\(.*\)"$/\1/p' pyproject.toml)"
NEEDS_UV=0
case "$REQUIRED_PY" in
  *3.1[2-9]* | *3.[2-9][0-9]* | *4.*) NEEDS_UV=1 ;;
esac
if ((NEEDS_UV == 1)) && [[ ! -f uv.lock ]]; then
  die "pyproject.toml requires Python $REQUIRED_PY, and Databricks Apps only honours that
       when it installs with uv, which needs a uv.lock beside pyproject.toml. There is no
       uv.lock, so the app would install with pip on Python 3.11 and fail to import.
       Run 'uv lock' and re-run."
fi

info "syncing the working tree to $WORKSPACE_SRC"
if OUT="$(databricks sync . "$WORKSPACE_SRC" --full "${DATABRICKS_ARGS[@]}" \
  "${SYNC_ARGS[@]}" 2>&1)"; then
  ok "source synced"
else
  die "databricks sync failed: $(printf '%s' "$OUT" | tail -3)"
fi

# That frontend/dist exists locally says nothing about whether it reached the
# workspace, and the workspace copy is what the container runs. Ask the
# workspace directly rather than reading the sync output: sync is incremental
# and reports only what changed, so an unchanged file is absent from its diff
# while being perfectly present remotely. Inferring presence from that diff
# gives a guard that refuses correct deploys.
# Deleting requirements.txt from this repository does not delete the copy an
# earlier sync already put in the workspace, and `databricks sync` removes only
# files it once uploaded and now sees changed -- not files that have simply
# ceased to exist locally. Since the runtime keys off mere presence, that
# leftover silently keeps the app on pip and 3.11 forever.
#
# Deliberately not gated on a local requirements.txt: the case this exists for is
# exactly the one where there is no local file to test. Every deploy asks the
# workspace directly, which costs one get-status and closes the hole for an
# installation that was deployed before the file was removed.
if ((NEEDS_UV == 1)); then
  if databricks workspace get-status "$WORKSPACE_SRC/requirements.txt" \
    "${DATABRICKS_ARGS[@]}" >/dev/null 2>&1; then
    if OUT="$(databricks workspace delete "$WORKSPACE_SRC/requirements.txt" \
      "${DATABRICKS_ARGS[@]}" 2>&1)"; then
      ok "removed a stale requirements.txt from the workspace tree"
    else
      die "A requirements.txt left by an earlier sync is still in $WORKSPACE_SRC, and it
           forces the app onto pip and Python 3.11 where this source does not parse.
           Deleting it failed: $(printf '%s' "$OUT" | tail -2)"
    fi
  fi
fi

if databricks workspace get-status "$WORKSPACE_SRC/frontend/dist/index.html" \
  "${DATABRICKS_ARGS[@]}" >/dev/null 2>&1; then
  ok "frontend/dist/index.html is present in the workspace tree"
else
  die "The synced tree has no frontend/dist/index.html, so every page of the deployed
       app would answer 503. 'databricks sync' applies .gitignore, and .gitignore
       excludes frontend/dist deliberately, so --include 'frontend/dist' is what puts
       it back. Check that 'cd frontend && npm run build' has produced dist/index.html,
       then re-run."
fi

# Retried, because the failure this hits in practice is not a failure of the
# deployment: it is one wheel out of fifty-odd timing out against the package
# proxy, a different one each time. Under uv that is likelier than under pip,
# since uv installs the whole dependency set into a fresh 3.12 environment
# rather than adding to a pre-warmed 3.11 one -- the cost of getting an
# interpreter this source parses on.
#
# The retry is deliberately narrow. Only a transport-shaped failure is retried;
# a version conflict or a package that will not build fails the same way every
# time, and re-running it four times would only delay the report. The pattern
# is matched against the build log rather than the CLI's output, because the CLI
# says no more than "the deployment reached FAILED, here is how to read the
# logs".
DEPLOY_ATTEMPTS=4
DEPLOY_OK=0
for attempt in $(seq 1 $DEPLOY_ATTEMPTS); do
  if ((attempt == 1)); then
    info "deploying"
  else
    info "retrying the deploy (attempt $attempt of $DEPLOY_ATTEMPTS)"
  fi

  if OUT="$(databricks apps deploy "$APP_NAME" --source-code-path "$WORKSPACE_SRC" \
    "${DATABRICKS_ARGS[@]}" 2>&1)"; then
    ok "deployment accepted"
    DEPLOY_OK=1
    break
  fi

  BUILD_LOG="$(app_logs 60 2>/dev/null | grep -v 'Updated file' || true)"

  TRANSIENT=0
  case "$BUILD_LOG" in
    *"operation timed out"* | *"error sending request"* | *"Request failed after"* | \
      *"Connection reset"* | *"Temporary failure in name resolution"*) TRANSIENT=1 ;;
  esac

  if ((TRANSIENT == 1)) && ((attempt < DEPLOY_ATTEMPTS)); then
    warn "a package download timed out against the workspace's proxy, which is transient"
    # uv keeps no cache between deployments, so a retry re-downloads everything
    # and hits a different wheel; the pause is there so a proxy that is briefly
    # unwell has a moment to recover rather than being hit again immediately.
    sleep 15
    continue
  fi

  say ""
  say "  ${BOLD}The build failed.${RESET} $(printf '%s' "$OUT" | tail -3)"
  say ""
  say "  Build log:"
  printf '%s\n' "$BUILD_LOG" | tail -20 | sed 's/^/    /' || say "    (logs unavailable)"
  say ""
  if ((TRANSIENT == 1)); then
    say "  All $DEPLOY_ATTEMPTS attempts failed to download packages. That is an outage"
    say "  on the workspace's package proxy rather than anything wrong with this tree:"
    say "  re-run when it is back."
    say ""
    # Measured, not assumed. A failed build *does* become active_deployment and
    # the app goes to UNAVAILABLE, serving 502 at the front door -- Databricks
    # Apps has no notion of keeping the last good deployment live. So a proxy
    # outage is not a no-op that leaves yesterday's app running: it takes the app
    # down. Saying otherwise, as an earlier version of this message did, would
    # send the operator away believing the demo still works.
    say "  ${BOLD}This has taken the app down.${RESET} A failed deployment replaces the"
    say "  previous one, so the app is now UNAVAILABLE and its URL answers 502."
    say "  There is no rollback: re-running this is the way back up."
  else
    say "  This is not a transport error, so re-running it will fail the same way."
    say "  Fix the dependency problem above and re-run."
  fi
  die "databricks apps deploy failed"
done
((DEPLOY_OK == 1)) || die "databricks apps deploy failed"

# A new deployment restarts the app, so this matters only when the secret moved
# and the source did not. Doing it unconditionally costs a minute and removes
# the whole class of "the seal moved but the process did not".
info "restarting so the new secret value is read at startup"
databricks apps stop "$APP_NAME" "${DATABRICKS_ARGS[@]}" >/dev/null 2>&1 ||
  info "stop reported nothing to stop"
if OUT="$(databricks apps start "$APP_NAME" "${DATABRICKS_ARGS[@]}" 2>&1)"; then
  ok "restart requested"
else
  warn "apps start returned an error: $(printf '%s' "$OUT" | tail -2)"
fi

# A deploy that reports success while the container crashes on startup is worse
# than no automation, and this app has exactly that failure mode. Poll for the
# real state, and on failure show the actual startup exception.
banner "Verifying the app actually starts"

DEADLINE=$((SECONDS + 600))
FINAL_COMPUTE="UNKNOWN"
FINAL_DEPLOY="UNKNOWN"
APP_STATE_JSON='{}'
APP_URL=""
while ((SECONDS < DEADLINE)); do
  APP_STATE_JSON="$(databricks apps get "$APP_NAME" "${DATABRICKS_ARGS[@]}" 2>/dev/null || echo '{}')"
  FINAL_COMPUTE="$(printf '%s' "$APP_STATE_JSON" | jq -r '.compute_status.state // "UNKNOWN"')"
  FINAL_DEPLOY="$(printf '%s' "$APP_STATE_JSON" | jq -r '.active_deployment.status.state // "NONE"')"
  APP_URL="$(printf '%s' "$APP_STATE_JSON" | jq -r '.url // empty')"
  if [[ "$FINAL_COMPUTE" == "ACTIVE" ]] &&
    [[ "$FINAL_DEPLOY" == "SUCCEEDED" || "$FINAL_DEPLOY" == "FAILED" ]]; then
    break
  fi
  [[ "$FINAL_COMPUTE" == "ERROR" || "$FINAL_COMPUTE" == "STOPPED" ]] && break
  printf '  ...   compute %s, deployment %s\n' "$FINAL_COMPUTE" "$FINAL_DEPLOY"
  sleep 15
done

APP_MESSAGE="$(printf '%s' "$APP_STATE_JSON" | jq -r '.compute_status.message // empty')"
DEPLOY_MESSAGE="$(printf '%s' "$APP_STATE_JSON" | jq -r '.active_deployment.status.message // empty')"

SERVING=0
if [[ "$FINAL_COMPUTE" == "ACTIVE" && "$FINAL_DEPLOY" == "SUCCEEDED" && -n "$APP_URL" ]]; then
  # compute ACTIVE and deployment SUCCEEDED describe the platform's view: the
  # container was scheduled and the build finished. Neither says the process
  # inside it is answering, and this app has a whole class of failures --
  # an unservable seal, a source tree that will not import -- that look exactly
  # like a clean deploy from the outside and 502 from the front door. So the
  # gate is an actual request.
  #
  # `databricks auth token` only supports U2M, so the app's front door needs a
  # token minted from the client credentials directly.
  info "asking the app itself whether it is serving"
  APP_TOKEN="$(curl -fsS --max-time 30 \
    -u "$DATABRICKS_CLIENT_ID:$DATABRICKS_CLIENT_SECRET" \
    -d 'grant_type=client_credentials&scope=all-apis' \
    "$DATABRICKS_HOST/oidc/v1/token" 2>/dev/null | jq -r '.access_token // empty')"
  if [[ -z "$APP_TOKEN" ]]; then
    warn "could not mint a token to probe $APP_URL, so the HTTP check was skipped.
        Check the app in a browser before trusting this deploy."
    SERVING=1
  else
    HTTP_DEADLINE=$((SECONDS + 180))
    while ((SECONDS < HTTP_DEADLINE)); do
      HTTP_BODY="$(curl -fsS --max-time 30 -H "Authorization: Bearer $APP_TOKEN" \
        "$APP_URL/api/health" 2>/dev/null || true)"
      if printf '%s' "$HTTP_BODY" | jq -e '.status' >/dev/null 2>&1; then
        SERVING=1
        ok "GET /api/health returned $(printf '%s' "$HTTP_BODY" | jq -c '.')"
        break
      fi
      printf '  ...   front door not answering yet\n'
      sleep 10
    done
    unset APP_TOKEN
  fi
fi

if [[ "$FINAL_COMPUTE" == "ACTIVE" && "$FINAL_DEPLOY" == "SUCCEEDED" ]] && ((SERVING == 1)); then
  ok "compute ACTIVE, deployment SUCCEEDED"
  [[ -n "$DEPLOY_MESSAGE" ]] && info "$DEPLOY_MESSAGE"

  # Record what was pushed so the next run can tell whether the seal has moved.
  LIVE_MS="$(databricks secrets list-secrets "$SECRET_SCOPE" "${DATABRICKS_ARGS[@]}" 2>/dev/null |
    jq -r --arg k "$SECRET_MANIFEST_KEY" \
      'if type=="array" then .[] else (.secrets // [])[] end
       | select(.key==$k) | .last_updated_timestamp // empty' 2>/dev/null || true)"
  # $SEAL_SHA and not a fresh read of manifest.json: this record's whole purpose
  # is to say which seal the app is serving, and the app is serving the snapshot
  # that was published above. Re-reading the live file here would record whatever
  # it says now, which after a concurrent write is a seal nobody ever pushed --
  # and the next run reads this field to decide whether a redeploy is needed.
  #
  # Through the environment for the same reason as bootstrap.json: these are
  # externally-sourced strings, and interpolating them into generated Python
  # makes one quotation mark in a path or a scope name a SyntaxError.
  DEPLOY_RECORD="$DEPLOY_RECORD" \
    APP_NAME="$APP_NAME" \
    APP_CLIENT_ID="${DATABRICKS_APP_CLIENT_ID:-}" \
    SECRET_SCOPE="$SECRET_SCOPE" \
    SECRET_MANIFEST_KEY="$SECRET_MANIFEST_KEY" \
    MANIFEST_PATH="$ANTI_DEMO_MANIFEST" \
    SEAL_SHA="$SEAL_SHA" \
    LIVE_MS="${LIVE_MS:-}" \
    WORKSPACE_SRC="$WORKSPACE_SRC" \
    DEPLOYED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    python3 - <<'PY' || die "could not record the deployed seal in $DEPLOY_RECORD"
import json, os, pathlib
path = pathlib.Path(os.environ["DEPLOY_RECORD"])
path.write_text(json.dumps({
    "app_name": os.environ["APP_NAME"],
    "app_client_id": os.environ["APP_CLIENT_ID"],
    "secret_scope": os.environ["SECRET_SCOPE"],
    "secret_key": os.environ["SECRET_MANIFEST_KEY"],
    "manifest": os.environ["MANIFEST_PATH"],
    "manifest_sha256": os.environ["SEAL_SHA"],
    "secret_updated_ms": os.environ["LIVE_MS"],
    "workspace_source": os.environ["WORKSPACE_SRC"],
    "deployed_at": os.environ["DEPLOYED_AT"],
}, indent=2) + "\n", encoding="utf-8")
path.chmod(0o600)
PY
  ok "recorded the deployed seal in $DEPLOY_RECORD"
  banner "Deployed"
  say "  $APP_URL"
  say ""
  say "  The app reads its seal from a secret, not from a file, so anything that"
  say "  rewrites the manifest -- setup, resume, renew -- leaves it a generation"
  say "  behind until you run:"
  say "      ./bootstrap.sh --deploy-only"
  say "  './bootstrap.sh' on its own reports that drift without changing anything."
else
  say ""
  if [[ "$FINAL_COMPUTE" == "ACTIVE" && "$FINAL_DEPLOY" == "SUCCEEDED" ]]; then
    say "  ${BOLD}The deploy succeeded and the app is not serving.${RESET}"
    say "  Databricks reports compute ACTIVE and the deployment SUCCEEDED, so the"
    say "  container was scheduled and built. It is the process inside it that is"
    say "  not answering, which means the logs below are the only useful evidence."
  else
    say "  ${BOLD}The app did not come up.${RESET} compute=$FINAL_COMPUTE deployment=$FINAL_DEPLOY"
  fi
  [[ -n "$APP_MESSAGE" ]] && say "  compute:    $APP_MESSAGE"
  [[ -n "$DEPLOY_MESSAGE" ]] && say "  deployment: $DEPLOY_MESSAGE"
  say ""
  say "  Last 60 log lines:"
  app_logs 60 | sed 's/^/    /' || say "    (logs unavailable)"
  say ""
  say "  An InvalidStateError above means the seal in the secret is not servable:"
  say "  app.py raises inside the FastAPI lifespan when the manifest status is not"
  say "  READY, and the container never finishes starting. Check './antidemo doctor',"
  say "  then republish with './bootstrap.sh --deploy-only'."
  say ""
  say "  A SyntaxError above means the source does not parse on the runtime's Python."
  say "  Databricks Apps installs with pip on 3.11 whenever a requirements.txt is in"
  say "  the deployed tree, whatever pyproject.toml asks for, and only uses uv and"
  say "  honours requires-python when that file is absent and uv.lock is present."
  die "the deploy reported success but the app is not serving"
fi
fi # DEPLOY_APP

if [[ "$MODE" == "apply" ]]; then
banner "Provisioned"
cat <<NEXT
  Local UI:   ./antidemo serve   then http://127.0.0.1:8000/
  Long-lived: ./antidemo serve --background   then   ./antidemo status
              (detached into its own session, so closing this terminal does not
               take it down; nohup is not enough. Logs to server-8000.log beside
               the manifest, rolled at 8 MiB.)
  Inspect:    ./antidemo doctor
  Stop spend: ./antidemo cleanup --dry-run   then   ./antidemo cleanup --yes
NEXT
if ((DEPLOY_APP == 0)); then
  cat <<'NEXT'

  The Databricks App was not deployed. Add --deploy-app to do both in one run,
  or run './bootstrap.sh --deploy-only' now. docs/DEPLOY.md explains what the
  deploy publishes and why the seal goes stale without it.
NEXT
fi
fi
