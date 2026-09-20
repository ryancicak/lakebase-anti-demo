#!/usr/bin/env bash
# Render, create and attach the three required operator policies from docs/iam/.
#
# `bootstrap.sh` cannot do this itself: creating and attaching IAM policies needs
# iam:CreatePolicy and iam:AttachUserPolicy, which is strictly more privilege than
# the operator set being granted. Any principal able to run this already has
# enough to provision. So this is a one-time administrator step, and it exists as
# a script rather than as prose in docs/iam/README.md so that the eight commands
# cannot be mistranscribed.
#
# You only need this if the IAM user pair in .env.bootstrap does NOT already hold
# the permissions listed under Prerequisites in README.md. An operator using an
# administrator pair needs nothing here.
#
# Usage:
#   scripts/attach-operator-policies.sh --user <iam-user> [--account ID] [--region R] [--profile P]
#   scripts/attach-operator-policies.sh --role <iam-role> [...]
#
# Idempotent, and now correct when re-run after docs/iam changes: an existing
# policy is updated in place -- a new default version -- only when its live
# document has drifted from the rendered one; existing attachments are left alone.
set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
PRINCIPAL_KIND=""
PRINCIPAL_NAME=""
ACCOUNT=""
REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-}}"
PROFILE_ARGS=()
INCLUDE_STATE=0
STATE_BUCKET=""

die() { printf 'ERROR %s\n' "$*" >&2; exit 1; }
note() { printf '  %s\n' "$*"; }

while (($#)); do
  case "$1" in
    --user) PRINCIPAL_KIND=user; PRINCIPAL_NAME="${2:?--user needs a name}"; shift 2 ;;
    --role) PRINCIPAL_KIND=role; PRINCIPAL_NAME="${2:?--role needs a name}"; shift 2 ;;
    --account) ACCOUNT="${2:?--account needs an id}"; shift 2 ;;
    --region) REGION="${2:?--region needs a region}"; shift 2 ;;
    --profile) PROFILE_ARGS=(--profile "${2:?--profile needs a name}"); shift 2 ;;
    --state-bucket) INCLUDE_STATE=1; STATE_BUCKET="${2:?--state-bucket needs a bucket}"; shift 2 ;;
    -h|--help) sed -n '2,22p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown argument $1" ;;
  esac
done

[[ -n "$PRINCIPAL_KIND" ]] || die "one of --user or --role is required"

command -v aws >/dev/null 2>&1 || die "the AWS CLI is not on PATH"
# Required to compare a live policy document against the rendered one. Without it
# `reconcile_policy` cannot tell a drifted policy from a current one and would
# fall back to reusing the stale document -- the exact bug this script now fixes.
# jq is already a project prerequisite (server/lifecycle.py:doctor checks it).
command -v jq >/dev/null 2>&1 || die "jq is not on PATH (required to compare policy documents)"

# Derive rather than ask: the account must be the one these credentials belong
# to, and guessing it wrong would render policies that grant nothing.
DERIVED_ACCOUNT="$(aws sts get-caller-identity "${PROFILE_ARGS[@]+"${PROFILE_ARGS[@]}"}" \
  --query Account --output text 2>/dev/null || true)"
[[ -n "$DERIVED_ACCOUNT" && "$DERIVED_ACCOUNT" != None ]] \
  || die "could not call sts:GetCallerIdentity; check your credentials or --profile"
if [[ -n "$ACCOUNT" && "$ACCOUNT" != "$DERIVED_ACCOUNT" ]]; then
  die "--account $ACCOUNT does not match the caller's account $DERIVED_ACCOUNT"
fi
ACCOUNT="$DERIVED_ACCOUNT"
[[ -n "$REGION" ]] || die "no region: pass --region, or set AWS_REGION/AWS_DEFAULT_REGION"

printf 'Rendering docs/iam operator policies for account %s in %s\n' "$ACCOUNT" "$REGION"

RENDER_DIR="$(mktemp -d)"
trap 'rm -rf "$RENDER_DIR"' EXIT

declare -a POLICY_NAMES=()
declare -a POLICY_FILES=()

render() {
  local source="$1" rendered="$RENDER_DIR/$(basename "$1")"
  [[ -f "$source" ]] || die "missing policy document $source"
  sed -e "s/<AWS_ACCOUNT_ID>/$ACCOUNT/g" -e "s/<AWS_REGION>/$REGION/g" "$source" > "$rendered"
  if grep -q '<AWS_ACCOUNT_ID>\|<AWS_REGION>' "$rendered"; then
    die "placeholders survived rendering in $rendered"
  fi
  printf '%s' "$rendered"
}

# IAM caps a customer-managed policy at five versions, so a policy that is updated
# in place will eventually refuse a sixth with LimitExceeded. Delete the oldest
# NON-default versions to make room -- never the default, which is the one in
# force. Called only just before a new version is created, and only when the limit
# is actually in the way.
prune_policy_versions() {
  local arn="$1" total oldest
  total="$(aws iam list-policy-versions --policy-arn "$arn" \
    "${PROFILE_ARGS[@]+"${PROFILE_ARGS[@]}"}" \
    --query 'length(Versions)' --output text 2>/dev/null || echo 0)"
  case "$total" in '' | *[!0-9]*) total=0 ;; esac
  while ((total >= 5)); do
    # Versions come back newest-first, so the last non-default is the oldest.
    oldest="$(aws iam list-policy-versions --policy-arn "$arn" \
      "${PROFILE_ARGS[@]+"${PROFILE_ARGS[@]}"}" \
      --query 'Versions[?IsDefaultVersion==`false`]|[-1].VersionId' --output text 2>/dev/null \
      || echo None)"
    [[ -n "$oldest" && "$oldest" != None ]] || break
    aws iam delete-policy-version --policy-arn "$arn" --version-id "$oldest" \
      "${PROFILE_ARGS[@]+"${PROFILE_ARGS[@]}"}"
    note "pruned old version $oldest to stay within IAM's five-version limit"
    total=$((total - 1))
  done
}

# Create the policy if it is absent; otherwise reuse it ONLY if its live default
# version already matches the rendered document. Reusing a named policy whose
# document is stale is the trap this closes: the previous version of this script
# reused it unconditionally, so an operator who re-ran after docs/iam grew a grant
# (SQS, iam:SimulatePrincipalPolicy) kept the old document and then failed the very
# apply the new grant was for. On drift, a new version is created and set as
# default; the old versions are pruned only if the five-version limit is in the way.
#
# AWS CLI v2 URL-decodes the stored policy document into a JSON object, and `jq -cS`
# canonicalises both sides so that key order or whitespace cannot masquerade as
# drift and churn a new version on every run.
reconcile_policy() {
  local name="$1" arn="$2" file="$3" default_version live_doc live_norm rendered_doc
  if ! aws iam get-policy --policy-arn "$arn" \
    "${PROFILE_ARGS[@]+"${PROFILE_ARGS[@]}"}" >/dev/null 2>&1; then
    aws iam create-policy --policy-name "$name" --policy-document "file://$file" \
      "${PROFILE_ARGS[@]+"${PROFILE_ARGS[@]}"}" --query 'Policy.Arn' --output text >/dev/null
    note "created $name"
    return 0
  fi
  default_version="$(aws iam get-policy --policy-arn "$arn" \
    "${PROFILE_ARGS[@]+"${PROFILE_ARGS[@]}"}" \
    --query 'Policy.DefaultVersionId' --output text 2>/dev/null || echo '')"
  live_doc="$(aws iam get-policy-version --policy-arn "$arn" --version-id "$default_version" \
    "${PROFILE_ARGS[@]+"${PROFILE_ARGS[@]}"}" \
    --query 'PolicyVersion.Document' --output json 2>/dev/null || echo '{}')"
  live_norm="$(printf '%s' "$live_doc" | jq -cS . 2>/dev/null || echo 'unparseable-live')"
  # Two ways the comparison cannot be trusted, and both must fall back to reuse
  # rather than re-version -- re-versioning on a comparison we cannot make is the
  # churn (a new default version and a prune on every run) this guards against:
  #   * AWS CLI v1 returns Document URL-encoded, so `jq -cS` yields a JSON *string*
  #     literal (starts with a quote), never the rendered object.
  #   * jq missing or the response unparseable -> the sentinel below.
  case "$live_norm" in
    '"'*)
      note "policy $name exists; this AWS CLI returns its document URL-encoded (v1), so drift"
      note "cannot be checked -- leaving it as-is. Upgrade to AWS CLI v2 to enable drift detection."
      return 0
      ;;
    unparseable-live)
      note "policy $name exists but its live document could not be parsed (is jq present, AWS"
      note "CLI v2?) -- leaving it as-is rather than risk churning versions."
      return 0
      ;;
  esac
  rendered_doc="$(jq -cS . "$file" 2>/dev/null || echo 'unreadable-rendered')"
  [[ "$rendered_doc" == "unreadable-rendered" ]] && die "could not read rendered policy $file"
  if [[ "$live_norm" == "$rendered_doc" ]]; then
    note "policy $name already matches the rendered document, leaving it"
    return 0
  fi
  note "policy $name has drifted from docs/iam; creating a new default version"
  prune_policy_versions "$arn"
  aws iam create-policy-version --policy-arn "$arn" --policy-document "file://$file" \
    --set-as-default "${PROFILE_ARGS[@]+"${PROFILE_ARGS[@]}"}" \
    --query 'PolicyVersion.VersionId' --output text >/dev/null
  note "updated $name to the current document"
}

POLICY_NAMES+=("AntiDemoOperatorNetwork")
POLICY_FILES+=("$(render "$REPO_ROOT/docs/iam/anti-demo-operator-1-network.json")")
POLICY_NAMES+=("AntiDemoOperatorDatabases")
POLICY_FILES+=("$(render "$REPO_ROOT/docs/iam/anti-demo-operator-2-databases.json")")
POLICY_NAMES+=("AntiDemoOperatorIdentity")
POLICY_FILES+=("$(render "$REPO_ROOT/docs/iam/anti-demo-operator-3-identity.json")")

if ((INCLUDE_STATE)); then
  STATE_SRC="$REPO_ROOT/docs/iam/anti-demo-operator-4-state.json"
  STATE_OUT="$RENDER_DIR/anti-demo-operator-4-state.json"
  sed -e "s/<AWS_ACCOUNT_ID>/$ACCOUNT/g" -e "s/<AWS_REGION>/$REGION/g" \
      -e "s/<STATE_BUCKET>/$STATE_BUCKET/g" "$STATE_SRC" > "$STATE_OUT"
  grep -q '<STATE_BUCKET>' "$STATE_OUT" && die "STATE_BUCKET placeholder survived"
  POLICY_NAMES+=("AntiDemoOperatorState")
  POLICY_FILES+=("$STATE_OUT")
fi

for index in "${!POLICY_NAMES[@]}"; do
  name="${POLICY_NAMES[$index]}"
  file="${POLICY_FILES[$index]}"
  arn="arn:aws:iam::$ACCOUNT:policy/$name"
  reconcile_policy "$name" "$arn" "$file"
  if [[ "$PRINCIPAL_KIND" == user ]]; then
    already="$(aws iam list-attached-user-policies --user-name "$PRINCIPAL_NAME" \
      "${PROFILE_ARGS[@]+"${PROFILE_ARGS[@]}"}" \
      --query "length(AttachedPolicies[?PolicyName=='$name'])" --output text 2>/dev/null || echo 0)"
    if [[ "$already" == "0" ]]; then
      aws iam attach-user-policy --user-name "$PRINCIPAL_NAME" --policy-arn "$arn" \
        "${PROFILE_ARGS[@]+"${PROFILE_ARGS[@]}"}"
      note "attached $name to user $PRINCIPAL_NAME"
    else
      note "$name already attached to user $PRINCIPAL_NAME"
    fi
  else
    already="$(aws iam list-attached-role-policies --role-name "$PRINCIPAL_NAME" \
      "${PROFILE_ARGS[@]+"${PROFILE_ARGS[@]}"}" \
      --query "length(AttachedPolicies[?PolicyName=='$name'])" --output text 2>/dev/null || echo 0)"
    if [[ "$already" == "0" ]]; then
      aws iam attach-role-policy --role-name "$PRINCIPAL_NAME" --policy-arn "$arn" \
        "${PROFILE_ARGS[@]+"${PROFILE_ARGS[@]}"}"
      note "attached $name to role $PRINCIPAL_NAME"
    else
      note "$name already attached to role $PRINCIPAL_NAME"
    fi
  fi
done

cat <<'DONE'

Attached. IAM is eventually consistent, so the first ./bootstrap.sh may still
report a denied probe or two for up to a minute. Re-run it before changing
anything; the policy documents are correct.
DONE
