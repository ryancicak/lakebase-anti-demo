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
# Idempotent: existing policies are reused, existing attachments are left alone.
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
  if aws iam get-policy --policy-arn "$arn" "${PROFILE_ARGS[@]+"${PROFILE_ARGS[@]}"}" >/dev/null 2>&1; then
    note "policy $name already exists, reusing it"
  else
    aws iam create-policy --policy-name "$name" --policy-document "file://$file" \
      "${PROFILE_ARGS[@]+"${PROFILE_ARGS[@]}"}" --query 'Policy.Arn' --output text >/dev/null
    note "created $name"
  fi
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
