#!/usr/bin/env bash
# Run a probe script with the same credentials the launcher gives the server.
#
# `./antidemo` sources .env.bootstrap in a subshell and carries exactly five AWS names
# across, deliberately, so nothing else in that file can leak into a process. This mirrors
# that and nothing more: same five names, same subshell, no echo of any value.
set -euo pipefail
cd "$(dirname "$0")"

ENV_FILE="${ANTI_DEMO_ENV_FILE:-.env.bootstrap}"
[[ -f "$ENV_FILE" ]] || { echo "no $ENV_FILE" >&2; exit 1; }

for name in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_DEFAULT_REGION AWS_REGION; do
  [[ -n "${!name:-}" ]] && continue
  value="$(
    set +u
    # shellcheck disable=SC1090
    . "$ENV_FILE" >/dev/null 2>&1 || true
    printf '%s' "${!name:-}"
  )"
  [[ -n "$value" ]] && export "$name=$value"
done
unset name value

export AWS_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-west-2}}"
export AWS_DEFAULT_REGION="$AWS_REGION"
export ANTI_DEMO_MANIFEST="${ANTI_DEMO_MANIFEST:-$PWD/.anti-demo-v7/manifest.json}"

# Probes live outside the tree so they are never linted, committed, or published with it.
PROBE="$1"; shift
[[ -f "$PROBE" ]] || PROBE="/tmp/r5-probes/$(basename "$PROBE")"
exec .venv-3.12/bin/python "$PROBE" "$@"
