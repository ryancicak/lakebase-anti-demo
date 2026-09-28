#!/usr/bin/env bash
# The release bar as one command, against a separate test installation:
#
#   preflight  the app serves the checkout's commit, clean, and every round is READY
#   chaos      every wave with all six rounds at once against Aurora, then against
#              RDS, then each round alone: against Aurora, and against RDS too for
#              the rounds that race an AWS lane
#   restart    redeploy the same build under live Round 2, 3 and 5 bouts; every
#              round must heal by itself
#   leaks      no per-bout AWS resource is left behind
#   lease      the app kept its own lease alive, and `terraform plan` is empty
#
# Usage: scripts/release_bar/run.sh CHECKOUT
#
#   CHECKOUT is the directory the test installation was made from. It holds that
#   installation's .env.bootstrap and .anti-demo-v*/ manifest, and the restart
#   step runs its ./bootstrap.sh --deploy-only, so it must be at the commit under
#   test with no uncommitted changes.
#
#   ANTI_DEMO_APP_URL  the test installation's app URL (required)
#   ANTI_DEMO_PROFILE  a Databricks CLI profile that can reach that app (required)
#   EVIDENCE           where to write (default: release-bar-evidence/<UTC time>)
#   STEPS              which steps to run (default: chaos,restart,leaks,lease)
#   PYTHON             an interpreter with this repository's dependencies
#                      (default: CHECKOUT/.venv/bin/python)
#
# It takes about seven hours and holds every round the whole time, so never point
# it at an installation someone is presenting from. `touch EVIDENCE/PAUSE` holds
# the next chaos wave; removing the file lets it go. Exit 0 only when every step
# passed. EVIDENCE/summary.md has the verdict and EVIDENCE/logs/ every step's output.
# docs/RELEASING.md has the rest of the bar and how to read a failure.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHECKOUT="${1:-}"
[[ -n "$CHECKOUT" && -d "$CHECKOUT" ]] || {
  echo "usage: scripts/release_bar/run.sh CHECKOUT (see the comment at the top of this file)" >&2
  exit 2
}
CHECKOUT="$(cd "$CHECKOUT" && pwd)"
: "${ANTI_DEMO_APP_URL:?set ANTI_DEMO_APP_URL to the app URL of the test installation}"
: "${ANTI_DEMO_PROFILE:?set ANTI_DEMO_PROFILE to a Databricks CLI profile that can reach it}"
export ANTI_DEMO_APP_URL ANTI_DEMO_PROFILE
PYTHON="${PYTHON:-$CHECKOUT/.venv/bin/python}"
STEPS="${STEPS:-chaos,restart,leaks,lease}"
EVIDENCE="${EVIDENCE:-release-bar-evidence/$(date -u +%Y%m%dT%H%M%SZ)}"

WAVES="pre-bell-cancel,early-towel,mid-stage-towel,late-towel,rapid-rearm-towel,finish-linger"
ROUNDS=(
  wake_idle_app
  make_schema_change_safely
  recover_deleted_order
  put_model_score_in_app
  survive_connection_spike
  analyze_live_orders_without_slowing_checkout
)
# The rounds whose bout races an AWS lane, so running alone against each competitor
# tests something different. Rounds 4 and 6 race Lakebase alone for now; add them
# here when they gain an AWS lane.
AWS_LANE_ROUNDS=(wake_idle_app make_schema_change_safely recover_deleted_order survive_connection_spike)
# The three rounds that hold per-bout AWS resources mid-race, and so can leak them.
RESTART_ROUNDS=(make_schema_change_safely recover_deleted_order survive_connection_spike)
RESTART_AFTER_BELL=90
# How long a round may take to come back READY before the next step starts.
READY_WAIT_SECONDS=2400

[[ -x "$PYTHON" ]] || { echo "no Python at $PYTHON; set PYTHON" >&2; exit 2; }
[[ -f "$CHECKOUT/.env.bootstrap" && -x "$CHECKOUT/bootstrap.sh" ]] || {
  echo "$CHECKOUT has no .env.bootstrap and bootstrap.sh, so it is not an installation" >&2
  exit 2
}
if [[ -e "$EVIDENCE" ]]; then
  echo "$EVIDENCE already exists; each run writes to a directory of its own" >&2
  exit 2
fi
mkdir -p "$EVIDENCE/logs" "$EVIDENCE/chaos" || exit 2
EVIDENCE="$(cd "$EVIDENCE" && pwd)"
: >"$EVIDENCE/steps.tsv"

now() { date -u +%Y-%m-%dT%H:%M:%SZ; }
say() { printf 'release-bar %s %s\n' "$(date -u +%H:%M:%SZ)" "$*" | tee -a "$EVIDENCE/run.log"; }
record() { printf '%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$(now)" >>"$EVIDENCE/steps.tsv"; }
want() { [[ ",$STEPS," == *",$1,"* ]]; }
wait_ready() { "$PYTHON" "$HERE/ready.py" --wait "$READY_WAIT_SECONDS" >>"$EVIDENCE/logs/ready.log" 2>&1; }

preflight() {
  local started head
  started="$(now)"
  head="$(git -C "$CHECKOUT" rev-parse HEAD 2>/dev/null)" || {
    say "preflight: $CHECKOUT is not a git checkout"
    record preflight 2 "$started"
    return 1
  }
  if [[ -n "$(git -C "$CHECKOUT" status --porcelain --untracked-files=no)" ]]; then
    say "preflight: $CHECKOUT has uncommitted changes, and the restart step would deploy them"
    record preflight 2 "$started"
    return 1
  fi
  if ! "$PYTHON" "$HERE/ready.py" --expect-commit "$head" --wait "$READY_WAIT_SECONDS" \
    >"$EVIDENCE/logs/preflight.log" 2>&1; then
    say "preflight failed:"
    tee -a "$EVIDENCE/run.log" <"$EVIDENCE/logs/preflight.log"
    record preflight 1 "$started"
    return 1
  fi
  say "preflight: the app serves ${head:0:12}, every round READY"
  record preflight 0 "$started"
}

# chaos_phase NAME [VAR=value ...]: one run of chaos.py with those settings.
chaos_phase() {
  local name="$1" started code
  shift
  started="$(now)"
  if ! wait_ready; then
    say "chaos-$name: the rounds never all came back READY; stopping"
    record "chaos-$name" 3 "$started"
    return 1
  fi
  say "chaos-$name start"
  env EVIDENCE_DIR="$EVIDENCE/chaos/$name" CHAOS_QUIET=1 CHAOS_PAUSE_FILE="$EVIDENCE/PAUSE" \
    CHAOS_SCENARIOS="$WAVES" CHAOS_LINGER_SECONDS=300 "$@" \
    "$PYTHON" -u "$HERE/chaos.py" >"$EVIDENCE/logs/chaos-$name.log" 2>&1
  code=$?
  record "chaos-$name" "$code" "$started"
  say "chaos-$name exit=$code: $("$PYTHON" "$HERE/summarize.py" --phase "$EVIDENCE/chaos/$name")"
}

chaos() {
  local round
  chaos_phase all-aurora CHAOS_COMPETITOR=aurora_serverless_v2 || return 1
  chaos_phase all-rds CHAOS_COMPETITOR=rds_postgres || return 1
  for round in "${ROUNDS[@]}"; do
    chaos_phase "$round-aurora" CHAOS_ROUNDS="$round" CHAOS_COMPETITOR=aurora_serverless_v2 ||
      return 1
    if [[ " ${AWS_LANE_ROUNDS[*]} " == *" $round "* ]]; then
      chaos_phase "$round-rds" CHAOS_ROUNDS="$round" CHAOS_COMPETITOR=rds_postgres || return 1
    fi
  done
}

restart() {
  local started dir pid code
  started="$(now)"
  dir="$EVIDENCE/restart"
  if ! wait_ready; then
    say "restart: the rounds never all came back READY; stopping"
    record restart 3 "$started"
    return 1
  fi
  say "restart: bouts on ${RESTART_ROUNDS[*]}, redeploy ${RESTART_AFTER_BELL}s after the bell"
  "$PYTHON" -u "$HERE/restart.py" --after-bell "$RESTART_AFTER_BELL" "$dir" "${RESTART_ROUNDS[@]}" \
    >"$EVIDENCE/logs/restart.log" 2>&1 &
  pid=$!
  until [[ -f "$dir/READY_FOR_RESTART" ]] || ! kill -0 "$pid" 2>/dev/null; do sleep 2; done
  if [[ -f "$dir/READY_FOR_RESTART" ]]; then
    # From the shell, not from restart.py: launched from a Python subprocess,
    # bootstrap.sh fails its Databricks identity check.
    say "restart: redeploying the same build"
    if (cd "$CHECKOUT" && ./bootstrap.sh --deploy-only --yes) \
      >"$EVIDENCE/logs/restart-deploy.log" 2>&1; then
      say "restart: redeployed; waiting for every round to heal"
    else
      say "restart: the redeploy failed; see logs/restart-deploy.log"
      : >"$dir/DEPLOY_FAILED"
    fi
  fi
  wait "$pid"
  code=$?
  record restart "$code" "$started"
  say "restart exit=$code"
}

leaks() {
  local started code
  started="$(now)"
  wait_ready
  # AWS can still be finishing a deletion that began before READY.
  "$PYTHON" "$HERE/leftovers.py" "$CHECKOUT" --wait 1800 >"$EVIDENCE/logs/leaks.log" 2>&1
  code=$?
  record leaks "$code" "$started"
  say "leaks exit=$code: $(tail -1 "$EVIDENCE/logs/leaks.log")"
}

lease() {
  local started code
  started="$(now)"
  "$PYTHON" "$HERE/lease_check.py" "$CHECKOUT" --plan >"$EVIDENCE/logs/lease.log" 2>&1
  code=$?
  record lease "$code" "$started"
  say "lease exit=$code: $(tail -1 "$EVIDENCE/logs/lease.log")"
}

say "release bar against $ANTI_DEMO_APP_URL from $CHECKOUT; evidence in $EVIDENCE"
if preflight; then
  stopped=0
  if want chaos; then chaos || stopped=1; fi
  if want restart && [[ "$stopped" == 0 ]]; then restart || stopped=1; fi
  if want leaks; then leaks; fi
  if want lease; then lease; fi
fi
"$PYTHON" "$HERE/summarize.py" "$EVIDENCE" | tee -a "$EVIDENCE/run.log"
exit "${PIPESTATUS[0]}"
