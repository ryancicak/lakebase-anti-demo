#!/usr/bin/env bash
# Exercises bootstrap.sh's generation-lock block in isolation.
#
# bootstrap.sh cannot be driven end-to-end here: it demands a Databricks service
# principal secret and real AWS keys long before it reaches this block. So the
# block is extracted verbatim -- not copied, extracted, so it cannot rot -- and
# run against a generation that a real, live process is holding.
#
# What is under test is the refusal that today's outage needed: bootstrap.sh must
# not write into, or run `./antidemo setup` against, a generation another process is
# mutating. Also under test is the ordering, because a lock taken after the first
# write would be theatre.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="$(mktemp -d)"
HOLDER_PID=""
cleanup() {
  [[ -n "$HOLDER_PID" ]] && kill -9 "$HOLDER_PID" 2>/dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT

BLOCK="$WORK/block.sh"
sed -n '/^# 7b\. Claim the generation/,/^STATE_FILE=/p' "$REPO/bootstrap.sh" |
  sed '$d' >"$BLOCK"
grep -q 'generation_lock acquire' "$BLOCK" ||
  { echo "FAIL: could not extract the generation-lock block from bootstrap.sh"; exit 1; }

FAILURES=0
expect() { # name expected_substring output
  local name="$1" expected="$2" output="$3"
  if [[ "$output" == *"$expected"* ]]; then
    printf 'pass  %s\n' "$name"
  else
    printf 'FAIL  %s\n      wanted: %s\n      got:    %s\n' \
      "$name" "$expected" "${output//$'\n'/ | }"
    FAILURES=$((FAILURES + 1))
  fi
}
refute() { # name forbidden_substring output
  local name="$1" forbidden="$2" output="$3"
  if [[ "$output" != *"$forbidden"* ]]; then
    printf 'pass  %s\n' "$name"
  else
    printf 'FAIL  %s\n      must not contain: %s\n      got:    %s\n' \
      "$name" "$forbidden" "${output//$'\n'/ | }"
    FAILURES=$((FAILURES + 1))
  fi
}

# A real holder, because the whole question is what the kernel does with two
# open file descriptions. A stub would prove nothing.
hold_generation() { # manifest_path
  # stdout goes to a file, never to this script's stdout: an orphaned holder
  # holding the pipe open would hang whatever is reading this harness's output.
  (cd "$REPO" && exec python3 - "$1" "$WORK" >"$WORK/holder.log" 2>&1 <<'PY'
import sys, time
from pathlib import Path

from server.generation_lock import hold_generation

manifest, work = Path(sys.argv[1]), Path(sys.argv[2])
deadline = time.monotonic() + 120  # never outlive the harness
with hold_generation(manifest, "antidemo reset"):
    (work / "holding").write_text("yes")
    while not (work / "stop").exists() and time.monotonic() < deadline:
        time.sleep(0.02)
PY
  ) &
  HOLDER_PID=$!
  for _ in $(seq 1 500); do
    [[ -f "$WORK/holding" ]] && return 0
    sleep 0.02
  done
  echo "FAIL: the holder never took the lock"
  cat "$WORK/holder.log" 2>/dev/null
  exit 1
}
stop_holding() {
  : >"$WORK/stop"
  wait "$HOLDER_PID" 2>/dev/null || true
  HOLDER_PID=""
  rm -f "$WORK/holding" "$WORK/stop"
}

run_block() { # mode generation_dir
  local mode="$1" dir="$2"
  (
    cd "$REPO"
    set +e
    die() { printf 'DIE %s\n' "$*"; exit 1; }
    ok() { printf 'OK %s\n' "$*"; }
    warn() { printf 'WARN %s\n' "$*"; }
    MODE="$mode"
    MANIFEST_DIR="$dir"
    ANTI_DEMO_MANIFEST="$dir/manifest.json"
    set -e
    source "$BLOCK"
    printf 'HELD=%s TOKEN=%s\n' \
      "$GENERATION_LOCK_HELD" "${ANTI_DEMO_GENERATION_LOCK_TOKEN:+set}"
  ) 2>&1 || true # a refusal is a `die`, and a die is the result under test
}

# --- a free generation ------------------------------------------------------
FREE="$WORK/.anti-demo-v7"
mkdir -p "$FREE"
FREE_OUT="$(run_block apply "$FREE")"
expect "an unheld generation is claimed" "HELD=1" "$FREE_OUT"
expect "and reports the lock it holds" "OK holding the generation lock" "$FREE_OUT"
expect "and exports the token that lets ./antidemo setup join it" "TOKEN=set" "$FREE_OUT"

# --- a generation somebody else is mutating ---------------------------------
BUSY="$WORK/.anti-demo-busy"
mkdir -p "$BUSY"
hold_generation "$BUSY/manifest.json"

APPLY_OUT="$(run_block apply "$BUSY")"
expect "--apply refuses a generation being mutated" \
  "DIE ANOTHER PROCESS IS MUTATING THIS GENERATION" "$APPLY_OUT"
expect "and names the operation that holds it" "antidemo reset" "$APPLY_OUT"
expect "and names the pid" "pid $HOLDER_PID" "$APPLY_OUT"
expect "and says reads still work" "./antidemo status" "$APPLY_OUT"
expect "and says not to delete the lock file" "Do not delete the lock file" "$APPLY_OUT"
refute "and never claims to hold it" "HELD=1" "$APPLY_OUT"

DEPLOY_OUT="$(run_block deploy "$BUSY")"
expect "--deploy-only refuses it too" \
  "DIE ANOTHER PROCESS IS MUTATING THIS GENERATION" "$DEPLOY_OUT"

# Check mode writes nothing in any case, so a busy generation costs it nothing
# but the knowledge that it was reading a moving target. It must still say so:
# every value it reported above was read from a manifest being rewritten.
CHECK_OUT="$(run_block check "$BUSY")"
expect "check mode continues read-only" "HELD=0" "$CHECK_OUT"
expect "and says what the holder means for what it just reported" \
  "WARN this generation is being mutated" "$CHECK_OUT"
expect "and names the moving target" "moving target" "$CHECK_OUT"
refute "and does not die" "DIE " "$CHECK_OUT"

stop_holding

RECOVERED_OUT="$(run_block apply "$BUSY")"
expect "the generation is claimable again once the holder is gone" \
  "HELD=1" "$RECOVERED_OUT"

# --- a killed holder leaves nothing behind ----------------------------------
KILLED="$WORK/.anti-demo-killed"
mkdir -p "$KILLED"
hold_generation "$KILLED/manifest.json"
kill -9 "$HOLDER_PID" 2>/dev/null || true
wait "$HOLDER_PID" 2>/dev/null || true
HOLDER_PID=""
rm -f "$WORK/holding" "$WORK/stop"
KILLED_OUT="$(run_block apply "$KILLED")"
expect "a SIGKILLed holder does not wedge the next run" "HELD=1" "$KILLED_OUT"
[[ -f "$KILLED/mutation.lock" ]] ||
  { echo "FAIL: the lock file should still exist; only the lock is gone"; FAILURES=$((FAILURES + 1)); }

# --- ordering ---------------------------------------------------------------
# A lock taken after the first write into the generation would not have
# prevented today's outage.
line_of() { grep -Fn -- "$1" "$REPO/bootstrap.sh" | head -1 | cut -d: -f1 || true; }
ACQUIRE="$(line_of 'generation_lock acquire')"
for anchor in 'if ((GENERATION_LOCK_HELD == 1)) && [[ "$MODE" != "check" ]]; then' \
  './antidemo "${SETUP_ARGS[@]}"' \
  'cp "$ANTI_DEMO_MANIFEST" "$SEAL_SNAPSHOT"' \
  'DEPLOY_RECORD="$DEPLOY_RECORD" \' \
  'databricks apps deploy "$APP_NAME"'; do
  WRITE="$(line_of "$anchor")"
  if [[ -z "$WRITE" ]]; then
    printf 'FAIL  ordering anchor vanished from bootstrap.sh: %s\n' "$anchor"
    FAILURES=$((FAILURES + 1))
  elif ((ACQUIRE < WRITE)); then
    printf 'pass  the lock is claimed before %s (line %s < %s)\n' "$anchor" "$ACQUIRE" "$WRITE"
  else
    printf 'FAIL  %s at line %s happens before the lock at line %s\n' "$anchor" "$WRITE" "$ACQUIRE"
    FAILURES=$((FAILURES + 1))
  fi
done

if ((FAILURES > 0)); then
  printf '\n%d failure(s)\n' "$FAILURES"
  exit 1
fi
printf '\nall bootstrap generation-lock cases pass\n'
