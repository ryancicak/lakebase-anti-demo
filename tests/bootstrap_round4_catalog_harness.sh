#!/usr/bin/env bash
# Exercises bootstrap.sh's Round 4 catalog resolution block in isolation.
#
# bootstrap.sh cannot be driven end-to-end in a test: it demands a Databricks
# service principal secret and real AWS keys before it reaches this block. So the
# block is extracted verbatim from bootstrap.sh -- not copied, extracted, so it
# cannot rot -- and run against a stub `databricks` and a stub manifest. What is
# under test is the resolution order that server/lifecycle.py:_round4_catalog
# implements: a seal outranks the environment, a contradiction is refused by
# name, and a first provision falls back to the module default.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# The block runs from the comment that opens it to the line before the state-key
# comment that follows it.
BLOCK="$WORK/block.sh"
sed -n "/^# Round 4's catalog resolves exactly as/,/^# The state key is per-installation/p" \
  "$REPO/bootstrap.sh" | sed '$d' >"$BLOCK"
grep -q 'ROUND4_DEFAULT_CATALOG' "$BLOCK" ||
  { echo "FAIL: could not extract the Round 4 catalog block from bootstrap.sh"; exit 1; }

# A `databricks` stub whose exit status is whatever VISIBLE_CATALOG says.
mkdir -p "$WORK/bin"
cat >"$WORK/bin/databricks" <<'STUB'
#!/usr/bin/env bash
[[ "$1" == "catalogs" && "$2" == "get" ]] || exit 0
[[ "$3" == "$VISIBLE_CATALOG" ]]
STUB
chmod +x "$WORK/bin/databricks"
PATH="$WORK/bin:$PATH"

DEFAULT_CATALOG="$(sed -n 's/^ROUND4_DEFAULT_CATALOG = "\(.*\)"$/\1/p' "$REPO/server/lifecycle.py")"

FAILURES=0
run_block() { # existing_install sealed_catalog env_catalog visible_catalog
  local existing="$1" sealed="$2" env_catalog="$3" visible="$4"
  local manifest="$WORK/manifest.json"
  if [[ -n "$sealed" ]]; then
    printf '{"round4": {"storage_catalog": "%s"}}\n' "$sealed" >"$manifest"
  else
    printf '{}\n' >"$manifest"
  fi
  (
    cd "$REPO"
    set +e
    # bootstrap.sh refuses through `die` in some steps and `fail` in others, and
    # which one this block uses is not what is under test. Both stubs print the
    # same prefix so an expectation cannot be pinned to the helper's name.
    die() { printf 'REFUSED %s\n' "$*"; exit 1; }
    fail() { printf 'REFUSED %s\n' "$*"; exit 1; }
    info() { printf 'INFO %s\n' "$*"; }
    ok() { printf 'OK %s\n' "$*"; }
    # The block records refusals instead of exiting on them, so that one run can
    # report a bad catalog *and* an ambiguous warehouse *and* a denied AWS probe
    # rather than only the first of the three. bootstrap.sh's preflight gate is
    # what turns a recorded failure into a non-zero exit, and it is deliberately
    # outside this block. What is under test here is unchanged: which catalog is
    # resolved, and that a refusal names the value that caused it.
    fail() { printf 'FAIL %s\n' "$*"; }
    EXISTING_INSTALL="$existing"
    DATABRICKS_OK=1
    ENV_FILE=".env.bootstrap"
    DATABRICKS_PROFILE="stub-profile"
    ANTI_DEMO_MANIFEST="$manifest"
    # bootstrap.sh builds this as (-p PROFILE -o json); it is never empty, and an
    # empty array would trip `set -u` on this machine's bash 3.2.
    DATABRICKS_ARGS=(-p stub-profile -o json)
    VISIBLE_CATALOG="$visible"
    export VISIBLE_CATALOG
    if [[ -n "$env_catalog" ]]; then ROUND4_CATALOG="$env_catalog"; else unset ROUND4_CATALOG; fi
    set -e
    source "$BLOCK"
    printf 'RESOLVED %s\n' "$ROUND4_CATALOG"
  ) 2>&1
}

expect() { # name expected_substring output
  local name="$1" expected="$2" output="$3"
  if [[ "$output" == *"$expected"* ]]; then
    printf 'pass  %s\n' "$name"
  else
    printf 'FAIL  %s\n      wanted: %s\n      got:    %s\n' "$name" "$expected" "${output//$'\n'/ | }"
    FAILURES=$((FAILURES + 1))
  fi
}

expect "a first provision falls back to the module default" \
  "RESOLVED $DEFAULT_CATALOG" \
  "$(run_block 0 '' '' "$DEFAULT_CATALOG")"

expect "a first provision takes ROUND4_CATALOG" \
  "RESOLVED customer_catalog" \
  "$(run_block 0 '' customer_catalog customer_catalog)"

expect "a sealed installation ignores an unset environment" \
  "RESOLVED sealed_catalog" \
  "$(run_block 1 sealed_catalog '' sealed_catalog)"

expect "a sealed installation accepts an environment that agrees" \
  "RESOLVED sealed_catalog" \
  "$(run_block 1 sealed_catalog sealed_catalog sealed_catalog)"

expect "a contradicted seal is refused by name" \
  "FAIL This installation sealed Round 4 into Unity Catalog 'sealed_catalog'" \
  "$(run_block 1 sealed_catalog other_catalog sealed_catalog)"

expect "an invisible catalog is refused before anything is spent" \
  "FAIL Round 4 needs Unity Catalog 'customer_catalog'" \
  "$(run_block 0 '' customer_catalog something_else)"

expect "the refusal names the variable that fixes it" \
  "Set ROUND4_CATALOG to a catalog it can create schemas in" \
  "$(run_block 0 '' customer_catalog something_else)"

# The compiled-in default is `main`, which is likely but not guaranteed, so on a
# workspace without it this is the message an operator actually meets. It has to
# say that the default was nobody's choice, or the operator reads it as "my
# catalog is broken" rather than "I never set this".
expect "an invisible compiled-in default says it was never chosen" \
  "which is the compiled-in" \
  "$(run_block 0 '' '' something_else)"

if ((FAILURES > 0)); then
  printf '\n%d failure(s)\n' "$FAILURES"
  exit 1
fi
printf '\nall Round 4 catalog resolution cases pass\n'
