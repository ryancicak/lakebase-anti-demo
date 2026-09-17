#!/usr/bin/env bash
# Local, deterministic Round 5 pre-deploy acceptance gate.
#
# Proves the *production* Round 5 route end-to-end BEFORE anyone spends a paid
# Aurora/RDS bout. It runs entirely against transactional in-memory fakes and the
# real ASGI control surface:
#
#   * NO AWS calls, NO SSM, NO sockets, NO deployment.
#   * The real Round5WarmCoordinator over the real (CAS) InMemoryRound5WarmStore.
#   * The real RunManager claim/bell/terminal/rewarm orchestration.
#   * The real FastAPI control router driven over ASGI (/api/sessions*).
#   * The real LiveConnectionSpikeSetupOrchestrator lane pipeline.
#
# Every gate is behavioral (state transitions, HTTP responses, call ordering,
# exact client counts, durable generation advancement). None of it asserts on
# source text, so appending a string to a file cannot make a broken route pass.
#
# Usage:
#   scripts/round5_pre_deploy_acceptance.sh            # run the gate
#   scripts/round5_pre_deploy_acceptance.sh -k towel   # forward pytest args
set -euo pipefail
cd "$(dirname "$0")/.."

# Hermetic: local operator (no SSO headers), no deployed-app behavior, no manifest
# read, no coordination network. The harness fakes the only two seams that would
# otherwise reach AWS.
unset DATABRICKS_APP_NAME || true
export ANTI_DEMO_ALLOW_INMEMORY_COORDINATION="${ANTI_DEMO_ALLOW_INMEMORY_COORDINATION:-1}"

# The primary harness plus the real-code behavioral suites that back the same
# route. Deliberately excludes source-string ("does the file contain X") checks:
# those pass for any implementation and are exactly the "fake tests that merely
# append strings" this gate exists to replace.
MODULES=(
  tests/test_round5_pre_deploy_acceptance.py
  tests/test_round5_warm.py
  tests/test_round5_v3_acceptance.py
)

# Prefer `uv run` (the repo's canonical runner); fall back to the project venv.
if command -v uv >/dev/null 2>&1; then
  RUNNER=(uv run --no-sync pytest)
  PYTHON=(uv run --no-sync python)
elif [[ -x .venv/bin/python ]]; then
  RUNNER=(.venv/bin/python -m pytest)
  PYTHON=(.venv/bin/python)
else
  RUNNER=(python -m pytest)
  PYTHON=(python)
fi

echo "== Round 5 pre-deploy acceptance gate =="
echo "runner: ${RUNNER[*]}"
echo "modules:"
printf '  - %s\n' "${MODULES[@]}"
echo

# -p no:cacheprovider keeps the run order stable and side-effect free.
status=0
"${RUNNER[@]}" -p no:cacheprovider -o addopts="" "${MODULES[@]}" "$@" || status=$?

echo
if [[ $status -eq 0 ]]; then
  echo "== ACCEPTANCE CHECKLIST (gate -> proving test) =="
  "${PYTHON[@]}" - <<'PY'
from tests.test_round5_pre_deploy_acceptance import ACCEPTANCE_CHECKLIST
for gate, test in ACCEPTANCE_CHECKLIST.items():
    print(f"  [PASS] {gate:<30} -> {test}")
PY
  echo
  echo "ROUND 5 PRE-DEPLOY GATE: PASS  (safe to spend a paid bout)"
else
  echo "ROUND 5 PRE-DEPLOY GATE: FAIL  (do NOT deploy or ring a paid bout)"
fi
exit $status
