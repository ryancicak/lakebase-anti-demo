#!/usr/bin/env bash
# Run every CI job locally, then commit and push only if all of them pass.
#
# Mirrors .github/workflows/ci.yml: publication guards, the Python matrix on 3.12
# and 3.14 with ruff, the slow bootstrap stub matrix, and the frontend job. CI
# asserts the runner holds no cloud credentials, so every Python leg here runs
# with them unset -- otherwise a test that is supposed to prove "this works with
# no credentials" would pass locally for the wrong reason and fail in CI.
#
# Usage:  ./ci-and-push.sh [--branch NAME] [--no-push]
#
# By default this commits to `main`, and it refuses unless the checkout is already
# at origin/main's tip -- so a feature branch would be main under another name.
# Pass --branch NAME to review the change as a pull request instead. It never
# touches a local `main` that has diverged from the published one; see the landing
# logic below.
set -euo pipefail

REPO="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
cd "$REPO"

BRANCH=""
DO_PUSH=1
while (($#)); do
  case "$1" in
    --branch) BRANCH="${2:?--branch needs a name}"; shift 2 ;;
    --no-push) DO_PUSH=0; shift ;;
    *) printf 'unknown argument %s\n' "$1" >&2; exit 64 ;;
  esac
done

RED=$'\033[31m'; GREEN=$'\033[32m'; RESET=$'\033[0m'
step() { printf '\n==> %s\n' "$*"; }
pass() { printf '  %sok%s   %s\n' "$GREEN" "$RESET" "$*"; }
die()  { printf '  %sFAIL%s %s\n' "$RED" "$RESET" "$*" >&2; exit 1; }

# Every Python leg runs through this so no credential can leak into a job that
# CI runs without one.
nocreds() {
  env -u AWS_PROFILE -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY \
      -u AWS_SESSION_TOKEN -u AWS_DEFAULT_PROFILE \
      -u DATABRICKS_HOST -u DATABRICKS_TOKEN \
      -u DATABRICKS_CLIENT_ID -u DATABRICKS_CLIENT_SECRET \
      PYTHONDONTWRITEBYTECODE=1 "$@"
}

step "0/6  Preconditions"
[[ -d .venv-3.12 ]] || die "missing .venv-3.12"
[[ -d .venv-3.14 ]] || die "missing .venv-3.14"
[[ -d frontend/node_modules ]] || die "missing frontend/node_modules (run npm ci in frontend/)"
pass "toolchains present"

# An installation directory makes tests/test_no_live_identifiers_committed.py compare
# the tree against a live install's output, which fails on identifiers the tree
# legitimately contains -- so CI cannot run beside one. Refusing outright was wrong:
# the normal reason to run this is that you have just installed something and want to
# publish the fix, and the manifest inside is the only record of what is billing, so
# "delete it first" is the one instruction that must never be given. Park it outside
# the tree instead and restore it from a trap, so an interrupted or failing run still
# puts it back.
PARKED_INSTALL=""
PARKED_AT=""
restore_parked_install() {
  if [[ -n "$PARKED_INSTALL" && -d "$PARKED_AT" && ! -e "$PARKED_INSTALL" ]]; then
    mv "$PARKED_AT" "$PARKED_INSTALL" \
      && printf '  restored %s\n' "$PARKED_INSTALL"
  elif [[ -n "$PARKED_INSTALL" && -d "$PARKED_AT" ]]; then
    printf '  %sWARN%s %s and %s both exist; left both for you to reconcile\n' \
      "$RED" "$RESET" "$PARKED_AT" "$PARKED_INSTALL" >&2
  fi
}
trap restore_parked_install EXIT INT TERM

INSTALL_DIRS=()
while IFS= read -r candidate; do
  [[ -n "$candidate" ]] && INSTALL_DIRS+=("$candidate")
done < <(find . -maxdepth 1 -type d -name '.anti-demo*' -print 2>/dev/null | sed 's|^\./||')

if ((${#INSTALL_DIRS[@]} > 1)); then
  die "more than one installation directory (${INSTALL_DIRS[*]}); park or remove all but one"
elif ((${#INSTALL_DIRS[@]} == 1)); then
  PARKED_INSTALL="$REPO/${INSTALL_DIRS[0]}"
  PARKED_AT="${TMPDIR:-/tmp}/anti-demo-parked-$$"
  [[ -e "$PARKED_AT" ]] && die "$PARKED_AT already exists"
  mv "$PARKED_INSTALL" "$PARKED_AT" || die "could not park $PARKED_INSTALL"
  pass "parked ${INSTALL_DIRS[0]} outside the tree for the duration (restored on exit)"
else
  pass "no live-artefact directory present"
fi

step "1/6  Publication guards"
nocreds UV_PROJECT_ENVIRONMENT=.venv-3.12 UV_PYTHON=3.12 \
  uv run --no-sync pytest -q -p no:cacheprovider \
    tests/test_deploy_hygiene.py \
    tests/test_no_live_identifiers_committed.py \
    tests/test_public_markdown_links.py \
  || die "publication guards"
pass "publication guards"

step "2/6  Lint (ruff)"
nocreds UV_PROJECT_ENVIRONMENT=.venv-3.12 UV_PYTHON=3.12 \
  uv run --no-sync ruff check . || die "ruff check"
pass "ruff clean"

step "3/6  Python suite 3.12"
nocreds UV_PROJECT_ENVIRONMENT=.venv-3.12 UV_PYTHON=3.12 \
  uv run --no-sync pytest -q -p no:cacheprovider || die "python 3.12 suite"
pass "python 3.12"

step "4/6  Python suite 3.14"
nocreds UV_PROJECT_ENVIRONMENT=.venv-3.14 UV_PYTHON=3.14 \
  uv run --no-sync pytest -q -p no:cacheprovider || die "python 3.14 suite"
pass "python 3.14"

step "5/6  Frontend"
( cd frontend && npm run typecheck ) || die "frontend typecheck"
( cd frontend && npm run lint )      || die "frontend lint"
( cd frontend && npm run build )     || die "frontend build"
( cd frontend && npm test -- --run ) || die "frontend tests"
pass "frontend"

step "6/6  Slow bootstrap stub matrix (about 5-6 minutes)"
# The frontend build above satisfies the harness's assertion that dist/ exists.
nocreds UV_PROJECT_ENVIRONMENT=.venv-3.12 UV_PYTHON=3.12 \
  uv run --no-sync pytest -q -p no:cacheprovider -m slow || die "slow bootstrap matrix"
pass "slow bootstrap matrix"

printf '\n%sAll CI jobs passed locally.%s\n' "$GREEN" "$RESET"

step "Commit"
if [[ -z "$(git status --porcelain)" ]]; then
  # A clean tree is not the same as nothing to publish. Exiting here meant that
  # running the gates, then running this again to push, silently pushed nothing --
  # the gates passed, the script said "clean", and the commits stayed local.
  UNPUSHED="$(git rev-list --count origin/main..HEAD 2>/dev/null || echo 0)"
  if [[ "$UNPUSHED" == "0" ]]; then
    printf '  nothing to commit and nothing unpushed; already published\n'
    exit 0
  fi
  printf '  tree is clean, and %s commit(s) are not on origin/main yet:\n' "$UNPUSHED"
  git log --oneline origin/main..HEAD | sed 's/^/    /'
  if ((DO_PUSH)); then
    step "Push"
    git push origin "HEAD:main"
    pass "pushed $UNPUSHED commit(s) to origin/main; CI runs on push"
  else
    printf '  --no-push given; push with: git push origin HEAD:main\n'
  fi
  exit 0
fi
git status --short

# HEAD is detached after `git checkout <sha>`, and a push needs a branch. Default
# to main: this commit is exactly origin/main's tip, so there is nothing to branch
# from. Refuse rather than guess if that stops being true, because committing onto
# a stale base would put unrelated reverts in the diff.
if [[ -n "$BRANCH" ]]; then
  git switch -c "$BRANCH"
  pass "created branch $BRANCH for pull-request review"
elif git symbolic-ref -q HEAD >/dev/null; then
  pass "already on branch $(git rev-parse --abbrev-ref HEAD)"
else
  HEAD_SHA="$(git rev-parse HEAD)"
  MAIN_SHA="$(git rev-parse origin/main 2>/dev/null || true)"
  [[ -n "$MAIN_SHA" ]] || die "origin/main not fetched; run: git fetch origin"
  if [[ "$HEAD_SHA" != "$MAIN_SHA" ]]; then
    die "detached HEAD $(git rev-parse --short HEAD) is not origin/main $(git rev-parse --short origin/main); rebase or pass --branch NAME"
  fi
  # A local `main` may exist and may be a private, diverged line of development.
  # In this repo it is: local main is 1a98679 ("Checkpoint ...") while origin/main
  # is eebc8a7 ("Release ..."), the same work rewritten before publishing. Moving
  # onto that branch would commit these fixes onto the wrong base and produce a
  # push that overwrites published history. Land on a branch that tracks the
  # published tip instead, and never touch local main.
  if git show-ref --verify --quiet refs/heads/main \
     && [[ "$(git rev-parse main)" != "$MAIN_SHA" ]]; then
    LANDING="main-publish"
    git switch -c "$LANDING" 2>/dev/null || git switch "$LANDING"
    pass "local main diverges from origin/main; landing on $LANDING at the published tip"
    printf '  push with: git push origin %s:main\n' "$LANDING"
  else
    git switch main 2>/dev/null || git switch -c main --track origin/main
    pass "on main (was detached at its exact tip)"
  fi
fi

git add -A \
  ci-and-push.sh \
  runner/round5_fanin.py \
  server/connection_fanin.py \
  tests/test_connection_fanin.py

# The list above is explicit so an unrelated edit cannot ride along. That makes
# the opposite mistake possible -- staging a subset and committing half a change
# -- and it has already happened once: the list was left pointing at a previous
# commit's files, so every gate passed and `git commit` then found nothing staged.
# Refuse when a tracked modification is left behind, naming it.
LEFT_BEHIND="$(git diff --name-only)"
if [[ -n "$LEFT_BEHIND" ]]; then
  printf '  %sFAIL%s these tracked files are modified but not in this script'"'"'s staging list:\n' \
    "$RED" "$RESET" >&2
  printf '    %s\n' $LEFT_BEHIND >&2
  die "add them to the list above, or revert them; refusing to commit part of a change"
fi

git commit --file - <<'MSG'
Hold 10,000 client connections on 6 backend sessions

Round 5's ramp had never reached its target. It stopped between two hundred and a few thousand
clients with zero connection failures, reported the run as a success, and left
`telemetry_failures: ['event_loop_pressure']` as the only trace. Two causes, both measured on
the sealed runner rather than guessed at.

The TLS handshake costs about 1.6 ms of event-loop CPU, and `LANE_CONNECT_CONCURRENCY` was 32,
so a single loop turn could complete an entire in-flight wave of handshakes: `phase_max_ms`
reported `ssl_handshake_process_cpu` between 53.9 and 55.6 ms against a 50 ms ceiling, spent
inside one callback. The obvious lever is unavailable, and says so where it is defined: capping
the ready or selector drain breaks the asyncio invariant that a turn consumes the readiness it
was handed, and under level-triggered epoll that turned O(N) service into O(N**2 / K), measured
at 32.5x. The available lever is how many handshakes can arrive in one turn, so the concurrency
is now derived from the handshake cost against half the ceiling: a full batch spends about half
the budget and leaves the rest for the turn's other work. Handshake CPU per turn fell to 27 ms
and the ramp went from 442 clients to 2,490.

The rest was misattribution. `classify_generator_owned_stall` exists to require corroborating
local work before blaming a wall-clock delay, and one of its two CPU clauses did that properly
while the other asked only for `thread_cpu_ms >= 5.0` and short-circuited it. So 20 ms of CPU was
held to account for a 78 ms turn, of which 58 ms was the loop waiting on the kernel, the network
or the scheduler. Both clauses are now proportional, which is what the docstring always claimed:
a genuinely CPU-bound turn still fails the gate, and 55 ms of handshake CPU against a 98 ms lag
still would. `OWNED_STALL_READY_BATCH` is proportional for the same reason in another currency,
derived from the connects this protocol deliberately keeps in flight, because a flat 16 was below
that number and made a completely healthy full batch count as our own amplification.

The gate itself is untouched. Raising the ceiling would have published a measurement taken under
pressure; with the attribution corrected, the real peak event-loop p99 during a full run is
0.027 ms, more than a thousand times inside the limit it was previously reported to be double.

Measured live, one lane, every gate passed: 10,000 clients initiated, 10,000 authenticated,
10,000 held at the gate, 12.6 seconds to the target, the full 30-second hold, zero terminal
failures, zero retries, none disconnected during the hold, all 64 sampled queries answered, and
a peak of 6 PostgreSQL backend sessions behind those 10,000 clients, read during the hold by a
second role on its own direct connection. Connect latency p50 58.5 ms, p99 141.1 ms.

A two-lane bout reaches about 9,100 per lane instead, because two lanes ramping in one process
put both their handshake batches in the same turn. That is the next change and it is the one the
round wants anyway: the lanes should not share a start at all.
MSG
pass "committed"

if ((DO_PUSH)); then
  step "Push"
  CURRENT="$(git rev-parse --abbrev-ref HEAD)"
  if [[ -n "$BRANCH" ]]; then
    git push -u origin "$CURRENT"
    pass "pushed $CURRENT; open a pull request to run CI"
  else
    # Explicit refspec so a diverged local `main` can never be the thing pushed.
    # Not forced: if origin/main moved, this is rejected and you rebase.
    git push origin "HEAD:main"
    pass "pushed to origin/main; CI runs on push"
  fi
else
  printf '  --no-push given; commit is local only\n'
  printf '  push with: git push origin HEAD:main\n'
fi
