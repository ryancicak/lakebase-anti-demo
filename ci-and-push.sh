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
  server/connection_spike_live.py \
  server/manager.py \
  tests/test_connection_spike_background_cleanup.py

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
Run each lane's 10,000 on its own clock, and Round 5 verifies

Ryan's instruction, and the measurement turned out to agree with it: "a starting barrier is a
DUMB idea as RDS proxy takes forever to start, that's not Lakebase's fault, so start Lakebase as
soon as the round starts." Lakebase should never wait.

It was not only unfair, it was inaccurate. Two lanes ramping in one process put both their
handshake batches in the same event-loop turn, so each lane got half the budget and both stopped
near 9,100 of 10,000 with no connection failures at all. Dispatched one at a time, each lane has
the whole loop: both reach exactly 10,000.

So `run` dispatches per lane, Lakebase first, and merges the results. `_fanin_request` builds one
lane's request; `_finalize_lane_payload` verifies each payload's seal on arrival and scores the
single lane it carries; `_merge_lane_results` holds the requirement that a scored Round 5 has two
lanes, which is where it belongs now that they arrive separately. The seal comparison is extracted
so both finalisers share one implementation rather than drifting apart. A payload is checked
against the lanes its own request asked for, not against every sealed lane, because a correct
single-lane result was being refused as having omitted a lane nobody asked it to run.

The last refusal was the manager validating a fan-in result with the bounded protocol's
arithmetic: 128 scheduled attempts, a 64-client witness phase that no longer exists, and backend
session counts below 64. Both lanes held 10,000 with every gate green and the round still refused.
A fan-in lane is now validated by `gates.passed`, the conjunction of the ten gates this protocol
actually defines, evaluated where the evidence is, plus the one thing those gates cannot know:
that the lane held the target this installation asked for. The bounded branch is untouched for a
stored result from the retired protocol, which really did run 128 attempts and a separate witness.

Verified live, both lanes, every gate:

  lakebase    10,000 clients held, p99 141.19 ms, pooled-path setup 3,387 ms
  competitor  10,000 clients held, p99 442.95 ms, pooled-path setup 870,157 ms

Zero client errors on either lane, both setup stop gates exact, and the round declared its own
verdict: Lakebase verified a pooled path 866.77 seconds sooner. Both lanes then held ten thousand
authenticated client connections, so the margin is a setup finding rather than one side failing.
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
