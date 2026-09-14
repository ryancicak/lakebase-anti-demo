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
  runner/connection_spike_runner.py \
  runner/round5_fanin.py \
  server/api.py \
  server/catalog.py \
  server/connection_fanin.py \
  server/connection_spike_live.py \
  server/lifecycle.py \
  server/manager.py \
  tests/test_catalog.py \
  tests/test_connection_fanin.py \
  tests/test_connection_spike.py \
  tests/test_fanin_observer_connect.py \
  tests/test_runner_result_credential_scan.py

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
Let Round 5 actually reach 10,000 clients, and say why when it does not

Seven live bouts against the sealed installation, each one finding a fault that could not be
reached until the fan-in protocol was dispatched for the first time. Every fix here is one of
those, in the order they surfaced.

The observer could not open its connection. `execute_fanin` puts `sslrootcert` into the
observer descriptor, the observer is the only path through `connect_runner_database`, and that
function's allowlist is exactly the six libpq fields, so every worker died on
`ValueError: runner database descriptor contains unsupported fields` before a single client
connected. The same three lines also passed `trust_bundle_path=None` while asking for
verify-full, so had it survived it would have verified against the system trust store instead
of the sealed bundle: workable for a public CA, wrong for Amazon RDS, and wrong in principle
for the one connection whose job is independent evidence.

The worker ready barrier was 60 seconds against work allowed 240. A worker reports ready only
after its observer connects and sees the lane quiet, and the observer owns both budgets. Its
two sibling constants were already derived from the fan-in module; this was the only bare
literal, and it reported a slow observer as a missing worker.

The quiesce gate demanded zero pre-existing client-role sessions, which a pooled lane cannot
give: setup proves the new RDS Proxy is ready by running a transaction through it, and a proxy
holds its pool. The baseline is now recorded and bounded by
`MAX_PREEXISTING_CLIENT_SESSIONS`, derived from the most connections this protocol has in
flight to one lane at once, and a lane over the ceiling fails its own `clean_start` gate rather
than being reported as an observer that was not separate.

The result guard threw away a completed bout. It matched the substring "password" in flattened
JSON, and `auth_method` legitimately holds `tls-cleartext-password`, one of the protocol's two
supported methods. The structural scan written for exactly this was already in the file and
unused here. It also now refuses a credential-named key, which the substring version caught and
a values-only scan would have lost.

The returned envelope could exceed what SSM will hand back. Four workers' diagnostics each
carry maps keyed by callback identity, phase name and GC generation, so the payload grew with
how varied the run was rather than with what it measured, and a finished bout was lost to
`result_too_large`. Those maps are bounded to their most expensive entries with the remainder
counted, and an overflow now reports its size and largest contributors.

Round 5 no longer requires two lanes. This is the change Ryan asked for in as many words: a
shared start barrier makes the AWS path's Proxy build a precondition for Lakebase's
measurement, and it is not Lakebase's fault that a Proxy takes eleven minutes. Under the
barrier Lakebase verified in 3.4 seconds, waited, suspended at its 60-second idle floor, and
arrived cold; the ramp also advanced both lanes in lockstep so it could not pass a failing
lane. A request may now name one lane or two, the executor runs the lanes it is given, sampling
keeps its 250ms offset between however many there are, and the aggregator collects the lanes
that ran instead of pre-seeding both. `_secrets_manager_region` returns "" for no ARNs rather
than refusing, because the Lakebase lane holds its credential outside Secrets Manager.

Three diagnostics, because every one of the failures above cost an eleven-minute Proxy build to
characterise. The burst log recorded only an exception class; a refused command discarded the
runner's own token; and the lane-identity gate reported ten fields under one word. All three
now name what happened, and a worker that authenticated nobody refuses by that name instead of
appearing as workers who disagreed about an auth method.

Copy that described the retired protocol is corrected where it is read aloud or scored: the
metric set makes time-to-10,000 primary and setup time secondary, the presenter's remembered
metric and stop condition match the bout, the burst lane status no longer says 128 attempts,
and the fight card stops telling a room that other rounds remain available while refusing all
six. The cost disclosures keep their 128-attempt wording on purpose, because they describe two
specific past bouts that really did run that protocol.

Measured and reproducible across seven bouts: Lakebase pooled-path setup 3.33 to 3.49 seconds,
the AWS path 653 to 746 seconds. One bout reached the full 10,000 with all four workers at
2,500 and the observer open. The ramp is currently capped below that by
`telemetry_failures: ['event_loop_pressure']` at a peak event-loop p99 of 98.65 ms against a
50 ms ceiling, with memory, file descriptors, CPU and ephemeral ports all far inside their
limits. That gate is doing its job and the number it is protecting is not yet earned, so no
time-to-10,000 is claimed here.
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
