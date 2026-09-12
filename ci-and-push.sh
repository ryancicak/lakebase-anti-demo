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
# By default this commits to `main`. eebc8a73 is exactly origin/main's tip, so a
# feature branch would be main under another name; these changes are also small,
# self-contained and verified green locally. Pass --branch NAME if you would
# rather review them as a pull request.
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

# A leftover .anti-demo* directory makes tests/test_no_live_identifiers_committed.py
# compare the tree against a previous install's output, which fails on tokens the
# tree legitimately contains. It must also be absent for a genuine first provision.
if compgen -G ".anti-demo*" >/dev/null; then
  die "a .anti-demo* directory exists; remove it before a first-run test and before CI"
fi
pass "no live-artefact directory present"

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
  printf '  nothing to commit; working tree is clean\n'
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

git add -A README.md docs/iam/README.md bootstrap.sh scripts/attach-operator-policies.sh ci-and-push.sh
git commit --file - <<'MSG'
Make a first-time install work without side work

bootstrap.sh reported "No default VPC in $AWS_REGION, and this is not a
permissions problem" whenever ec2:DescribeVpcs was denied, because the discovery
query swallows the error into "None". The advice that follows -- change region or
plumb Terraform network variables -- is wrong for a healthy default VPC, so the
verdict now consults PERMISSION_FAILURES and reports the network as unknown
rather than absent, with no second failure for one cause.

The README's prerequisite said "an AWS account with permission to ...", which
reads as satisfied by an administrator console login. bootstrap authenticates as
the pair in .env.bootstrap and nothing else, so that pair must hold the
permissions itself. Says so, and points at the least-privilege alternative.

docs/iam/README.md described a narrow app principal separate from the operator
and warned against attaching the operator set to the app. bootstrap.sh:729-731
publishes one pair to both, and bootstrap.env.example offers only one, so a
reader who followed that page declined the attachment and could not provision.
The separation is now marked as the target state rather than the current runtime.

scripts/attach-operator-policies.sh replaces eight hand-transcribed commands from
that page. It derives the account from sts:GetCallerIdentity rather than trusting
a flag, refuses if a placeholder survives rendering, and is idempotent on both
the policies and the attachments. bootstrap cannot do this itself: creating and
attaching IAM policies is more privilege than the operator set it would grant.

ci-and-push.sh runs the four CI jobs locally with cloud credentials unset the way
CI asserts, and refuses to commit unless all of them pass.
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
