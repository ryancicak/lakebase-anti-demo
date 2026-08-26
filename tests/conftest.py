from __future__ import annotations

import os

import botocore.endpoint
import botocore.httpsession
import pytest

from server.aws_auth import _CONFLICTING_CREDENTIAL_NAMES
from server.lifecycle import ANTI_DEMO_RUNTIME_PRINCIPALS_ENV
from server.manifest import LOCAL_OPERATOR_ENV_NAMES, MANIFEST_JSON_ENV
from server.process_registry import (
    MANIFEST_ENV,
    SERVER_HOST_ENV,
    SERVER_LOG_PATH_ENV,
    SERVER_PORT_ENV,
)
from server.server_launch import LOG_KEEP_ENV, LOG_MAX_BYTES_ENV

# ---------------------------------------------------------------------------
# No test may reach AWS.
#
# `hide_ambient_aws_credentials` below closes the *credential* path and cannot
# close the *resolution* path, which is a different hole: botocore falls back to
# `~/.aws/credentials`, to `~/.aws/config`, and finally to the instance metadata
# service, none of which is an environment variable and none of which this file
# can delete. A test that pins the profile off -- `Session(session_vars={
# "profile": (None, None, None, None)})`, which two files here do deliberately
# so that an unrelated `AWS_PROFILE` cannot break them -- routes itself straight
# at the default chain. So on the machine this repository is aimed at, somebody
# who cloned it and already has AWS credentials, deleting more variables cannot
# be the answer. The hole is structural and it has to be closed structurally.
#
# These two patches are that. They are applied at import, permanently, rather
# than through an autouse fixture, so that they also cover module import,
# collection, and session-scoped fixtures -- everything a fixture would miss.
#
# Where they intercept, and why exactly there:
#
#   `Endpoint.make_request` is what `BaseClient._make_api_call` reaches only
#   *after* the `before-call` event has been given a chance to answer and has
#   not. That ordering is the whole design. `botocore.stub.Stubber` answers on
#   `before-call` -- and so does `moto`, and so does anything else built the
#   ordinary way -- so a
#   stubbed call never arrives here and the guard is invisible to the roughly
#   ten tests in this suite that validate parameters against the real service
#   model through `Stubber` -- which is the point, because those tests are the
#   good kind and a guard that broke them would not be shippable. Anything not
#   stubbed arrives here, and it arrives before `create_request` signs it, which
#   is what makes this safe: no credential is resolved, no packet is built.
#
#   `URLLib3Session.send` is the backstop, one layer down. `Endpoint` is not the
#   only thing in botocore that talks to the network: `IMDSFetcher` builds its
#   own session to ask 169.254.169.254 for instance-role credentials, and that
#   path bypasses `Endpoint` entirely. Nothing in this suite should reach either.
#
# The message is long on purpose. A future test that forgets to stub its session
# should be told what happened and what to do, not handed a botocore traceback.
# ---------------------------------------------------------------------------

_AWS_GUARD_REMEDY = """
tests/conftest.py forbids the test suite from reaching AWS. Left alone, the
call above would have been signed with whatever credentials this machine
resolves -- an SSO profile, a [default] entry in ~/.aws/credentials, an
instance role -- and sent to that real account. This repository is public;
the account it lands in belongs to whoever cloned it.

Nothing here needs a live account, so this is a missing double, not a missing
credential. Two ways to give the code one:

  * botocore.stub.Stubber around the client, when the test is making a claim
    about the exact parameters or the exact response shape. Stubber answers on
    the `before-call` event, which runs above this guard, so a stubbed call is
    allowed through untouched. Several tests here already do this.
  * monkeypatch the boto3.Session that the module under test imported, when the
    test only needs the call to return something -- for example
    monkeypatch.setattr("server.targets.boto3.Session", lambda **_: FakeSession()).

If what the test means to assert is "this path fails when AWS cannot be
reached", make the stub raise the error you mean. A call that escapes and
happens to fail on a missing profile is not evidence of anything: it is the
same passing test whether or not the code under test is correct.
""".strip()


class RealAwsCallAttempted(AssertionError):
    """A test was about to send a signed request to a real AWS account."""


def _attempting_test() -> str:
    """Name the test that reached the wire.

    pytest maintains PYTEST_CURRENT_TEST for the whole of setup, call and
    teardown, and it is the only handle available this far down: botocore knows
    nothing about the test that got here.
    """

    return os.environ.get("PYTEST_CURRENT_TEST", "<no test running>")


def _refuse_aws_call(what: str) -> RealAwsCallAttempted:
    return RealAwsCallAttempted(
        f"Blocked a real AWS API call: {what}\n"
        f"\n"
        f"  attempted by: {_attempting_test()}\n"
        f"\n"
        f"{_AWS_GUARD_REMEDY}"
    )


def _forbid_endpoint_request(self, operation_model, request_dict):
    del request_dict
    service = operation_model.service_model.service_id
    raise _refuse_aws_call(f"{service}.{operation_model.name} to {self.host}")


def _forbid_wire_send(self, request):
    del self
    raise _refuse_aws_call(f"{request.method} {request.url}")


botocore.endpoint.Endpoint.make_request = _forbid_endpoint_request
botocore.httpsession.URLLib3Session.send = _forbid_wire_send


# Every name that can change which credential source the code selects. The list is
# imported rather than retyped so it cannot drift from server/aws_auth.py.
AMBIENT_AWS_NAMES = (
    *_CONFLICTING_CREDENTIAL_NAMES,
    "AWS_PROFILE",
    "AWS_AUTH_MODE",
    "AWS_CREDENTIAL_EXPIRATION",
)

# Every name that decides which installation the code operates on, or how a
# launch behaves. Imported from the modules that own them for the same reason as
# the list above. Four kinds, all with the same consequence:
#
# * The endpoint variables, which a launch path writes into the real `os.environ`
#   and so leak from one test to the next.
# * `ANTI_DEMO_MANIFEST`, which `state_dir_from_environ` turns into a directory to
#   read and write. The documentation tells operators to export it -- bootstrap.sh
#   prints it and `antidemo serve` requires it -- so the ordinary state of a shell
#   belonging to somebody who followed the instructions is a shell where the suite
#   would read the *live* state directory.
# * `ANTI_DEMO_MANIFEST_JSON`, for the same reason one level up. This list was
#   first written *without* it, on the argument that it is the deployed app's
#   `app.yaml` variable rather than anything a laptop operator exports, so
#   including it would widen the contract past the real hazard. That argument was
#   wrong about where the hazard is, and here is what refuted it: `load_manifest`
#   consults this variable *first*, ahead of `ANTI_DEMO_MANIFEST` and ahead of the
#   file, so scrubbing only the lower-precedence name leaves the higher one live.
#   `test_round_construction.py` reaches it -- it is the one test in that file
#   passing `manifest=None`, which lands in `app._owned_manifest_or_none(None)` and
#   from there in `load_manifest()` -- so an exported manifest turns it red, and
#   *which* of its three assertions trips depends on what the ambient manifest
#   happens to seal. Whether an operator would export it is not the test: the read
#   is reachable from the suite, so containment is not about operator habits. Do
#   not re-narrow this to `ANTI_DEMO_MANIFEST` alone.
# * The local operator identity, which is the endpoint variables' defect again in
#   a second family and was found while closing the one above.
#   `apply_manifest_environment` writes all three into the *real* `os.environ`, and
#   `server/api.py::operator_from_request` reads them back to decide who a local
#   bout belongs to -- so the test that exercises that write hands its operator to
#   every later test in the session. The symptom is
#   `test_api.py::test_double_posted_run_returns_one_bout_over_http` answering 409
#   instead of 200, and only when it runs after `test_manifest.py`; alphabetical
#   collection puts `test_api` first, so a full run is green and the defect looks
#   like a phantom. Exactly the `ANTI_DEMO_SERVER_PORT` story, told twice.
# * The log limits, which `log_limits` deliberately *raises* on rather than
#   silently defaulting. Correct for an operator who mistyped a cap, and it makes
#   an exported `ANTI_DEMO_LOG_MAX_BYTES=8M` fail tests that have nothing to do
#   with logging.
# * `ANTI_DEMO_RUNTIME_PRINCIPAL_ARNS`, the fourth telling of the same story and
#   the most inviting of them: `docs/DEPLOYED-NETWORK-PATH.md` hands the operator
#   a literal `export ANTI_DEMO_RUNTIME_PRINCIPAL_ARNS='...'` line, and
#   `docs/DEPLOY.md`, `docs/BOOTSTRAP.md`, `docs/iam/README.md` and `README.md`
#   all name it. `lifecycle.anti_demo_runtime_principals` consults it on any
#   manifest that sealed nothing, so an exported value silently makes every
#   unsealed installation in the suite look like a sealed one -- and
#   `_expected_aws_state_addresses` then expects the seven extra
#   `anti_demo_runtime` IAM addresses that the fixtures' Terraform state does not
#   have. Measured, not predicted: exporting it turns 24 tests red across
#   `test_anti_demo_runtime.py`, `test_expiry_renew.py` and `test_lifecycle.py`,
#   which is a suite that fails for precisely the reader who followed the
#   instructions. Tests that care about this variable -- all of them in
#   `test_anti_demo_runtime.py` -- set it explicitly with `monkeypatch.setenv`,
#   which still works: this runs first and they overwrite it.
AMBIENT_INSTALLATION_NAMES = (
    SERVER_HOST_ENV,
    SERVER_PORT_ENV,
    SERVER_LOG_PATH_ENV,
    MANIFEST_ENV,
    MANIFEST_JSON_ENV,
    *LOCAL_OPERATOR_ENV_NAMES,
    LOG_MAX_BYTES_ENV,
    LOG_KEEP_ENV,
    ANTI_DEMO_RUNTIME_PRINCIPALS_ENV,
)

# ---------------------------------------------------------------------------
# The two lists above name variables one at a time, and every one of them was
# added after a run went red. Five separate times. That is the shape of a defect
# class being paid off in instalments, and the instalments were still arriving:
# measured on this tree, `ANTI_DEMO_STARTUP_REAP` turns three tests in
# `test_reap.py` red, `ROUND4_CATALOG=main` turns two in `test_lifecycle.py` red,
# `DATABRICKS_APP_NAME` turns `test_api.py::
# test_double_posted_run_returns_one_bout_over_http` red by routing
# `operator_from_request` down its deployed branch, `ANTI_DEMO_RECOVERY_TOKEN`
# turns one in `test_selfheal.py` red, and `ANTI_DEMO_SETTLEMENT_ATTEMPTS` and
# `ANTI_DEMO_CLEANUP_RETRY_ATTEMPTS` turn one each red in `test_settlement.py`
# and `test_manager.py` -- nine tests across six files, none of them named above,
# all of them green in CI because a clean runner exports nothing.
#
# So the list is not the mechanism. The naming convention is: every variable this
# project owns is prefixed, and a variable that does not match one of these
# prefixes is not this project's to read. Sweeping the prefix contains the
# variable nobody has written down yet, which is the only kind that can still
# cause instance six. A new `ANTI_DEMO_ANYTHING` is contained on the day it is
# written rather than on the day it turns a run red.
#
# `AWS_` and `DATABRICKS_` are swept whole for a second reason beyond outcome
# stability. This laptop holds a *production* AWS profile beside the sandbox one
# and a real workspace host, and the guard at the top of this file can only
# refuse a call that botocore is already making. Deleting the variables means the
# call is never composed. Nothing here is made more willing to pick up an ambient
# value; the sweep only ever deletes.
#
# The explicit tuples above are kept rather than folded into this: they are
# imported from the modules that own them, so they still catch a *rename* that a
# prefix match would silently keep passing, and their comments are the record of
# what each one cost to find.
AMBIENT_NAME_PREFIXES = (
    "ANTI_DEMO_",
    "AWS_",
    "DATABRICKS_",
    "LAKEBASE_",
    "RDS_",
    "AURORA_",
    "TF_VAR_",
)

# The four the convention misses, because they predate it. `ROUND4_CATALOG` and
# `ROUND5_APP_PRINCIPAL_ARN` are both printed as `export` lines by
# `bootstrap.sh --print-env`, which `CONTRIBUTING.md` tells a contributor to
# `eval` immediately before it tells them to run `uv run pytest` -- so the
# documented setup is the hazard, again, and `ROUND4_CATALOG` is one of the two
# that were measured red.
AMBIENT_EXTRA_NAMES = (
    "ROUND4_CATALOG",
    "ROUND5_APP_PRINCIPAL_ARN",
    "EXPECTED_POSTGRES_MAJOR",
    "PGUSER",
)

# Read by `server/` and deliberately left alone, with the reason, so that a later
# reader can tell "considered and rejected" from "not yet noticed".
#
#   USER                    `pipeline_power` reads it for a display name. It is
#                           set on every developer machine and on the CI runner,
#                           so deleting it would not make the suite independent
#                           of the machine -- it would swap one machine-dependent
#                           value for a machine-dependent absence, on a path that
#                           several subprocess harnesses inherit. Measured: the
#                           full suite is green with it set to a foreign value.
#   UV_PROJECT_ENVIRONMENT  `server_launch` reads it to find the interpreter a
#                           relaunch must use. `uv run` -- the invocation CI uses
#                           and CONTRIBUTING.md documents -- sets it itself, so
#                           deleting it would break the supported way of running
#                           this suite in order to contain a variable that was
#                           measured inert.
DELIBERATELY_NOT_CONTAINED = ("USER", "UV_PROJECT_ENVIRONMENT")


def ambient_names(environ):
    """Every name present in `environ` that this project has no business reading."""

    swept = {
        name
        for name in environ
        if name.startswith(AMBIENT_NAME_PREFIXES) or name in AMBIENT_EXTRA_NAMES
    }
    return swept.union(AMBIENT_AWS_NAMES, AMBIENT_INSTALLATION_NAMES)


# ---------------------------------------------------------------------------
# Applied at import as well as per test, and that is the whole of the fix for the
# `AWS_PROFILE` collection error rather than an optimisation.
#
# An autouse fixture runs at test *setup*, which is after collection has already
# finished. `tests/test_round3_iam_dependency.py` computes
# `round_actions(3) - round_actions(2)` at module level, so importing it reaches
# `server/aws_permissions.py::_available_services`, which builds a botocore
# session -- and a botocore session resolves the profile before it will hand over
# so much as the list of service names. With `AWS_PROFILE` naming a profile that
# is not in `~/.aws/config`, that raises `ProfileNotFound` during collection:
#
#     E   botocore.exceptions.ProfileNotFound: The config profile
#         (definitely-not-a-real-profile-xyz) could not be found
#     !!!! Interrupted: 1 error during collection !!!!
#
# pytest then abandons the *entire* session -- 1784 tests do not run and the exit
# code is 2 -- so the reader is told the repository is broken rather than that one
# variable is set. The variable was already in `AMBIENT_AWS_NAMES`; the fixture
# holding it simply could not run early enough. Deleting at import is what runs
# early enough, and it is the same reasoning as the two botocore patches above:
# collection and session-scoped work is exactly what a fixture misses.
#
# Note the value has to be a profile that does not exist. The sandbox profile
# this machine does have resolves fine, which is why the error looked like it
# belonged to one broken file rather than to the ambient environment.
# ---------------------------------------------------------------------------
for _name in ambient_names(os.environ):
    os.environ.pop(_name, None)
del _name


@pytest.fixture(autouse=True)
def contain_app_state():
    """One test's `/readyz` inputs must not decide the next test's verdict.

    `app.app` is built once at import and its `.state` is a single mutable bag
    shared by every test that renders a route or calls `_readiness_response()`
    off it. Most tests set what they need through `monkeypatch.setattr(state,
    ..., raising=False)`, which puts it back -- but `app.py::_open_runtime` writes
    `startup_credential_verdict`, `startup_reap`, `coordination_mode` and five
    more directly, and a test that exercises the real startup path therefore
    leaves them on the singleton with nothing recording that it did.

    The observed consequence: `test_api.py` runs the deployed startup preflight
    and leaves `startup_credential_verdict=CredentialVerdict(state='absent')`
    behind. `test_reap.py::_readyz_on_a_healthy_box` then renders `/readyz` for
    what it calls a healthy box -- it pins six attributes and not that one -- so
    `/readyz` reports `degraded` on the inherited credential fault and
    `test_a_failing_reap_still_lets_the_server_start` fails asserting that a
    failed sweep does *not* degrade the box. It fails on somebody else's fault,
    which is why it looked like a phantom: alphabetical collection puts
    `test_api` after `test_reap`, so a default run never sees it. Four of sixteen
    randomised orders did.

    Cleared wholesale rather than against a curated list of attribute names,
    because the list is `app.py`'s to change and a copy of it here would drift
    from it silently.

    Cleared at *setup* rather than restored at teardown, and that is not a
    stylistic choice. `monkeypatch.setattr(state, name, ..., raising=False)`
    records "there was nothing here" and undoes itself with `delattr`, and
    Starlette's `State.__delattr__` is a bare `del self._state[key]` that raises
    `KeyError` rather than `AttributeError` on a missing key. A teardown-time
    restore finalises before `monkeypatch` undoes -- measured, not assumed -- so
    it deletes the keys monkeypatch is about to delete and turns every one of
    those tests into a teardown error. Resetting on the way in has the same
    effect on the next test and cannot collide with anything.

    `{}` is the correct baseline rather than an approximation: `app.app.state`
    holds nothing until a lifespan or a test puts something there, verified on a
    fresh interpreter, and there is no module- or session-scoped fixture in this
    suite that could be relying on state surviving a test.

    `app` is only touched if a test has already imported it: importing it here
    would drag the whole application into collection for the many test files that
    never look at it.
    """
    import sys

    module = sys.modules.get("app")
    if module is not None:
        module.app.state._state.clear()


@pytest.fixture(autouse=True)
def hide_ambient_aws_credentials(monkeypatch):
    """Make the suite independent of the developer's AWS environment.

    server/aws_auth.py deliberately refuses ambiguity: real keys in the
    environment while a manifest seals profile mode is an error, and a lone
    AWS_ACCESS_KEY_ID is an error. That is right in production and poison in a
    test process, because it made roughly twenty tests -- every one that reaches
    _aws_session behind a fake boto3 -- pass or fail according to whether the
    shell that launched pytest happened to have credentials exported. bootstrap.sh
    tells operators to export exactly those variables, so the trap was easy to
    step in. Tests that care about ambient credentials set them explicitly with
    monkeypatch, which still works: this runs first and they overwrite it.
    """
    for name in AMBIENT_AWS_NAMES:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture(autouse=True)
def contain_ambient_installation_environment(monkeypatch):
    """Stop the operator's shell, and one test's launch, from deciding the next.

    `cli._serve` and `serve_in_background` have to write the host, port and log
    path into the *real* `os.environ`: the server registers itself and reads them
    back, and on the background path a `fork` is the only thing that carries
    them. That is correct in production and it leaks in a test process, because
    the test that exercises the foreground path calls `_serve` for real and every
    later test in the session inherits its port.

    The visible symptom was `test_process_registry.py` failing only when it ran
    after `test_server_launch.py`: `build_record` reads `ANTI_DEMO_SERVER_PORT`
    to tell the supported launcher from a bare uvicorn, so an inherited port made
    an ad-hoc launch record itself as `launcher`. A full run happens to be safe
    -- files are collected alphabetically, so `test_process_registry` runs first
    -- which is exactly what made this look like a phantom.

    The other names here are ambient rather than leaked, and the hazard is
    *invited* rather than hypothetical: the documented way to use this repository
    is to export `ANTI_DEMO_MANIFEST`, so a shell belonging to somebody who read
    the instructions is a shell where `state_dir_from_environ` resolves to the
    live state directory. Four tests had already reached for a local
    `monkeypatch.delenv` to defend themselves one at a time, which is the same
    fix written five times; this is it written once.

    Deleted rather than merely restored, so a documented operator shell cannot
    change the outcome of a run. Tests that care set these explicitly with
    monkeypatch, which still works: this runs first and they overwrite it. No test
    reads an ambient value -- every one of them either sets what it needs or
    deletes what it must not have.

    Swept by prefix as well as by name, for the reason argued at
    `AMBIENT_NAME_PREFIXES`. The import-time sweep above has already removed
    whatever the shell arrived with; this is what removes what a *test* left
    behind, which is the other half of the same defect and the half a one-off
    sweep cannot reach -- `cli._serve` writes the live port into the real
    `os.environ`, and `apply_manifest_environment` writes the operator identity
    there, so the set is re-read per test rather than computed once.
    """
    for name in ambient_names(os.environ):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture(autouse=True)
def seal_operator_ingress_probe(monkeypatch):
    """No test may reach the public-IP service, and none may inherit a verdict.

    `server.lifecycle.operator_ingress_drift` caches one verdict per process --
    that is what keeps it off the request path -- so an unsealed cache would make
    one test's observation decide the next test's answer, and any test holding a
    valid manifest would make a real network call. Seeding the cache with a
    never-expiring "no drift" closes both: the probe is unreachable by default.

    Tests that exercise drift call `reset_operator_ingress_cache()` and stub
    `detect_operator_cidr`; monkeypatch restores this afterwards either way.
    """
    from server import lifecycle

    monkeypatch.setattr(lifecycle, "_OPERATOR_INGRESS_VERDICT", (float("inf"), None))


@pytest.fixture(autouse=True)
def seal_installation_presence_sweep(monkeypatch):
    """No test may sweep the real AWS account, and none may inherit a verdict.

    Same hazard as the ingress probe above, one layer out: the presence check
    caches one verdict per process to keep three paginated describes off the
    request path, so an unsealed cache would let one test's sweep decide the
    next test's answer -- and any test holding a valid manifest would reach the
    live account. Seeded with a never-expiring "present" so the sweep is
    unreachable by default.

    Tests that exercise a reap call `reset_installation_presence_cache()` and
    stub the sweep; monkeypatch restores this afterwards either way.
    """
    from server import lifecycle
    from server.reconcile import PRESENCE_PRESENT, InstallationPresence

    monkeypatch.setattr(
        lifecycle,
        "_INSTALLATION_PRESENCE",
        (float("inf"), InstallationPresence(PRESENCE_PRESENT), None),
    )


@pytest.fixture(autouse=True)
def seal_deployed_aws_posture(monkeypatch):
    """The third process-wide cache in `server.lifecycle`, and the one that leaked.

    Same hazard as the two above, and it was the live one: `deployed_aws_posture`
    memoises for `OPERATOR_INGRESS_TTL_SECONDS` -- five minutes -- and the whole
    suite runs in well under one, so a posture written by any test survives to the
    end of the session. `tests/test_operator_ingress.py::sealed_egress_manifest`
    writes exactly that: a manifest sealing the published Databricks egress
    prefixes. `server/api.py::_availability_signals` then reads
    `posture.egress_sealed` off the leftover, and the three AWS-lane rounds --
    `wake_idle_app`, `make_schema_change_safely`, `recover_deleted_order` -- are
    offered as `ready` in the deployed app instead of refused.

    The symptom is
    `test_api.py::test_catalog_reports_round_five_ready_without_instantiating_factory`
    asserting `unavailable` and getting `ready`, and it only appears when a
    posture-sealing test runs first. Alphabetical collection puts `test_api`
    ahead of `test_operator_ingress`, so a default full run is green and the
    defect looks like a phantom -- the `ANTI_DEMO_SERVER_PORT` story a third time.

    Seeded with a never-expiring *empty* posture rather than a reset one, because
    "this installation has sealed nothing" is what a test holding no manifest
    computes anyway, and it is the conservative direction: the refusals this feeds
    can only take readiness away. Tests that exercise the posture call
    `reset_deployed_aws_posture_cache()`; monkeypatch restores this afterwards
    either way.
    """
    from server import lifecycle

    monkeypatch.setattr(
        lifecycle,
        "_DEPLOYED_AWS_POSTURE",
        (float("inf"), lifecycle.DeployedAwsPosture()),
    )


@pytest.fixture(autouse=True)
def contain_round4_pipeline_activations(monkeypatch):
    """One test's Round 4 activation must not be handed to the next.

    `model_score_live._ACTIVATIONS` is keyed by pipeline ID and deliberately
    never pruned -- that is what makes "an arm cancels the pending release" true
    across bouts in a long-lived server, and it is correct there. In a test
    process it is a shared object with no owner: `build_model_score_engine`
    populates it as a side effect, every manifest built by `make_manifest()`
    carries the same sealed pipeline ID, and the entry then outlives the test
    that made it for the rest of the session.

    Measured rather than assumed: `test_lifecycle.py::
    test_the_app_holds_select_on_every_unity_catalog_object_round4_reads` builds
    an engine through `_inspect_sync_urls` and leaves an activation holding that
    test's throwaway workspace stub. Every later `build_model_score_engine` for
    the same pipeline is handed that object instead of one built from its own
    stub. No test asserts on the activation today, so nothing is red because of
    it -- this is containment of a demonstrated leak, not a repair of an
    observed failure.

    A fresh dict per test rather than a clear on teardown: production reads the
    module attribute at call time, so rebinding it is enough, and monkeypatch
    puts the real one back even if a test raises. `server/model_score_live.py` is
    left alone -- the sharing is right for the server and wrong only here.
    """
    from server import model_score_live

    monkeypatch.setattr(model_score_live, "_ACTIVATIONS", {})


@pytest.fixture(autouse=True)
def isolate_artifact_root(tmp_path, monkeypatch, contain_ambient_installation_environment):
    """Keep bout receipts out of the real .anti-demo-v7 during tests.

    Every terminal event now writes a receipt, so without this the suite would
    litter the live artifact directory that the running server owns.

    Depends on the containment fixture rather than merely being defined after it,
    because `ANTI_DEMO_ARTIFACT_ROOT` matches the `ANTI_DEMO_` sweep: the two
    fixtures now touch the same name, and ordering them by position in this file
    would be an invariant nothing states and a re-order would silently break --
    the sweep would run second and delete the isolation, sending every receipt
    this suite writes into the live artifact directory.
    """
    del contain_ambient_installation_environment
    monkeypatch.setenv("ANTI_DEMO_ARTIFACT_ROOT", str(tmp_path / "artifacts"))
