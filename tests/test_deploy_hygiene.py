"""Guards on the two repository-shape facts the Databricks Apps deploy rests on.

Neither of these is enforced by anything at runtime, both were arrived at after a
real outage, and both are the kind of thing a tool regenerates without being
asked. So they are asserted rather than assumed.

**`requirements.txt` must not exist.** The Apps runtime chooses its installer
from which files are present, and `requirements.txt` wins unconditionally: it
puts the app on pip and Python 3.11 whatever `pyproject.toml` asks for. Only when
it is absent, and `pyproject.toml` and `uv.lock` are both present, does the
runtime install with uv and honour `requires-python`. Since this source uses a
PEP 695 `type` alias, the pip path cannot even import it -- and the failure
arrives as a `SyntaxError` inside a container the platform reports as SUCCEEDED,
which is the worst available shape for it.

**`.python-version` must not exist.** uv reads it and discards a project
environment built on any other interpreter. A stray one pinning 3.12 deleted this
tree's provisioned 3.14 environment and began rebuilding it seconds before a
demo, which from the outside is indistinguishable from a hang. `requires-python`
is the one place the interpreter constraint is allowed to live, because it
constrains without pinning.

The third guard is the one that makes the first two matter: every Python file in
this tree must parse on the *oldest* interpreter `requires-python` admits. A file
that only parses on something newer turns `requires-python` into a lie, and the
symptom is again a `SyntaxError` at import inside a deploy that looked fine.

**`uv.lock` must name only hosts a build container can reach.** This is the
fourth guard and it was bought at the highest price of the four: twenty-two
consecutive deploys over three days failed because every one of the lockfile's
772 URLs pointed at `pypi-proxy.dev.databricks.com`, an internal dev-fleet mirror
the Apps build container has no route to. Nothing in this tree put it there --
a machine-global `~/.config/uv/uv.toml` did, invisibly, on whichever laptop last
resolved. The hostnames have since been rewritten to public PyPI, but that was a
one-off edit with nothing standing behind it, and re-poisoning the file takes one
`uv add` run with the wrong index active. Verified on 2026-08-22: resolving this
project through the internal mirror rewrites all 775 URLs straight back. So the
correction is asserted here, where it costs a test run to discover, rather than
in a deploy, where it cost three days.

**`frontend/package-lock.json` must too**, and it is the fifth guard. It had the
identical defect, from the identical cause: all 57 `resolved` URLs named
`npm-proxy.cloud.databricks.com`, written there by a machine-global `~/.npmrc`
that is not in this repository. It broke no deploy, because the Apps build
installs Python and the frontend bundle is committed as `dist`. It breaks
something worse instead: `npm ci` pins to the host in the lockfile, so for the
public repository this is intended to become, nobody outside Databricks could
build the frontend at all. Both lockfiles are now checked, so the next mirror to
leak in fails a test rather than a stranger's first `npm ci`.

**CI's own credential guard must be able to see the variables it names.** Sixth,
and the only one here about a file in `.github/`, because it is the same kind of
fact: a config file nothing at runtime enforces, arrived at after a real
near-miss, and wrong in a way that reads as correct. The guard grepped `env` for
`^(AWS_|DATABRICKS_|ANTI_DEMO_MANIFEST=)`, and that final `=` -- there to keep
the match exact -- excluded `ANTI_DEMO_MANIFEST_JSON`, the source
`manifest.load_manifest` consults *first* and the one that carries an entire
sealed installation inline instead of a path. Harmless on the runners it has run
on so far; the point is that it was the precise trap the step exists to set.
"""

from __future__ import annotations

import ast
import os
import re
import shutil
import subprocess
import tempfile
import tomllib
from pathlib import Path

import pytest

from server.manifest import PROJECT_ROOT

BANNED_FILES = ("requirements.txt", ".python-version")

#: The only hosts `uv.lock` may name. `files.pythonhosted.org` serves the wheels
#: and `pypi.org` is the index recorded beside them; both are reachable from any
#: build environment, which is the whole property being asserted. Anything else
#: is either an internal mirror or a host nobody has checked the build container
#: can route to, and the two failure modes are indistinguishable from here.
PUBLIC_PACKAGE_HOSTS = frozenset({"files.pythonhosted.org", "pypi.org"})

#: The same property, for npm. `frontend/package-lock.json` had all 57 of its
#: `resolved` URLs pointing at `npm-proxy.cloud.databricks.com`, put there by a
#: machine-global `~/.npmrc` carrying `registry=https://npm-proxy.cloud.databricks.com/`
#: -- the exact shape of the `~/.config/uv/uv.toml` problem above, in a different
#: package manager. It cost nothing here only because nothing outside Databricks
#: has ever tried to build this frontend; `npm ci` resolves that host from
#: nowhere else, so for a public repository it means the frontend cannot be built
#: at all.
PUBLIC_NPM_HOSTS = frozenset({"registry.npmjs.org"})

_LOCK_URL = re.compile(r"https://([^/\s\"']+)")
_RESOLVED_URL = re.compile(r'"resolved"\s*:\s*"https://([^/"]+)')


def foreign_package_hosts(lock_text: str) -> dict[str, int]:
    """Hosts named in a lockfile that are not public PyPI, and how often.

    Kept separate from the test that calls it so the guard itself can be tested
    against a known-bad lockfile. A guard that has only ever seen a passing input
    is not evidence of anything.
    """

    counts: dict[str, int] = {}
    for host in _LOCK_URL.findall(lock_text):
        if host not in PUBLIC_PACKAGE_HOSTS:
            counts[host] = counts.get(host, 0) + 1
    return counts


def foreign_npm_hosts(lock_text: str) -> dict[str, int]:
    """Hosts in `package-lock.json`'s `resolved` fields that are not public npm.

    Scoped to `resolved` rather than to every URL in the file, unlike the uv
    equivalent, because `package-lock.json` also carries `funding` links --
    opencollective, github, tidelift, eslint.org. Those are metadata that npm
    prints and never fetches, so flagging them would report 86 findings that
    nothing installs from, and a guard with 86 false positives is a guard nobody
    reads.

    Separate from its test for the same reason as `foreign_package_hosts`: it has
    to be runnable against a known-bad lockfile, or the only thing the test
    demonstrates is that it does not crash.
    """

    counts: dict[str, int] = {}
    for host in _RESOLVED_URL.findall(lock_text):
        if host not in PUBLIC_NPM_HOSTS:
            counts[host] = counts.get(host, 0) + 1
    return counts

#: Directories with no bearing on what gets deployed or what `uv sync` builds.
_SKIP_DIRECTORIES = {
    ".git",
    ".venv",
    "node_modules",
    "__pycache__",
    ".pytest_cache",
    ".ruff_cache",
    ".mypy_cache",
    "dist",
    "dist-before",
}


def _declared_requires_python() -> str:
    data = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    return data["project"]["requires-python"]


def _minimum_feature_version() -> tuple[int, int]:
    """The oldest interpreter `requires-python` admits, as an `ast` feature version."""
    match = re.search(r">=\s*(\d+)\.(\d+)", _declared_requires_python())
    assert match is not None, f"cannot read a lower bound from {_declared_requires_python()!r}"
    return (int(match.group(1)), int(match.group(2)))


def _python_sources() -> list[Path]:
    found: list[Path] = []
    for path in PROJECT_ROOT.rglob("*.py"):
        relative = path.relative_to(PROJECT_ROOT)
        if _SKIP_DIRECTORIES.intersection(relative.parts):
            continue
        found.append(path)
    return sorted(found)


@pytest.mark.parametrize("name", BANNED_FILES)
def test_banned_file_is_absent_from_the_repository_root(name: str) -> None:
    """The presence of either file is the bug; there is no benign version of it."""
    assert not (PROJECT_ROOT / name).exists(), (
        f"{name} is back in the repository root. It is banned, not merely unused -- "
        "see this module's docstring for which deploy or which local serve it breaks."
    )


@pytest.mark.parametrize("name", BANNED_FILES)
def test_banned_file_cannot_be_committed(name: str) -> None:
    """`.gitignore` is the layer that stops `git add -A` from resurrecting one.

    Asserted through git itself rather than by grepping `.gitignore`, so a rule
    that is present but shadowed by a later negation still fails.
    """
    result = subprocess.run(
        ["git", "check-ignore", "-q", "--no-index", name],
        cwd=PROJECT_ROOT,
        capture_output=True,
    )
    assert result.returncode == 0, (
        f"{name} is not gitignored, so a tool that regenerates it can get it committed."
    )


def test_pyproject_still_requires_the_version_this_source_needs() -> None:
    """`requires-python` is the whole mechanism now that no file pins anything."""
    assert _minimum_feature_version() >= (3, 12), (
        "server/connection_spike_journal.py uses a PEP 695 `type` alias, which needs "
        "3.12. Lowering requires-python below that deploys a tree that cannot import."
    )


def test_uv_lock_exists_and_agrees_with_pyproject() -> None:
    """uv.lock is what makes `requires-python` binding on the Apps runtime.

    Without it beside `pyproject.toml` the runtime falls back to pip on 3.11 --
    the same failure `requirements.txt` causes, reached a different way.
    """
    lock = PROJECT_ROOT / "uv.lock"
    assert lock.exists(), "no uv.lock, so the deployed app would install with pip on 3.11"
    declared = _declared_requires_python()
    locked = re.search(
        r'^requires-python = "([^"]+)"', lock.read_text(encoding="utf-8"), re.MULTILINE
    )
    assert locked is not None, "uv.lock records no requires-python"
    assert locked.group(1) == declared, (
        f"uv.lock was resolved for requires-python {locked.group(1)!r} and pyproject.toml "
        f"now says {declared!r}. Run 'uv lock'."
    )


def test_uv_lock_names_only_hosts_a_build_container_can_reach() -> None:
    """The guard on the twenty-two-deploy outage. See this module's docstring.

    Asserted over the whole file rather than over the known-bad hostname, because
    the next wrong index will not be the last one: any host that is not public
    PyPI is one nobody has shown the Apps builder can route to.
    """
    lock = PROJECT_ROOT / "uv.lock"
    foreign = foreign_package_hosts(lock.read_text(encoding="utf-8"))
    assert not foreign, (
        "uv.lock names hosts that are not public PyPI: "
        + ", ".join(f"{host} ({count} URLs)" for host, count in sorted(foreign.items()))
        + ". This is how twenty-two consecutive deploys failed: the Apps build "
        "container cannot route to an internal mirror. It usually means a `uv add` "
        "or `uv lock` resolved through a different index -- see docs/BOOTSTRAP.md "
        "for the two-step that adds a dependency without re-poisoning the lockfile."
    )


def test_the_lockfile_guard_catches_the_host_that_failed_twenty_two_deploys() -> None:
    """The guard, run against the lockfile shape that actually shipped.

    Without this the guard above is untested in the only direction that matters:
    it would pass just as happily if `foreign_package_hosts` always returned
    nothing, which is precisely the "surface reporting health it never checked"
    failure this repository keeps rediscovering.
    """
    poisoned = (
        '[[package]]\nname = "boto3"\n'
        'source = { registry = "https://pypi-proxy.dev.databricks.com/simple/" }\n'
        'wheels = [{ url = "https://pypi-proxy.dev.databricks.com/packages/aa/bb/'
        'boto3-1.43.71-py3-none-any.whl", size = 100 }]\n'
    )
    assert foreign_package_hosts(poisoned) == {"pypi-proxy.dev.databricks.com": 2}
    clean = poisoned.replace(
        "https://pypi-proxy.dev.databricks.com/packages/",
        "https://files.pythonhosted.org/packages/",
    ).replace("https://pypi-proxy.dev.databricks.com/simple/", "https://pypi.org/simple")
    assert foreign_package_hosts(clean) == {}


def test_package_lock_names_only_hosts_a_build_container_can_reach() -> None:
    """The same guard as `uv.lock`'s, for the lockfile with the same problem.

    All 57 `resolved` URLs named `npm-proxy.cloud.databricks.com`, and 54 of them
    predated the day this was found, so it was never a new mistake -- just one
    nobody outside Databricks had been in a position to hit. `npm ci` pins to the
    host in the lockfile, so the frontend of a public repository would have been
    unbuildable by everybody who does not work here.

    The rewrite was hostname-only: 266 package entries, identical key sets,
    identical versions, identical integrity hashes, and a byte delta of exactly
    57 x 12 characters of hostname. That the proxy is a transparent mirror is not
    assumed either -- tarballs fetched through it validate against the very
    `integrity` hashes recorded here, which npm computed upstream, so the bytes
    installed cannot depend on which of the two hosts serves them.
    """
    lock = PROJECT_ROOT / "frontend" / "package-lock.json"
    assert lock.exists(), "no frontend/package-lock.json, so `npm ci` has nothing to pin to"
    foreign = foreign_npm_hosts(lock.read_text(encoding="utf-8"))
    assert not foreign, (
        "frontend/package-lock.json names hosts that are not the public npm registry: "
        + ", ".join(f"{host} ({count} URLs)" for host, count in sorted(foreign.items()))
        + ". `npm ci` will fail for anyone who cannot route to them, which for a "
        "public repository is everybody. It usually means an `npm install` ran "
        "with a registry configured outside this repository -- check `~/.npmrc`, "
        "which is where the last one came from, and repoint the hostnames rather "
        "than regenerating the lockfile, because regenerating it re-resolves "
        "versions as well."
    )


def test_the_npm_lockfile_guard_catches_the_proxy_that_was_actually_shipped() -> None:
    """The npm guard, run against the lockfile shape that was committed.

    Verified in both directions, because a host check that always returns nothing
    passes the test above forever.
    """
    poisoned = (
        '{"packages": {"node_modules/react": {"version": "19.2.0", '
        '"resolved": "https://npm-proxy.cloud.databricks.com/react/-/react-19.2.0.tgz", '
        '"integrity": "sha512-AAAA"}}}'
    )
    assert foreign_npm_hosts(poisoned) == {"npm-proxy.cloud.databricks.com": 1}

    clean = poisoned.replace(
        "https://npm-proxy.cloud.databricks.com/", "https://registry.npmjs.org/"
    )
    assert foreign_npm_hosts(clean) == {}
    # Hostname-only, exactly as the real rewrite was: same version, same hash.
    assert '"version": "19.2.0"' in clean
    assert '"integrity": "sha512-AAAA"' in clean

    # And a `funding` link to a host npm never fetches from must not be reported,
    # or the guard drowns in its own output.
    funding = '{"packages": {"node_modules/x": {"funding": {"url": "https://opencollective.com/x"}}}}'
    assert foreign_npm_hosts(funding) == {}


def test_every_source_file_parses_on_the_oldest_supported_interpreter() -> None:
    """Otherwise `requires-python` claims support for a version that cannot import.

    This is the check that would have caught the original 502 before it was
    deployed: the tree parsed on the developer's 3.14 and not on the runtime's.
    """
    feature_version = _minimum_feature_version()
    failures: list[str] = []
    for path in _python_sources():
        source = path.read_text(encoding="utf-8")
        try:
            ast.parse(source, filename=str(path), feature_version=feature_version)
        except SyntaxError as error:
            relative = path.relative_to(PROJECT_ROOT)
            failures.append(f"{relative}:{error.lineno}: {error.msg}")
    version = ".".join(str(part) for part in feature_version)
    assert not failures, (
        f"these files do not parse on Python {version}, the oldest interpreter "
        "pyproject.toml's requires-python admits:\n  " + "\n  ".join(failures)
    )


def test_the_ci_credential_guard_can_see_the_variables_it_exists_to_catch() -> None:
    """The guard's pattern was blind to the higher-precedence manifest source.

    Every CI job that runs the suite or the bootstrap harness first asserts that no
    cloud credential and no live installation is present in the runner's
    environment, by grepping `env`. The pattern anchored the manifest term with a
    bare `ANTI_DEMO_MANIFEST=`, which made the match exact -- correct -- and in
    doing so excluded `ANTI_DEMO_MANIFEST_JSON`, which `manifest.load_manifest`
    reads *before* `ANTI_DEMO_MANIFEST` and which carries a whole sealed
    installation inline rather than a path to one. The variable with the higher
    precedence was the variable the guard could not see.

    Asserted by running the pattern rather than by reading it, because the defect
    was invisible in the text: `ANTI_DEMO_MANIFEST=` looks like it covers the
    manifest, and only feeding it both spellings shows that it does not.
    """

    workflow = (PROJECT_ROOT / ".github" / "workflows" / "ci.yml").read_text(
        encoding="utf-8"
    )
    patterns = re.findall(r"^\s*leaks='([^']+)'", workflow, re.MULTILINE)
    assert patterns, "no credential guard pattern found in .github/workflows/ci.yml"

    # Both jobs that run this repository's code carry the guard, and they have to
    # agree: the weaker of the two would be the one that decides.
    assert len(set(patterns)) == 1, f"the CI guards disagree: {sorted(set(patterns))}"

    must_catch = (
        "ANTI_DEMO_MANIFEST=/x/.anti-demo-v7/manifest.json",
        'ANTI_DEMO_MANIFEST_JSON={"manifest_version": 7}',
        "AWS_PROFILE=fe-vm",
        "AWS_SESSION_TOKEN=x",
        "DATABRICKS_TOKEN=x",
        # `_bind_deployed_runtime` exports these from a real seal and
        # `aws_auth.APP_AWS_BINDINGS` requires them, so they name a live
        # installation just as squarely as a credential does.
        "AURORA_SECRET_ARN=arn:aws:secretsmanager:us-west-2:1:secret:x",
        "LAKEBASE_ENDPOINT_NAME=projects/x/branches/production/endpoints/primary",
    )
    # Exactness has to survive the widening: the point of the `=` was that a
    # neighbouring name must not be swept up with the one being asserted about.
    must_ignore = (
        "ANTI_DEMO_MANIFESTO=nothing to do with a manifest",
        "ANTI_DEMO_SERVER_PORT=8001",
        "PATH=/usr/bin",
    )

    for pattern in set(patterns):
        compiled = re.compile(pattern)
        for line in must_catch:
            assert compiled.search(line), f"{pattern!r} lets {line.split('=')[0]} through"
        for line in must_ignore:
            assert not compiled.search(line), f"{pattern!r} wrongly flags {line.split('=')[0]}"


# ---------------------------------------------------------------------------
# The launcher's credential precedence
# ---------------------------------------------------------------------------
#
# Seventh guard, and the one with two live victims rather than a hypothetical
# one. `bootstrap.sh` reads .env.bootstrap and nothing else ever did, so a
# first-time user could finish `./bootstrap.sh --apply`, follow the script's own
# closing advice to run `./antidemo serve`, and get a server with no AWS
# credentials -- quietly, because `validate_runtime_auth`'s raise is caught by
# the credential probe and becomes the verdict `absent`, which degrades /readyz
# to a 200 and takes four of the six rounds off the card. A working-looking app,
# missing two thirds of its content, with nothing on screen saying why.
#
# The obvious fix -- `set -a; . .env.bootstrap; set +a` ahead of the serve --
# breaks the maintainer's laptop, which is the second victim and the reason this
# is asserted rather than assumed. Both AWS key fields in his .env.bootstrap are
# deliberately empty, so that `--deploy-only` does not republish whatever
# credential his shell is carrying; `set -a` assigns those empty strings over
# his live exported session and the machine this is demonstrated from stops
# working. So the rule is "fill only what is unset or empty", and it is exactly
# the kind of rule that reads as an implementation detail and gets simplified
# away by the next person to touch the file.
#
# Executed rather than pattern-matched. A test that greps the launcher for the
# string `:-` would pass on a launcher that no longer works.

LAUNCHER = PROJECT_ROOT / "antidemo"

#: Everything the launcher is allowed to carry out of the file, plus one name it
#: must not. `ROUND4_CATALOG` stands in for the whole rest of the file: it is a
#: real setting in `docs/bootstrap.env.example`, and a launcher that exported it
#: would let a stale line in a working directory silently retarget a serve.
_CARRIED = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION")
_NOT_CARRIED = "ROUND4_CATALOG"


def _launcher_environment(env_file_body: str, exported: dict[str, str]) -> dict[str, str]:
    """Run the launcher's loader against one file and one shell, report the result.

    The loader is lifted out of `antidemo` by the same markers the file is
    written around rather than copied here, so this cannot keep passing against
    a launcher that has changed underneath it. `exec uv run` is dropped: the
    point is what the environment looks like when it is reached.
    """

    body = LAUNCHER.read_text(encoding="utf-8")
    start = body.index("# The one-time setup file, carried forward to run time")
    end = body.index("exec uv run")
    loader = body[start:end]

    probe = (
        "set -euo pipefail\n"
        'SCRIPT_DIR="$PWD"\n'
        "set -- status\n"  # not `serve`, so the banner stays out of this
        f"{loader}\n"
        + "".join(
            f'printf "%s=%s\\n" {name} "${{{name}:-}}"\n'
            for name in (*_CARRIED, _NOT_CARRIED)
        )
    )
    with tempfile.TemporaryDirectory() as directory:
        (Path(directory) / ".env.bootstrap").write_text(env_file_body, encoding="utf-8")
        environment = {"PATH": os.environ.get("PATH", ""), "HOME": directory}
        environment.update(exported)
        result = subprocess.run(
            ["bash", "-c", probe],
            cwd=directory,
            capture_output=True,
            text=True,
            timeout=60,
            env=environment,
        )
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
    return dict(
        line.split("=", 1) for line in result.stdout.splitlines() if "=" in line
    )


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash is not on PATH")
def test_an_empty_value_in_the_file_cannot_clobber_a_working_environment() -> None:
    """The maintainer's laptop, stated as a test.

    Both AWS key fields empty in the file, real session credentials exported.
    The exported pair must survive untouched. This is the assertion that fails
    if anybody replaces the loader with `set -a; . .env.bootstrap`.
    """

    resolved = _launcher_environment(
        "AWS_ACCESS_KEY_ID=\nAWS_SECRET_ACCESS_KEY=\nAWS_DEFAULT_REGION=us-west-2\n",
        {"AWS_ACCESS_KEY_ID": "ASIALIVESESSION", "AWS_SECRET_ACCESS_KEY": "live-secret"},
    )
    assert resolved["AWS_ACCESS_KEY_ID"] == "ASIALIVESESSION"
    assert resolved["AWS_SECRET_ACCESS_KEY"] == "live-secret"
    # And the region, which the shell did not have, still arrives from the file:
    # filling blanks is the whole mechanism, not a special case for credentials.
    assert resolved["AWS_REGION"] == "us-west-2"


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash is not on PATH")
def test_a_filled_in_file_reaches_a_serve_that_exported_nothing() -> None:
    """The stranger's laptop: the gap this whole mechanism exists to close."""

    resolved = _launcher_environment(
        "AWS_ACCESS_KEY_ID=AKIAFROMTHEFILE\n"
        "AWS_SECRET_ACCESS_KEY=file-secret\n"
        "AWS_DEFAULT_REGION=us-west-2\n"
        f"{_NOT_CARRIED}=someone_elses_catalog\n",
        {},
    )
    assert resolved["AWS_ACCESS_KEY_ID"] == "AKIAFROMTHEFILE"
    assert resolved["AWS_SECRET_ACCESS_KEY"] == "file-secret"
    assert resolved["AWS_REGION"] == "us-west-2"
    assert resolved[_NOT_CARRIED] == "", (
        f"the launcher exported {_NOT_CARRIED} out of .env.bootstrap. Only the AWS "
        "credential variables may be carried: everything else is the manifest's "
        "business at run time, and a stale line in a working directory must not be "
        "able to retarget a serve"
    )


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash is not on PATH")
def test_a_deliberate_export_outranks_the_file() -> None:
    """Both sides populated. The shell wins, so an override stays an override."""

    resolved = _launcher_environment(
        "AWS_ACCESS_KEY_ID=AKIAFROMTHEFILE\nAWS_SECRET_ACCESS_KEY=file-secret\n",
        {"AWS_ACCESS_KEY_ID": "AKIAEXPORTED", "AWS_SECRET_ACCESS_KEY": "exported-secret"},
    )
    assert resolved["AWS_ACCESS_KEY_ID"] == "AKIAEXPORTED"
    assert resolved["AWS_SECRET_ACCESS_KEY"] == "exported-secret"


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash is not on PATH")
def test_a_serve_with_no_credentials_anywhere_says_so_unmissably() -> None:
    """The silence is the defect; this is the assertion that it ended.

    Not a check that the launcher refuses -- it deliberately does not, because
    Rounds 4 and 6 reach Lakebase and no AWS and genuinely work. It is a check
    that the operator is told, by name, which file and which two variables.
    """

    body = LAUNCHER.read_text(encoding="utf-8")
    start = body.index("# The one-time setup file, carried forward to run time")
    end = body.index("exec uv run")
    probe = (
        "set -euo pipefail\n"
        'SCRIPT_DIR="$PWD"\n'
        "set -- serve\n"
        f"{body[start:end]}\n"
    )
    with tempfile.TemporaryDirectory() as directory:
        result = subprocess.run(
            ["bash", "-c", probe],
            cwd=directory,
            capture_output=True,
            text=True,
            timeout=60,
            env={"PATH": os.environ.get("PATH", ""), "HOME": directory},
        )
    assert result.returncode == 0, result.stderr
    warning = result.stderr
    assert "AWS_ACCESS_KEY_ID" in warning and "AWS_SECRET_ACCESS_KEY" in warning
    assert ".env.bootstrap" in warning, "the warning must name the file to fill in"
    assert "credentials_state" in warning, (
        "the warning must say how to confirm the fix from the server rather than "
        "from the absence of this message"
    )
    # Nothing is printed when the credentials are there, or the banner becomes
    # noise an operator learns to scroll past.
    with tempfile.TemporaryDirectory() as directory:
        quiet = subprocess.run(
            ["bash", "-c", probe],
            cwd=directory,
            capture_output=True,
            text=True,
            timeout=60,
            env={
                "PATH": os.environ.get("PATH", ""),
                "HOME": directory,
                "AWS_ACCESS_KEY_ID": "AKIAPRESENT",
                "AWS_SECRET_ACCESS_KEY": "present-secret",
            },
        )
    assert quiet.returncode == 0, quiet.stderr
    assert "AWS_ACCESS_KEY_ID" not in quiet.stderr
