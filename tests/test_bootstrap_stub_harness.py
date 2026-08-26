from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
HARNESS = REPO / "tests" / "bootstrap_stub_harness.sh"

# Every case name, listed so a case that stops being registered fails here
# rather than silently going unrun.
EXPECTED_CASES = {
    "case_check_clean",
    "case_banned_files",
    "case_print_env",
    "case_s3_refuses_existing",
    "case_s3_needs_tf_111",
    "case_s3_needs_patch",
    "case_s3_bucket_states",
    "case_drift",
    "case_deploy_refusals",
    "case_deploy_happy",
    "case_deploy_seal_only",
    "case_deploy_record_merge",
    "case_deploy_seal_snapshot",
    "case_deploy_idempotent",
    "case_deploy_failures",
    "case_deploy_retry",
    "case_deploy_reads_app_yaml",
    "case_generated_artefacts",
    "case_no_regression",
}


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash is not on PATH")
def test_every_harness_case_is_registered():
    """A case that is defined but not registered runs nowhere.

    Cheap, and it catches the failure mode where a branch appears to be covered
    because someone wrote a case for it and forgot the CASES entry.
    """
    result = subprocess.run(
        ["bash", str(HARNESS), "--list"],
        cwd=REPO,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
    registered = set(result.stdout.split())
    defined = {
        line.split("(")[0]
        for line in HARNESS.read_text(encoding="utf-8").splitlines()
        if line.startswith("case_") and line.rstrip().endswith("() {")
    }
    assert registered == EXPECTED_CASES, registered ^ EXPECTED_CASES
    assert defined == registered, defined ^ registered


@pytest.mark.slow
@pytest.mark.skipif(shutil.which("bash") is None, reason="bash is not on PATH")
def test_bootstrap_shell_paths_hold_against_stubs():
    """Run the whole bootstrap.sh stub matrix.

    bootstrap.sh is shell, and the branches worth pinning are the ones that spend
    money or break a live demo: the refusal to migrate an existing generation's
    Terraform state, the refusal to deploy a manifest the app's FastAPI lifespan
    will reject, drift detection on the deployed seal, and every failure path in
    between. None of that is reachable from Python, so it is covered by a harness
    of stubbed `aws`, `databricks` and `terraform` binaries.

    The harness prints one line per assertion and exits non-zero on any failure;
    its output is attached so a failure names the case.

    Marked `slow` because it is 310 of the suite's 344 seconds -- 90% of the wall
    time for one test out of sixteen hundred. It is deselected by default and has
    its own CI job. Run it on its own with:

        uv run pytest -m slow

    `-m slow` is required even when naming this test by node id, because the
    default `-m 'not slow'` in pyproject.toml would otherwise deselect it and the
    run would report no tests rather than an error.
    """
    result = subprocess.run(
        ["bash", str(HARNESS)],
        cwd=REPO,
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
    assert " 0 failed" in result.stdout, result.stdout


#: Every executable bootstrap.sh requires before it will do anything at all.
REQUIRED_TOOLS = ("uv", "node", "npm", "databricks", "aws", "terraform", "psql", "python3", "jq")

#: The system directories the gate needs on PATH so bash has a shell and
#: `dirname` to run in. Mirrored rather than used directly -- see
#: `_sanitised_system_path`.
SYSTEM_PATH_DIRS = ("/usr/bin", "/bin")


def _sanitised_system_path(scratch: Path, sources: tuple[str, ...]) -> Path:
    """`sources` mirrored as symlinks, with every REQUIRED_TOOLS name left out.

    The gate below has to run with a real system PATH -- bootstrap.sh resolves
    its own directory with `dirname` before it reaches step 1, and a run that
    dies on a missing coreutil proves nothing about the tool check. But putting
    /usr/bin on PATH also decides, per machine, which of the nine required tools
    the gate can see, and that is not something a test may leave to the machine.

    It was left to the machine, and CI found it: `psql` is in /opt/homebrew/bin
    on the author's laptop and in /usr/bin on GitHub's ubuntu-latest image, so
    `test_missing_prerequisites_are_all_named_in_one_run` asked for three
    absences, got two, and had been passing locally for exactly as long as
    nobody ran it anywhere else. Mirroring the directories minus the nine names
    makes the answer the same everywhere: present iff this test shimmed it.

    Symlinks rather than a curated allowlist of coreutils, because an allowlist
    is the same bet in a different place -- it silently changes what this gate
    is tested against the moment anything new is called above step 1.
    """

    sanitised = scratch / "system-bin"
    sanitised.mkdir()
    for source in sources:
        directory = Path(source)
        if not directory.is_dir():
            continue
        for entry in directory.iterdir():
            if entry.name in REQUIRED_TOOLS:
                continue
            link = sanitised / entry.name
            # Earlier sources win, which is what PATH itself does. /bin is a
            # symlink to /usr/bin on most Linux images and on macOS, so without
            # this the second pass raises on every entry.
            if link.is_symlink() or link.exists():
                continue
            link.symlink_to(entry)
    return sanitised


def _run_tool_gate(
    tmp_path: Path,
    present: tuple[str, ...],
    script: str,
    *,
    credentials: bool = True,
    system_dirs: tuple[str, ...] = SYSTEM_PATH_DIRS,
) -> str:
    """Run `script` with only `present` on PATH and return everything it printed.

    `bootstrap.sh` does `cd` to its own directory, so a copy of it on its own is
    the tool check isolated from the repository -- nothing this step touches
    lives beside it. The run cannot reach a cloud: it exits inside step 1.

    `credentials=False` supplies no env file at all, which is the state of the
    laptop this gate exists for.

    Which tools the run can see is asserted before the run rather than described
    in a comment: every name in `present` must resolve and every other required
    tool must not. That assertion is the one this helper exists for -- a gate
    that reports a tool present because the runner happened to ship it measures
    the runner, not bootstrap.sh.
    """

    tree = tmp_path / "tree"
    bin_dir = tmp_path / "bin"
    tree.mkdir()
    bin_dir.mkdir()
    (tree / "bootstrap.sh").write_text(script, encoding="utf-8")
    for tool in present:
        shim = bin_dir / tool
        shim.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
        shim.chmod(0o755)
    path = f"{bin_dir}:{_sanitised_system_path(tmp_path, system_dirs)}"
    for tool in REQUIRED_TOOLS:
        resolved = shutil.which(tool, path=path)
        if tool in present:
            assert resolved is not None, f"{tool} was shimmed but does not resolve on {path}"
        else:
            assert resolved is None, (
                f"{tool} is supposed to be absent for this run but resolves to "
                f"{resolved}; the gate below would report it present and prove nothing"
            )
    argv = [shutil.which("bash") or "bash", str(tree / "bootstrap.sh")]
    if credentials:
        (tmp_path / "env").write_text(
            "DATABRICKS_HOST=https://example.cloud.databricks.com\n"
            "DATABRICKS_CLIENT_ID=stub\n"
            "DATABRICKS_CLIENT_SECRET=stub\n"
            "AWS_ACCESS_KEY_ID=AKIASTUB\n"
            "AWS_SECRET_ACCESS_KEY=stub\n"
            "AWS_DEFAULT_REGION=us-west-2\n",
            encoding="utf-8",
        )
        argv += ["--env-file", str(tmp_path / "env")]
    result = subprocess.run(
        argv,
        cwd=tree,
        env={"PATH": path, "HOME": str(tmp_path)},
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode != 0, f"{result.stdout}\n{result.stderr}"
    return result.stdout + result.stderr


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash is not on PATH")
def test_missing_prerequisites_are_all_named_in_one_run(tmp_path):
    """Nine tools are required, so one run must name every one that is absent.

    This is the first thing a stranger who has just cloned the repository runs,
    and it is the step most likely to find something missing. Reporting only the
    first absence turns a laptop short of three into three runs of the installer
    -- terraform, then psql, then jq -- with a download and a shell restart
    between each. The failure is invisible from inside a warm tree, which is
    every tree this has ever been run on.
    """

    script = (REPO / "bootstrap.sh").read_text(encoding="utf-8")
    # Absent because `_run_tool_gate` asserts they are unresolvable, not because
    # of where any particular machine keeps them. Choosing these three on the
    # grounds that "none of them is ever in /usr/bin" is what this file used to
    # do, and ubuntu-latest keeps psql there.
    absent = ("databricks", "terraform", "psql")
    output = _run_tool_gate(
        tmp_path, tuple(t for t in REQUIRED_TOOLS if t not in absent), script
    )
    for tool in absent:
        assert f"{tool} is not on PATH" in output, output
    # The summary line, not the per-tool lines: an operator acts on the refusal,
    # so the refusal itself has to carry all three names.
    refusal = output.split("FAIL")[-1]
    for tool in absent:
        assert tool in refusal, f"{tool} is missing but the refusal does not name it:\n{output}"


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash is not on PATH")
def test_a_required_tool_in_a_system_directory_does_not_leak_into_the_gate(tmp_path):
    """The runner's own copy of a required tool must not answer for a shim.

    This is the CI failure that found the bug, reproduced without needing the
    runner. The three tests around it choose which tools are absent and then
    assert bootstrap.sh names them; if a system directory on PATH supplies one,
    the gate reports it present, the assertion fails, and the failure reads as a
    bug in bootstrap.sh rather than in the harness. On GitHub's ubuntu-latest
    that tool is `psql`, at /usr/bin/psql.

    So a directory holding a `psql` is planted ahead of the real ones and the
    same absence is demanded. Passing means the mirror in
    `_sanitised_system_path` dropped it; failing means a machine's layout is
    once again deciding what these tests measure.
    """

    planted = tmp_path / "distro-bin"
    planted.mkdir()
    # Named for the tool CI actually leaked, and executable, because a file that
    # `command -v` would skip anyway would make this pass for the wrong reason.
    (planted / "psql").write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    (planted / "psql").chmod(0o755)
    assert shutil.which("psql", path=str(planted)) == str(planted / "psql")

    script = (REPO / "bootstrap.sh").read_text(encoding="utf-8")
    output = _run_tool_gate(
        tmp_path,
        tuple(t for t in REQUIRED_TOOLS if t != "psql"),
        script,
        system_dirs=(str(planted), *SYSTEM_PATH_DIRS),
    )
    assert "psql is not on PATH" in output, output
    # Singular, so the run really did see the other eight: a mirror that dropped
    # more than psql would still satisfy the line above.
    assert "required tools are not on PATH" not in output, output


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash is not on PATH")
def test_a_single_missing_prerequisite_is_named_in_the_singular(tmp_path):
    """One absence must not be reported as a list, and must still exit non-zero."""

    script = (REPO / "bootstrap.sh").read_text(encoding="utf-8")
    output = _run_tool_gate(
        tmp_path, tuple(t for t in REQUIRED_TOOLS if t != "databricks"), script
    )
    assert "databricks is not on PATH" in output, output
    assert "required tools are not on PATH" not in output, output


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash is not on PATH")
def test_the_tool_gate_runs_before_a_single_credential_is_asked_for(tmp_path):
    """The two tests above pass credentials, which is not the state this gate is for.

    The gate used to sit *below* the five interactive prompts. An operator on a
    fresh laptop -- no .env.bootstrap, `terraform` not installed -- was therefore
    asked for DATABRICKS_HOST, refused for not having it on a run with no
    terminal, and never told a binary was missing. Every subsequent run said the
    same thing, because supplying the credential only moved the refusal one
    prompt along. So the work that made this step name all nine absences in one
    run only ever reached people who already had credentials in place, and the
    laptop that needed it most got the least useful message in the script.

    Asserting the absence of "DATABRICKS_HOST" is the whole point: with the gate
    back in its old position this run still exits non-zero and still says
    something, which is why the positive assertions alone would not catch it.
    """

    script = (REPO / "bootstrap.sh").read_text(encoding="utf-8")
    output = _run_tool_gate(
        tmp_path,
        tuple(t for t in REQUIRED_TOOLS if t != "terraform"),
        script,
        credentials=False,
    )
    assert "terraform is not on PATH" in output, output
    assert "DATABRICKS_HOST" not in output, (
        "the run asked for a credential before telling the operator a tool was "
        f"missing:\n{output}"
    )


def _extract(source: str, opening: str, closing: str) -> str:
    """Lift one block verbatim out of bootstrap.sh, so a test cannot drift from it."""

    start = source.find(opening)
    assert start >= 0, f"bootstrap.sh no longer contains {opening!r}"
    end = source.find(closing, start)
    assert end >= 0, f"bootstrap.sh has no {closing!r} closing {opening!r}"
    return source[start : end + len(closing)]


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash is not on PATH")
def test_a_failed_provision_says_what_is_already_billing(tmp_path):
    """A provision that dies part-way has spent money, and must say so on the way out.

    Terraform creates the fleet before anything downstream of it can fail, so a
    non-zero `antidemo setup` normally means billed resources exist. The one warning
    that said so was printed before the confirmation -- half an hour and several
    thousand lines of Terraform output earlier, and skipped altogether under
    --yes. Left to `set -e`, the installer's last word was whatever Python
    printed, and an operator who walked away from that has a fleet nothing reaps.

    The block is lifted out of bootstrap.sh rather than restated, so this cannot
    pass against a copy of the code that no longer runs.
    """

    source = (REPO / "bootstrap.sh").read_text(encoding="utf-8")
    script = tmp_path / "block.sh"
    script.write_text(
        "set -euo pipefail\n"
        "RED=''; RESET=''\n"
        "ANTI_DEMO_MANIFEST=/tmp/gen/manifest.json\n"
        "MANIFEST_DIR=/tmp/gen\n"
        "SETUP_ARGS=(setup --owner me --no-serve)\n"
        + _extract(source, "die() {\n", "\n}\n")
        + _extract(source, "SETUP_STATUS=0\n", "\nfi\n"),
        encoding="utf-8",
    )
    # Named for the launcher the extracted block actually invokes. If this name
    # drifts from bootstrap.sh, bash cannot find it and the block still takes the
    # failure branch -- so `returncode != 0` and every instruction assertion below
    # pass against a run that never executed a provision at all. die() exits 1
    # either way, which is why the return code cannot tell them apart. The status
    # the message quotes can: the stub's 3, versus whatever bash reports for a
    # missing file (1 on bash 3.2, 127 elsewhere -- either way, not 3).
    launcher = tmp_path / "antidemo"
    launcher.write_text("#!/usr/bin/env bash\nexit 3\n", encoding="utf-8")
    launcher.chmod(0o755)

    result = subprocess.run(
        [shutil.which("bash") or "bash", str(script)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
    )
    # Non-zero, because the deploy stage below it must not run against a fleet
    # that was never finished.
    assert result.returncode != 0, result.stdout
    assert "exited 3" in result.stderr, (
        "the block did not run the stub launcher -- it reported "
        f"a different status:\n{result.stderr}"
    )
    for instruction in (
        "./antidemo cleanup --dry-run",
        "./antidemo cleanup --yes",
        "./bootstrap.sh --apply",
        "/tmp/gen/manifest.json",
    ):
        assert instruction in result.stderr, f"{instruction!r} missing from:\n{result.stderr}"


def _run_ready_gate(tmp_path, source: str, *, reset_ready: int, status: str = "ready"):
    """Run bootstrap.sh's own ready-install gate with nothing else around it.

    The gate is two functions -- the condition and the refusal -- called from two
    places in the script: a cheap probe just after argument parsing, and the
    provision step once the manifest has really been resolved. Both are lifted
    verbatim, so this cannot pass against a copy of the code that no longer runs,
    and it drives the condition against a real manifest file rather than a
    pre-set MANIFEST_STATUS -- which is what the early call site has to do.
    """

    manifest = tmp_path / f"manifest-{reset_ready}-{status}.json"
    manifest.write_text(
        f'{{"run_id": "ad-test-ready", "status": "{status}"}}\n', encoding="utf-8"
    )
    script = tmp_path / f"gate-{reset_ready}-{status}.sh"
    script.write_text(
        "set -euo pipefail\n"
        "RED=''; RESET=''\n"
        'MODE="apply"\n'
        f"RESET_READY={reset_ready}\n"
        + _extract(source, "die() {\n", "\n}\n")
        + _extract(source, "apply_would_reset_a_ready_install() {", "\n}\n")
        + _extract(source, "refuse_ready_install() {", "\n}\n")
        + f'\nif apply_would_reset_a_ready_install "{manifest}"; then\n'
        '  refuse_ready_install "$(jq -r .run_id ' + f'"{manifest}")"\n'
        "fi\n"
        'printf "REACHED THE PROVISION\\n"\n',
        encoding="utf-8",
    )
    return subprocess.run(
        [shutil.which("bash") or "bash", str(script)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
    )


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash is not on PATH")
@pytest.mark.skipif(shutil.which("jq") is None, reason="jq is not on PATH")
def test_apply_against_a_ready_install_refuses_and_names_the_redeploy_path(tmp_path):
    """--apply is not a resume of a finished install, and it must say so first.

    On a manifest that already reads `ready`, `antidemo setup` reconciles and
    then RESETS: both database lanes reseeded, the Round 3 anchors cleared. A
    bout running at the time dies. The only sentence that said so was printed
    under `ASSUME_YES == 0` and *after* the operator had committed to this path,
    so `--apply --yes` against a ready install said nothing at all -- and the
    reason an operator is usually there is that they wanted the app redeployed,
    which is `--deploy-only` and touches no database.

    The block is lifted out of bootstrap.sh rather than restated, so this cannot
    pass against a copy of the code that no longer runs.
    """

    source = (REPO / "bootstrap.sh").read_text(encoding="utf-8")

    refused = _run_ready_gate(tmp_path, source, reset_ready=0)
    assert refused.returncode != 0, refused.stdout
    assert "REACHED THE PROVISION" not in refused.stdout, refused.stdout
    # The refusal has to carry both the thing that would have happened and the
    # command that does what the operator actually wanted.
    for phrase in (
        "already 'ready'",
        "--apply is not a resume",
        "RESET of both database lanes",
        "./bootstrap.sh --deploy-only",
        "./bootstrap.sh --apply --reset-ready",
    ):
        assert phrase in refused.stderr, f"{phrase!r} missing from:\n{refused.stderr}"

    # And the explicit opt-in still works, or the gate is an outage rather than
    # a guard: an infra diff to apply has to remain reachable.
    allowed = _run_ready_gate(tmp_path, source, reset_ready=1)
    assert allowed.returncode == 0, allowed.stderr
    assert "REACHED THE PROVISION" in allowed.stdout, allowed.stdout

    # Any status other than `ready` is the interrupted provision --apply
    # genuinely does resume, and must not be caught by this.
    resumable = _run_ready_gate(tmp_path, source, reset_ready=0, status="provisioning")
    assert resumable.returncode == 0, resumable.stderr
    assert "REACHED THE PROVISION" in resumable.stdout, resumable.stdout


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash is not on PATH")
@pytest.mark.skipif(shutil.which("jq") is None, reason="jq is not on PATH")
def test_apply_against_a_ready_install_refuses_before_writing_anything(tmp_path):
    """"Nothing was changed" has to be true when the refusal above says it.

    The refusal used to live only at the provision step. By the time it fired the
    run had prompted for five credentials, written an OAuth profile into the
    operator's real ~/.databrickscfg, and -- under --apply -- possibly created
    the Databricks App to read its service principal. No money and no AWS, so it
    was never dangerous; it simply was not the "before anything runs" the script
    claimed. The header comment says so now, and this is what keeps the sentence
    honest: the whole script, a planted `ready` manifest, and an assertion that
    the profile the very next step would have written does not exist.

    Two generations are planted because the probe has to pick the highest by
    NUMBER: `.anti-demo-v10` sorts before `.anti-demo-v9` lexically, and a
    refusal quoting the wrong installation would send an operator to look at a
    generation that is not the one they were about to reset.
    """

    tree = tmp_path / "tree"
    bin_dir = tmp_path / "bin"
    home = tmp_path / "home"
    for directory in (tree, bin_dir, home):
        directory.mkdir()
    shutil.copy(REPO / "bootstrap.sh", tree / "bootstrap.sh")
    # jq and python3 are real: the probe reads the manifest with jq, and a stub
    # answering nothing would make this pass for the wrong reason.
    for tool in REQUIRED_TOOLS:
        real = shutil.which(tool) if tool in ("jq", "python3") else None
        shim = bin_dir / tool
        if real:
            shim.symlink_to(real)
        else:
            shim.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            shim.chmod(0o755)
    for generation, run_id in ((9, "ad-test-v9"), (10, "ad-test-v10-highest")):
        directory = tree / f".anti-demo-v{generation}"
        directory.mkdir()
        (directory / "manifest.json").write_text(
            f'{{"run_id": "{run_id}", "status": "ready"}}\n', encoding="utf-8"
        )

    def run(*args):
        return subprocess.run(
            [shutil.which("bash") or "bash", str(tree / "bootstrap.sh"), *args],
            cwd=tree,
            env={"PATH": f"{bin_dir}:/usr/bin:/bin", "HOME": str(home)},
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=60,
        )

    refused = run("--apply")
    assert refused.returncode != 0, refused.stdout
    assert "already 'ready'" in refused.stderr, f"{refused.stdout}\n{refused.stderr}"
    assert "ad-test-v10-highest" in refused.stderr, refused.stderr
    # No prompt was reached, so no credential was asked for on the way to a
    # refusal that never needed one.
    assert "DATABRICKS_HOST" not in refused.stdout + refused.stderr, refused.stdout
    assert not (home / ".databrickscfg").exists(), "the refusal wrote a Databricks profile"

    # The opt-in and the modes that are not a reset all have to get past it, or
    # the probe is an outage. Each then stops at the credential prompts, which is
    # the next thing in the script and proof the probe let it through.
    for args in (
        ("--apply", "--reset-ready"),
        ("--apply", "--new-generation"),
        (),
        ("--deploy-only",),
    ):
        allowed = run(*args)
        combined = allowed.stdout + allowed.stderr
        assert "already 'ready'" not in combined, f"{args} was refused:\n{combined}"
        assert "DATABRICKS_HOST is not set" in combined, f"{args}:\n{combined}"


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash is not on PATH")
def test_bootstrap_is_bash_3_2_compatible():
    """macOS still ships bash 3.2 as /bin/bash.

    `#!/usr/bin/env bash` takes whatever is first on PATH, which on a stock Mac
    is 3.2. A bash-4 construct here is a script that dies mid-run on a laptop
    that has never installed a newer bash -- and it dies inside an error path,
    where the operator most needs the message.
    """
    # Full-line comments are stripped so the note explaining *why* ${var^^} is
    # banned does not itself trip the ban.
    source = "\n".join(
        line
        for line in (REPO / "bootstrap.sh").read_text(encoding="utf-8").splitlines()
        if not line.lstrip().startswith("#")
    )
    forbidden = {
        "${var^^} / ${var,,} case modification": ("^^}", ",,}"),
        "mapfile / readarray": ("mapfile ", "readarray "),
        "associative arrays": ("declare -A", "local -A"),
    }
    for label, needles in forbidden.items():
        for needle in needles:
            assert needle not in source, f"{label}: found {needle!r}"
