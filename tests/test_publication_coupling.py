"""Nothing that ships may depend on a document publication withholds.

This is the standing guard for a coupling class that has now bitten three times,
and each time the instance was fixed and the class was left open:

* `tests/test_pipeline_power.py` read `OPEN-FINDINGS.md` to scan it for figures,
  so the public repository's first CI run was a `FileNotFoundError` against a
  tree that was correct;
* `tests/test_no_live_identifiers_committed.py` failed outright on an exemption
  keyed to the same file;
* and `README.md` shipped two links to `TASKS.md`, which resolve here and 404
  there.

`tests/test_public_markdown_links.py` closed the first two shapes -- a link in a
shipping document, and a Python module in `tests/`, `server/` or `app.py` that
builds a path to a withheld name without probing for it first. This file is the
rest of the perimeter, and it exists separately because the three checks below
are about the *inventory* of withheld documents rather than about markdown:

1. **The inventory is written down three times and nothing compared the copies.**
   `.gitignore` is the one that actually governs publication -- the public
   repository is built by a fresh first commit, where every path starts untracked
   and `.gitignore` is the only thing consulted -- while `UNPUBLISHED_MARKDOWN`
   is what the two guards enforce and `LOCAL_ONLY_PROBES` is what asserts the
   rules still exist. When this was written the three had already drifted:
   `DEMO-MORNING.md` and `docs/DEPLOYED-NETWORK-PATH.md` were withheld by
   `.gitignore`, named in `LOCAL_ONLY_PROBES`, and reported *publishable* by
   `is_unpublished`. Two of the seven withheld documents were therefore outside
   both guards, and a module reading either would have passed a check whose whole
   purpose is to catch exactly that.

2. **The AST guard scanned three directories, not the repository.**
   `runner/connection_spike_runner.py` is tracked, is copied into the public
   repository, runs on the Round 5 EC2 runner, and is in none of `tests/`,
   `server/` or `app.py`. Scanning every tracked `*.py` costs nothing and removes
   the question of whether a new top-level package is covered.

3. **A shell script or a CI workflow can read a withheld file, and no AST can
   see it.** `withheld_file_reads` parses Python. `bootstrap.sh` is the first
   thing a stranger executes and the harnesses under `tests/` drive it; a
   `.github/workflows` step that ran `pytest OPEN-FINDINGS.md` or `grep`ped the
   findings log would be a red first run for a reader and is invisible to every
   other guard here. Nothing does this today, which is the point: the check is
   cheap and the class is proven.

**What is deliberately *not* checked: a mention.** `server/selfheal.py` cites
`OPEN-FINDINGS.md` in its module docstring, and that is correct and stays --
prose that names a withheld document is a reader losing a reference, not a
build that dies. The same distinction
`withheld_file_reads` draws for Python is drawn here for shell: what is reported
is a name in a position that *executes or reads*, not a name in prose.

**The other half of this class is not a shape any of these can see**, and it is
written here because this is where somebody will look. Making a read
*conditional* is necessary and not sufficient: a check that quietly measures less
in the public repository than in the private one has been weakened exactly where
nobody runs it. `test_pipeline_power.py::test_no_figure_for_this_meter_escapes_its_own_band`
is the live example -- it scans `OPEN-FINDINGS.md` where it exists and carries a
lower anti-vacuity floor where it does not -- and the fix there was to assert
*both* floors on every private run, so a public-only regression is red here
rather than there. Any future conditional read owes the same.

Every check below has a planted input beside it. A guard that has only ever seen
a passing tree is indistinguishable from a guard that cannot fail, and this
repository has already shipped one of those.
"""

from __future__ import annotations

import re
import subprocess

from test_public_markdown_links import Finding, is_unpublished, withheld_file_reads
from test_publish_runbook_is_ignored_and_present import LOCAL_ONLY_PROBES

from server.manifest import PROJECT_ROOT

#: Executable contexts no AST in this tree parses. Shell scripts, the `antidemo`
#: entry point, and the CI workflows -- the three places a withheld document
#: could be read by something that runs.
_EXECUTABLE_SUFFIXES = (".sh", ".yml", ".yaml")
_EXECUTABLE_PATHS = ("antidemo",)


def gitignored_documents(gitignore: str) -> tuple[frozenset[str], tuple[str, ...]]:
    """The withheld inventory, read from the mechanism rather than from a list.

    Returns the exact repository-relative paths and the filename prefixes.
    `.gitignore` is the authority because the public repository is produced by a
    fresh first commit: every path starts untracked, and a rule here is the only
    thing that decides whether it is copied.

    Only markdown is considered. The rest of `.gitignore` is build output, caches
    and live environment state -- none of it a document anything could link to or
    read, and all of it absent from a clone for reasons that have nothing to do
    with publication.
    """

    exact: set[str] = set()
    prefixes: list[str] = []
    for raw in gitignore.splitlines():
        rule = raw.strip()
        if not rule or rule.startswith("#") or not rule.endswith(".md"):
            continue
        if rule.endswith("*.md"):
            # `HANDOFF-*.md`: a family, matched unanchored by git and therefore
            # by prefix here.
            prefixes.append(rule[: -len("*.md")].lstrip("/"))
        elif "*" not in rule:
            # Anchored (`/TASKS.md`, `/docs/DEPLOYED-NETWORK-PATH.md`) or bare.
            exact.add(rule.lstrip("/"))
    return frozenset(exact), tuple(prefixes)


def withheld_names_in(
    text: str, exact: frozenset[str], prefixes: tuple[str, ...]
) -> list[tuple[int, str]]:
    """Every `(line, name)` where a withheld document is named in `text`.

    Used only against shell and workflow sources, where a bare filename has no
    innocent reading: these files do not link and do not narrate, and every
    document named in one is a document something is about to open.
    """

    patterns = [re.escape(name) for name in sorted(exact)]
    patterns += [re.escape(prefix) + r"[\w.-]*\.md" for prefix in sorted(prefixes)]
    if not patterns:
        return []
    found = re.compile("|".join(patterns))
    return [
        (number, match.group(0))
        for number, line in enumerate(text.splitlines(), 1)
        for match in found.finditer(line)
    ]


def _tracked() -> list[str]:
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return [name for name in result.stdout.split("\0") if name]


def _is_executable_source(name: str) -> bool:
    return name.endswith(_EXECUTABLE_SUFFIXES) or name in _EXECUTABLE_PATHS


def test_the_three_copies_of_the_withheld_inventory_agree() -> None:
    """`.gitignore`, `UNPUBLISHED_MARKDOWN` and `LOCAL_ONLY_PROBES` must not drift.

    `.gitignore` is the authority, so the assertion is one-directional: every
    document it withholds must be a document `is_unpublished` reports as
    withheld. The reverse is deliberately allowed -- `UNPUBLISHED_MARKDOWN` may
    name something `.gitignore` does not, which costs a false positive on a file
    nobody reads, and is the safe direction.

    The failure this catches is the one that had already happened: a rule added
    to `.gitignore` and not to the guards, which leaves the document withheld in
    fact and publishable as far as every check is concerned.
    """

    exact, prefixes = gitignored_documents(
        (PROJECT_ROOT / ".gitignore").read_text(encoding="utf-8")
    )
    assert exact, "no withheld markdown found in .gitignore -- the rules have been removed"
    assert prefixes, "the HANDOFF-*.md family rule is gone from .gitignore"

    unguarded = sorted(name for name in exact if not is_unpublished(name))
    assert not unguarded, (
        f".gitignore withholds {unguarded} from publication, but "
        f"tests/test_public_markdown_links.is_unpublished reports them publishable. "
        f"Both guards in that module key on it, so a link to one of these ships as "
        f"a 404 and a module reading one dies on the public repository's first CI "
        f"run. Add them to UNPUBLISHED_MARKDOWN."
    )
    for prefix in prefixes:
        probe = f"{prefix}probe-that-need-not-exist.md"
        assert is_unpublished(probe), (
            f".gitignore withholds the {prefix}*.md family and is_unpublished does "
            f"not, so an eleventh note is publishable. Add {prefix!r} to "
            f"UNPUBLISHED_PREFIXES."
        )

    # And the third copy, which asserts the rules exist rather than enforcing
    # them. A name here that no rule matches is a document believed withheld and
    # committable in fact; `test_local_only_notes_are_ignored` would catch that,
    # but only for the paths it already lists.
    for probe in LOCAL_ONLY_PROBES:
        assert is_unpublished(probe), (
            f"{probe} is asserted gitignored by "
            f"tests/test_publish_runbook_is_ignored_and_present.LOCAL_ONLY_PROBES "
            f"but is_unpublished reports it publishable, so neither guard in "
            f"tests/test_public_markdown_links.py can see it."
        )


def test_no_tracked_python_module_reads_a_withheld_document() -> None:
    """The AST guard, widened from three directories to the whole repository.

    `tests/test_public_markdown_links.py` scans `tests/`, `server/` and `app.py`.
    Every tracked `*.py` is copied into the public repository, so every tracked
    `*.py` is in scope -- `runner/connection_spike_runner.py` was not, and runs
    on the Round 5 EC2 runner where a `FileNotFoundError` is a dead round rather
    than a red test.
    """

    planted = withheld_file_reads(
        "from pathlib import Path\n"
        'root = Path(".")\n'
        'text = (root / "DEMO-MORNING.md").read_text()\n',
        "planted.py",
    )
    assert [finding.target for finding in planted] == ["DEMO-MORNING.md"], (
        "the guard no longer fires on a withheld document this file added to the "
        "inventory, so widening its scope proves nothing"
    )

    findings = [
        finding
        for name in _tracked()
        if name.endswith(".py")
        for finding in withheld_file_reads(
            (PROJECT_ROOT / name).read_text(encoding="utf-8"), name
        )
    ]
    detail = "\n".join(f"  {finding}" for finding in findings)
    assert not findings, f"source that will not run in the public repository:\n{detail}"


def test_no_shell_script_or_workflow_names_a_withheld_document() -> None:
    """The contexts no AST here parses, and the ones that run first.

    `bootstrap.sh` is the first thing a stranger executes; the CI workflows are
    the first thing their fork runs. A withheld document named in either is a
    failure on a correct tree, and `withheld_file_reads` -- which parses Python
    -- cannot see it.

    Any occurrence counts, deliberately. These files do not link and do not
    narrate: a document named in a shell script or a workflow step is a document
    something is about to read.
    """

    exact, prefixes = gitignored_documents(
        (PROJECT_ROOT / ".gitignore").read_text(encoding="utf-8")
    )

    planted = withheld_names_in(
        "- name: Test\n  run: uv run pytest -q\n  # inspect OPEN-FINDINGS.md\n"
        "  run: grep -c AKIA HANDOFF-anything-at-all.md\n",
        exact,
        prefixes,
    )
    assert [(line, name) for line, name in planted] == [
        (3, "OPEN-FINDINGS.md"),
        (4, "HANDOFF-anything-at-all.md"),
    ], "the scan must fire on an exact name and on a prefix match"

    findings: list[Finding] = []
    for name in _tracked():
        if not _is_executable_source(name):
            continue
        text = (PROJECT_ROOT / name).read_text(encoding="utf-8", errors="replace")
        findings += [
            Finding(
                name,
                line,
                target,
                f"names {target}, which publication withholds. In the public "
                f"repository the file is absent by design, so whatever this line "
                f"does with it fails on a correct tree -- and no AST guard in this "
                f"suite parses shell or YAML, so nothing else would say a word.",
            )
            for line, target in withheld_names_in(text, exact, prefixes)
        ]
    detail = "\n".join(f"  {finding}" for finding in findings)
    assert not findings, f"executable sources that need a withheld document:\n{detail}"


def test_the_inventory_is_read_from_the_rules_and_not_from_a_list() -> None:
    """The derivation itself, against input with every rule shape in it.

    A parser this guard depends on has to be shown working on known input, or
    a `.gitignore` reformat could silently reduce the inventory to nothing and
    every assertion above would pass by finding nothing to check.
    """

    exact, prefixes = gitignored_documents(
        "# a comment mentioning /NOT-A-RULE.md\n"
        "\n"
        "/TASKS.md\n"
        "/docs/DEPLOYED-NETWORK-PATH.md\n"
        "HANDOFF-*.md\n"
        "frontend/dist/\n"
        "*.log\n"
        "  /DEMO-MORNING.md  \n"
    )
    assert exact == frozenset(
        {"TASKS.md", "docs/DEPLOYED-NETWORK-PATH.md", "DEMO-MORNING.md"}
    )
    assert prefixes == ("HANDOFF-",)
    # Neither build output nor a commented rule is a withheld document.
    assert not any(name.endswith(("/", ".log")) for name in exact)
