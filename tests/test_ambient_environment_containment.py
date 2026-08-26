"""Every environment variable the code reads must be contained, or excused here.

Six times now, a run has gone red because of something exported in the shell that
launched it and nothing else: the AWS credential variables, `ANTI_DEMO_MANIFEST`,
`ANTI_DEMO_MANIFEST_JSON`, the local operator identity, the log limits, and
`ANTI_DEMO_RUNTIME_PRINCIPAL_ARNS` -- that last one turning twenty-four tests red
for precisely the contributor who followed `docs/DEPLOYED-NETWORK-PATH.md`.  CI
cannot catch any of them, because a GitHub runner exports nothing, so the suite
is green there and red on the machine of whoever cloned it.  Each was fixed by
adding one more name to a list in `tests/conftest.py`, and the next one arrived
anyway.

`conftest.ambient_names` stopped that by sweeping the project's *prefixes* rather
than a list of names, so a variable is contained on the day it is written.  What
a prefix rule cannot cover is a variable named outside the convention, and six of
those already exist -- `ROUND4_CATALOG`, `ROUND5_APP_PRINCIPAL_ARN`,
`EXPECTED_POSTGRES_MAJOR`, `PGUSER`, `USER` and `UV_PROJECT_ENVIRONMENT`.  One of
them, `ROUND4_CATALOG`, is not hypothetical: exported as `main`, which is both the
default and what `bootstrap.sh --print-env` prints, it turns two tests in
`test_lifecycle.py` red.  So off-convention names are added, and they do cost
runs.  This is the check that the next one is noticed when it is written rather
than when it breaks somebody's afternoon.

Derived from the source on both sides rather than written down.  The read set
comes out of an AST walk of what `server/` and `app.py` actually ask the
environment for, and containment is decided by *calling* `ambient_names`, not by
matching strings against the tuples that feed it -- so neither half can drift
from what the suite really does.  The only hand-written part is the excuse list,
which is the part that has to be hand-written: "why this one is left alone" is
not derivable from anything.
"""

from __future__ import annotations

import ast
from pathlib import Path

from conftest import DELIBERATELY_NOT_CONTAINED, ambient_names

REPO = Path(__file__).resolve().parents[1]

#: What a `_read_sites` that has quietly stopped working would no longer find.
#: Without this, a scan broken by a refactor reports an empty read set and every
#: assertion below passes for the wrong reason -- the failure mode this project
#: has learned to write a second assertion against rather than trust.  Chosen to
#: span all three shapes the walk understands and both halves of the tree: a bare
#: literal, a name bound to a module constant, and an off-convention name.
CANARIES = frozenset(
    {
        "ANTI_DEMO_ARTIFACT_ROOT",  # os.environ.get with a literal, server/receipts.py
        "ANTI_DEMO_MANIFEST",  # via the MANIFEST_ENV constant, server/process_registry.py
        "DATABRICKS_APP_NAME",  # read from app.py as well as server/
        "ROUND4_CATALOG",  # off-convention, and measured red when exported
        "PGUSER",  # off-convention, and not this project's prefix at all
    }
)

_READ_METHODS = frozenset({"get", "pop", "setdefault"})


def _looks_like_an_env_name(value: object) -> bool:
    """Screen out the incidental string constants without screening out `PGUSER`.

    An underscore is *not* required, and the first version of this required one.
    `CANARIES` caught that immediately: `PGUSER` and `USER` are both read by
    `server/`, neither has an underscore, and a scan that skipped them would have
    reported a clean sweep while leaving two variables uncovered -- the guard
    passing vacuously on its first run, which is the thing this file is most at
    risk of being.
    """

    return (
        isinstance(value, str)
        and len(value) >= 3
        and value.isupper()
        and value.replace("_", "").isalnum()
    )


def _read_sites(path: Path) -> dict[str, int]:
    """Every environment variable `path` reads, mapped to the line that reads it.

    Three shapes, which are the three this codebase writes: `os.environ.get("X")`
    and its `pop`/`setdefault` siblings, `os.getenv("X")`, and `os.environ["X"]`.
    Module-level `NAME = "X"` constants are resolved first, because the names
    that matter most are reached that way -- `os.environ.get(MANIFEST_ENV)` is
    the documented style here, precisely so the writer and the reader cannot
    drift, and a scan that only understood literals would miss every one of them.

    A read whose key this cannot resolve -- computed, or passed in as an argument
    -- is not reported.  That is a floor rather than a promise, and `CANARIES` is
    what keeps the floor from silently becoming zero.
    """

    tree = ast.parse(path.read_text(encoding="utf-8"))

    constants: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant):
            if _looks_like_an_env_name(node.value.value):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        constants[target.id] = node.value.value

    def resolve(node: ast.expr) -> str | None:
        if isinstance(node, ast.Constant) and _looks_like_an_env_name(node.value):
            return str(node.value)
        if isinstance(node, ast.Name):
            return constants.get(node.id)
        return None

    found: dict[str, int] = {}

    def record(name: str | None, node: ast.AST) -> None:
        if name is not None:
            found.setdefault(name, node.lineno)

    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.args:
            owner = ast.unparse(node.func.value)
            if node.func.attr == "getenv" and owner.endswith("os"):
                record(resolve(node.args[0]), node)
            elif node.func.attr in _READ_METHODS and owner.endswith("environ"):
                record(resolve(node.args[0]), node)
        elif isinstance(node, ast.Subscript) and isinstance(node.value, ast.Attribute):
            if node.value.attr == "environ":
                record(resolve(node.slice), node)

    return found


def environment_reads() -> dict[str, str]:
    """Variable name -> the `path:line` that reads it, across the shipped code."""

    sources = [*sorted(REPO.glob("server/**/*.py")), REPO / "app.py"]
    reads: dict[str, str] = {}
    for path in sources:
        for name, line in _read_sites(path).items():
            reads.setdefault(name, f"{path.relative_to(REPO)}:{line}")
    return reads


def test_the_scan_still_finds_the_reads_it_is_supposed_to_find() -> None:
    """Keeps the check below from passing because the walk stopped working.

    An AST walk against code somebody else is free to refactor is exactly the
    kind of derivation that fails open: rename a module, move a read behind a
    helper that takes the name as an argument, and the set shrinks silently while
    the guard keeps reporting success.  A guard that fires on nothing is worse
    than no guard, because it reads as coverage.
    """

    reads = environment_reads()

    assert len(reads) > 50, (
        f"the environment scan found only {len(reads)} reads across server/ and "
        "app.py, which is far below what this codebase has; the walk in "
        "_read_sites has probably stopped understanding a shape it used to"
    )
    missing = CANARIES - set(reads)
    assert not missing, (
        f"the environment scan no longer finds {sorted(missing)}. Either those "
        "reads are genuinely gone -- update CANARIES -- or _read_sites has "
        "stopped recognising how they are written, in which case every "
        "assertion in this file is now passing vacuously."
    )


def test_every_variable_the_code_reads_is_contained_or_deliberately_not() -> None:
    """The plan itself: nothing the code reads may be left to the ambient shell.

    Containment is checked by asking `ambient_names` -- the function the autouse
    fixtures and the import-time sweep both call -- what it would do with that
    one variable present.  Asserting against the behaviour rather than against
    `AMBIENT_NAME_PREFIXES` and friends is what makes this a check on the suite's
    real hermeticity instead of a check that two lists in the same file agree.
    """

    reads = environment_reads()
    uncontained = {
        name: site
        for name, site in reads.items()
        if name not in DELIBERATELY_NOT_CONTAINED and name not in ambient_names({name: "set"})
    }

    assert uncontained == {}, (
        "these environment variables are read by the shipped code, and "
        "tests/conftest.py neither contains them nor excuses them:\n"
        + "\n".join(f"  {name:38s} read at {site}" for name, site in sorted(uncontained.items()))
        + "\n\nA variable in this state decides test outcomes from whatever the "
        "developer happens to have exported, and CI cannot see it because a "
        "clean runner exports nothing. Either add it to conftest's "
        "AMBIENT_EXTRA_NAMES -- or give it a name matching AMBIENT_NAME_PREFIXES "
        "-- or add it to DELIBERATELY_NOT_CONTAINED with the reason, next to the "
        "two that are already there."
    )


def test_no_excuse_has_gone_stale() -> None:
    """An excuse for a variable nobody reads any more is a comment pretending to be a rule.

    The same obligation `test_no_live_identifiers_committed.py` puts on
    `KNOWN_UNCLEARED`: an exemption has to keep earning its place, or the list
    grows into a record of things that used to be true.
    """

    reads = environment_reads()
    stale = sorted(name for name in DELIBERATELY_NOT_CONTAINED if name not in reads)

    assert stale == [], (
        f"conftest.DELIBERATELY_NOT_CONTAINED excuses {stale}, which the shipped "
        "code no longer reads. Delete the entry: leaving it there claims a "
        "decision was made about a variable that no longer exists."
    )
