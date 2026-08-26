"""Every relative link in a document that ships must resolve in the public tree.

This guard exists because of a class of defect the other guards in this tree
structurally cannot see. All of them grep for something that must be **absent**:
a live account ID, an internal proxy host, a ticket number, `requirements.txt`.
A link is the opposite shape. `[TASKS.md](TASKS.md)` is a correct link today --
the file is right there, on disk and tracked -- and it becomes a clickable 404
the moment the file is withheld from publication. Nothing that looks for
forbidden strings can notice a reference to something that is missing *by
design*, because the string is not forbidden and the target is not missing here.

Two of these shipped as far as the publish checklist, both in `README.md`, both
on the landing page, which is the first thing a stranger sees. They were caught
by eye. Only one instance of a class ever gets caught by eye, so the class is
asserted instead.

**What "ships" means.** The public repository is produced by copying `HEAD` and
then *not* copying a named set of files -- the owner's working notes and the
publish procedure itself. So a shipping document is any tracked `*.md` whose path
is not in `UNPUBLISHED_MARKDOWN` and does not start with an
`UNPUBLISHED_PREFIX`. Those names are hardcoded rather than read from the
document that lists them, because that document is itself one of the excluded
files: in the public tree there is nothing to read it from. The list being wrong
in the safe direction costs a false pass on one file; being wrong in the unsafe
direction is impossible, since a name that never existed matches nothing.

**Three failures are reported, not one.** A target can be missing on disk, or
present on disk but untracked (so `git archive HEAD` will not copy it, which is
the `frontend/dist` shape), or present and tracked but deliberately excluded from
publication. The third is the one that shipped, and it is invisible to every
check that runs against *this* tree, because here the file exists.

**Anchors are checked too.** `docs/DEPLOY.md#what-is-untested` is a broken link
if that heading is renamed, and a renamed heading is a routine edit that nothing
else in this tree would notice. Heading slugs are computed the way GitHub
computes them, which includes one subtlety that produced two false positives
while this was being written: inline code in a heading contributes its *text*, so
`### Why \\`frontend/dist\\` is not in git` is `#why-frontenddist-is-not-in-git`.
Blanking code spans before reading headings -- correct for finding links, since a
link inside backticks is not a link -- silently changed every slug that contained
any. Hence `blank_fences` and `blank_code_spans` are separate, and headings only
get the first.

The other false-negative worth naming: a link may be **wrapped across a line**,
with `[the text` on one line and `](target)` on the next. Both instances in
`docs/BOOTSTRAP.md` are that shape, so a line-at-a-time scan misses them and
reports a clean run over a file it only partly read. The scan is therefore whole
-document with offsets converted back to line numbers.

**Commands are the other half, and they are the half a link check cannot see.**
`blank_fences` removes fenced blocks -- correct for links, since a link in a
shell example is not a link -- and a documented command lives in nothing else.
So when the CLI entry point `demo` was renamed `antidemo`, every `[DEMO.md]`
link failed this guard immediately and loudly, and `./demo cleanup --yes` in the
README's cost warning did not, because it is not a link. That one is the worse
of the two: it is the single command that stops a ~$30/day installation from
billing, printed above the fold for a stranger who needs to stop the bleeding,
and it named an executable that no longer existed. Hence
`unresolvable_executables`, which reads the fences the link scan throws away and
resolves `./…` against the repository root -- where a reader of these documents
is standing when they paste one.

**The third class is the inverse of a link: a sibling that asserts what another
shipping document *says*.** A link check asks whether the target exists. It never
reads it. So `NOTICE.md` could tell a reader that "`README.md` marks those two
rows `LOG-DERIVED`" while `README.md` contained no such marker, and every guard
in this tree passed: the link resolves, the heading exists, no forbidden string
appears, and the sentence is ordinary English. The reader who follows the
project's own disclaimer file to check a claim is the one who finds out, and
finding nothing is worse for them than never having been pointed there.
`unsupported_attributions` is that check, and the shape it needs is
**attribution**: not "does this document contain this sentence" -- which any tree
satisfies and which is the vacuous form this repository has spent a day deleting
-- but "one document says another contains X, so does it".

Every helper here is pure and every failure mode below has a test that feeds it
known-bad input, because a guard that has only ever seen a passing tree is not
evidence of anything.
"""

from __future__ import annotations

import ast
import posixpath
import re
import subprocess
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import NamedTuple

import pytest

from server.manifest import PROJECT_ROOT

#: Documents the publish procedure deliberately does not copy into the public
#: repository. Each is the owner's internal working record; `PUBLISH.md` is the
#: procedure itself, which must not ship inside the thing it produces. A link to
#: any of them resolves here and 404s there.
#:
#: The last two were a hole in this set rather than an addition to it. Both are
#: withheld by an anchored `.gitignore` rule and both are named in
#: `test_publish_runbook_is_ignored_and_present.LOCAL_ONLY_PROBES`, so the
#: intention to withhold them was recorded twice -- and `is_unpublished` returned
#: False for both, which made this file's two guards blind to five of the seven
#: withheld documents' worth of coupling. They were invisible to
#: `withheld_file_reads` in particular: a module reading `DEMO-MORNING.md` was
#: exactly the `FileNotFoundError`-on-first-CI-run shape that guard exists to
#: catch, and the guard would have passed it. Nothing read them, so this closes a
#: gap rather than fixing a break. `tests/test_publication_coupling.py` derives
#: the same set from `.gitignore` and fails if these three copies drift again.
UNPUBLISHED_MARKDOWN = frozenset(
    {
        "OPEN-FINDINGS.md",
        "ROUND5_FIX_CHECKPOINT.md",
        "TASKS.md",
        "PUBLISH.md",
        "DEMO-MORNING.md",
        "docs/DEPLOYED-NETWORK-PATH.md",
    }
)

#: The agent handoff notes, of which there are ten and counting. Matched by
#: prefix so that writing an eleventh does not silently become publishable.
UNPUBLISHED_PREFIXES = ("HANDOFF-",)

_FENCE = re.compile(r"^[ \t]{0,3}(?:```|~~~).*$", re.MULTILINE)

#: Inline markdown links and images. The target alternation tolerates one level
#: of balanced parentheses so a URL ending in `(disambiguation)` is not truncated.
#: `re.DOTALL` is what lets the link text span a line break.
_LINK = re.compile(
    r"!?\[(?:[^\[\]]|\[[^\[\]]*\])*\]\((?P<target>[^()\s]*(?:\([^()]*\)[^()\s]*)*)\)",
    re.DOTALL,
)

#: A repository-relative command a document tells the reader to run. Anchored on
#: `./` deliberately: a bare `antidemo` is indistinguishable from prose about the
#: project, while `./` is unambiguously a path. Across every shipping document
#: this matches exactly two things, `./antidemo` and `./bootstrap.sh`, so the
#: false-positive budget is spent on nothing.
_EXECUTABLE = re.compile(r"(?:^|[^\w./-])(?P<target>\./[\w./-]+)")

_ATX_HEADING = re.compile(r"^[ \t]{0,3}(#{1,6})[ \t]+(.*?)[ \t]*#*[ \t]*$")
_EXPLICIT_ANCHOR = re.compile(r'<a\s+(?:id|name)="([^"]+)"')
_SCHEME = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*:")


class Finding(NamedTuple):
    """One link that will not resolve for a reader of the public repository."""

    source: str
    line: int
    target: str
    reason: str

    def __str__(self) -> str:  # pragma: no cover - failure formatting only
        return f"{self.source}:{self.line}  [{self.target}]  {self.reason}"


def is_unpublished(relative_path: str) -> bool:
    """Whether a repository-relative path is withheld from the public repo."""
    if relative_path in UNPUBLISHED_MARKDOWN:
        return True
    return any(relative_path.startswith(prefix) for prefix in UNPUBLISHED_PREFIXES)


def _blank(text: str, spans: Iterable[tuple[int, int]]) -> str:
    """Overwrite the given spans with spaces, keeping every newline in place.

    Offsets and line numbers therefore survive, which is what lets one pass over
    the whole document still report the line a finding is on.
    """
    out = list(text)
    for start, end in spans:
        for index in range(start, end):
            if out[index] != "\n":
                out[index] = " "
    return "".join(out)


def blank_fences(text: str) -> str:
    """Blank fenced code blocks. A link in a shell example is not a link."""
    fences = list(_FENCE.finditer(text))
    pairs = [
        (fences[i].start(), fences[i + 1].end()) for i in range(0, len(fences) - 1, 2)
    ]
    return _blank(text, pairs)


def blank_code_spans(text: str) -> str:
    """Blank inline code spans. Not applied before reading headings -- see module docstring."""
    return _blank(
        text, [m.span() for m in re.finditer(r"(`+)(?:(?!\1).)*?\1", text, re.DOTALL)]
    )


def heading_slugs(text: str) -> set[str]:
    """The anchors GitHub generates for a markdown document.

    Lowercase, punctuation dropped, spaces to hyphens, duplicates suffixed `-1`,
    `-2`. Inline code contributes its text, so fenced blocks are removed and code
    spans are not.
    """
    slugs: set[str] = set()
    for line in blank_fences(text).splitlines():
        match = _ATX_HEADING.match(line)
        if match is None:
            continue
        title = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", match.group(2))
        # Backticks and emphasis asterisks contribute nothing, but underscores
        # survive: `_` is a word character to GitHub's slugger, so
        # `### \`ANTI_DEMO_MANIFEST\` is required` is
        # `#anti_demo_manifest-is-required` and stripping them silently breaks
        # every anchor into a snake_case heading. This tree has many and no
        # heading that uses `_` as an emphasis marker.
        title = re.sub(r"[`*]", "", title).lower()
        base = re.sub(r"[^\w\s-]", "", title, flags=re.UNICODE).strip().replace(" ", "-")
        if not base:
            continue
        if base not in slugs:
            slugs.add(base)
            continue
        suffix = 1
        while f"{base}-{suffix}" in slugs:
            suffix += 1
        slugs.add(f"{base}-{suffix}")
    slugs.update(_EXPLICIT_ANCHOR.findall(text))
    return slugs


def relative_links(text: str) -> list[tuple[int, str]]:
    """Every `(line number, target)` for a link that is not an absolute URL."""
    body = blank_code_spans(blank_fences(text))
    found: list[tuple[int, str]] = []
    for match in _LINK.finditer(body):
        target = " ".join(match.group("target").split())
        if target.startswith("<") and target.endswith(">"):
            target = target[1:-1]
        if not target or _SCHEME.match(target) or target.startswith("//"):
            continue
        found.append((body.count("\n", 0, match.start()) + 1, target))
    return found


def executable_references(text: str) -> list[tuple[int, str]]:
    """Every `(line number, path)` a document tells the reader to execute.

    Fences and code spans are deliberately **not** blanked, which is the exact
    inverse of `relative_links` and is the entire point: a command only ever
    appears inside a fenced block or a code span, and blanking those is what
    makes the stale ones invisible.
    """
    found: list[tuple[int, str]] = []
    for match in _EXECUTABLE.finditer(text):
        # A trailing period is sentence punctuation, never part of a filename.
        # `./bootstrap.sh` is untouched by this; `./antidemo.` is not a path.
        target = match.group("target").rstrip(".")
        if target in {".", "./"}:
            continue
        # `start("target")`, not `start()`: the pattern consumes the character
        # before the path, and at the start of a line that character is the
        # previous line's newline, which would report every command one line early.
        found.append((text.count("\n", 0, match.start("target")) + 1, target))
    return found


def unresolvable_executables(
    root: Path, shipping: Sequence[str], tracked: frozenset[str]
) -> list[Finding]:
    """Every `./…` a shipping document says to run that will not be there.

    Resolved against `root` rather than the document's own directory, because
    that is where the reader is standing: `docs/BOOTSTRAP.md` says `./antidemo
    setup` meaning the one at the top of the checkout, not `docs/antidemo`.
    """
    root = root.resolve()
    findings: list[Finding] = []
    for source in sorted(shipping):
        text = (root / source).read_text(encoding="utf-8", errors="replace")
        for line, target in executable_references(text):
            resolved = (root / target).resolve()
            try:
                target_relative = str(resolved.relative_to(root))
            except ValueError:
                findings.append(
                    Finding(source, line, target, "resolves outside the repository")
                )
                continue
            if not resolved.exists():
                findings.append(
                    Finding(
                        source,
                        line,
                        target,
                        "no such command in the repository root, so this instruction "
                        "cannot be followed -- the usual cause is an entry point that "
                        "was renamed and a document that was not",
                    )
                )
                continue
            if target_relative not in tracked:
                findings.append(
                    Finding(source, line, target, "untracked, so it is never published")
                )
    return findings


def audit_markdown(
    root: Path, shipping: Sequence[str], tracked: frozenset[str]
) -> list[Finding]:
    """Resolve every relative link in `shipping` and return what will not resolve.

    `root` is the repository root, `shipping` the repository-relative paths of the
    documents that get published, and `tracked` every path git knows about --
    which is the real test of whether a target gets copied at all.
    """
    # Resolved, because link targets are resolved and `relative_to` compares text.
    # A checkout under a symlinked path -- `/tmp` and `/var` are symlinks on
    # macOS -- otherwise reports every single link as escaping the repository.
    root = root.resolve()
    slug_cache: dict[str, set[str]] = {}

    def slugs_for(relative_path: str, path: Path) -> set[str]:
        if relative_path not in slug_cache:
            slug_cache[relative_path] = heading_slugs(
                path.read_text(encoding="utf-8", errors="replace")
            )
        return slug_cache[relative_path]

    findings: list[Finding] = []
    for source in sorted(shipping):
        source_path = root / source
        text = source_path.read_text(encoding="utf-8", errors="replace")
        for line, target in relative_links(text):
            path_part, _, fragment = target.partition("#")

            if not path_part:
                if fragment and fragment not in slugs_for(source, source_path):
                    findings.append(
                        Finding(source, line, target, "no such heading in this document")
                    )
                continue

            resolved = (source_path.parent / path_part).resolve()
            try:
                target_relative = str(resolved.relative_to(root))
            except ValueError:
                findings.append(
                    Finding(source, line, target, "resolves outside the repository")
                )
                continue

            if is_unpublished(target_relative):
                findings.append(
                    Finding(
                        source,
                        line,
                        target,
                        f"{target_relative} is withheld from the public repository, "
                        "so this link exists here and 404s there",
                    )
                )
                continue
            if not resolved.exists():
                findings.append(Finding(source, line, target, "no such path"))
                continue
            if resolved.is_file() and target_relative not in tracked:
                findings.append(
                    Finding(source, line, target, "untracked, so it is never published")
                )
                continue
            if resolved.is_dir() and not any(
                name.startswith(f"{target_relative}/") for name in tracked
            ):
                findings.append(
                    Finding(source, line, target, "directory holds no tracked file")
                )
                continue
            if fragment and resolved.is_file() and resolved.suffix == ".md":
                known = slugs_for(target_relative, resolved)
                if known and fragment not in known:
                    findings.append(
                        Finding(
                            source, line, target, f"no such heading in {target_relative}"
                        )
                    )
    return findings


# --------------------------------------------------------------------------- #
# The same defect one class up: source that *reads* a withheld file
# --------------------------------------------------------------------------- #
#
# A broken link 404s for a reader. A test that reads one of these files does
# something worse: it dies with `FileNotFoundError` on the public repository's
# very first CI run, against a tree that is correct. That has now happened
# twice -- `test_pipeline_power.py` read `OPEN-FINDINGS.md` to scan it for
# figures, and `test_no_live_identifiers_committed.py` failed outright on an
# exemption keyed to it -- and both times the instance was fixed and the class
# was not. This is the class.
#
# It lives in this file rather than in one of its own because the list of
# withheld names is already here and a second copy of a list is how the two
# drift. `PUBLISH.md` has the authoritative copy, and deriving from it was the
# obvious idea and is the wrong one: `PUBLISH.md` is itself withheld, so a guard
# reading it would have to skip in the public repository -- which is the only
# place this guard matters, and it would be an instance of the very bug it
# polices. The trade is that this list and the runbook's can disagree. That is
# one-directional: a name here that is not withheld costs a false positive on a
# file nobody reads, and a name withheld that is missing here costs one missed
# read. Both are cheaper than a guard that is asleep where it is needed.

#: Reading a path is the bug. *Asking whether it is there* is the fix -- it is
#: how `test_pipeline_power` and `test_publish_runbook_is_ignored_and_present`
#: both stay correct in either repository -- so these are the one legitimate
#: thing to do with a path to a withheld file.
PRESENCE_ATTRS = frozenset({"exists", "is_file", "is_dir"})

#: Callables that turn a bare filename into something readable.
_PATH_CALLS = frozenset({"open", "Path"})


def _string_aliases(tree: ast.Module) -> dict[str, str]:
    """Module-level ``NAME = "OPEN-FINDINGS.md"`` bindings.

    Without these the guard is defeated by one variable, which is not a
    hypothetical: `test_publish_runbook_is_ignored_and_present` writes exactly
    this shape, correctly, and a future file could write it incorrectly.
    """
    aliases: dict[str, str] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign):
            targets, value = node.targets, node.value
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            targets, value = [node.target], node.value
        else:
            continue
        if not (isinstance(value, ast.Constant) and isinstance(value.value, str)):
            continue
        for target in targets:
            if isinstance(target, ast.Name):
                aliases[target.id] = value.value
    return aliases


def _withheld_name(node: ast.expr, aliases: dict[str, str]) -> str | None:
    """The withheld filename this expression denotes, if it denotes one.

    Only an exact filename counts. `"/usr/bin/vim TASKS.md"` in
    `test_process_registry.py` is a fake command line, not a path this tree
    opens, and a substring rule would report it forever.
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        value: str | None = node.value
    elif isinstance(node, ast.Name):
        value = aliases.get(node.id)
    else:
        return None
    return value if value is not None and is_unpublished(value) else None


def withheld_file_reads(source: str, where: str) -> list[Finding]:
    """Every place `source` builds a path to a withheld file without guarding it.

    Purely static: nothing is imported and nothing is executed. Mentioning one of
    these names in a docstring, a comment, a message or an exclusion list is
    fine and stays fine -- what is reported is a path *constructed* from the
    name, unless it is either handed straight to a presence predicate or bound to
    a name the module tests for presence somewhere.
    """
    tree = ast.parse(source, filename=where)
    aliases = _string_aliases(tree)
    parents = {
        id(child): node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)
    }
    guarded_names = {
        node.func.value.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in PRESENCE_ATTRS
        and isinstance(node.func.value, ast.Name)
    }

    findings: list[Finding] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
            withheld = _withheld_name(node.right, aliases)
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in _PATH_CALLS
            and node.args
        ):
            withheld = _withheld_name(node.args[0], aliases)
        else:
            continue
        if withheld is None:
            continue

        parent = parents.get(id(node))
        if isinstance(parent, ast.Attribute) and parent.attr in PRESENCE_ATTRS:
            continue
        if (
            isinstance(parent, ast.Assign)
            and len(parent.targets) == 1
            and isinstance(parent.targets[0], ast.Name)
            and parent.targets[0].id in guarded_names
        ):
            continue
        findings.append(
            Finding(
                where,
                node.lineno,
                withheld,
                f"builds a path to {withheld}, which publication withholds, without "
                f"first asking whether it exists. In the public repository the file "
                f"is absent by design, so this is a FileNotFoundError on the first "
                f"CI run. Guard it -- `if p.exists()` -- and say in the test what "
                f"the check is worth without it.",
            )
        )
    return findings


# --------------------------------------------------------------------------- #
# One class further out: a sibling document asserting what another one contains
# --------------------------------------------------------------------------- #
#
# Three real instances, all of them staged and none of them ever committed, so
# they are recoverable from the index blobs rather than from any commit --
# `NOTICE.md` at 95dd988 and `docs/DEPLOY.md` at b2d0597:
#
#   NOTICE.md:23        "`README.md` marks those two rows `LOG-DERIVED`"
#   docs/DEPLOY.md:1071 "which is why `README.md` marks both rows `LOG-DERIVED`"
#   docs/DEPLOY.md:1084 "of the eighteen receipt codes `README.md` quotes"
#
# `README.md` contained no `LOG-DERIVED` marker and quoted one receipt code, not
# eighteen. All three sentences are grammatical, all three targets resolve, and
# all three were written truthfully about a `README.md` that has since been
# rewritten -- which is the mechanism: the claim is made in one file and
# invalidated by an edit to another, and nothing joins the two.
#
# The join is deliberately narrow. Only three kinds of assertion are checked --
# a backticked 8-hex receipt code, a backticked SCREAMING-KEBAB marker, and a
# count of receipt codes -- because those are the three that appear in this tree
# and each is a literal string the target either holds or does not. Widening it
# to arbitrary numeric claims is the obvious next thought and is why the docstring
# below says what this cannot catch: the evidence table's own "11 verified
# ... races" is exactly that shape, it was four rows wrong on the day this was
# written, and no rule that would catch it survives contact with English prose.

#: Verbs that turn a mention of another document into a claim about its contents.
#: A document may be named without any claim being made -- "see `ROUNDS.md`" is
#: a pointer, not an assertion -- and only the assertions are checkable.
ATTRIBUTION_VERBS = (
    "marks", "quotes", "says", "states", "lists", "names", "contains",
    "flags", "records", "credits", "carries", "shows", "reads", "spells",
)
_ATTRIBUTION_VERB = re.compile(rf"\b(?:{'|'.join(ATTRIBUTION_VERBS)})\b")

#: Checked only in the span between the mention and the token, never over the
#: whole sentence. Sentence-wide was tried first and suppressed the real
#: `NOTICE.md` finding, whose sentence happens to contain "kept no receipt of
#: either bout" thirty words earlier.
_NEGATION = re.compile(r"\b(?:no|not|never|neither|nor|nothing|none|without)\b")

#: A sealed receipt code as every document in this tree writes one.
_RECEIPT_CODE = re.compile(r"`([0-9A-F]{8})`")

#: `LOG-DERIVED`, and any other backticked screaming-kebab badge. At least one
#: hyphen is required, which is what keeps `EECDD4D6` out of this pattern and
#: `ANTI_DEMO_MANIFEST` -- underscored, and there are many -- out of it too.
_MARKER = re.compile(r"`([A-Z][A-Z0-9]*(?:-[A-Z0-9]+)+)`")

#: A period ends a sentence only before whitespace or a closing delimiter. Even
#: then it is read off a masked copy, because `README.md` is three sentences to a
#: naive splitter and the mention and the claim then land either side of a break.
_TERMINATOR = re.compile(r"[.!?](?=[\s\"')\]]|$)")

#: Enough of English to read the counts this tree actually writes. `single` and
#: the articles are here because "quotes a single receipt code" is the live
#: instance; without them the one true attribution in the tree is unchecked and
#: the count rule is asleep.
NUMBER_WORDS = {
    "zero": 0, "no": 0, "a": 1, "an": 1, "one": 1, "single": 1, "two": 2,
    "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8,
    "nine": 9, "ten": 10, "eleven": 11, "twelve": 12, "thirteen": 13,
    "fourteen": 14, "fifteen": 15, "sixteen": 16, "seventeen": 17,
    "eighteen": 18, "nineteen": 19, "twenty": 20,
}
_QUANTITY = "|".join(sorted(NUMBER_WORDS, key=len, reverse=True))

#: Both orders, because both are written: "the eighteen receipt codes X quotes"
#: and "X quotes a single receipt code".
_CODE_COUNT = re.compile(
    rf"\b(?:the\s+)?(?P<before>{_QUANTITY}|\d+)\s+receipt\s+codes?\b[^.!?]*?\bquotes\b"
    rf"|\bquotes\b[^.!?]*?\b(?:a\s+)?(?P<after>{_QUANTITY}|\d+)\s+receipt\s+codes?\b",
    re.IGNORECASE | re.DOTALL,
)


class Claim(NamedTuple):
    """One assertion a document makes about what another document contains."""

    line: int
    kind: str
    value: str


def _period_free(text: str) -> str:
    """`text` with code spans and link targets stripped of sentence punctuation.

    Same length, same newlines, so every offset still maps back to a line. Only
    the characters that would fool the sentence splitter are replaced: a filename
    in backticks or in a link target is not a sentence boundary.
    """
    out = list(text)
    for pattern in (
        re.compile(r"(`+)(?:(?!\1).)*?\1", re.DOTALL),
        re.compile(r"\]\([^()\s]*\)"),
    ):
        for match in pattern.finditer(text):
            for index in range(*match.span()):
                if out[index] not in "\n ":
                    out[index] = "x"
    return "".join(out)


def sentence_spans(text: str) -> list[tuple[int, int]]:
    """`(start, end)` for each sentence, measured on a masked copy of `text`."""
    masked = _period_free(text)
    spans: list[tuple[int, int]] = []
    start = 0
    for match in _TERMINATOR.finditer(masked):
        spans.append((start, match.end()))
        start = match.end()
    if text[start:].strip():
        spans.append((start, len(text)))
    return spans


_CODE_MENTION = re.compile(r"`([\w./-]+\.md)`")
_LINK_MENTION = re.compile(r"\]\((?P<target>[^()\s#]*\.md)(?:#[^()\s]*)?\)")


def _resolve_mention(written: str, source: str | None) -> str:
    """The repository-relative path a written document name denotes.

    Two conventions, both of them the tree's own. A **link** resolves against the
    directory of the document it is written in, exactly as
    :func:`audit_markdown` resolves one -- `](../NOTICE.md)` in `docs/DEPLOY.md`
    is `NOTICE.md`. A **code span** resolves against the repository root, because
    that is how these documents are written: `` `README.md` `` in
    `docs/DEPLOY.md` means the landing page, not `docs/README.md`.

    Getting this wrong is not a theoretical risk. Matching on the basename alone
    -- the first version of this -- made one true sentence about `README.md` into
    three findings against `brand/README.md`, `docs/iam/README.md` and
    `infra/aws/README.md`, none of which it mentions.
    """
    base = posixpath.dirname(source) if source else ""
    return posixpath.normpath(posixpath.join(base, written)).lstrip("./") or written


def _mention_spans(sentence: str, document: str, source: str | None) -> list[tuple[int, int]]:
    """Where `sentence` names `document`, as inline code or as a link target.

    Prose alone -- "the README", "the landing page" -- does not count. A document
    named in prose cannot be resolved to a path, and guessing which one is meant
    is how a guard starts reporting on sentences that never made a claim.
    """
    spans: list[tuple[int, int]] = []
    for match in _CODE_MENTION.finditer(sentence):
        if _resolve_mention(match.group(1), None) == document:
            spans.append(match.span())
    for match in _LINK_MENTION.finditer(sentence):
        if _resolve_mention(match.group("target"), source) == document:
            spans.append(match.span())
    return sorted(spans)


def _negated(sentence: str, mentions: list[tuple[int, int]], span: tuple[int, int]) -> bool:
    """Whether the nearest mention and this token are separated by a negation.

    "carries no `LOG-DERIVED` marker" is a claim about absence and checking it
    would invert the guard. The window is the text strictly between the two, so a
    negation elsewhere in a long sentence does not reach it.
    """
    if not mentions:
        return False
    nearest = min(
        mentions,
        key=lambda m: min(abs(m[0] - span[1]), abs(span[0] - m[1])),
    )
    lo, hi = sorted((nearest, span))
    return bool(_NEGATION.search(sentence[lo[1] : hi[0]]))


def receipt_codes(text: str) -> set[str]:
    """Every distinct sealed receipt code a document quotes."""
    return set(_RECEIPT_CODE.findall(blank_fences(text)))


def attributed_claims(text: str, document: str, source: str | None = None) -> list[Claim]:
    """Every checkable assertion `text` makes about the contents of `document`.

    A sentence qualifies when it both names `document` and uses an attribution
    verb. Sentence scope is the whole of the precision here: it is what stops a
    receipt code four paragraphs below an unrelated mention from being read as a
    claim about it, and it is also this guard's main blind spot -- see
    :func:`unsupported_attributions`.

    `source` is the repository-relative path of the document `text` came from, and
    is needed only to resolve relative links the way a reader's browser would.
    """
    body = blank_fences(text)
    claims: list[Claim] = []
    for start, end in sentence_spans(body):
        sentence = body[start:end]
        mentions = _mention_spans(sentence, document, source)
        if not mentions or not _ATTRIBUTION_VERB.search(sentence):
            continue

        def line_of(offset: int, _start: int = start) -> int:
            return body.count("\n", 0, _start + offset) + 1

        for pattern, kind in ((_RECEIPT_CODE, "receipt code"), (_MARKER, "marker")):
            for match in pattern.finditer(sentence):
                if _negated(sentence, mentions, match.span()):
                    continue
                claims.append(Claim(line_of(match.start()), kind, match.group(1)))

        # Read off the masked sentence: the quantity and the verb are routinely
        # separated by the mention itself, and `README.md` holds a period.
        counted = _CODE_COUNT.search(_period_free(sentence))
        if counted is not None:
            written = counted.group("before") or counted.group("after")
            if not _negated(sentence, mentions, counted.span()):
                claims.append(
                    Claim(line_of(counted.start()), "receipt-code count", written.lower())
                )
    return claims


def unsupported_attributions(root: Path, shipping: Sequence[str]) -> list[Finding]:
    """Every claim one shipping document makes about another that is not true.

    Symmetric: `README.md` asserting something about `docs/DEPLOY.md` is checked
    the same way round. A document is never checked against itself, because a
    sentence describing its own contents is satisfied by the sentence.

    **What this cannot catch**, in descending order of how much it matters:

    * **A claim about a figure rather than a string.** "`README.md` states 11
      verified races" is the same defect class and is not checked, because no rule
      that reads a count out of English prose survives the prose in this tree.
      That exact row was four rounds out of date on the day this guard was
      written and this guard would not have said so.
    * **A paraphrase.** "`README.md` flags those rows as log-derived" asserts the
      same thing as the sentence that failed, in prose, with no token to compare.
    * **A claim split across a sentence boundary.** "`README.md` is the record. It
      marks both rows `LOG-DERIVED`." -- the second sentence names no document, so
      nothing is attributed. This is a chosen limit rather than a forced one, and
      the honest version of the reasoning is that widening the window to a
      paragraph was measured against this tree and is *also* clean on it today, so
      the narrower rule is not buying anything that can currently be shown. It is
      kept because a paragraph in these documents routinely names three or four
      documents, and then which one a loose code belongs to is a guess -- and a
      guessing guard reports on sentences that made no claim, which is how a guard
      gets deleted. Revisit it the day a real instance escapes through this hole.
    * **A document named only in prose.** "the landing page", "the README".
    * **A negated claim.** "carries no `LOG-DERIVED` marker" is skipped, so a true
      claim reworded as a negation stops being checked. One-directional: the cost
      is a missed finding, never a false one.
    * **Placement.** The target holding the string *somewhere* is all that is
      asserted. A marker that has drifted into an unrelated section still passes.
    """
    root = root.resolve()
    text_of = {
        name: (root / name).read_text(encoding="utf-8", errors="replace")
        for name in shipping
    }
    findings: list[Finding] = []
    for source in sorted(shipping):
        for document in sorted(shipping):
            if document == source:
                continue
            target_text = text_of[document]
            for claim in attributed_claims(text_of[source], document, source):
                if claim.kind == "receipt-code count":
                    stated = NUMBER_WORDS.get(claim.value)
                    if stated is None:
                        stated = int(claim.value)
                    actual = len(receipt_codes(target_text))
                    if stated == actual:
                        continue
                    findings.append(
                        Finding(
                            source,
                            claim.line,
                            claim.value,
                            f"says {document} quotes {stated} receipt code(s); it quotes "
                            f"{actual}. A reader who follows this sentence to check it "
                            f"finds a different document than the one described.",
                        )
                    )
                    continue
                if claim.value in target_text:
                    continue
                findings.append(
                    Finding(
                        source,
                        claim.line,
                        claim.value,
                        f"attributed to {document} as a {claim.kind}, which does not "
                        f"appear anywhere in it. The link resolves and the claim does "
                        f"not: this is the shape a reader checking a disclaimer hits.",
                    )
                )
    return findings


def _tracked_paths() -> frozenset[str]:
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return frozenset(name for name in result.stdout.split("\0") if name)


def shipping_markdown(tracked: Iterable[str]) -> list[str]:
    """Tracked markdown minus the documents publication withholds."""
    return sorted(
        name
        for name in tracked
        if name.endswith(".md") and not is_unpublished(name)
    )


def test_every_reference_in_a_shipping_document_resolves() -> None:
    """The guard itself, against the real tree.

    A failure here is a link a stranger clicks on the landing page and gets a
    404 from, an in-page anchor that silently scrolls nowhere, or a command they
    paste into a terminal that does not exist.
    """
    tracked = _tracked_paths()
    shipping = shipping_markdown(tracked)
    assert shipping, "no shipping markdown found -- the exclusion list has eaten everything"

    findings = [
        *audit_markdown(PROJECT_ROOT, shipping, tracked),
        *unresolvable_executables(PROJECT_ROOT, shipping, tracked),
    ]
    detail = "\n".join(f"  {finding}" for finding in findings)
    assert not findings, (
        f"references that will not resolve for a reader of the public repository:\n{detail}"
    )


def test_no_module_reads_a_file_that_publication_withholds() -> None:
    """No test or server module may read one of the withheld documents.

    Both directions in one test, deliberately. A guard that has only ever been
    run against a passing tree is indistinguishable from a guard that cannot
    fail, and this repository has already shipped one of those; the planted
    sources below are what make the difference observable every run rather than
    on the day somebody remembers to check.
    """

    caught = withheld_file_reads(
        'from pathlib import Path\n'
        'root = Path(".")\n'
        'files = [root / "OPEN-FINDINGS.md"]\n'
        'text = (root / "TASKS.md").read_text()\n'
        'RUNBOOK = "PUBLISH.md"\n'
        'body = open(RUNBOOK).read()\n'
        'notes = Path("HANDOFF-anything-at-all.md").read_text()\n',
        "planted.py",
    )
    assert [finding.target for finding in caught] == [
        "OPEN-FINDINGS.md",
        "TASKS.md",
        "PUBLISH.md",
        "HANDOFF-anything-at-all.md",
    ], "the guard must fire on a literal, an alias, `open`, `Path` and a prefix match"

    # And must stay quiet on the four shapes that are correct, or it is unusable
    # and somebody deletes it: naming one in prose, listing one, probing for one,
    # and reading one behind that probe -- which is how the two real readers of
    # `OPEN-FINDINGS.md` in this tree are written.
    assert (
        withheld_file_reads(
            'from pathlib import Path\n'
            '"""See OPEN-FINDINGS.md for the full account."""\n'
            'WITHHELD = ("OPEN-FINDINGS.md", "TASKS.md")\n'
            'MARKER = "OPEN-FINDINGS.md"\n'
            'root = Path(".")\n'
            'if (root / MARKER).is_file():\n'
            '    pass\n'
            'findings = root / "OPEN-FINDINGS.md"\n'
            'if findings.exists():\n'
            '    text = findings.read_text()\n',
            "innocent.py",
        )
        == []
    )

    sources = [
        *sorted((PROJECT_ROOT / "tests").rglob("*.py")),
        *sorted((PROJECT_ROOT / "server").rglob("*.py")),
        PROJECT_ROOT / "app.py",
    ]
    findings = [
        finding
        for path in sources
        if path.is_file()
        for finding in withheld_file_reads(
            path.read_text(encoding="utf-8"), str(path.relative_to(PROJECT_ROOT))
        )
    ]
    detail = "\n".join(f"  {finding}" for finding in findings)
    assert not findings, (
        f"source that will not run in the public repository:\n{detail}"
    )


def test_the_documents_that_do_not_ship_are_not_audited() -> None:
    """The exclusion list has to actually exclude, or the guard fails on itself.

    `OPEN-FINDINGS.md` and the handoff notes reference each other freely and are
    allowed to; they are the working record, not published documents.
    """
    for name in (*UNPUBLISHED_MARKDOWN, "HANDOFF-anything-at-all.md"):
        assert is_unpublished(name), f"{name} should be treated as unpublished"
    for name in ("README.md", "CONTRIBUTING.md", "docs/DEPLOY.md", ".github/SECURITY.md"):
        assert not is_unpublished(name), f"{name} ships and must be audited"


#: The three sentences this guard was built from, copied out of the index blobs
#: named in the section comment above rather than retyped. None of them is in any
#: commit -- they were staged, fixed in the working tree, and never committed --
#: so `git show :NOTICE.md` and `git show :docs/DEPLOY.md` are where they came
#: from and `git log` will not find them.
PRE_FIX_NOTICE = (
    "receipt, while the two specific Round 4 and Round 6 times quoted in the walkthrough were\n"
    "established from the deployed app's own log stream, because that app kept no receipt of "
    "either\n"
    "bout. `README.md` marks those two rows `LOG-DERIVED`; later bouts of both rounds are\n"
    "receipt-backed, so the rounds themselves no longer rest on log lines.\n"
)

PRE_FIX_DEPLOY_MARKER = (
    "is retro-fit a receipt onto the two bouts quoted above. Those two stay\n"
    "log-derived, which is why `README.md` marks both rows `LOG-DERIVED`. Treat the\n"
    "deploy as proven, those two rounds as proven from it, those two figures as\n"
    "log-derived, and the deployed runtime as no longer receipt-less.\n"
)

PRE_FIX_DEPLOY_COUNT = (
    "with no matching file therefore places the bout outside this filesystem. The\n"
    "inference holds, and it is checkable by hand: of the eighteen receipt codes\n"
    "`README.md` quotes, the only two with a file under `.anti-demo-v*/receipts/` are\n"
    "the two Round 3 recoveries it credits to a locally run server, and the other\n"
    "sixteen have none. But it is still an inference, and its strength is not\n"
)

#: What replaced them. Kept beside the failures because the green half of a
#: red-then-green demonstration is the half that proves the guard is not simply
#: allergic to the subject matter.
POST_FIX_DEPLOY = (
    "inference holds, and it is checkable by hand, though no longer against\n"
    "`README.md`: that file now quotes a single receipt code, `EECDD4D6`, and that one\n"
    "does have a file. Check it against the receipt tree instead.\n"
)

#: A `README.md` of the shape the sentences above were true of before it was
#: rewritten -- no marker, one code -- which is what makes them findings.
README_AFTER_REWRITE = (
    "# Lakebase: The Anti-Demo\n"
    "\n"
    "Every figure here traces to one sealed receipt: receipt `EECDD4D6`.\n"
    "\n"
    "| Round 6 | 7 receipt-backed runs and 1 earlier log-derived run |\n"
)


def test_the_three_staged_attributions_are_all_findings(tmp_path: Path) -> None:
    """Red before green, on the real sentences rather than on invented ones.

    Two documents asserted that `README.md` carried a `LOG-DERIVED` marker and one
    asserted it quoted eighteen receipt codes. It carried no marker and quoted one
    code. Every link in all three sentences resolved, so nothing else in this file
    saw them.
    """
    marker = [c for c in attributed_claims(PRE_FIX_NOTICE, "README.md")]
    assert marker == [Claim(3, "marker", "LOG-DERIVED")], (
        "the negation window must not be the whole sentence -- this one says "
        '"kept no receipt of either bout" before it makes its claim'
    )

    assert attributed_claims(PRE_FIX_DEPLOY_MARKER, "README.md") == [
        Claim(2, "marker", "LOG-DERIVED")
    ]
    assert attributed_claims(PRE_FIX_DEPLOY_COUNT, "README.md") == [
        Claim(2, "receipt-code count", "eighteen")
    ]

    # And the same three, resolved against the README they were left behind by.
    findings = unsupported_attributions(
        *_attribution_tree(
            tmp_path,
            {
                "README.md": README_AFTER_REWRITE,
                "NOTICE.md": PRE_FIX_NOTICE,
                "docs/DEPLOY.md": PRE_FIX_DEPLOY_MARKER + "\n" + PRE_FIX_DEPLOY_COUNT,
            },
        )
    )
    assert [(f.source, f.line, f.target) for f in findings] == [
        ("NOTICE.md", 3, "LOG-DERIVED"),
        ("docs/DEPLOY.md", 2, "LOG-DERIVED"),
        ("docs/DEPLOY.md", 7, "eighteen"),
    ]
    assert "does not appear anywhere in it" in findings[0].reason
    assert "quotes 18 receipt code(s); it quotes 1" in findings[2].reason


def test_the_sentences_that_replaced_them_are_not_findings(tmp_path: Path) -> None:
    """Green, and not by abstention: the replacement makes a checkable claim.

    `docs/DEPLOY.md` still tells the reader that `README.md` quotes one receipt
    code and names it. Both halves are verified against `README.md` and both hold.
    A guard whose green run is green because it found nothing to look at would be
    the vacuous thing this was written instead of, so the claim count is asserted
    as well as the finding count.
    """
    claims = attributed_claims(POST_FIX_DEPLOY, "README.md")
    assert sorted(claims) == [
        Claim(2, "receipt code", "EECDD4D6"),
        Claim(2, "receipt-code count", "single"),
    ], "the live attribution must be seen, or this guard is asleep on the real tree"

    assert (
        unsupported_attributions(
            *_attribution_tree(
                tmp_path,
                {"README.md": README_AFTER_REWRITE, "docs/DEPLOY.md": POST_FIX_DEPLOY},
            )
        )
        == []
    )


def test_a_pointer_is_not_a_claim_and_a_denial_is_not_checked() -> None:
    """The two shapes that must stay quiet, or the guard is unusable.

    A cross-reference asserts nothing about its target's contents, and a sentence
    saying a document does *not* contain something cannot be checked by looking
    for it. The second is a deliberate false negative and is named in
    `unsupported_attributions`.
    """
    assert attributed_claims("See [`ROUNDS.md`](ROUNDS.md) and `README.md`.\n", "README.md") == []
    assert (
        attributed_claims(
            "`README.md` quotes neither figure, so it carries no `LOG-DERIVED` marker.\n",
            "README.md",
        )
        == []
    )
    # A mention with no verb, four sentences away from a code, is not a claim.
    assert (
        attributed_claims(
            "See `README.md`. The cost work is elsewhere. Receipt `EECDD4D6` is the one.\n",
            "README.md",
        )
        == []
    )


def test_the_check_runs_in_both_directions(tmp_path: Path) -> None:
    """`README.md` describing a sibling is the same defect and is checked too.

    The three instances all pointed at `README.md`, so a README-only rule would
    have covered every known case and none of the next ones. Direction is not part
    of the defect: the landing page telling a reader that `NOTICE.md` marks
    something is as followable, and as wrong when the marker is not there.
    """
    findings = unsupported_attributions(
        *_attribution_tree(
            tmp_path,
            {
                "README.md": "See [`NOTICE.md`](NOTICE.md), which marks that row `LOG-DERIVED`.\n",
                "NOTICE.md": "# Notices\n\nNothing here is marked.\n",
            },
        )
    )
    assert [(f.source, f.target) for f in findings] == [("README.md", "LOG-DERIVED")]
    assert "attributed to NOTICE.md" in findings[0].reason


def test_no_shipping_document_asserts_what_another_does_not_contain() -> None:
    """The guard itself, against the real tree.

    A failure here is a reader following one of this project's own documents to
    verify a claim about another and finding the claim is not there. That is
    strictly worse than not having pointed them at it, because the pointer is what
    made the claim look checked.
    """
    tracked = _tracked_paths()
    shipping = shipping_markdown(tracked)
    findings = unsupported_attributions(PROJECT_ROOT, shipping)
    detail = "\n".join(f"  {finding}" for finding in findings)
    assert not findings, f"documents that describe each other inaccurately:\n{detail}"


def test_the_real_tree_has_something_for_this_guard_to_check() -> None:
    """A guard that fires on nothing looks like coverage and is not.

    Asserted separately from the run above so that the day the last cross-document
    attribution is edited away, this fails and says the guard has gone quiet --
    rather than the green run above continuing to imply it is watching something.
    """
    tracked = _tracked_paths()
    shipping = shipping_markdown(tracked)
    checked = [
        (source, document, claim)
        for source in shipping
        for document in shipping
        if document != source
        for claim in attributed_claims(
            (PROJECT_ROOT / source).read_text(encoding="utf-8"), document, source
        )
    ]
    assert checked, (
        "no shipping document makes a checkable claim about another's contents, so "
        "the guard above is passing on an empty set. Either an attribution was "
        "removed, or the extraction stopped recognising the shapes in this tree."
    )


def _attribution_tree(root: Path, files: dict[str, str]) -> tuple[Path, list[str]]:
    for name, body in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body, encoding="utf-8")
    return root, sorted(files)


def _tree(root: Path, files: dict[str, str]) -> tuple[list[str], frozenset[str]]:
    for name, body in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body, encoding="utf-8")
    tracked = frozenset(files)
    return shipping_markdown(tracked), tracked


def test_a_link_to_a_withheld_document_is_a_finding(tmp_path: Path) -> None:
    """The defect that shipped: target present and tracked, still a public 404."""
    shipping, tracked = _tree(
        tmp_path,
        {
            "README.md": "See [TASKS.md](TASKS.md) for build status.\n",
            "TASKS.md": "# Build tasks\n",
        },
    )
    findings = audit_markdown(tmp_path, shipping, tracked)
    assert [f.target for f in findings] == ["TASKS.md"]
    assert findings[0].line == 1
    assert "withheld" in findings[0].reason


def test_a_renamed_command_in_a_fenced_block_is_a_finding(tmp_path: Path) -> None:
    """The defect the link guard structurally cannot see, and why it cannot.

    Both halves are asserted together on purpose. If some future edit taught
    `audit_markdown` to read fences, the first assertion fails and says so,
    rather than leaving this check quietly redundant.
    """
    shipping, tracked = _tree(
        tmp_path,
        {
            "README.md": (
                "Stop the spend:\n"
                "\n```bash\n"
                "./demo cleanup --yes\n"
                "./antidemo cleanup --yes\n"
                "./build.sh\n"
                "```\n"
                "\nAnd `./antidemo doctor` changes nothing.\n"
            ),
            "antidemo": "#!/usr/bin/env bash\n",
        },
    )
    (tmp_path / "build.sh").write_text("#!/bin/sh\n")  # present, never committed

    assert audit_markdown(tmp_path, shipping, tracked) == [], (
        "the link guard blanks fences, which is exactly why commands need their own check"
    )

    findings = unresolvable_executables(tmp_path, shipping, tracked)
    assert [(f.line, f.target) for f in findings] == [(4, "./demo"), (6, "./build.sh")]
    assert "renamed" in findings[0].reason
    assert "untracked" in findings[1].reason


def test_a_missing_path_and_an_untracked_file_are_both_findings(tmp_path: Path) -> None:
    shipping, tracked = _tree(
        tmp_path,
        {"README.md": "[gone](docs/GONE.md) and [built](frontend/dist/index.html)\n"},
    )
    (tmp_path / "frontend" / "dist").mkdir(parents=True)
    (tmp_path / "frontend" / "dist" / "index.html").write_text("<html></html>")

    reasons = {f.target: f.reason for f in audit_markdown(tmp_path, shipping, tracked)}
    assert reasons["docs/GONE.md"] == "no such path"
    assert "untracked" in reasons["frontend/dist/index.html"]


def test_a_missing_anchor_is_a_finding_and_a_present_one_is_not(tmp_path: Path) -> None:
    shipping, tracked = _tree(
        tmp_path,
        {
            "README.md": "[here](#the-real-heading) and [gone](#renamed-last-week)\n"
            "\n## The real heading\n",
        },
    )
    findings = audit_markdown(tmp_path, shipping, tracked)
    assert [f.target for f in findings] == ["#renamed-last-week"]


def test_inline_code_in_a_heading_still_produces_githubs_slug(tmp_path: Path) -> None:
    """The false positive that blanking code spans too early would reintroduce."""
    assert heading_slugs("### Why `frontend/dist` is not in git\n") == {
        "why-frontenddist-is-not-in-git"
    }
    body = (
        "[why](#why-frontenddist-is-not-in-git)\n"
        "\n### Why `frontend/dist` is not in git\n"
    )
    shipping, tracked = _tree(tmp_path, {"README.md": body})
    assert audit_markdown(tmp_path, shipping, tracked) == []


def test_a_link_wrapped_across_two_lines_is_still_read(tmp_path: Path) -> None:
    """The false negative a line-at-a-time scan produces, which reads as a clean run."""
    shipping, tracked = _tree(
        tmp_path,
        {"README.md": "See the [Python\ndependencies](#no-such-section) section.\n"},
    )
    findings = audit_markdown(tmp_path, shipping, tracked)
    assert [f.target for f in findings] == ["#no-such-section"]
    assert findings[0].line == 1, "the line reported must be where the link opens"


def test_links_inside_fenced_and_inline_code_are_not_links(tmp_path: Path) -> None:
    body = (
        "Real: [ok](#kept)\n"
        "\n```bash\n# [fake](does/not/exist.md)\n```\n"
        "\nInline `[fake](also/missing.md)` stays quoted.\n"
        "\n## Kept\n"
    )
    shipping, tracked = _tree(tmp_path, {"README.md": body})
    assert audit_markdown(tmp_path, shipping, tracked) == []


def test_absolute_urls_are_not_resolved() -> None:
    text = (
        "[docs](https://docs.databricks.com/aws/en/oltp/) "
        "[mail](mailto:someone@example.com) "
        "[rel](docs/DEPLOY.md)\n"
    )
    assert relative_links(text) == [(1, "docs/DEPLOY.md")]


@pytest.mark.parametrize(
    ("heading", "slug"),
    [
        ("# Lakebase: The Anti-Demo", "lakebase-the-anti-demo"),
        ("## What this is not", "what-this-is-not"),
        (
            "### `ANTI_DEMO_MANIFEST` is required and has no default",
            "anti_demo_manifest-is-required-and-has-no-default",
        ),
        ("## Rounds 1–3 cost money", "rounds-13-cost-money"),
    ],
)
def test_heading_slug_matches_github(heading: str, slug: str) -> None:
    assert heading_slugs(heading + "\n") == {slug}


def test_repeated_headings_get_githubs_numeric_suffixes() -> None:
    assert heading_slugs("## Using it\n## Using it\n## Using it\n") == {
        "using-it",
        "using-it-1",
        "using-it-2",
    }
