"""`PUBLISH.md` must stay un-committable, and must not vanish unnoticed.

`PUBLISH.md` is the runbook for building the public repository. Step 2 of that
runbook excludes it from the repository it produces, so it is never tracked --
and because its subject matter *is* live identifiers, it attracts them. One
appendix row pasted in two live install-run IDs as evidence that they were
safely contained; they were not, because they were now in a file that was
untracked but *not ignored*, and therefore staged by any `git add -A`. The
owner's checkpoint commit is that command. Live values would have entered this
repository's history permanently.

So the file is gitignored, which closes that class by mechanism. The cost of
ignoring it is that `git clean -xfd` deletes it with no way back, and it is the
one document the owner needs at the moment of publishing. The tests below are
what make that loss loud instead of silent, and what stop the rule being
removed again without anybody noticing.

No test here can simply assert the file exists: in the public repository it is
*supposed* to be absent, and a test demanding it there would fail on a
stranger's clone for no reason. That is the same coupling that `KNOWN_UNCLEARED`
has with the documents the publish step withholds -- an assertion about a file
that is correctly missing over there. It is handled here by keying on a marker of
the private working repo rather than by asserting unconditionally, and by
degrading to a *skip* rather than a failure when that marker is absent.

**The marker itself failed once, silently, and that is why it is what it is
now.** It used to be `OPEN-FINDINGS.md` being present on disk. The owner deleted
that file while preparing publication -- deliberately, it was 4,098 lines of
internal notes -- and the deletion turned the first test below from checking into
skipping: in the private repository, during publication prep, which is the exact
situation it was written for. Nothing went red, and the suite reported less than
it had an hour earlier. See `in_private_working_repository` for what replaced it
and why no file being present can do this job.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from server.manifest import PROJECT_ROOT

#: The runbook, relative to the repository root.
RUNBOOK = "PUBLISH.md"


def _git(*args: str, root: Path = PROJECT_ROOT) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )


def _withheld_inventory(root: Path) -> tuple[frozenset[str], tuple[str, ...]]:
    """The withheld-document inventory, derived from `root`'s own `.gitignore`.

    Imported inside the function deliberately: `tests/test_publication_coupling.py`
    imports `LOCAL_ONLY_PROBES` from this module at import time, so a top-level
    import back into it is a cycle that fails during collection.
    """

    from test_publication_coupling import gitignored_documents

    exact, prefixes = gitignored_documents(
        (root / ".gitignore").read_text(encoding="utf-8")
    )
    assert exact or prefixes, (
        f"{root}/.gitignore withholds no markdown at all, so the marker below "
        f"cannot tell a private working repository from a published copy and the "
        f"runbook guard would fall silent again. Restore the rules."
    )
    return exact, prefixes


def in_private_working_repository(root: Path) -> bool:
    """Whether `root`'s history carries a document publication withholds.

    This is the "am I in the owner's working repository?" question, and it is
    asked of history rather than of the filesystem because every filesystem
    answer has already been shown to rot. The previous marker was
    `OPEN-FINDINGS.md` being present; the owner deleted it and this guard went
    quiet. Any *file being there* has that failure available to it -- each
    private-only document in this tree is either gitignored, so `git clean -xfd`
    takes it, or a note the owner is entitled to tidy away.

    History is neither. `git clean` does not touch `.git/`; deleting a file
    leaves every commit that carried it; and the published repository is built by
    a fresh `git init` whose first commit consults only `.gitignore`, so no
    commit there can carry a withheld document -- not in the first commit and not
    in the hundredth, because the rules that withheld them ship too. `PUBLISH.md`
    §3b pins that from the other side: history depth exactly 1, and no commit of
    this repository resolving in the new one.

    So this is true here, false in a published copy, false in a stranger's clone,
    and false in a source export with no history at all -- which is the one case
    that is a genuine "cannot tell", and skipping is the right answer for it.

    The inventory comes from `.gitignore` rather than from a list of names, which
    is the precedent `tests/test_publication_coupling.py` sets for exactly this
    question: key on the rules that govern publication, not on which files
    happen to exist today.
    """

    exact, prefixes = _withheld_inventory(root)
    # `:(top)` anchors to the repository root, matching an anchored `/NAME.md`
    # rule; the glob form matches a family rule, which git applies unanchored.
    pathspecs = [f":(top){name}" for name in sorted(exact)]
    pathspecs += [f":(top,glob)**/{prefix}*.md" for prefix in sorted(prefixes)]
    carried = _git("rev-list", "--all", "--max-count=1", "--", *pathspecs, root=root)
    return bool(carried.stdout.strip())


def runbook_loss_complaint(root: Path) -> str | None:
    """The failure message for a lost runbook, or `None` if it is still there.

    Separate from the test so the planted case below can watch it fire against a
    tree that is unmistakably private and has no runbook in it.
    """

    if (root / RUNBOOK).is_file():
        return None
    return (
        f"{RUNBOOK} is gone from the private working repository. It is gitignored, "
        f"so `git clean -xfd` removes it unrecoverably and git cannot restore it. "
        f"Recover it from a backup or an agent transcript before publishing -- it "
        f"is the procedure for building the public repository, including the "
        f"exclusion list and the pre-commit hook workaround."
    )


def test_publish_runbook_has_not_been_lost() -> None:
    """In the private repo the runbook must still be on disk.

    It is ignored, so git is not protecting it. Nothing else would say a word if
    it were cleaned away, and the owner would find out at the moment he reached
    for it.
    """
    if not in_private_working_repository(PROJECT_ROOT):
        pytest.skip(
            "no commit in this repository's history carries a document that "
            "publication withholds, so this is a published copy, a stranger's "
            f"clone or an export without history -- where {RUNBOOK} is correctly "
            "absent"
        )

    complaint = runbook_loss_complaint(PROJECT_ROOT)
    assert complaint is None, complaint


def _init_repository(root: Path) -> None:
    """A repository shaped like a published copy: real rules, nothing withheld."""

    root.mkdir(parents=True, exist_ok=True)
    _git("init", "-q", "-b", "main", root=root)
    # The real rules, so the derivation under test is exercised against the file
    # that actually governs publication rather than against a stub of it. They
    # ship, so a published copy has them too -- which is the point: the rules are
    # not the discriminator, history is.
    (root / ".gitignore").write_text(
        (PROJECT_ROOT / ".gitignore").read_text(encoding="utf-8"), encoding="utf-8"
    )
    (root / "README.md").write_text("a document that ships\n", encoding="utf-8")


def _commit(root: Path, *paths: str) -> None:
    _git("add", "-f", *paths, root=root)
    committed = _git(
        "-c",
        "user.name=planted",
        "-c",
        "user.email=planted@invalid.example",
        "commit",
        "-q",
        "--no-verify",
        "-m",
        "planted",
        root=root,
    )
    assert committed.returncode == 0, committed.stderr


def test_the_private_repository_marker_is_not_something_cleanup_can_delete(
    tmp_path: Path,
) -> None:
    """Watch the guard fail, and watch it stay quiet where it must.

    The check above is worth exactly what this test proves, and the reason this
    test exists is that its predecessor did not: the marker was a file being
    present, the owner deleted the file, and the guard switched itself off in the
    private repository during publication prep without anything going red.

    Two shapes of private repository, one per `.gitignore` rule shape -- an
    anchored name and a family prefix -- each committed and then *deleted from the
    working tree*, which is the tidy-up that broke the old marker. In both, the
    runbook is absent and the complaint must fire. And one repository shaped like
    a published copy: same rules, one commit, no withheld document in it, where
    the marker must read false so a stranger's clone skips rather than fails.
    """

    exact, prefixes = _withheld_inventory(PROJECT_ROOT)
    # Named from the rules rather than written out here, and never the runbook
    # itself: the private cases below turn on the runbook being *absent*.
    candidates = [sorted(exact - {RUNBOOK})[0], f"{sorted(prefixes)[0]}planted-note.md"]

    published = tmp_path / "published"
    _init_repository(published)
    _commit(published, ".gitignore", "README.md")
    assert not in_private_working_repository(published), (
        "a repository whose history carries no withheld document was read as the "
        "private working repository. This guard would then fail on every clone of "
        "the public repository, which is worse than the skip it replaced."
    )

    for withheld in candidates:
        private = tmp_path / f"private-{withheld.replace('/', '-')}"
        _init_repository(private)
        document = private / withheld
        document.parent.mkdir(parents=True, exist_ok=True)
        document.write_text("internal working notes\n", encoding="utf-8")
        _commit(private, ".gitignore", "README.md", withheld)

        # The tidy-up: the owner deletes his notes. Nothing about this tree is
        # any less private afterwards.
        document.unlink()
        assert not any((private / name).exists() for name in exact), (
            f"{withheld} is still on disk, so this case would pass under the "
            f"presence-based marker too and proves nothing about the fix"
        )

        assert in_private_working_repository(private), (
            f"a repository whose history carries {withheld} was not recognised as "
            f"a private working repository, so the guard is asleep in exactly the "
            f"place it was asleep before"
        )
        complaint = runbook_loss_complaint(private)
        assert complaint is not None, (
            f"{RUNBOOK} is absent from a tree whose history carries {withheld}, "
            f"and the guard said nothing -- which is the whole defect, restated"
        )
        assert RUNBOOK in complaint and "git clean" in complaint, complaint


def test_publish_runbook_cannot_be_committed() -> None:
    """If the runbook is present, `git add -A` must not be able to stage it.

    Two conditions, because either one alone is a false negative: a .gitignore
    rule has no effect on an already-tracked file, and a file being untracked
    today says nothing about the next `git add -A`.
    """
    if not (PROJECT_ROOT / RUNBOOK).is_file():
        pytest.skip(f"{RUNBOOK} is not present, so there is nothing to stage")

    # `--no-index` asks the question we actually mean -- does a rule match this
    # path -- rather than the question git answers by default, which quietly
    # excludes tracked paths and would report "no rule" for the worst case.
    ignored = _git("check-ignore", "-v", "--no-index", RUNBOOK)
    assert ignored.returncode == 0, (
        f"no .gitignore rule matches {RUNBOOK}, so the owner's checkpoint "
        f"`git add -A` will stage it. This file records live identifiers while "
        f"discussing them; two live install-run IDs reached it once already. "
        f"Restore the `/{RUNBOOK}` rule in .gitignore."
    )

    tracked = _git("ls-files", "--error-unmatch", RUNBOOK)
    assert tracked.returncode != 0, (
        f"{RUNBOOK} is TRACKED, which makes the .gitignore rule "
        f"({ignored.stdout.strip()}) decorative -- .gitignore does not apply to "
        f"files already in the index, so every future edit is committable. Run "
        f"`git rm --cached {RUNBOOK}` and check whether any past commit already "
        f"carries it."
    )


#: The other local-only documents, withheld from publication by the same settled
#: decision as the runbook: every agent handoff note, the owner's findings log,
#: and the four internal working documents beneath them. A probe name stands in
#: for the note pattern so this asserts the *rule* rather than the state of
#: whichever notes happen to exist today; the rest are named because their rules
#: are anchored to one exact path each.
#:
#: The last four were the gap this list closed. `TASKS.md` and
#: `ROUND5_FIX_CHECKPOINT.md` were already named in
#: `tests/test_public_markdown_links.py` as `UNPUBLISHED_MARKDOWN`, so the
#: intention to withhold them was recorded and tested *as a link target* while
#: the files themselves stayed committable. `DEMO-MORNING.md` and
#: `docs/DEPLOYED-NETWORK-PATH.md` were untracked *and* unignored, which is the
#: exact state the runbook rule exists to prevent.
LOCAL_ONLY_PROBES = (
    "HANDOFF-probe-that-need-not-exist.md",
    "OPEN-FINDINGS.md",
    "TASKS.md",
    "ROUND5_FIX_CHECKPOINT.md",
    "DEMO-MORNING.md",
    "docs/DEPLOYED-NETWORK-PATH.md",
)


def test_local_only_notes_are_ignored() -> None:
    """A rule must match every local-only document.

    Deliberately a weaker assertion than the runbook's above: it checks only
    that a rule *matches*, never that these paths are untracked. Nine notes and
    the findings log are already in the index and are meant to stay there, so an
    untracked assertion would fail forever while proving nothing. It would also
    ask the wrong question. These rules exist for the publication repository,
    which is built by a fresh first commit where every path starts untracked and
    .gitignore is the only thing consulted -- so there the rules govern every
    copy, and here they govern the next file created.

    What this catches is a rule being deleted or narrowed. Nothing else would
    notice: every one of these rules is new, none has any effect on this
    repository's working state, and a `git clean -xfd` or a rebase that dropped
    them would look entirely benign. The way the owner would find out instead is
    at publication, in a copy already pushed. For the handoff notes that means the
    instance, VPC, security-group and public-IPv4 identifiers they collect by the
    dozen -- one alone held twenty-one. For the four documents beneath them it is
    internal working state about one real installation rather than identifiers:
    the run-of-show procedure and the unapplied network-path investigation.
    """
    for probe in LOCAL_ONLY_PROBES:
        # `--no-index` for the same reason as above: it asks whether a rule
        # matches the path, rather than the question git answers by default,
        # which excludes tracked paths and so would report "no rule" for
        # exactly the files that are already in the index.
        ignored = _git("check-ignore", "-v", "--no-index", probe)
        assert ignored.returncode == 0, (
            f"no .gitignore rule matches {probe}, so it is committable and would "
            f"ship in the published repository. Every one of these documents is "
            f"an internal working record against a real environment, and the "
            f"handoff notes carry live identifiers throughout. Restore the "
            f"`HANDOFF-*.md`, `/OPEN-FINDINGS.md`, `/TASKS.md`, "
            f"`/ROUND5_FIX_CHECKPOINT.md`, `/DEMO-MORNING.md` and "
            f"`/docs/DEPLOYED-NETWORK-PATH.md` rules in .gitignore."
        )
