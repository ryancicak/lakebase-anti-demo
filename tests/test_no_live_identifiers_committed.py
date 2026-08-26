"""Keep real accounts, workspaces, hosts and instances out of a public repository.

This repository is intended to be published. A value committed here and removed
later still lives in history, so the only useful time to catch one is before it
is committed at all. This file is that catch.

It replaces a version of itself that was theatre. Sixteen live identifiers, in
seventeen files, fifty-one occurrences, were found by a human reading the tree,
and this test passed while every one of them was present. Five separate reasons
for that are recorded against the checks below, because a guard that cannot be
shown to fail is worth less than no guard at all -- it converts "nobody looked"
into "something looked and said it was fine".
"""

from __future__ import annotations

import ipaddress
import re
import shutil
import subprocess
from pathlib import Path

import pytest
from test_public_markdown_links import is_unpublished
from test_publish_runbook_is_ignored_and_present import in_private_working_repository

REPO = Path(__file__).resolve().parent.parent

# Directories .gitignore keeps out of the repository, holding the output of real
# runs: manifests, receipts, Terraform state, cost pulls, server logs. Anything
# that appears both here and in a committed file is a real identifier that leaked
# out of somebody's install and into the repository.
#
# Discovered by glob rather than enumerated, and that is a deliberate widening.
# This read `(".anti-demo-v7", ".anti-demo")`, so on a tree that had moved on to
# `.anti-demo-v8/` the cross-reference below compared committed tokens against
# two *superseded* generations and never against the live one. It reported
# nothing and was indistinguishable from a clean run, which is the shape of
# FAILURE 3 again: a thing that is not listed is not searched for. `.gitignore`
# matches `.anti-demo*/` for the same reason. A hand-written list of generations
# goes stale on the next `bootstrap.sh`, and the moment it goes stale is the
# moment the newest identifiers stop being compared against anything.
LIVE_ARTEFACT_DIR_GLOB = ".anti-demo*"


# --------------------------------------------------------------------------- #
# Identifier shapes
# --------------------------------------------------------------------------- #
# FAILURE 1 of 5 -- the anchors never worked at all.
#
# The previous version of this file passed its shapes to `git grep -E`, and used
# `\b` to anchor them. POSIX extended regular expressions have no `\b`; the
# platform regex engine behind `git grep -E` reads it as a literal backspace, so
# `\b[0-9a-f]{16}\b` requires two 0x08 bytes around the digits and can never
# match anything. Three of the five shapes it declared -- the 16-hex, the 32-hex
# and the 12-to-20-digit numeric -- were dead text for the whole life of the
# file. Only the UUID and the run ID, the two written without `\b`, ever ran.
#
# Every shape below is therefore compiled and applied by Python's own `re`, which
# does support the constructs written here. Nothing is handed to an external
# regex engine.
#
# FAILURE 2 of 5 -- `\b` is the wrong anchor even where it is supported.
#
# `\b` treats `_` as a word character. One of the sixteen was a live IAM Identity
# Center permission-set suffix, sixteen hex characters, and it sat inside
# `AWSReservedSSO_databricks-sandbox-admin_<suffix>`, where the character before
# the digits is `_`. There is no word boundary there, so even a working
# `\b[0-9a-f]{16}\b` would have walked past it.
#
# The fix is the anchor, not another pattern. The previous file had already been
# through this once: a 32-hex shape was bolted on when a session ID was missed,
# with a comment blaming the missing boundary at offset 16 -- a plausible
# diagnosis that was not the real one, so the same class of miss was set up to
# happen a third time. Adding shapes does not fix an anchor.
#
# `SEP`/`END` below anchor against alphanumerics only, so `_`, `-`, `/`, `"` and
# `:` all read as boundaries and none of them can hide a value.
SEP = r"(?<![0-9A-Za-z])"
END = r"(?![0-9A-Za-z])"

# `.` is deliberately a boundary for these two and not for the others. A cost
# figure like `0.002681858103919233` is a decimal fraction, not an identifier,
# and there are hundreds of them in PRICING_DISCOVERY.md and in the cost
# fixtures. Treating `.` as a separator for digit runs would report every one.
NUM_SEP = r"(?<![0-9A-Za-z.])"
NUM_END = r"(?![0-9A-Za-z.])"

# FAILURE 3 of 5 -- whole shape classes were absent.
#
# Six of the sixteen findings matched no pattern at all: an IPv4 address, two EC2
# instance IDs, an IAM role unique ID, and Lakebase endpoint IDs. A shape that is
# not listed is not searched for, and nothing about the old file's result
# distinguished "looked and found nothing" from "never looked".
SHAPES: dict[str, str] = {
    "uuid": SEP + r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}" + END,
    # One shape for every hexadecimal run, rather than one per length. A
    # warehouse ID is 16, a session ID is 32, a digest is 64, and the next thing
    # to leak will be whatever length nobody enumerated. `{16,}` has no length
    # to forget.
    "hex_run": NUM_SEP + r"[0-9a-f]{16,}" + NUM_END,
    "numeric_id": NUM_SEP + r"[0-9]{12,20}" + NUM_END,
    "install_run_id": SEP + r"ad-20[0-9]{6}-[0-9]{4}-[0-9a-z]{4}" + END,
    "aws_resource_id": (
        SEP
        + r"(?:i|sg|vpc|subnet|eni|vol|snap|ami|rtb|igw|nat|acl|pl|fl|dopt|eipalloc)"
        + r"-[0-9a-f]{8,17}"
        + END
    ),
    "iam_unique_id": SEP + r"(?:AROA|AIDA|ASIA|AKIA|ANPA|AGPA|APKA|AIPA)[A-Z0-9]{12,}" + END,
    # The endpoint *slug*, not the endpoint hostname. See the note on
    # interpolation below -- this choice is the whole reason two of the sixteen
    # were findable.
    "lakebase_endpoint": SEP + r"ep-[a-z]+-[a-z]+-[a-z0-9]{8}" + END,
    "ipv4": r"(?<![0-9A-Za-z.])(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?![0-9A-Za-z.])",
    # Not one of the sixteen -- this class was found while rebuilding the file,
    # and three occurrences were still in the tree at 8f3109c. An absolute home
    # directory publishes the operator's account name, and one of the three
    # pointed at an `aws_credentials.txt` in an unrelated project, which
    # advertises both the location and the purpose of a long-lived key file. The
    # negative lookahead spares the placeholder home the fixtures use.
    "operator_home_path": (
        # `/home/app` is the Databricks Apps container home and `/home/ubuntu` the
        # runner's -- fixed platform paths, identical for every customer, so
        # neither names a person. `/Users/Shared` is macOS's, and Homebrew owns
        # `/home/linuxbrew`.
        #
        # Both classes accept a capitalised account name, and that is not
        # cosmetic: the first character class here read `[a-z]`, so a home
        # directory whose name begins with a capital -- the spelling macOS itself
        # shows in Finder, and the spelling of any account named after a person
        # who capitalises it -- did not match. A planted one passed this whole
        # file green while the lowercase form beside it was caught. That is
        # FAILURE 2's lesson a third time: the pattern was right and the
        # character class it was anchored with quietly excluded half the real
        # inputs. `test_known_false_positives_stay_quiet` holds the positive case,
        # assembled from fragments so that stating it does not create a finding.
        r"/(?:Users|home)/"
        r"(?![Dd]emo\b|[Ee]xample\b|[Oo]perator\b|[Rr]unner\b|ubuntu\b|root\b"
        r"|app\b|Shared\b|[Ll]inuxbrew\b)"
        r"[A-Za-z][A-Za-z0-9._-]*"
    ),
    # An RDS endpoint hostname carries a twelve-character token AWS mints per
    # account and region, so it identifies the account as surely as the account ID
    # does. `.gitignore`'s own header lists "RDS and Lakebase endpoint hostnames"
    # among the things a generation directory must never publish, and the Lakebase
    # half had a shape here while the RDS half had none.
    "rds_endpoint": SEP
    + r"[0-9a-z][0-9a-z-]*\.[0-9a-z]{12}\.[0-9a-z-]+\.rds\.amazonaws\.com"
    + END,
    # FAILURE 6 of 6 -- the shapes above cover AWS thoroughly and Databricks
    # barely, and Databricks is the half of this installation that holds a
    # bearer token.
    #
    # Every shape below was found by planting a realistic value of it in a file
    # inside this tree and running the two checks at the bottom of this file. All
    # six passed green. The account of what that means is worth more than the
    # patterns: `hex_run` is anchored with `NUM_SEP`, which refuses a match whose
    # preceding character is alphanumeric -- and a Databricks personal access
    # token is the four letters `dapi` immediately followed by thirty-two hex
    # characters. The `i` is alphanumeric, so the lookbehind fails at the only
    # offset where the run begins, and a live token was invisible to every shape
    # in this file. That is FAILURE 2 exactly, one class over: an anchor chosen
    # for one job silently declining another.
    #
    # A token, a workspace hostname and a deployed application URL are the three
    # values a reader needs to reach somebody else's workspace, and none of the
    # three was searched for. `test_deploy_hygiene.py` guards hostnames in the
    # two lockfiles and `app.yaml` only, so nothing looked at the other 300
    # files.
    "databricks_token": SEP + r"(?:dapi|dose)[0-9a-f]{32}(?:-[0-9]+)?" + END,
    # A workspace hostname names one customer's workspace and nothing else. The
    # `-2` suffix on the token shape and the `.<n>.` infix here are both real
    # platform spellings, not defensive padding.
    "databricks_workspace_host": SEP
    + (
        r"(?:dbc-[0-9a-f]{8}-[0-9a-f]{4}\.cloud\.databricks\.com"
        r"|adb-[0-9]{10,20}\.[0-9]{1,2}\.azuredatabricks\.net)"
    )
    + END,
    # The deployed app's own URL. The digit run is the workspace ID, so this
    # publishes the workspace as well as the app.
    "databricks_apps_host": SEP
    + r"[0-9a-z][0-9a-z-]*-[0-9]{8,}\.(?:aws|azure|gcp)\.databricksapps\.com"
    + END,
    # An IAM Identity Center start URL names the organisation whose SSO it is.
    # There is no placeholder form of one in this tree and no reason for a public
    # repository to carry one.
    "internal_sso_host": SEP + r"[0-9a-z][0-9a-z.-]*\.awsapps\.com" + END,
    # An internal tracker ID is not a credential and cannot be used to reach
    # anything, which is exactly why it reads as harmless and gets left in. What
    # it publishes is that an internal ticket exists, its number, and -- from the
    # sentence around it -- what an internal team was told about this account.
    "internal_ticket": SEP + r"SSE-[0-9]{3,6}" + END,
    # Deliberately NOT in SELF_EVIDENT_SHAPES: this tree legitimately carries
    # two dozen placeholder addresses (`operator@databricks.com`,
    # `ringside@example.com`) that are indistinguishable by shape from a real
    # one. Refusing the shape would report all of them and this entry would be
    # deleted within the week -- the mistake FAILURE 5 describes.
    #
    # It earns its place through the cross-reference check instead, and that is
    # the whole argument for adding it: a real operator's address is written into
    # every manifest, every Terraform state and every server log by the code
    # itself, so it is in the gitignored output of any real run and a placeholder
    # never is. The one live finding in this tree at the time of writing was
    # found precisely this way and is recorded in KNOWN_UNCLEARED below.
    "email": r"(?<![0-9A-Za-z._%+-])[0-9A-Za-z._%+-]+@[0-9A-Za-z][0-9A-Za-z.-]*\.[A-Za-z]{2,}",
}

COMPILED = {name: re.compile(pattern) for name, pattern in SHAPES.items()}

# Shapes are applied most-structured first, and each match is masked out of the
# text before the next shape runs. Without that, the final twelve-digit group of
# every synthetic UUID is also a twelve-digit run, so the digit shape reports a
# fragment of a value the UUID shape has already cleared -- and because those
# UUIDs are generated by the code, the fragment genuinely does appear in real run
# output, so the cross-reference check would report it forever and nobody would
# be able to make it stop. Masking makes each value the responsibility of exactly
# one shape.
# `email` sits after `ipv4` so that an address at a dotted-quad domain is still
# reported as the routable address it contains, and before `hex_run` and
# `numeric_id` so neither reports a fragment of a local part. `databricks_token`
# must precede `hex_run` for the same reason it exists: the thirty-two hex
# characters of a token belong to the token.
SHAPE_PRIORITY = (
    "operator_home_path",
    "uuid",
    "install_run_id",
    "aws_resource_id",
    "iam_unique_id",
    "lakebase_endpoint",
    "rds_endpoint",
    "databricks_token",
    "databricks_workspace_host",
    "databricks_apps_host",
    "internal_sso_host",
    "internal_ticket",
    "ipv4",
    "email",
    "hex_run",
    "numeric_id",
)
assert set(SHAPE_PRIORITY) == set(SHAPES), "every shape needs a place in the priority order"

# Shapes for which the *shape alone* is the finding, with no cross-referencing
# needed. See FAILURE 5. Every one of these is a value that AWS or Lakebase
# minted for a specific real resource; a public repository has no legitimate use
# for one, so anything matching that is not a recorded placeholder is a leak.
#
# The other four shapes -- UUID, hex run, digit run, run ID -- are deliberately
# not in this set. Synthetic test values of those shapes are everywhere in the
# suite and always will be, so refusing them outright would report a few hundred
# false positives and this file would be deleted within the week.
SELF_EVIDENT_SHAPES = (
    "aws_resource_id",
    "iam_unique_id",
    "lakebase_endpoint",
    "ipv4",
    # Two of the sixteen were installation run IDs. A real suffix is four random
    # base-36 characters and the date is a real date, so there is no synthetic
    # form that is not either already in ALLOWED or worth one line to add.
    "install_run_id",
    "operator_home_path",
    # A bearer token, a workspace hostname, an application URL, an SSO start URL
    # and an internal ticket number are all minted for one real thing. None has a
    # legitimate placeholder form that matches these patterns -- the one
    # placeholder hostname this tree does carry is all zeros and is in ALLOWED
    # below -- so for these the shape is the finding, with no cross-reference
    # needed and no dependence on an artefact directory still existing.
    "databricks_token",
    "databricks_workspace_host",
    "databricks_apps_host",
    "internal_sso_host",
    "internal_ticket",
    "rds_endpoint",
)

# A bare UUID cannot be refused on shape: the suite is full of synthetic ones
# that are indistinguishable from real ones by inspection, and refusing the shape
# would report a dozen legitimate fixtures. But a UUID sitting immediately after
# a key that says what it identifies is a different proposition -- that is a
# deployment, a statement, a service principal or a project, and it is exactly
# where the two findings that FAILURE 5 could not reach were sitting.
#
# This costs five allowlist entries today, which is the price of covering the one
# class where the cross-reference had nothing to compare against.
IDENTITY_BEARING_KEY = re.compile(
    r"(?:deployment|statement|bout|client|pipeline|endpoint|project|instance"
    r"|workspace|account|service_principal|app)[_a-z]*"
    # `_id`, ` id`, `-uid`, `:uuid` -- prose and JSON keys spell it both ways, and
    # the finding that motivated this rule was in prose.
    r"(?:[\s_:-]{0,3}u?u?id)?"
    r"\W{0,4}"
    r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
    re.IGNORECASE,
)

# Reading every file is cheaper than maintaining a list of extensions, and it
# cannot go stale. The old file searched eleven globs, which silently excluded
# `.env.example`, the extensionless `antidemo` launcher, `eslint.config.js` and
# `.terraform.lock.hcl`.
EXCLUDED_PATHS = frozenset(
    {
        # Registry metadata for third-party packages: thousands of upstream
        # integrity digests, none of them anybody's identifier. Host names in
        # these three are guarded by test_deploy_hygiene.py instead.
        "frontend/package-lock.json",
        "uv.lock",
        "infra/aws/.terraform.lock.hcl",
    }
)

MAX_FILE_BYTES = 4 * 1024 * 1024


# --------------------------------------------------------------------------- #
# Allowlist
# --------------------------------------------------------------------------- #
# Grouped by reason, because the reason is the part worth reviewing. A token here
# is a claim that somebody checked; keep the claim next to the value so the next
# reader can re-check it instead of trusting it.
ALLOWED: dict[str, frozenset[str]] = {
    "AWS's own published documentation values, reserved by AWS precisely so that "
    "documentation does not have to name a real account. Recognisably nobody's.": frozenset(
        {
            "111122223333",
            "123456789012",
            "1111222233334444",
        }
    ),
    "Replace-me sentinels. `.env.example` ships `000000000000` and "
    "tests/test_api.py asserts it never reaches a served configuration, so its "
    "presence is a checked precondition rather than a leak.": frozenset(
        {
            "000000000000",
            "999999999999",
            "00000000-0000-0000-0000-000000000000",
        }
    ),
    "Synthetic values the code itself generates or the fixtures spell out. They "
    "turn up in a real manifest without ever having been anyone's identifier, "
    "which is why cross-referencing alone cannot clear them.": frozenset(
        {
            "00000000-0000-4000-8000-000000000006",
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-000000000002",
            "00000000-0000-0000-0000-000000000003",
            "00000000-0000-0000-0000-000000000004",
        }
    ),
    "Sequential-hex placeholders, this repository's convention for AWS resource "
    "IDs in tests and handoff notes. `0123456789abcdef` and its reverse are not "
    "allocations AWS ever makes.": frozenset(
        {
            "i-0123456789abcdef0",
            "i-0fedcba9876543210",
            "sg-0123456789abcdef0",
            "sg-1123456789abcdef0",
            "sg-0123abcd",
            "sg-abcd1234",
            "subnet-0123456789abcdef0",
            "subnet-1123456789abcdef0",
            # In infra/aws/terraform.tfvars.example, which the old eleven-glob
            # file list never looked at -- `.tfvars.example` matches neither
            # `*.tfvars` nor any other glob it searched.
            "subnet-0fedcba9876543210",
            "vpc-0123456789abcdef0",
        }
    ),
    "Counter placeholders: zeros and an index, generated by the safe-change "
    "fixtures to stand in for security groups they never create.": frozenset(
        {
            "sg-00000000000000001",
            "sg-00000000000000002",
        }
    ),
    "AWS's documented `EXAMPLE` convention for principal and access-key IDs. The "
    "literal string EXAMPLE cannot occur in a minted AWS unique ID, which is "
    "base-32 over a different alphabet.": frozenset(
        {
            "AIDAEXAMPLEEXAMPLE",
            "AKIAEXAMPLEEXAMPLE",
        }
    ),
    "Lakebase endpoint slugs invented for the posted-usage tests. Real ones are "
    "two dictionary words and a random suffix; `example` is not a word the "
    "generator draws from.": frozenset(
        {
            "ep-example-one-d1000001",
            "ep-example-ring-d1000000",
            "ep-example-six-d1000006",
        }
    ),
    "Installation run IDs with hand-written suffixes. A real suffix is four "
    "random base-36 characters; `abcd` and `dcba` are neither random nor "
    "produced by any run.": frozenset(
        {
            "ad-20260818-1200-abcd",
            "ad-20260818-2000-abcd",
            "ad-20260819-0009-dcba",
            "ad-20260819-1800-abcd",
            "ad-20260820-1446-abcd",
        }
    ),
    "Fixture UUIDs that sit next to a key naming what they identify, so "
    "IDENTITY_BEARING_KEY reaches them. Each is invented for a test and none "
    "appears in any run artefact; the repeated-digit three are placeholders on "
    "sight, and the other two were checked against the gitignored output of every "
    "real run on this machine and found nowhere in it.": frozenset(
        {
            "11111111-1111-4111-8111-111111111111",
            "11111111-2222-3333-4444-555555555555",
            "22222222-2222-4222-8222-222222222222",
            "9db34e86-cb48-4b34-9be2-c93309ff6417",
            "f2af6c88-7da3-40cf-881f-7971e50a6b18",
        }
    ),
    "SHA-256 of source files in this repository. `contract_sha256` is recomputed "
    "from the tree by server/manifest.py, so it is reproducible from any clone, "
    "identical for every user, and names nothing. It appears in a real manifest "
    "only because a real manifest recomputes it too.": frozenset(
        {
            "f0e9a6960fb22cc052486b62cf01e32dcaabacf70508a2bc087ddc25deafa81c",
        }
    ),
    "Cost-ledger micro-unit integers. These are money scaled to an integer "
    "(`2.6` becomes `260000000000000000`), so a fixture and a real receipt agree "
    "on them by arithmetic, not by one having been copied from the other.": frozenset(
        {
            "260000000000000000",
            "100000000000000000",
        }
    ),
    "The replace-me workspace hostname in docs/bootstrap.env.example. It keeps "
    "the real `dbc-<8 hex>-<4 hex>` shape so the file still shows an operator "
    "what to paste, and every hex character is a zero, which is not an allocation "
    "Databricks makes. The stub harness's other hostnames -- `dbc-stub-0000`, "
    "`dbc-unrelated-9999` -- are not listed because `stub` and `unrelated` are "
    "not hexadecimal, so the shape never reaches them.": frozenset(
        {
            "dbc-00000000-0000.cloud.databricks.com",
        }
    ),
    "Digest and hostname placeholders that keep their shape so the surrounding "
    "prose and assertions still read correctly.": frozenset(
        {
            "0123456789abcdef",
            "abcdef0123456789",
            "0123456789abcdef0123456789abcdef",
            "8c4a0ca3deadbeefcafe0123456789ab",
            "aaaabbbbccccdddd",
            "aaaaaaaaaaaaaaaa",
            "bbbbbbbbbbbbbbbb",
            "0000000000000000",
            "1234567890123456",
            "1123456789abcdef0",
            "9876543210987654321",
        }
    ),
}

ALLOWED_TOKENS = frozenset().union(*ALLOWED.values())

# Findings that are real, are not cleared, and are not this file's to clear.
# Keyed by (path, shape) so an exemption cannot spread beyond the one thing it
# was written for, and each carries the obligation rather than a dismissal.
#
# test_no_uncleared_exemption_has_gone_stale asserts every entry still fires, so
# clearing the finding makes the suite ask for the entry to be deleted. An
# exemption that outlives its finding is how a guard rots.
#
# **It is empty, and that is the mechanism working rather than a list being
# tidied away.** It held five entries, all of them findings inside documents
# `.gitignore` withholds: an operator home path, that operator's work address and
# an internal team alias in `OPEN-FINDINGS.md`, and an internal tracker number and
# the employer's IAM Identity Center start URL in `HANDOFF-aws-auth-design.md`.
# Every one of those documents has now been deleted from this working tree ahead
# of publication, so none of them is scannable -- `_scannable_files` lists tracked
# and untracked files while honouring `.gitignore`, and these are now neither
# tracked nor unignored. The findings did not get cleared by redaction; they left
# with the files. An exemption that outlives the file it excused is precisely the
# staleness the test below exists to report, so the entries went.
#
# The obligation the first of them carried -- "MUST be cleared before publication"
# -- is discharged by that deletion rather than dropped. What it protected against
# was the operator's home path shipping; the file it was in does not ship, and
# does not exist.
KNOWN_UNCLEARED: dict[tuple[str, str], str] = {}

# Not an identifier by any shape here, and recorded only because a reader who
# arrives via the managed pre-commit hook will be looking for it. The hook's
# `linkedin-client-secret` rule matches the literal `LinkedIn post`, a colon, and
# any sixteen-character token; in frontend/src/App.tsx the token it lands on is
# the React state variable `cardRenderFailed`, which is sixteen characters long.
# CONTRIBUTING.md's "A note on the secret scanner" section has the full account,
# including the inline `gitleaks:allow` marker that now silences the rule and why
# it has to stay on that exact line. Named by section rather than by line number,
# which had already drifted. There is no credential in that file,
# and no shape below matches a mixed-case identifier, so nothing here fires on
# it -- test_known_false_positives_stay_quiet pins that.
DOCUMENTED_NON_SECRET = "cardRenderFailed"


def _ip_is_publishable(text: str) -> tuple[bool, str]:
    """Decide whether an IPv4 literal is safe to publish, and say why.

    Everything Python calls non-global is safe and for a reason worth keeping:
    RFC1918 private space, RFC5737 documentation space (192.0.2.0/24,
    198.51.100.0/24, 203.0.113.0/24), loopback, link-local, the unspecified
    address and the broadcast address. None of them routes to anybody, so none of
    them can identify anybody's host. What is left -- a globally routable address
    -- was allocated to someone, and the public IPv4 of a provisioned resource
    was one of the sixteen findings.
    """

    try:
        addr = ipaddress.IPv4Address(text)
    except ValueError:
        return True, "not a valid IPv4 address"
    if addr.is_multicast:
        return True, "multicast, so not any single host"
    if not addr.is_global:
        return True, "not globally routable (private, documentation, loopback or reserved)"
    return False, "globally routable, so it was allocated to a real host"


# --------------------------------------------------------------------------- #
# File enumeration
# --------------------------------------------------------------------------- #
def _scannable_files(root: Path) -> list[Path]:
    """Every file a commit from this tree could carry, read from the working tree.

    FAILURE 4 of 5, and the one that let ten of the sixteen through.

    The old file ran `git grep` with no revision argument. That searches the
    *index*, so a file that has never been `git add`ed is invisible to it, and so
    is any unstaged edit to a file that has. The findings were concentrated in
    exactly those files -- ten of the sixteen were in files that were untracked
    when the scan ran, which are of course the files nobody has reviewed yet. A
    guard that inspects only what has already been staged is looking behind
    itself.

    `--cached --others --exclude-standard` lists tracked *and* untracked files
    while still honouring `.gitignore`, and the bytes are then read from disk
    rather than from a blob, so unstaged modifications are seen too.
    """

    listing = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=120,
    )
    if listing.returncode != 0:
        pytest.skip(f"git ls-files unavailable: {listing.stderr.strip()}")

    paths = []
    for rel in listing.stdout.split("\0"):
        if not rel or rel in EXCLUDED_PATHS:
            continue
        path = root / rel
        if not path.is_file() or path.is_symlink():
            continue
        try:
            if path.stat().st_size > MAX_FILE_BYTES:
                continue
        except OSError:
            continue
        paths.append(path)
    return paths


def _read_text(path: Path) -> str | None:
    try:
        raw = path.read_bytes()
    except OSError:
        return None
    if b"\0" in raw:  # binary; no identifier is readable from it anyway
        return None
    return raw.decode("utf-8", errors="replace")


def scan_text(text: str) -> dict[str, set[str]]:
    """Return every identifier-shaped token in `text`, keyed by shape name."""

    found: dict[str, set[str]] = {}
    remaining = list(text)
    for name in SHAPE_PRIORITY:
        haystack = "".join(remaining)
        tokens: set[str] = set()
        for match in COMPILED[name].finditer(haystack):
            tokens.add(match.group(0))
            # Newline keeps offsets stable and is a boundary for every shape.
            for index in range(*match.span()):
                remaining[index] = "\n"
        if name == "ipv4":
            tokens = {token for token in tokens if not _ip_is_publishable(token)[0]}
        tokens -= ALLOWED_TOKENS
        if tokens:
            found[name] = tokens
    return found


def scan_identity_context(text: str) -> set[str]:
    """UUIDs that a neighbouring key declares to be somebody's identifier."""

    return {
        match.group(1)
        for match in IDENTITY_BEARING_KEY.finditer(text)
        if match.group(1) not in ALLOWED_TOKENS
    }


def _drop_uncleared(found: dict[str, dict[str, set[str]]]) -> dict[str, dict[str, set[str]]]:
    """Remove the recorded, obligation-bearing exemptions in KNOWN_UNCLEARED."""

    trimmed: dict[str, dict[str, set[str]]] = {}
    for shape, tokens in found.items():
        for token, paths in tokens.items():
            kept = {path for path in paths if (path, shape) not in KNOWN_UNCLEARED}
            if kept:
                trimmed.setdefault(shape, {})[token] = kept
    return trimmed


def scan_tree(root: Path) -> dict[str, dict[str, set[str]]]:
    """Map shape name -> token -> set of paths, over a whole working tree."""

    result: dict[str, dict[str, set[str]]] = {}
    for path in _scannable_files(root):
        text = _read_text(path)
        if text is None:
            continue
        rel = str(path.relative_to(root))
        for name, tokens in scan_text(text).items():
            bucket = result.setdefault(name, {})
            for token in tokens:
                bucket.setdefault(token, set()).add(rel)
        for token in scan_identity_context(text):
            result.setdefault("uuid_in_identity_context", {}).setdefault(token, set()).add(rel)
    return _drop_uncleared(result)


def _artefact_text(root: Path) -> str | None:
    """Every byte of the gitignored output of real runs, as one searchable string."""

    dirs = sorted(path for path in root.glob(LIVE_ARTEFACT_DIR_GLOB) if path.is_dir())
    if not dirs:
        return None
    chunks = []
    for directory in dirs:
        for path in directory.rglob("*"):
            if not path.is_file() or path.is_symlink():
                continue
            text = _read_text(path)
            if text is not None:
                chunks.append(text)
    return "\n".join(chunks)


# --------------------------------------------------------------------------- #
# The checks
# --------------------------------------------------------------------------- #
pytestmark = pytest.mark.skipif(shutil.which("git") is None, reason="git is not on PATH")


def test_no_committable_file_holds_a_self_evidently_real_identifier():
    """Refuse AWS and Lakebase resource identifiers on shape alone.

    FAILURE 5 of 5 -- the check could only see values that still existed locally.

    The old file's one mechanism was a cross-reference: take each
    identifier-shaped token out of the repository, look it up in the gitignored
    output of real runs, and report the ones found in both. That is a genuinely
    good mechanism, because it needs no list of secrets and catches shapes nobody
    anticipated. But it inherits the lifetime of the artefacts. Delete
    `.anti-demo-v7/`, or run a round whose receipt has since rotated, and the
    evidence goes with it while the committed copy stays. A bout statement ID and
    an Apps deployment ID were both in that hole: still in the tree, no longer
    anywhere on disk to compare against.

    So this check needs no artefacts. For the four shapes in
    `SELF_EVIDENT_SHAPES`, the shape *is* the finding: AWS mints `i-`, `sg-`,
    `vpc-`, `AROA…` and `AIDA…` for specific real resources, Lakebase mints
    `ep-…` for specific real endpoints, and a globally routable IPv4 belongs to
    somebody. A public repository has no use for a real one. Anything matching
    that is not in `ALLOWED` is therefore a leak, whether or not this machine can
    still prove where it came from.
    """

    found = scan_tree(REPO)
    refused = (*SELF_EVIDENT_SHAPES, "uuid_in_identity_context")
    offenders = {name: tokens for name, tokens in found.items() if name in refused}

    assert not offenders, (
        "identifiers that only a real AWS or Lakebase resource has are committable:\n"
        + "\n".join(
            f"  [{name}] {token}\n      in {sorted(paths)}"
            for name, tokens in sorted(offenders.items())
            for token, paths in sorted(tokens.items())
        )
        + "\n\nEither redact it to the placeholder convention already used in this"
        "\ntree, or -- if it genuinely names nothing -- add it to ALLOWED with the"
        "\nreason it is safe. Do not add it without a reason."
    )


def test_no_committable_file_repeats_an_identifier_from_a_real_run():
    """Cross-reference every identifier-shaped token against real run output.

    This is the mechanism the old file had, kept because it is the only one that
    catches a shape nobody thought of: any token in the tree that also appears in
    the gitignored output of a real run came out of somebody's install. It is
    self-maintaining and it does not contain a single real identifier, which
    would defeat its own purpose.

    What has changed is everything feeding it: the anchors now work, the shapes
    now include the classes that were missing, and the file list is the working
    tree rather than the index. It still skips rather than passing quietly where
    it cannot check, and the shape check above now runs even then.
    """

    artefacts = _artefact_text(REPO)
    if artefacts is None:
        pytest.skip("no live artefact directory on this machine, so nothing to compare against")

    found = scan_tree(REPO)
    assert found, "found no identifier-shaped tokens at all, so this test proved nothing"

    leaked: list[str] = []
    for name, tokens in sorted(found.items()):
        for token, paths in sorted(tokens.items()):
            if token in artefacts:
                leaked.append(f"  [{name}] {token}\n      in {sorted(paths)}")

    assert not leaked, (
        "values from a real run are committable -- each of these appears both in "
        "the tree and in the gitignored output of somebody's install:\n" + "\n".join(leaked)
    )


def _finding_survives(root: Path, rel: str, shape: str) -> bool:
    """Whether `root/rel` still carries a `shape` finding for an exemption to cover."""

    path = root / rel
    if not path.exists():
        return False
    text = _read_text(path)
    assert text is not None, f"{rel} is not readable as text"
    return bool(scan_text(text).get(shape)) or (
        shape == "uuid_in_identity_context" and bool(scan_identity_context(text))
    )


def test_no_uncleared_exemption_has_gone_stale(tmp_path):
    """Every recorded exemption must still be covering a live finding.

    An exemption that outlives the thing it excused is how a guard becomes
    theatre again: the next reader sees a carve-out, assumes it is load-bearing,
    and widens it. Asserting that each one still fires means the moment somebody
    clears the underlying finding, this test asks for the entry to be deleted.

    A missing file is normally exactly that staleness and is reported as such --
    unless the file is one the publish step withholds *and this is not the
    private working repository*, in which case its absence means "you are reading
    the public repository", not "the finding is cleared". This job runs in CI
    against that repository, so an unconditional failure there would turn a
    correctly-published tree red on its first run.

    **That second clause is new, and its absence had already cost this test its
    teeth.** The skip used to be keyed on withheld-and-absent alone, which reads
    "published copy" off a fact that is equally true of the private repository the
    moment the owner deletes his notes -- and he deleted them, so this test
    skipped in the private repository during publication prep, and skipped at the
    *first* absent entry, taking every other exemption with it. The marker now
    comes from `tests/test_publish_runbook_is_ignored_and_present.py`, which asks
    history rather than the filesystem for the same reason and after the same
    failure.

    The planted pair below is what keeps this honest while `KNOWN_UNCLEARED` is
    empty: an empty loop proves nothing, and this repository has already shipped a
    guard that could not fail.
    """

    # Assembled rather than written out, for the reason in test_the_guard_can_fail.
    instance = "i-0" + "9c4f1b2e8d7a06b3"[:16]
    (tmp_path / "still-there.md").write_text(f"runner instance {instance}\n")
    (tmp_path / "cleared.md").write_text("nothing identifying in here\n")
    assert _finding_survives(tmp_path, "still-there.md", "aws_resource_id"), (
        "the staleness check cannot see a finding that is plainly there, so every "
        "assertion below would pass by measuring nothing"
    )
    assert not _finding_survives(tmp_path, "cleared.md", "aws_resource_id")
    assert not _finding_survives(tmp_path, "never-existed.md", "aws_resource_id")

    for (rel, shape), reason in KNOWN_UNCLEARED.items():
        absent = not (REPO / rel).exists()
        if absent and is_unpublished(rel) and not in_private_working_repository(REPO):
            pytest.skip(
                f"{rel} is withheld from publication, is absent, and no commit in "
                f"this repository's history carries a withheld document -- so this "
                f"is a published copy or a clone, and the exemption cannot be "
                f"checked here."
            )
        assert not absent, (
            f"KNOWN_UNCLEARED names {rel}, which does not exist in this working "
            f"tree. If it is a document publication withholds and it has since "
            f"been deleted from here, the finding left with the file; either way "
            f"the entry is stale. Delete it. Its reason was: {reason}"
        )
        assert _finding_survives(REPO, rel, shape), (
            f"{rel} no longer has a {shape} finding, so this exemption is stale "
            f"and must be deleted from KNOWN_UNCLEARED. Its reason was: {reason}"
        )


def test_the_scan_reads_the_working_tree_and_not_the_index(tmp_path):
    """Prove the fix for FAILURE 4 on an untracked file and an unstaged edit.

    Ten of the sixteen findings were in untracked files, so this is the single
    most load-bearing behaviour in the file. It is asserted against a throwaway
    repository rather than this one so that it cannot depend on the state of a
    tree three other agents are editing.
    """

    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)

    committed = tmp_path / "committed.md"
    committed.write_text("nothing interesting here\n")
    subprocess.run(["git", "add", "committed.md"], cwd=tmp_path, check=True)

    # A real instance ID shape, never allocated: not in ALLOWED, so it must be
    # reported. Assembled rather than written out for the reason given in
    # test_the_guard_can_fail.
    instance = "i-0" + "9c4f1b2e8d7a06b3"[:16]

    untracked = tmp_path / "untracked-note.md"
    untracked.write_text(f"runner instance {instance}\n")

    found = scan_tree(tmp_path)
    assert found.get("aws_resource_id") == {instance: {"untracked-note.md"}}, (
        "an untracked file was not scanned, which is exactly the gap that let ten "
        "of the sixteen findings through"
    )

    # And the same value arriving as an unstaged edit to an already-tracked file.
    untracked.unlink()
    committed.write_text(f"runner instance {instance}\n")
    found = scan_tree(tmp_path)
    assert found.get("aws_resource_id") == {instance: {"committed.md"}}, (
        "an unstaged modification was not scanned, so the bytes on disk and the "
        "bytes checked are different bytes"
    )


def test_the_anchors_survive_underscores_and_hyphens():
    """Prove the fix for FAILURES 1 and 2 on the shape that actually got through.

    A live IAM Identity Center permission-set suffix was committed in
    `infra/aws/README.md` inside a role ARN, behind an underscore, and it is the
    reason this file exists in its current form. The suffix used below is
    invented: the real one is read from git history by
    test_the_guard_fails_on_the_real_pre_redaction_content, which is the right
    place for it, because writing a real identifier into the file whose job is
    keeping real identifiers out would be absurd.
    """

    suffix = "d41f8" + "3a0c7" + "b2e916"
    arn = (
        "arn:aws:iam::123456789012:role/aws-reserved/sso.amazonaws.com/us-west-2/"
        f"AWSReservedSSO_databricks-sandbox-admin_{suffix}"
    )

    assert scan_text(arn).get("hex_run") == {suffix}, (
        "the character before the digits is `_`, which `\\b` treats as a word "
        "character; anchoring against alphanumerics only is what makes this visible"
    )

    # The same anchoring must not start reporting decimal fractions, which is the
    # failure mode that would get this file deleted.
    assert "hex_run" not in scan_text("standing cost 0.002681858103919233 per hour")
    assert "numeric_id" not in scan_text("standing cost 0.002681858103919233 per hour")


def test_interpolated_hostnames_are_still_visible():
    """An interpolated value never appears as a literal, so match the smallest unit.

    Two of the sixteen were Lakebase endpoints written as
    `f"ep-<slug>.{region}.<domain>"`. No search for a hostname could ever have
    seen those, because the hostname does not exist anywhere in the source -- it
    is assembled at runtime. They were found only because somebody searched for
    the slug on its own.

    `lakebase_endpoint` therefore matches the slug and stops, rather than trying
    to match a hostname. This closes the blind spot for every value whose
    identifying part survives intact in the source, which is the ordinary case:
    interpolation appends a region or a domain, it does not cut the slug in half.

    It does not close the case where the identifying part is itself split across
    a concatenation. That residual is stated in the docstring of
    test_a_split_identifier_is_a_known_residual below rather than papered over.
    """

    slug = "ep-" + "quiet-river" + "-d1zzzz09"
    source = f'    host = f"{slug}.{{region}}.database.example.invalid"\n'

    assert scan_text(source).get("lakebase_endpoint") == {slug}


def test_a_split_identifier_is_a_known_residual():
    """State the limit rather than implying there is none.

    A value broken across a concatenation -- `"ep-quiet" "-river-d1zzzz09"`, or a
    slug rebuilt from a list -- has no complete literal in the source and no
    textual scan can see it. This test pins that as known, so that a later reader
    finds a recorded limitation instead of assuming coverage that is not there.

    The mitigation is not regex. It is that values reaching this repository come
    from manifests and receipts by copy-paste, which preserves them whole, and
    that the cross-reference check reads the *artefacts* rather than guessing
    shapes. Deliberate obfuscation is out of scope; nobody is hiding an
    identifier from themselves.
    """

    halves = '"ep-quiet" + "-river-d1zzzz09"'
    assert scan_text(halves) == {}, (
        "if this starts passing, a scan of concatenated literals was added and "
        "this docstring needs rewriting"
    )


def test_known_false_positives_stay_quiet():
    """Near-zero false positives, or this file gets ignored and then deleted.

    Each of these is a value somebody has already had to defend once. Pinning
    them means the defence does not have to be repeated, and means a change to
    the shapes that would re-open one of these arguments fails here first.
    """

    # The React state variable the managed pre-commit hook's
    # `linkedin-client-secret` rule lands on. Sixteen characters, mixed case, and
    # not hexadecimal.
    assert scan_text(f"const [{DOCUMENTED_NON_SECRET}, set] = useState(false)") == {}

    # RFC5737 documentation addresses and RFC1918 private space.
    for address in ("192.0.2.1", "198.51.100.7", "203.0.113.9", "10.0.0.1", "192.168.1.1"):
        publishable, why = _ip_is_publishable(address)
        assert publishable, f"{address} should be publishable: {why}"
        assert scan_text(f"    server_bind = {address}\n") == {}

    # A digest of code in this repository, reproducible from any clone.
    contract = "f0e9a6960fb22cc052486b62cf01e32dcaabacf70508a2bc087ddc25deafa81c"
    assert scan_text(f'"contract_sha256": "{contract}"') == {}

    # AWS's published documentation account, and its EXAMPLE key convention.
    assert scan_text("arn:aws:iam::111122223333:user/x AKIAEXAMPLEEXAMPLE") == {}

    # The replace-me workspace hostname an operator is meant to overwrite, and the
    # stub harness's two, which the shape never reaches because `stub` and
    # `unrelated` are not hexadecimal. If a future widening starts reporting
    # these, `docs/bootstrap.env.example` and the harness both go red for nothing.
    for host in (
        "dbc-00000000-0000.cloud.databricks.com",
        "dbc-stub-0000.cloud.databricks.com",
        "dbc-unrelated-9999.cloud.databricks.com",
        "example.cloud.databricks.com",
        "stub.aws.databricksapps.com",
    ):
        assert "databricks_workspace_host" not in scan_text(f"DATABRICKS_HOST=https://{host}\n")
        assert "databricks_apps_host" not in scan_text(f'"url":"https://{host}"\n')

    # `email` is reported as a shape -- that is what feeds the cross-reference --
    # but it must never be refused on shape alone, because two dozen placeholder
    # addresses in this tree are indistinguishable from a real one. This is the
    # pin that stops somebody "strengthening" the guard into reporting all of them.
    assert "email" not in SELF_EVIDENT_SHAPES
    for address in ("operator@databricks.com", "ringside@example.com", "you@example.com"):
        found = scan_text(f"owner = {address}\n")
        assert set(found) <= {"email"}, f"{address} matched something other than `email`"

    # Platform home directories, which are the same path for every user of the
    # platform and so name nobody. Widening `operator_home_path` to accept
    # capitalised account names put these at risk of being reported, and a guard
    # that fires on `/home/app` in a Databricks Apps runbook gets switched off.
    for path in (
        "/home/app/.databricks",
        "/home/ubuntu/actions-runner",
        "/home/runner/work/repo",
        "/home/root/.aws",
        "/home/linuxbrew/.linuxbrew",
        "/Users/Shared/Fonts",
        "/Users/demo/project",
        "/Users/example/x",
        "/Users/operator/.anti-demo",
    ):
        assert "operator_home_path" not in scan_text(f"path = {path}\n"), path

    # And the capitalisation that used to walk straight past, kept as a positive
    # so a future narrowing of the character class fails here. Assembled from
    # fragments for the reason given in test_the_guard_can_fail -- written out, it
    # is itself an operator-home-path finding in this file, and the check above
    # reports it. That it does is the guard working.
    home = "/Users/" + "Rcicak"
    assert scan_text(f"see {home}/Documents\n").get("operator_home_path") == {home}


def test_the_guard_can_fail():
    """A guard that cannot be shown to fail is the thing this file replaces.

    Every literal here is assembled from fragments so that the shape exists at
    run time but no searchable value is published in this file. The values are
    invented, not redacted -- proving the machinery fires needs a real *shape*,
    not a real *identifier*, and writing real identifiers into the test that
    exists to keep real identifiers out would be self-defeating.

    The companion test below feeds it genuinely real pre-redaction content.
    """

    cases = {
        "aws_resource_id": "i-0" + "a1b2c3d4e5f60718",
        "iam_unique_id": "AROA" + "U" * 17,
        "lakebase_endpoint": "ep-" + "silent-forge" + "-d1zzzz42",
        # 100.64.0.0/10 is RFC6598 shared space and 198.18.0.0/15 is the
        # benchmarking range, so neither is globally routable and neither would
        # fire. This one is.
        "ipv4": ".".join(("45", "45", "45", "45")),
        "install_run_id": "ad-20" + "260101-0101-" + "z9q2",
        "uuid": "-".join(("7c9e1a2b", "4d3f", "4a5b", "9c8d", "0e1f2a3b4c5d")),
        # The six shapes of FAILURE 6. Each of these values was planted in a real
        # file in this tree and passed every check in this file before the shapes
        # above it existed; the token is the one that matters most, because a
        # bearer token is the only value here that grants access on its own.
        "databricks_token": "dapi" + "9f2c4a1e7b3d8065" + "cf14a29e6b7d0538",
        "databricks_workspace_host": "dbc-" + "a1b2c3d4-e5f6" + ".cloud.databricks.com",
        "databricks_apps_host": "anti-demo-" + "4821067391" + ".aws.databricksapps.com",
        "internal_sso_host": "example-org-" + "root" + ".awsapps.com",
        "internal_ticket": "SSE-" + "4762",
        "email": "someone" + "@" + "invented-employer.example",
        "rds_endpoint": "anti-demo-db." + "c9xk2mqrt1zv" + ".us-west-2.rds.amazonaws.com",
    }
    for shape, value in cases.items():
        found = scan_text(f"value: {value}\n")
        assert found.get(shape) == {value}, f"{shape} did not fire on {value}"

    # And the self-evident subset must fail the real assertion, not merely be
    # findable. A tree containing one of these must not pass.
    assert set(cases) & set(SELF_EVIDENT_SHAPES)

    # The contextual UUID rule, which is the only thing standing behind the two
    # findings the cross-reference could not reach.
    deployment = "-".join(("01f0a3c2", "5b7d", "4e19", "8f2a", "6c3d9e1b4a70"))
    assert scan_identity_context(f'"deployment_id": "{deployment}"') == {deployment}
    assert scan_identity_context(f'  statement id {deployment} succeeded') == {deployment}
    # The same UUID with nothing claiming it identifies anything is not refused,
    # which is the deliberate limit that keeps synthetic fixtures working.
    assert scan_identity_context(f"round seed {deployment}") == set()


def test_the_guard_fails_on_the_real_pre_redaction_content():
    """Feed it the actual bytes that were redacted, and require it to object.

    This is the only test here that uses genuinely real identifiers, and it takes
    them from git history rather than carrying them: commit d17ad52 predates the
    redaction, so `infra/aws/README.md` at that revision still contains the live
    AWS account ID and the live permission-set suffix that were removed in
    8f3109c. Nothing real is written into this file; it is read from an object
    that already exists in the repository.

    It skips when that history is absent, which is the expected state of the
    published repository -- the intent is to republish as a fresh first commit,
    so the pre-redaction blobs will not exist there. A skip in that context means
    "the evidence is gone", not "the guard is fine", and the six tests above do
    not depend on history at all.
    """

    pre_redaction = "d17ad52"
    probe = subprocess.run(
        ["git", "cat-file", "-e", f"{pre_redaction}:infra/aws/README.md"],
        cwd=REPO,
        capture_output=True,
    )
    if probe.returncode != 0:
        pytest.skip(f"{pre_redaction} is not in this repository's history")

    blob = subprocess.run(
        ["git", "show", f"{pre_redaction}:infra/aws/README.md"],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=True,
    ).stdout

    found = scan_text(blob)
    flat = {token for tokens in found.values() for token in tokens}

    assert found, (
        "the pre-redaction content produced no findings at all, which would mean "
        "this file is still theatre"
    )
    # The permission-set suffix behind an underscore: FAILURES 1 and 2.
    assert any(len(token) == 16 and re.fullmatch(r"[0-9a-f]{16}", token) for token in flat), (
        "the 16-hex permission-set suffix was not reported"
    )
    # The live AWS account ID, which is a 12-digit run that is not a documented
    # placeholder.
    assert any(re.fullmatch(r"[0-9]{12}", token) for token in flat), (
        "the live AWS account ID was not reported"
    )

    # And the redacted version of the same file must now be clean, so the test
    # measures the redaction rather than just the regex.
    current = (REPO / "infra" / "aws" / "README.md").read_text()
    assert scan_text(current) == {}, (
        "the current infra/aws/README.md still reports findings, so either the "
        "redaction is incomplete or ALLOWED is missing an entry"
    )
