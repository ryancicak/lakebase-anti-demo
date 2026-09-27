"""The release this app is, and the commit it was deployed from.

One number, three places: pyproject.toml (read here), frontend/package.json
(what the screen shows, see frontend/src/version.ts) and the git tag. A test
holds the first two equal. The commit is what tells two builds of the same
release apart -- the fixes between tags -- and only a deploy knows it:
`bootstrap.sh` writes `build-info.json` beside the synced source, and this reads
it back. A tree deployed any other way simply has no commit to report.
"""

from __future__ import annotations

import json
import re
import tomllib
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

#: Written by bootstrap.sh into the workspace copy of the source, never into the
#: local working tree, so a checkout never carries a stale one.
BUILD_INFO_NAME = "build-info.json"

_COMMIT = re.compile(r"^[0-9a-f]{7,40}$")


def _read_version(root: Path) -> str:
    try:
        with (root / "pyproject.toml").open("rb") as handle:
            return str(tomllib.load(handle)["project"]["version"])
    except (OSError, KeyError, TypeError, tomllib.TOMLDecodeError):
        return "unknown"


APP_VERSION = _read_version(PROJECT_ROOT)


def build_info(root: Path = PROJECT_ROOT) -> dict[str, object]:
    """The release and, when a deploy recorded one, the exact commit.

    Never raises: this answers an endpoint whose whole purpose is being readable
    when something else is wrong. A missing, unreadable or malformed stamp is
    reported as no commit rather than guessed at.
    """

    commit: str | None = None
    dirty = False
    try:
        stamp = json.loads((root / BUILD_INFO_NAME).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        stamp = None
    if isinstance(stamp, dict):
        candidate = str(stamp.get("commit") or "").strip().lower()
        if _COMMIT.fullmatch(candidate):
            commit = candidate[:12]
            dirty = stamp.get("dirty") is True
    return {"version": APP_VERSION, "commit": commit, "dirty": dirty}
