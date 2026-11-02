"""Every Python `requires-python` admits must have a wheel for every compiled dependency.

The Databricks Apps runtime installs this project with uv, and uv takes the newest
CPython that `requires-python` admits. While the bound was only ``>=3.12``, that
became CPython 3.15 the day uv could fetch it, and from then on every deploy failed
(2026-11-02), on both clouds, for code that had not changed:

    error: Distribution `psycopg-binary==3.3.4` can't be installed because it
    doesn't have a source distribution or wheel for the current platform

``psycopg-binary`` had no ``cp315`` wheel and no sdist, and ``pydantic-core``,
``uvloop``, ``httptools`` and ``pyyaml`` had no ``cp315`` wheel either. So the bound
has an upper end, and these tests hold the two facts that keep a deploy installable:
the bound is capped, and inside the cap every compiled package in ``uv.lock`` has a
wheel the Apps build image (Linux, x86_64) can install. Raise the cap only when the
second test passes for the new version.
"""

from __future__ import annotations

import tomllib
from pathlib import Path

from packaging.specifiers import SpecifierSet

PROJECT_ROOT = Path(__file__).resolve().parents[1]
#: Far beyond any CPython that exists: a bound that admits it has no upper end.
UNRELEASED_MINOR = 99


def _requires_python() -> SpecifierSet:
    data = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    return SpecifierSet(data["project"]["requires-python"])


def _admitted_minors(bound: SpecifierSet) -> list[int]:
    return [minor for minor in range(12, UNRELEASED_MINOR + 1) if f"3.{minor}.0" in bound]


def _installs_on_linux_x86_64(platform_tag: str) -> bool:
    return platform_tag == "any" or any(
        part.endswith("_x86_64") and (part.startswith("manylinux") or part.startswith("linux"))
        for part in platform_tag.split(".")
    )


def _covers(filename: str, minor: int) -> bool:
    """Whether a wheel installs on CPython 3.<minor> (the default build) on Linux x86_64."""
    python_tags, abi_tag, platform_tag = filename.removesuffix(".whl").split("-")[-3:]
    if not _installs_on_linux_x86_64(platform_tag):
        return False
    for tag in python_tags.split("."):
        if tag == "py3" and abi_tag == "none":
            return True
        if tag == f"cp3{minor}" and abi_tag in (f"cp3{minor}", "abi3", "none"):
            return True
        if abi_tag == "abi3" and tag.startswith("cp3") and int(tag[3:]) <= minor:
            return True
    return False


def test_requires_python_has_an_upper_bound() -> None:
    bound = _requires_python()
    assert f"3.{UNRELEASED_MINOR}.0" not in bound, (
        f"requires-python is {str(bound)!r}. With no upper bound, uv installs the newest "
        "CPython it can fetch, and a release the compiled dependencies have no wheels for yet "
        "fails every deploy (CPython 3.15, 2026-11-02). Cap it below the first version uv.lock "
        "has no wheels for."
    )


def test_every_compiled_package_has_a_wheel_for_every_admitted_python() -> None:
    lock = tomllib.loads((PROJECT_ROOT / "uv.lock").read_text(encoding="utf-8"))
    minors = _admitted_minors(_requires_python())
    missing = [
        f"{package['name']} {package.get('version')} on CPython 3.{minor}"
        for package in lock.get("package", [])
        if (files := [wheel["url"].rsplit("/", 1)[-1] for wheel in package.get("wheels", [])])
        for minor in minors
        if not any(_covers(name, minor) for name in files)
    ]
    assert not missing, (
        "requires-python admits a Python these locked packages have no Linux x86_64 wheel "
        f"for, so a deploy that lands on it fails to install: {missing}"
    )


def test_the_admitted_versions_are_the_ones_the_lock_covers() -> None:
    assert _admitted_minors(SpecifierSet(">=3.12, <3.15")) == [12, 13, 14]
    assert UNRELEASED_MINOR in _admitted_minors(SpecifierSet(">=3.12"))


def test_the_wheel_check_reads_tags_the_way_the_installer_does() -> None:
    linux = "manylinux_2_17_x86_64.manylinux2014_x86_64"
    # The wheel that failed every deploy: built for 3.14, not 3.15.
    assert _covers(f"psycopg_binary-3.3.4-cp314-cp314-{linux}.whl", 14)
    assert not _covers(f"psycopg_binary-3.3.4-cp314-cp314-{linux}.whl", 15)
    # Version-agnostic wheels, pure or binary, cover every version.
    assert _covers("fastapi-0.116.0-py3-none-any.whl", 15)
    assert _covers(f"ruff-0.16.3-py3-none-{linux}.whl", 15)
    # The stable ABI covers its own version and every later one.
    assert _covers("cryptography-46.0.0-cp311-abi3-manylinux_2_34_x86_64.whl", 15)
    # Another platform, or the free-threaded build, does not count.
    assert not _covers("pydantic_core-2.46.4-cp314-cp314-macosx_11_0_arm64.whl", 14)
    assert not _covers(f"uvloop-0.22.1-cp314-cp314t-{linux}.whl", 14)
