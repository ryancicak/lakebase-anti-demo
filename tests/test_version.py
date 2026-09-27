"""One release number, stated in the places that must agree on it.

The title screen and the staff roll show frontend/package.json's version, the
backend reports pyproject.toml's, and the git tag names the release. A bump
that misses one of them would put two different answers to "which version is
this" in front of whoever asked, so they are held equal here, lockfiles
included -- CI's `uv sync --locked` and `npm ci` read those.
"""

from __future__ import annotations

import json
import re
import tomllib
from pathlib import Path

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from server import version
from server.api import router

REPO = Path(__file__).resolve().parent.parent


def _pyproject_version() -> str:
    return tomllib.loads((REPO / "pyproject.toml").read_text(encoding="utf-8"))["project"][
        "version"
    ]


def test_a_release_is_a_plain_semantic_version() -> None:
    assert re.fullmatch(r"\d+\.\d+\.\d+", _pyproject_version())


def test_the_backend_reports_pyprojects_version() -> None:
    assert version.APP_VERSION == _pyproject_version()


def test_the_screen_shows_the_release_the_backend_reports() -> None:
    package = json.loads((REPO / "frontend" / "package.json").read_text(encoding="utf-8"))
    lock = json.loads((REPO / "frontend" / "package-lock.json").read_text(encoding="utf-8"))
    assert package["version"] == _pyproject_version()
    assert lock["version"] == lock["packages"][""]["version"] == _pyproject_version()


def test_uv_lock_names_the_same_release() -> None:
    lock = tomllib.loads((REPO / "uv.lock").read_text(encoding="utf-8"))
    own = [item["version"] for item in lock["package"] if item["name"] == "lakebase-anti-demo"]
    assert own == [_pyproject_version()]


def test_a_stamped_deploy_reports_its_commit(tmp_path) -> None:
    (tmp_path / version.BUILD_INFO_NAME).write_text(
        json.dumps({"commit": "0123456789ABCDEF0123456789abcdef01234567", "dirty": True}),
        encoding="utf-8",
    )

    assert version.build_info(tmp_path) == {
        "version": version.APP_VERSION,
        "commit": "0123456789ab",
        "dirty": True,
    }


def test_an_unstamped_tree_reports_the_release_alone(tmp_path) -> None:
    assert version.build_info(tmp_path) == {
        "version": version.APP_VERSION,
        "commit": None,
        "dirty": False,
    }


@pytest.mark.parametrize(
    "stamp",
    ["{not json", json.dumps({"commit": "not-a-commit"}), json.dumps(["a"]), json.dumps({})],
)
def test_a_malformed_stamp_is_no_commit_rather_than_a_guess(tmp_path, stamp) -> None:
    (tmp_path / version.BUILD_INFO_NAME).write_text(stamp, encoding="utf-8")

    info = version.build_info(tmp_path)

    assert info["commit"] is None
    assert info["dirty"] is False


def test_an_unreadable_pyproject_reports_unknown(tmp_path) -> None:
    assert version._read_version(tmp_path) == "unknown"


async def test_the_version_route_answers_without_any_database() -> None:
    app = FastAPI()
    app.include_router(router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://anti-demo.test") as c:
        response = await c.get("/api/version")

    assert response.status_code == 200
    assert response.json()["version"] == _pyproject_version()
