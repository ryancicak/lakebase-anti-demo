"""server/home_cloud.by_cloud: one keyword per installation cloud, held to the Literal.

The server has no type checker, so the exhaustiveness the frontend gets from a `Record` over
`CompetitorKind` is a signature here: `by_cloud` takes one keyword-only argument per member of
`InstallationCloud`. Growing the Literal (a third cloud) fails this file until the helper grows
too, and the helper's new keyword then fails every call site that has not decided for the new
cloud. `home_cloud.CLOUDS` is held to the same list, so a workspace host of the new cloud cannot
be recognized before the rest of the server knows the cloud exists.
"""

from __future__ import annotations

import inspect
from typing import get_args

import pytest

from server import home_cloud
from server.home_cloud import by_cloud, installation_cloud
from server.models import InstallationCloud


def test_by_cloud_takes_exactly_one_keyword_per_installation_cloud() -> None:
    parameters = inspect.signature(by_cloud).parameters
    keywords = tuple(
        name for name, parameter in parameters.items()
        if parameter.kind is inspect.Parameter.KEYWORD_ONLY
    )
    assert keywords == get_args(InstallationCloud)


def test_the_home_clouds_cover_the_literal_in_order() -> None:
    assert tuple(cloud.name for cloud in home_cloud.CLOUDS) == get_args(InstallationCloud)


def test_by_cloud_chooses_by_name_and_refuses_the_rest() -> None:
    assert by_cloud("aws", aws="a", azure="b") == "a"
    assert by_cloud("azure", aws="a", azure="b") == "b"
    with pytest.raises(ValueError, match="unknown installation cloud 'gcp'"):
        by_cloud("gcp", aws="a", azure="b")
    with pytest.raises(TypeError):
        by_cloud("aws", "a", "b")  # type: ignore[misc]  # positional choices are refused


def test_a_branch_passed_as_a_callable_runs_only_for_its_cloud() -> None:
    ran: list[str] = []
    by_cloud("azure", aws=lambda: ran.append("aws"), azure=lambda: ran.append("azure"))()
    assert ran == ["azure"]


def test_a_manifest_without_the_field_is_the_aws_editions() -> None:
    class Stub:
        pass

    class Azure:
        cloud = "azure"

    assert installation_cloud(Stub()) == "aws"
    assert installation_cloud(Azure()) == "azure"
