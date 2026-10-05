"""The posted billing read waits for a person, so an idle installation leaves its warehouse off.

2026-10-02. The read ran every fifteen minutes on a timer. It is a statement on the SQL
warehouse the seal names, and that warehouse stops only after ten quiet minutes, so it barely
stopped. Replayed from the R6 test installation's own query history with nobody on the app,
the warehouse was up 68% of the time: about 8.2 DBU an hour, $138 a day at the published
$0.70, in no panel and no document.
"""

from __future__ import annotations

import pytest
from starlette.requests import Request

import app as app_module
from server.posted_usage import PostedUsageCache

HOUR = 3600.0
MINUTE = 60.0


class _Stop(Exception):
    """Ends the refresh loop once the test has seen enough of it."""


class _Cache:
    interval_seconds = HOUR

    def __init__(self) -> None:
        self.reads: list[float] = []
        self.clock = 0.0

    def refresh(self) -> None:
        self.reads.append(self.clock)


async def _run(watched_at, *, hours: float) -> list[float]:
    """Run the loop for ``hours`` of simulated time; ``watched_at(t)`` says if a person is on."""

    cache = _Cache()

    async def sleep(seconds: float) -> None:
        cache.clock += seconds
        if cache.clock >= hours * HOUR:
            raise _Stop

    with pytest.raises(_Stop):
        await app_module._refresh_posted_usage(
            cache,
            lambda: watched_at(cache.clock),
            check_seconds=MINUTE,
            clock=lambda: cache.clock,
            sleep=sleep,
        )
    return cache.reads


async def test_nobody_on_the_app_means_no_warehouse_statement_all_day() -> None:
    assert await _run(lambda t: False, hours=24) == []


async def test_a_person_on_the_app_is_read_for_once_an_hour() -> None:
    assert await _run(lambda t: True, hours=3) == [0.0, HOUR, 2 * HOUR]


async def test_the_reads_stop_when_the_person_leaves() -> None:
    # On for the first ten minutes, then gone for the rest of the day.
    assert await _run(lambda t: t < 10 * MINUTE, hours=24) == [0.0]


async def test_a_person_arriving_later_is_read_for_within_a_minute() -> None:
    reads = await _run(lambda t: t >= 5 * HOUR, hours=6)
    assert reads == [5 * HOUR]


async def test_a_failed_read_is_logged_and_the_loop_carries_on(caplog) -> None:
    class Failing(_Cache):
        def refresh(self) -> None:
            super().refresh()
            raise RuntimeError("warehouse unavailable")

    cache = Failing()

    async def sleep(seconds: float) -> None:
        cache.clock += seconds
        if cache.clock >= 2 * HOUR:
            raise _Stop

    with caplog.at_level("WARNING"), pytest.raises(_Stop):
        await app_module._refresh_posted_usage(
            cache, lambda: True, check_seconds=MINUTE, clock=lambda: cache.clock, sleep=sleep
        )
    assert cache.reads == [0.0, HOUR]
    assert "Could not refresh posted Databricks usage" in caplog.text


def test_the_cache_reads_hourly_at_most() -> None:
    assert PostedUsageCache(None).interval_seconds == HOUR


def _request(method: str, path: str, headers: tuple[tuple[str, str], ...]) -> Request:
    return Request(
        {
            "type": "http",
            "method": method,
            "path": path,
            "query_string": b"",
            "headers": [(key.encode(), value.encode()) for key, value in headers],
        }
    )


async def test_a_page_load_marks_the_app_watched_and_a_polling_tab_does_not(monkeypatch) -> None:
    monkeypatch.setenv("DATABRICKS_APP_NAME", "lakebase-anti-demo")
    monkeypatch.setattr(app_module.app.state, "lease_keeper", None, raising=False)
    monkeypatch.setattr(app_module.app.state, "round4_warm_keeper", None, raising=False)
    monkeypatch.setattr(app_module.app.state, "last_person_action", None, raising=False)
    signed_in = (("x-forwarded-email", "someone@example.com"),)
    watched = app_module._watched_by_a_person(app_module.app)

    async def call_next(request):
        return "response"

    await app_module.note_installation_use(_request("GET", "/api/catalog", signed_in), call_next)
    await app_module.note_installation_use(_request("GET", "/api/bout/all", signed_in), call_next)
    assert not watched()

    page_load = signed_in + (("sec-fetch-dest", "document"),)
    await app_module.note_installation_use(_request("GET", "/", page_load), call_next)
    assert watched()


def test_twenty_quiet_minutes_and_the_app_is_no_longer_watched(monkeypatch) -> None:
    now = {"t": 10_000.0}
    monkeypatch.setattr(app_module.time, "monotonic", lambda: now["t"])
    monkeypatch.setattr(app_module.app.state, "last_person_action", now["t"], raising=False)
    watched = app_module._watched_by_a_person(app_module.app)

    now["t"] += 19 * MINUTE
    assert watched()
    now["t"] += 2 * MINUTE
    assert not watched()


def test_the_app_starts_the_loop_with_the_person_signal() -> None:
    import inspect

    source = inspect.getsource(app_module)
    assert "_refresh_posted_usage(posted_usage_cache, _watched_by_a_person(app))" in source
    assert inspect.iscoroutinefunction(app_module._refresh_posted_usage)


def test_the_app_gates_storage_re_probes_on_the_same_person() -> None:
    # The catalog's Round 4 and Round 6 storage probe is the other warehouse statement an
    # open tab sends (tests/test_manager.py holds the manager to it).
    import inspect

    assert "delta_storage_probe_watched=_watched_by_a_person(app)" in inspect.getsource(
        app_module
    )
