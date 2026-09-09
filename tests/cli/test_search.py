from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

from click.testing import CliRunner

from event_search.cli.commands.search import search_command
from event_search.domain.errors import InvalidTimeRangeError
from event_search.domain.models import SearchFilters, TimeRange


def make_time_range() -> TimeRange:
    return TimeRange(
        local_from=datetime(
            2026,
            9,
            10,
            10,
            0,
            tzinfo=UTC,
        ),
        local_to=datetime(
            2026,
            9,
            10,
            11,
            0,
            tzinfo=UTC,
        ),
        utc_from=datetime(
            2026,
            9,
            10,
            10,
            0,
            tzinfo=UTC,
        ),
        utc_to=datetime(
            2026,
            9,
            10,
            11,
            0,
            tzinfo=UTC,
        ),
    )


def make_application(
    *,
    default_limit: int = 100,
    max_limit: int = 1000,
) -> SimpleNamespace:
    return SimpleNamespace(
        time_range_resolver=MagicMock(),
        partition_resolver=MagicMock(),
        renderer=MagicMock(),
        sync_service=MagicMock(),
        search_service=MagicMock(),
        settings=SimpleNamespace(
            search=SimpleNamespace(
                default_limit=default_limit,
                max_limit=max_limit,
            ),
        ),
    )


def test_search_uses_default_limit_when_limit_is_omitted() -> None:
    runner = CliRunner()

    app = make_application(
        default_limit=100,
        max_limit=1000,
    )

    time_range = make_time_range()

    app.time_range_resolver.resolve.return_value = time_range
    app.partition_resolver.resolve.return_value = [
        "2026/09/10/10",
    ]
    app.search_service.search.return_value = []

    result = runner.invoke(
        search_command,
        [],
        obj=app,
    )

    assert result.exit_code == 0

    app.time_range_resolver.resolve.assert_called_once_with(
        shortcut=None,
        date_from=None,
        date_to=None,
    )

    app.partition_resolver.resolve.assert_called_once_with(
        time_range,
    )

    app.renderer.render_range.assert_called_once_with(
        time_range,
    )

    app.sync_service.sync.assert_called_once_with(
        [
            "2026/09/10/10",
        ]
    )

    app.search_service.search.assert_called_once()

    call = app.search_service.search.call_args

    assert call.kwargs["time_range"] == time_range
    assert call.kwargs["partitions"] == [
        "2026/09/10/10",
    ]
    assert call.kwargs["limit"] == 100

    filters = call.kwargs["filters"]

    assert filters == SearchFilters(
        event_id=None,
        user_id=None,
        organization_id=None,
        event_name=None,
        category=None,
    )

    app.renderer.render_results.assert_called_once_with(
        [],
    )


def test_search_passes_explicit_limit() -> None:
    runner = CliRunner()

    app = make_application(
        default_limit=100,
        max_limit=1000,
    )

    time_range = make_time_range()

    app.time_range_resolver.resolve.return_value = time_range
    app.partition_resolver.resolve.return_value = [
        "2026/09/10/10",
    ]
    app.search_service.search.return_value = []

    result = runner.invoke(
        search_command,
        [
            "--limit",
            "250",
        ],
        obj=app,
    )

    assert result.exit_code == 0

    app.search_service.search.assert_called_once()

    assert app.search_service.search.call_args.kwargs["limit"] == 250


def test_search_rejects_limit_above_configured_maximum() -> None:
    runner = CliRunner()

    app = make_application(
        default_limit=100,
        max_limit=500,
    )

    time_range = make_time_range()

    app.time_range_resolver.resolve.return_value = time_range

    result = runner.invoke(
        search_command,
        [
            "--limit",
            "501",
        ],
        obj=app,
    )

    assert result.exit_code == 2

    assert "Maximum limit is 500" in result.output
    assert "--limit" in result.output

    app.partition_resolver.resolve.assert_not_called()
    app.sync_service.sync.assert_not_called()
    app.search_service.search.assert_not_called()
    app.renderer.render_results.assert_not_called()


def test_search_rejects_non_positive_limit() -> None:
    runner = CliRunner()

    app = make_application()

    result = runner.invoke(
        search_command,
        [
            "--limit",
            "0",
        ],
        obj=app,
    )

    assert result.exit_code == 2

    app.time_range_resolver.resolve.assert_not_called()
    app.search_service.search.assert_not_called()


def test_search_converts_invalid_time_range_to_click_error() -> None:
    runner = CliRunner()

    app = make_application()

    app.time_range_resolver.resolve.side_effect = InvalidTimeRangeError(
        "--to requires --from",
    )

    result = runner.invoke(
        search_command,
        [
            "--to",
            "2026-09-10T12:00",
        ],
        obj=app,
    )

    assert result.exit_code == 1

    assert "--to requires --from" in result.output

    app.partition_resolver.resolve.assert_not_called()
    app.sync_service.sync.assert_not_called()
    app.search_service.search.assert_not_called()


def test_search_passes_relative_time_range() -> None:
    runner = CliRunner()

    app = make_application()

    time_range = make_time_range()

    app.time_range_resolver.resolve.return_value = time_range
    app.partition_resolver.resolve.return_value = [
        "2026/09/10/10",
    ]
    app.search_service.search.return_value = []

    result = runner.invoke(
        search_command,
        [
            "--time",
            "last 2 hours",
        ],
        obj=app,
    )

    assert result.exit_code == 0

    app.time_range_resolver.resolve.assert_called_once_with(
        shortcut="last 2 hours",
        date_from=None,
        date_to=None,
    )


def test_search_passes_explicit_time_range() -> None:
    runner = CliRunner()

    app = make_application()

    time_range = make_time_range()

    app.time_range_resolver.resolve.return_value = time_range
    app.partition_resolver.resolve.return_value = [
        "2026/09/10/10",
    ]
    app.search_service.search.return_value = []

    result = runner.invoke(
        search_command,
        [
            "--from",
            "2026-09-10T10:00",
            "--to",
            "2026-09-10T11:00",
        ],
        obj=app,
    )

    assert result.exit_code == 0

    app.time_range_resolver.resolve.assert_called_once_with(
        shortcut=None,
        date_from="2026-09-10T10:00",
        date_to="2026-09-10T11:00",
    )


def test_search_passes_all_filters() -> None:
    runner = CliRunner()

    app = make_application()

    time_range = make_time_range()

    app.time_range_resolver.resolve.return_value = time_range
    app.partition_resolver.resolve.return_value = [
        "2026/09/10/10",
        "2026/09/10/11",
    ]

    search_results = [
        MagicMock(),
        MagicMock(),
    ]

    app.search_service.search.return_value = search_results

    result = runner.invoke(
        search_command,
        [
            "--event-id",
            "event-001",
            "--user-id",
            "user-001",
            "--organization-id",
            "org-001",
            "--event-name",
            "LOGIN",
            "--category",
            "AUTH",
            "--limit",
            "50",
        ],
        obj=app,
    )

    assert result.exit_code == 0

    app.search_service.search.assert_called_once_with(
        filters=SearchFilters(
            event_id="event-001",
            user_id="user-001",
            organization_id="org-001",
            event_name="LOGIN",
            category="AUTH",
        ),
        time_range=time_range,
        partitions=[
            "2026/09/10/10",
            "2026/09/10/11",
        ],
        limit=50,
    )

    app.renderer.render_results.assert_called_once_with(
        search_results,
    )


def test_search_syncs_before_querying() -> None:
    runner = CliRunner()

    app = make_application()

    time_range = make_time_range()

    partitions = [
        "2026/09/10/10",
    ]

    app.time_range_resolver.resolve.return_value = time_range
    app.partition_resolver.resolve.return_value = partitions
    app.search_service.search.return_value = []

    calls: list[str] = []

    app.sync_service.sync.side_effect = lambda value: calls.append("sync")

    app.search_service.search.side_effect = lambda **kwargs: (
        calls.append("search"),
        [],
    )[1]

    result = runner.invoke(
        search_command,
        [],
        obj=app,
    )

    assert result.exit_code == 0

    assert calls == [
        "sync",
        "search",
    ]


def test_search_renders_range_before_results() -> None:
    runner = CliRunner()

    app = make_application()

    time_range = make_time_range()

    app.time_range_resolver.resolve.return_value = time_range
    app.partition_resolver.resolve.return_value = [
        "2026/09/10/10",
    ]
    app.search_service.search.return_value = []

    calls: list[str] = []

    app.renderer.render_range.side_effect = lambda value: calls.append("range")

    app.renderer.render_results.side_effect = lambda value: calls.append("results")

    result = runner.invoke(
        search_command,
        [],
        obj=app,
    )

    assert result.exit_code == 0

    assert calls == [
        "range",
        "results",
    ]


def test_search_help() -> None:
    runner = CliRunner()

    result = runner.invoke(
        search_command,
        [
            "--help",
        ],
    )

    assert result.exit_code == 0

    assert "--time RANGE" in result.output
    assert "--from DATETIME" in result.output
    assert "--to DATETIME" in result.output
    assert "--event-id TEXT" in result.output
    assert "--user-id TEXT" in result.output
    assert "--organization-id TEXT" in result.output
    assert "--event-name TEXT" in result.output
    assert "--category TEXT" in result.output
    assert "--limit INTEGER" in result.output
