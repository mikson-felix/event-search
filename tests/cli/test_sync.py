from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

from click.testing import CliRunner

from event_search.cli.commands.sync import sync_command
from event_search.domain.errors import InvalidTimeRangeError
from event_search.domain.models import SyncResult, TimeRange


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


def make_application() -> SimpleNamespace:
    return SimpleNamespace(
        time_range_resolver=MagicMock(),
        partition_resolver=MagicMock(),
        sync_service=MagicMock(),
        renderer=MagicMock(),
    )


def test_sync_uses_default_time_range_when_arguments_are_omitted() -> None:
    runner = CliRunner()

    app = make_application()
    time_range = make_time_range()

    partitions = [
        "2026/09/10/10",
    ]

    sync_result = SyncResult(
        discovered=2,
        materialized=1,
        skipped=1,
    )

    app.time_range_resolver.resolve.return_value = time_range
    app.partition_resolver.resolve.return_value = partitions
    app.sync_service.sync.return_value = sync_result

    result = runner.invoke(
        sync_command,
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
        partitions,
    )

    app.renderer.render_sync.assert_called_once_with(
        sync_result,
    )


def test_sync_passes_relative_time_range() -> None:
    runner = CliRunner()

    app = make_application()
    time_range = make_time_range()

    app.time_range_resolver.resolve.return_value = time_range
    app.partition_resolver.resolve.return_value = [
        "2026/09/10/10",
    ]

    app.sync_service.sync.return_value = SyncResult(
        discovered=0,
        materialized=0,
        skipped=0,
    )

    result = runner.invoke(
        sync_command,
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


def test_sync_passes_explicit_time_range() -> None:
    runner = CliRunner()

    app = make_application()
    time_range = make_time_range()

    app.time_range_resolver.resolve.return_value = time_range
    app.partition_resolver.resolve.return_value = [
        "2026/09/10/10",
    ]

    app.sync_service.sync.return_value = SyncResult(
        discovered=0,
        materialized=0,
        skipped=0,
    )

    result = runner.invoke(
        sync_command,
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


def test_sync_converts_invalid_time_range_to_click_error() -> None:
    runner = CliRunner()

    app = make_application()

    app.time_range_resolver.resolve.side_effect = InvalidTimeRangeError(
        "--to requires --from",
    )

    result = runner.invoke(
        sync_command,
        [
            "--to",
            "2026-09-10T11:00",
        ],
        obj=app,
    )

    assert result.exit_code == 1

    assert "--to requires --from" in result.output

    app.partition_resolver.resolve.assert_not_called()
    app.sync_service.sync.assert_not_called()
    app.renderer.render_range.assert_not_called()
    app.renderer.render_sync.assert_not_called()


def test_sync_renders_range_before_synchronization_result() -> None:
    runner = CliRunner()

    app = make_application()
    time_range = make_time_range()

    partitions = [
        "2026/09/10/10",
    ]

    sync_result = SyncResult(
        discovered=1,
        materialized=1,
        skipped=0,
    )

    app.time_range_resolver.resolve.return_value = time_range
    app.partition_resolver.resolve.return_value = partitions
    app.sync_service.sync.return_value = sync_result

    calls: list[str] = []

    app.renderer.render_range.side_effect = lambda value: calls.append("range")

    app.sync_service.sync.side_effect = lambda value: (
        calls.append("sync"),
        sync_result,
    )[1]

    app.renderer.render_sync.side_effect = lambda value: calls.append("result")

    result = runner.invoke(
        sync_command,
        [],
        obj=app,
    )

    assert result.exit_code == 0

    assert calls == [
        "range",
        "sync",
        "result",
    ]


def test_sync_help() -> None:
    runner = CliRunner()

    result = runner.invoke(
        sync_command,
        [
            "--help",
        ],
    )

    assert result.exit_code == 0

    output = " ".join(result.output.split())

    assert "Synchronize immutable NDJSON blobs into the local Parquet cache" in output

    assert "--time RANGE" in output
    assert "--from DATETIME" in output
    assert "--to DATETIME" in output
