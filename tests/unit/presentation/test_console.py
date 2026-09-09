from __future__ import annotations

from datetime import UTC, datetime
from io import StringIO
from pathlib import Path

from rich.console import Console

from event_search.domain.models import (
    CacheStatus,
    EventDetails,
    EventLocator,
    SearchSummary,
    SyncResult,
    TimeRange,
)
from event_search.presentation.console import ConsoleRenderer


def make_renderer() -> tuple[ConsoleRenderer, StringIO]:
    stream = StringIO()

    console = Console(
        file=stream,
        force_terminal=False,
        width=200,
    )

    return ConsoleRenderer(console), stream


def normalize_output(
    stream: StringIO,
) -> str:
    return " ".join(stream.getvalue().split())


def test_render_range_outputs_local_and_utc_ranges() -> None:
    renderer, stream = make_renderer()

    value = TimeRange(
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
            30,
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
            30,
            tzinfo=UTC,
        ),
    )

    renderer.render_range(value)

    output = normalize_output(stream)

    assert "Local:" in output
    assert "UTC:" in output
    assert "2026-09-10 10:00:00 UTC" in output
    assert "2026-09-10 11:30:00 UTC" in output


def test_render_results_outputs_table_with_event_data() -> None:
    renderer, stream = make_renderer()

    result = SearchSummary(
        event_id="event-123",
        user_id="user-123",
        organization_id="org-123",
        event_name="VERY_LONG_EVENT_NAME_THAT_SHOULD_BE_SHORTENED",
        category="VERY_LONG_CATEGORY_NAME_THAT_SHOULD_BE_SHORTENED",
        timestamp=datetime(
            2026,
            9,
            10,
            12,
            30,
            tzinfo=UTC,
        ),
        locator=EventLocator(
            parquet_path=Path("/tmp/events.parquet"),
            source_line=42,
            blob_partition="2026/09/10/12",
            blob_name="events.ndjson",
        ),
    )

    renderer.render_results([result])

    output = normalize_output(stream)

    assert "Events (1)" in output

    assert "ID" in output
    assert "User ID" in output
    assert "Organization ID" in output
    assert "Event" in output
    assert "Category" in output
    assert "Timestamp" in output

    assert "event-123" in output
    assert "user-123" in output
    assert "org-123" in output

    assert "VERY_LONG_EVENT_NAM…" in output
    assert "VERY_LONG…" in output

    assert "2026-09-10T12:30:00+00:00" in output

    assert "Path" not in output
    assert "Blob" not in output
    assert "2026/09/10/12" not in output
    assert "events.ndjson" not in output
    assert "/tmp/events.parquet" not in output


def test_render_results_uses_dash_for_missing_optional_values() -> None:
    renderer, stream = make_renderer()

    result = SearchSummary(
        event_id="event-002",
        user_id=None,
        organization_id=None,
        event_name=None,
        category=None,
        timestamp=datetime(
            2026,
            9,
            10,
            10,
            20,
            tzinfo=UTC,
        ),
        locator=EventLocator(
            blob_partition="2026/09/10/10",
            blob_name="events-002.ndjson",
            source_line=5,
            parquet_path=Path("/tmp/events-002.parquet"),
        ),
    )

    renderer.render_results([result])

    output = normalize_output(stream)

    assert "event-002" in output
    assert "-" in output

    assert "2026/09/10/10" not in output
    assert "events-002.ndjson" not in output


def test_render_results_handles_empty_list() -> None:
    renderer, stream = make_renderer()

    renderer.render_results([])

    output = normalize_output(stream)

    assert "Events (0)" in output
    assert "ID" in output
    assert "User ID" in output
    assert "Organization ID" in output
    assert "Event" in output
    assert "Category" in output
    assert "Timestamp" in output

    assert "Path" not in output
    assert "Blob" not in output


def test_render_event_outputs_metadata_and_raw_json() -> None:
    renderer, stream = make_renderer()

    result = EventDetails(
        event_id="event-001",
        blob_partition="2026/09/10/10",
        blob_name="events-001.ndjson",
        source_line=42,
        raw_json=('{"event_id":"event-001","timestamp":"2026-09-10T10:15:00Z"}'),
    )

    renderer.render_event(result)

    output = normalize_output(stream)

    assert "Event" in output
    assert "ID:" in output
    assert "event-001" in output
    assert "Source:" in output
    assert "2026/09/10/10/events-001.ndjson" in output
    assert "Line:" in output
    assert "42" in output
    assert "timestamp" in output
    assert "2026-09-10T10:15:00Z" in output


def test_render_status_outputs_cache_statistics() -> None:
    renderer, stream = make_renderer()

    status = CacheStatus(
        blobs_count=12,
        events_count=3456,
        parquet_size_bytes=1024 * 1024 * 2,
        last_materialized_at=datetime(
            2026,
            9,
            10,
            12,
            30,
            tzinfo=UTC,
        ),
    )

    renderer.render_status(status)

    output = normalize_output(stream)

    assert "Local cache" in output
    assert "Cached blobs" in output
    assert "12" in output
    assert "Cached events" in output
    assert "3,456" in output
    assert "Parquet size" in output
    assert "2.0 MB" in output
    assert "Last materialization" in output
    assert "2026-09-10T12:30:00+00:00" in output


def test_render_status_outputs_dash_without_materialization_time() -> None:
    renderer, stream = make_renderer()

    status = CacheStatus(
        blobs_count=0,
        events_count=0,
        parquet_size_bytes=0,
        last_materialized_at=None,
    )

    renderer.render_status(status)

    output = normalize_output(stream)

    assert "Local cache" in output
    assert "Cached blobs" in output
    assert "Cached events" in output
    assert "0.0 B" in output
    assert "-" in output


def test_render_sync_outputs_sync_statistics() -> None:
    renderer, stream = make_renderer()

    result = SyncResult(
        discovered=10,
        materialized=7,
        skipped=3,
    )

    renderer.render_sync(result)

    output = normalize_output(stream)

    assert "Discovered: 10" in output
    assert "materialized: 7" in output
    assert "cached: 3" in output


def test_shorten_returns_dash_for_none() -> None:
    assert ConsoleRenderer._shorten(None) == "-"


def test_shorten_keeps_short_value() -> None:
    assert ConsoleRenderer._shorten("AUDIT_LOG") == "AUDIT_LOG"


def test_shorten_truncates_long_value() -> None:
    assert ConsoleRenderer._shorten("VERY_LONG_EVENT_NAME_THAT_SHOULD_BE_SHORTENED") == "VERY_LONG_EVENT_NAM…"


def test_format_size_bytes() -> None:
    assert ConsoleRenderer._format_size(0) == "0.0 B"
    assert ConsoleRenderer._format_size(512) == "512.0 B"
    assert ConsoleRenderer._format_size(1023) == "1023.0 B"


def test_format_size_kilobytes() -> None:
    assert ConsoleRenderer._format_size(1024) == "1.0 KB"
    assert ConsoleRenderer._format_size(1536) == "1.5 KB"


def test_format_size_megabytes() -> None:
    assert (
        ConsoleRenderer._format_size(
            1024**2,
        )
        == "1.0 MB"
    )


def test_format_size_gigabytes() -> None:
    assert (
        ConsoleRenderer._format_size(
            1024**3,
        )
        == "1.0 GB"
    )


def test_format_size_terabytes() -> None:
    assert (
        ConsoleRenderer._format_size(
            1024**4,
        )
        == "1.0 TB"
    )


def test_format_size_petabytes() -> None:
    assert (
        ConsoleRenderer._format_size(
            1024**5,
        )
        == "1.0 PB"
    )
