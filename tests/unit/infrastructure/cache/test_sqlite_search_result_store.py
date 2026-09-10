from datetime import UTC, datetime, timedelta
from pathlib import Path

from event_search.domain.models import (
    EventLocator,
    SearchSummary,
)
from event_search.infrastructure.cache.sqlite_search_result_store import (
    SQLiteSearchResultStore,
)


def make_result(
    *,
    event_id: str,
    timestamp: datetime | None = None,
) -> SearchSummary:
    return SearchSummary(
        event_id=event_id,
        user_id="user-001",
        organization_id="organization-001",
        event_name="document.opened",
        category="document",
        timestamp=timestamp
        or datetime(
            2026,
            9,
            10,
            8,
            30,
            tzinfo=UTC,
        ),
        locator=EventLocator(
            parquet_path=Path(".cache/parquet/2026/09/10/08/events.parquet"),
            source_line=17,
            blob_partition="2026/09/10/08",
            blob_name="events.ndjson",
        ),
    )


def test_get_returns_none_for_unknown_event(
    tmp_path: Path,
) -> None:
    store = SQLiteSearchResultStore(tmp_path / "event_search.sqlite")

    result = store.get("unknown")

    assert result is None


def test_replace_and_get_result(
    tmp_path: Path,
) -> None:
    store = SQLiteSearchResultStore(tmp_path / "event_search.sqlite")

    expected = make_result(event_id="event-001")

    store.replace([expected])

    result = store.get(expected.event_id)

    assert result is not None

    assert result.event_id == expected.event_id
    assert result.user_id == expected.user_id
    assert result.organization_id == expected.organization_id
    assert result.event_name == expected.event_name
    assert result.category == expected.category
    assert result.timestamp == expected.timestamp

    assert result.locator.parquet_path == expected.locator.parquet_path.resolve()
    assert result.locator.source_line == expected.locator.source_line
    assert result.locator.blob_partition == expected.locator.blob_partition
    assert result.locator.blob_name == expected.locator.blob_name


def test_replace_supports_nullable_fields(
    tmp_path: Path,
) -> None:
    store = SQLiteSearchResultStore(tmp_path / "event_search.sqlite")

    expected = SearchSummary(
        event_id="event-001",
        user_id=None,
        organization_id=None,
        event_name=None,
        category=None,
        timestamp=datetime(
            2026,
            9,
            10,
            8,
            0,
            tzinfo=UTC,
        ),
        locator=EventLocator(
            parquet_path=(tmp_path / "events.parquet"),
            source_line=1,
            blob_partition="2026/09/10/08",
            blob_name="events.ndjson",
        ),
    )

    store.replace([expected])

    result = store.get(expected.event_id)

    assert result is not None
    assert result.user_id is None
    assert result.organization_id is None
    assert result.event_name is None
    assert result.category is None


def test_replace_removes_previous_results(
    tmp_path: Path,
) -> None:
    store = SQLiteSearchResultStore(tmp_path / "event_search.sqlite")

    old_result = make_result(event_id="old-event")

    new_result = make_result(event_id="new-event")

    store.replace([old_result])

    store.replace([new_result])

    assert store.get(old_result.event_id) is None

    assert store.get(new_result.event_id) is not None


def test_replace_with_empty_list_clears_results(
    tmp_path: Path,
) -> None:
    store = SQLiteSearchResultStore(tmp_path / "event_search.sqlite")

    store.replace(
        [
            make_result(event_id="event-001"),
        ]
    )

    store.replace([])

    assert store.get("event-001") is None


def test_find_ids_returns_matching_prefix(
    tmp_path: Path,
) -> None:
    store = SQLiteSearchResultStore(tmp_path / "event_search.sqlite")

    store.replace(
        [
            make_result(event_id="event-003"),
            make_result(event_id="other-001"),
            make_result(event_id="event-001"),
            make_result(event_id="event-002"),
        ]
    )

    result = store.find_ids(
        prefix="event-",
    )

    assert result == [
        "event-001",
        "event-002",
        "event-003",
    ]


def test_find_ids_respects_limit(
    tmp_path: Path,
) -> None:
    store = SQLiteSearchResultStore(tmp_path / "event_search.sqlite")

    store.replace(
        [
            make_result(event_id="event-003"),
            make_result(event_id="event-001"),
            make_result(event_id="event-002"),
        ]
    )

    result = store.find_ids(
        prefix="event-",
        limit=2,
    )

    assert result == [
        "event-001",
        "event-002",
    ]


def test_find_ids_returns_empty_list_when_no_match(
    tmp_path: Path,
) -> None:
    store = SQLiteSearchResultStore(tmp_path / "event_search.sqlite")

    store.replace(
        [
            make_result(event_id="event-001"),
        ]
    )

    result = store.find_ids(
        prefix="missing",
    )

    assert result == []


def test_clear_removes_all_results(
    tmp_path: Path,
) -> None:
    store = SQLiteSearchResultStore(tmp_path / "event_search.sqlite")

    store.replace(
        [
            make_result(event_id="event-001"),
            make_result(event_id="event-002"),
        ]
    )

    store.clear()

    assert store.get("event-001") is None

    assert store.get("event-002") is None

    assert store.find_ids(prefix="") == []


def test_preserves_timezone_aware_timestamp(
    tmp_path: Path,
) -> None:
    store = SQLiteSearchResultStore(tmp_path / "event_search.sqlite")

    timestamp = datetime(
        2026,
        9,
        10,
        11,
        30,
        tzinfo=timezone_offset(),
    )

    expected = make_result(
        event_id="event-001",
        timestamp=timestamp,
    )

    store.replace([expected])

    result = store.get(expected.event_id)

    assert result is not None
    assert result.timestamp == timestamp
    assert result.timestamp.utcoffset() == timedelta(hours=3)


def timezone_offset():
    from datetime import timezone

    return timezone(timedelta(hours=3))
