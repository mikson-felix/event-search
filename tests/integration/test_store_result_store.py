from datetime import UTC, datetime
from pathlib import Path

from event_search.domain.models import (
    EventLocator,
    SearchSummary,
)
from event_search.infrastructure.cache.search_result_store import (
    DuckDBSearchResultStore,
)


def make_summary(
    event_id: str,
    *,
    parquet_path: Path,
) -> SearchSummary:
    return SearchSummary(
        event_id=event_id,
        user_id=f"user-{event_id}",
        organization_id="org-001",
        event_name="LOGIN",
        category="AUTH",
        timestamp=datetime(
            2026,
            9,
            9,
            8,
            30,
            tzinfo=UTC,
        ),
        locator=EventLocator(
            parquet_path=parquet_path,
            source_line=1,
            blob_partition="2026/09/09/08",
            blob_name="events.ndjson",
        ),
    )


def test_replace_and_get(
    tmp_path: Path,
) -> None:
    store = DuckDBSearchResultStore(
        database_path=tmp_path / "events.duckdb",
    )

    summary = make_summary(
        "event-001",
        parquet_path=tmp_path / "events.parquet",
    )

    store.replace([summary])

    result = store.get("event-001")

    assert result == summary


def test_replace_removes_previous_search(
    tmp_path: Path,
) -> None:
    store = DuckDBSearchResultStore(
        database_path=tmp_path / "events.duckdb",
    )

    first = make_summary(
        "event-001",
        parquet_path=tmp_path / "a.parquet",
    )

    second = make_summary(
        "event-002",
        parquet_path=tmp_path / "b.parquet",
    )

    store.replace([first])

    store.replace([second])

    assert store.get("event-001") is None

    assert store.get("event-002") == second


def test_empty_replace_clears_results(
    tmp_path: Path,
) -> None:
    store = DuckDBSearchResultStore(
        database_path=tmp_path / "events.duckdb",
    )

    store.replace(
        [
            make_summary(
                "event-001",
                parquet_path=tmp_path / "a.parquet",
            )
        ]
    )

    store.replace([])

    assert store.get("event-001") is None


def test_find_ids_by_prefix(
    tmp_path: Path,
) -> None:
    store = DuckDBSearchResultStore(
        database_path=tmp_path / "events.duckdb",
    )

    store.replace(
        [
            make_summary(
                "abc-001",
                parquet_path=tmp_path / "a.parquet",
            ),
            make_summary(
                "abc-002",
                parquet_path=tmp_path / "b.parquet",
            ),
            make_summary(
                "def-001",
                parquet_path=tmp_path / "c.parquet",
            ),
        ]
    )

    assert store.find_ids(prefix="abc") == [
        "abc-001",
        "abc-002",
    ]


def test_find_ids_respects_limit(
    tmp_path: Path,
) -> None:
    store = DuckDBSearchResultStore(
        database_path=tmp_path / "events.duckdb",
    )

    store.replace(
        [
            make_summary(
                "abc-001",
                parquet_path=tmp_path / "a.parquet",
            ),
            make_summary(
                "abc-002",
                parquet_path=tmp_path / "b.parquet",
            ),
            make_summary(
                "abc-003",
                parquet_path=tmp_path / "c.parquet",
            ),
        ]
    )

    result = store.find_ids(
        prefix="abc",
        limit=2,
    )

    assert result == [
        "abc-001",
        "abc-002",
    ]


def test_clear(
    tmp_path: Path,
) -> None:
    store = DuckDBSearchResultStore(
        database_path=tmp_path / "events.duckdb",
    )

    store.replace(
        [
            make_summary(
                "event-001",
                parquet_path=tmp_path / "events.parquet",
            )
        ]
    )

    store.clear()

    assert store.get("event-001") is None
