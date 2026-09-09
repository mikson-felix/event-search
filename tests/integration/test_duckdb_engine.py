from datetime import UTC, datetime
from pathlib import Path

from event_search.domain.models import (
    SearchFilters,
    TimeRange,
)
from event_search.infrastructure.query.duckdb_engine import (
    DuckDBQueryEngine,
)


def make_range(
    start_hour: int,
    end_hour: int,
) -> TimeRange:
    start = datetime(
        2026,
        9,
        9,
        start_hour,
        0,
        tzinfo=UTC,
    )

    end = datetime(
        2026,
        9,
        9,
        end_hour,
        0,
        tzinfo=UTC,
    )

    return TimeRange(
        local_from=start,
        local_to=end,
        utc_from=start,
        utc_to=end,
    )


def make_row(
    *,
    event_id: str,
    timestamp: str,
    user_id: str = "user-001",
    organization_id: str = "org-001",
    event_name: str = "LOGIN",
    category: str = "AUTH",
    partition: str = "2026/09/09/08",
    source_line: int = 1,
) -> dict:
    return {
        "event_id": event_id,
        "user_id": user_id,
        "organization_id": organization_id,
        "event_name": event_name,
        "category": category,
        "timestamp": timestamp,
        "blob_partition": partition,
        "blob_name": "events.ndjson",
        "source_line": source_line,
        "raw_json": f'{{"event_id":"{event_id}"}}',
    }


def test_search_reads_selected_partition(
    tmp_path: Path,
    make_parquet,
) -> None:
    make_parquet(
        partition="2026/09/09/08",
        file_name="events.parquet",
        rows=[
            make_row(
                event_id="event-001",
                timestamp="2026-09-09T08:30:00Z",
            )
        ],
    )

    engine = DuckDBQueryEngine(
        parquet_root=tmp_path / "parquet",
    )

    result = engine.search(
        time_range=make_range(
            8,
            9,
        ),
        partitions=["2026/09/09/08"],
        filters=SearchFilters(),
        limit=100,
    )

    assert [item.event_id for item in result] == ["event-001"]


def test_time_range_is_half_open(
    tmp_path: Path,
    make_parquet,
) -> None:
    make_parquet(
        partition="2026/09/09/08",
        file_name="events.parquet",
        rows=[
            make_row(
                event_id="at-start",
                timestamp="2026-09-09T08:00:00Z",
                source_line=1,
            ),
            make_row(
                event_id="inside",
                timestamp="2026-09-09T08:30:00Z",
                source_line=2,
            ),
            make_row(
                event_id="at-end",
                timestamp="2026-09-09T09:00:00Z",
                source_line=3,
            ),
        ],
    )

    engine = DuckDBQueryEngine(
        parquet_root=tmp_path / "parquet",
    )

    result = engine.search(
        time_range=make_range(
            8,
            9,
        ),
        partitions=["2026/09/09/08"],
        filters=SearchFilters(),
        limit=100,
    )

    ids = {item.event_id for item in result}

    assert "at-start" in ids
    assert "inside" in ids
    assert "at-end" not in ids


def test_filters_by_event_id(
    tmp_path: Path,
    make_parquet,
) -> None:
    make_parquet(
        partition="2026/09/09/08",
        file_name="events.parquet",
        rows=[
            make_row(
                event_id="event-001",
                timestamp="2026-09-09T08:10:00Z",
            ),
            make_row(
                event_id="event-002",
                timestamp="2026-09-09T08:20:00Z",
                source_line=2,
            ),
        ],
    )

    engine = DuckDBQueryEngine(
        parquet_root=tmp_path / "parquet",
    )

    result = engine.search(
        time_range=make_range(
            8,
            9,
        ),
        partitions=["2026/09/09/08"],
        filters=SearchFilters(
            event_id="event-002",
        ),
        limit=100,
    )

    assert [item.event_id for item in result] == ["event-002"]


def test_combines_filters_with_and(
    tmp_path: Path,
    make_parquet,
) -> None:
    make_parquet(
        partition="2026/09/09/08",
        file_name="events.parquet",
        rows=[
            make_row(
                event_id="match",
                timestamp="2026-09-09T08:10:00Z",
                organization_id="org-target",
                event_name="LOGIN",
                category="AUTH",
            ),
            make_row(
                event_id="wrong-org",
                timestamp="2026-09-09T08:20:00Z",
                organization_id="other",
                event_name="LOGIN",
                category="AUTH",
                source_line=2,
            ),
            make_row(
                event_id="wrong-event",
                timestamp="2026-09-09T08:30:00Z",
                organization_id="org-target",
                event_name="LOGOUT",
                category="AUTH",
                source_line=3,
            ),
        ],
    )

    engine = DuckDBQueryEngine(
        parquet_root=tmp_path / "parquet",
    )

    result = engine.search(
        time_range=make_range(
            8,
            9,
        ),
        partitions=["2026/09/09/08"],
        filters=SearchFilters(
            organization_id="org-target",
            event_name="LOGIN",
            category="AUTH",
        ),
        limit=100,
    )

    assert [item.event_id for item in result] == ["match"]


def test_results_are_sorted_by_timestamp_descending(
    tmp_path: Path,
    make_parquet,
) -> None:
    make_parquet(
        partition="2026/09/09/08",
        file_name="events.parquet",
        rows=[
            make_row(
                event_id="old",
                timestamp="2026-09-09T08:10:00Z",
            ),
            make_row(
                event_id="new",
                timestamp="2026-09-09T08:50:00Z",
                source_line=2,
            ),
        ],
    )

    engine = DuckDBQueryEngine(
        parquet_root=tmp_path / "parquet",
    )

    result = engine.search(
        time_range=make_range(
            8,
            9,
        ),
        partitions=["2026/09/09/08"],
        filters=SearchFilters(),
        limit=100,
    )

    assert [item.event_id for item in result] == [
        "new",
        "old",
    ]


def test_limit_is_applied(
    tmp_path: Path,
    make_parquet,
) -> None:
    make_parquet(
        partition="2026/09/09/08",
        file_name="events.parquet",
        rows=[
            make_row(
                event_id=f"event-{index}",
                timestamp=(f"2026-09-09T08:{index:02d}:00Z"),
                source_line=index,
            )
            for index in range(1, 6)
        ],
    )

    engine = DuckDBQueryEngine(
        parquet_root=tmp_path / "parquet",
    )

    result = engine.search(
        time_range=make_range(
            8,
            9,
        ),
        partitions=["2026/09/09/08"],
        filters=SearchFilters(),
        limit=2,
    )

    assert len(result) == 2


def test_locator_contains_exact_parquet_path(
    tmp_path: Path,
    make_parquet,
) -> None:
    parquet_path = make_parquet(
        partition="2026/09/09/08",
        file_name="events.parquet",
        rows=[
            make_row(
                event_id="event-001",
                timestamp="2026-09-09T08:30:00Z",
                source_line=42,
            )
        ],
    )

    engine = DuckDBQueryEngine(
        parquet_root=tmp_path / "parquet",
    )

    [result] = engine.search(
        time_range=make_range(
            8,
            9,
        ),
        partitions=["2026/09/09/08"],
        filters=SearchFilters(),
        limit=100,
    )

    assert result.locator.parquet_path == parquet_path.resolve()

    assert result.locator.source_line == 42


def test_missing_partition_returns_empty_list(
    tmp_path: Path,
) -> None:
    engine = DuckDBQueryEngine(
        parquet_root=tmp_path / "parquet",
    )

    result = engine.search(
        time_range=make_range(
            8,
            9,
        ),
        partitions=["2026/09/09/08"],
        filters=SearchFilters(),
        limit=100,
    )

    assert result == []
