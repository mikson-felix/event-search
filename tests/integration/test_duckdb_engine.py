from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import pytest
from faker import Faker

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


@pytest.fixture
def make_row(faker: Faker) -> Callable[..., dict]:
    def factory(
        *,
        event_id: str,
        timestamp: str,
        application: str | None = None,
        user_id: str | None = None,
        organization_id: str | None = None,
        event_name: str | None = None,
        category: str | None = None,
        partition: str = "2026/09/09/08",
        source_line: int = 1,
    ) -> dict:
        return {
            "event_id": event_id,
            "application": application if application is not None else faker.word(),
            "user_id": user_id if user_id is not None else faker.uuid4(),
            "organization_id": (organization_id if organization_id is not None else faker.uuid4()),
            "event_name": event_name if event_name is not None else faker.word(),
            "category": category if category is not None else faker.word(),
            "timestamp": timestamp,
            "blob_partition": partition,
            "blob_name": "events.ndjson",
            "source_line": source_line,
            "raw_json": f'{{"event_id":"{event_id}"}}',
        }

    return factory


def test_search_reads_selected_partition(
    tmp_path: Path,
    make_parquet,
    faker: Faker,
    make_row: Callable[..., dict],
) -> None:
    event_id = faker.uuid4()

    make_parquet(
        partition="2026/09/09/08",
        file_name="events.parquet",
        rows=[
            make_row(
                event_id=event_id,
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

    assert [item.event_id for item in result] == [event_id]


def test_time_range_is_half_open(
    tmp_path: Path,
    make_parquet,
    faker: Faker,
    make_row: Callable[..., dict],
) -> None:
    at_start_id = faker.uuid4()
    inside_id = faker.uuid4()
    at_end_id = faker.uuid4()

    make_parquet(
        partition="2026/09/09/08",
        file_name="events.parquet",
        rows=[
            make_row(
                event_id=at_start_id,
                timestamp="2026-09-09T08:00:00Z",
                source_line=1,
            ),
            make_row(
                event_id=inside_id,
                timestamp="2026-09-09T08:30:00Z",
                source_line=2,
            ),
            make_row(
                event_id=at_end_id,
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

    assert at_start_id in ids
    assert inside_id in ids
    assert at_end_id not in ids


def test_filters_by_event_id(
    tmp_path: Path,
    make_parquet,
    faker: Faker,
    make_row: Callable[..., dict],
) -> None:
    first_event_id = faker.uuid4()
    second_event_id = faker.uuid4()

    make_parquet(
        partition="2026/09/09/08",
        file_name="events.parquet",
        rows=[
            make_row(
                event_id=first_event_id,
                timestamp="2026-09-09T08:10:00Z",
            ),
            make_row(
                event_id=second_event_id,
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
            event_id=second_event_id,
        ),
        limit=100,
    )

    assert [item.event_id for item in result] == [second_event_id]


def test_filters_by_user_id(
    tmp_path: Path,
    make_parquet,
    faker: Faker,
    make_row: Callable[..., dict],
) -> None:
    target_event_id = faker.uuid4()
    other_event_id = faker.uuid4()
    target_user_id = faker.uuid4()
    other_user_id = faker.uuid4()

    make_parquet(
        partition="2026/09/09/08",
        file_name="events.parquet",
        rows=[
            make_row(
                event_id=target_event_id,
                timestamp="2026-09-09T08:10:00Z",
                user_id=target_user_id,
            ),
            make_row(
                event_id=other_event_id,
                timestamp="2026-09-09T08:20:00Z",
                user_id=other_user_id,
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
            user_id=target_user_id,
        ),
        limit=100,
    )

    assert [item.event_id for item in result] == [target_event_id]


def test_filters_by_application(
    tmp_path: Path,
    make_parquet,
    faker: Faker,
    make_row: Callable[..., dict],
) -> None:
    target_event_id = faker.uuid4()
    other_event_id = faker.uuid4()
    target_application = faker.unique.word()
    other_application = faker.unique.word()

    make_parquet(
        partition="2026/09/09/08",
        file_name="events.parquet",
        rows=[
            make_row(
                event_id=target_event_id,
                timestamp="2026-09-09T08:10:00Z",
                application=target_application,
            ),
            make_row(
                event_id=other_event_id,
                timestamp="2026-09-09T08:20:00Z",
                application=other_application,
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
            application=target_application,
        ),
        limit=100,
    )

    assert [item.event_id for item in result] == [target_event_id]


def test_combines_filters_with_and(
    tmp_path: Path,
    make_parquet,
    faker: Faker,
    make_row: Callable[..., dict],
) -> None:
    match_event_id = faker.uuid4()
    wrong_org_event_id = faker.uuid4()
    wrong_event_name_event_id = faker.uuid4()

    target_organization_id = faker.uuid4()
    other_organization_id = faker.uuid4()

    target_event_name = faker.unique.word()
    other_event_name = faker.unique.word()

    category = faker.word()

    make_parquet(
        partition="2026/09/09/08",
        file_name="events.parquet",
        rows=[
            make_row(
                event_id=match_event_id,
                timestamp="2026-09-09T08:10:00Z",
                organization_id=target_organization_id,
                event_name=target_event_name,
                category=category,
            ),
            make_row(
                event_id=wrong_org_event_id,
                timestamp="2026-09-09T08:20:00Z",
                organization_id=other_organization_id,
                event_name=target_event_name,
                category=category,
                source_line=2,
            ),
            make_row(
                event_id=wrong_event_name_event_id,
                timestamp="2026-09-09T08:30:00Z",
                organization_id=target_organization_id,
                event_name=other_event_name,
                category=category,
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
            organization_id=target_organization_id,
            event_name=target_event_name,
            category=category,
        ),
        limit=100,
    )

    assert [item.event_id for item in result] == [match_event_id]


def test_results_are_sorted_by_timestamp_descending(
    tmp_path: Path,
    make_parquet,
    faker: Faker,
    make_row: Callable[..., dict],
) -> None:
    old_event_id = faker.uuid4()
    new_event_id = faker.uuid4()

    make_parquet(
        partition="2026/09/09/08",
        file_name="events.parquet",
        rows=[
            make_row(
                event_id=old_event_id,
                timestamp="2026-09-09T08:10:00Z",
            ),
            make_row(
                event_id=new_event_id,
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
        new_event_id,
        old_event_id,
    ]


def test_limit_is_applied(
    tmp_path: Path,
    make_parquet,
    faker: Faker,
    make_row: Callable[..., dict],
) -> None:
    make_parquet(
        partition="2026/09/09/08",
        file_name="events.parquet",
        rows=[
            make_row(
                event_id=faker.uuid4(),
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
    faker: Faker,
    make_row: Callable[..., dict],
) -> None:
    parquet_path = make_parquet(
        partition="2026/09/09/08",
        file_name="events.parquet",
        rows=[
            make_row(
                event_id=faker.uuid4(),
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
