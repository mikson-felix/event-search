from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import pytest
from faker import Faker

from event_search.application.search_service import SearchService
from event_search.domain.models import (
    EventDetails,
    EventLocator,
    SearchFilters,
    SearchSummary,
    TimeRange,
)


@pytest.fixture
def make_summary(faker: Faker) -> Callable[..., SearchSummary]:
    def factory(
        *,
        event_id: str | None = None,
        user_id: str | None = None,
        organization_id: str | None = None,
        event_name: str | None = None,
        category: str | None = None,
    ) -> SearchSummary:
        return SearchSummary(
            event_id=event_id if event_id is not None else faker.uuid4(),
            user_id=user_id if user_id is not None else faker.uuid4(),
            organization_id=(organization_id if organization_id is not None else faker.uuid4()),
            event_name=event_name if event_name is not None else faker.word(),
            category=category if category is not None else faker.word(),
            timestamp=datetime(
                2026,
                9,
                9,
                8,
                30,
                tzinfo=UTC,
            ),
            locator=EventLocator(
                parquet_path=Path("/tmp/events.parquet"),
                source_line=1,
                blob_partition="2026/09/09/08",
                blob_name="events.ndjson",
            ),
        )

    return factory


class FakeQueryEngine:
    def __init__(self, results: list[SearchSummary]) -> None:
        self.results = results
        self.calls: list[dict] = []

    def search(
        self,
        *,
        time_range,
        partitions,
        filters,
        limit,
    ):
        self.calls.append(
            {
                "time_range": time_range,
                "partitions": partitions,
                "filters": filters,
                "limit": limit,
            }
        )

        return self.results


class FakeResultStore:
    def __init__(self) -> None:
        self.results: dict[str, SearchSummary] = {}
        self.find_ids_result: list[str] = []

    def replace(
        self,
        results: list[SearchSummary],
    ) -> None:
        self.results = {result.event_id: result for result in results}

    def get(
        self,
        event_id: str,
    ) -> SearchSummary | None:
        return self.results.get(event_id)

    def find_ids(
        self,
        *,
        prefix: str,
        limit: int = 20,
    ) -> list[str]:
        del prefix, limit

        return self.find_ids_result


class FakeDetailsReader:
    def __init__(
        self,
        details: EventDetails | None,
    ) -> None:
        self.details = details
        self.calls: list[tuple[str, EventLocator]] = []

    def read(
        self,
        *,
        event_id: str,
        locator: EventLocator,
    ) -> EventDetails | None:
        self.calls.append(
            (
                event_id,
                locator,
            )
        )

        return self.details


def make_time_range() -> TimeRange:
    utc_from = datetime(
        2026,
        9,
        9,
        8,
        0,
        tzinfo=UTC,
    )

    utc_to = datetime(
        2026,
        9,
        9,
        9,
        0,
        tzinfo=UTC,
    )

    return TimeRange(
        local_from=utc_from,
        local_to=utc_to,
        utc_from=utc_from,
        utc_to=utc_to,
    )


def test_search_queries_engine_and_replaces_latest_results(
    faker: Faker,
    make_summary: Callable[..., SearchSummary],
) -> None:
    summary = make_summary()

    engine = FakeQueryEngine([summary])

    store = FakeResultStore()

    reader = FakeDetailsReader(
        details=None,
    )

    service = SearchService(
        query_engine=engine,
        result_store=store,
        details_reader=reader,
    )

    filters = SearchFilters(
        event_name=faker.word(),
    )

    time_range = make_time_range()

    result = service.search(
        time_range=time_range,
        partitions=["2026/09/09/08"],
        filters=filters,
        limit=100,
    )

    assert result == [summary]

    assert store.get(summary.event_id) == summary

    assert len(engine.calls) == 1
    assert engine.calls[0]["limit"] == 100


def test_empty_search_clears_previous_results(
    make_summary: Callable[..., SearchSummary],
) -> None:
    summary = make_summary()

    store = FakeResultStore()
    store.replace([summary])

    service = SearchService(
        query_engine=FakeQueryEngine([]),
        result_store=store,
        details_reader=FakeDetailsReader(None),
    )

    service.search(
        time_range=make_time_range(),
        partitions=["2026/09/09/08"],
        filters=SearchFilters(),
        limit=100,
    )

    assert store.get(summary.event_id) is None


def test_get_from_last_search_reads_exact_locator(
    make_summary: Callable[..., SearchSummary],
) -> None:
    summary = make_summary()

    details = EventDetails(
        event_id=summary.event_id,
        blob_partition="2026/09/09/08",
        blob_name="events.ndjson",
        source_line=1,
        raw_json=f'{{"event_id":"{summary.event_id}"}}',
    )

    store = FakeResultStore()
    store.replace([summary])

    reader = FakeDetailsReader(
        details=details,
    )

    service = SearchService(
        query_engine=FakeQueryEngine([]),
        result_store=store,
        details_reader=reader,
    )

    result = service.get_from_last_search(summary.event_id)

    assert result == details

    assert reader.calls == [
        (
            summary.event_id,
            summary.locator,
        )
    ]


def test_get_from_last_search_does_not_scan_if_event_is_unknown(
    faker: Faker,
) -> None:
    reader = FakeDetailsReader(
        details=None,
    )

    service = SearchService(
        query_engine=FakeQueryEngine([]),
        result_store=FakeResultStore(),
        details_reader=reader,
    )

    result = service.get_from_last_search(faker.uuid4())

    assert result is None
    assert reader.calls == []


def test_complete_event_ids_delegates_to_store(
    faker: Faker,
) -> None:
    store = FakeResultStore()

    matching_ids = [
        faker.uuid4(),
        faker.uuid4(),
    ]

    store.find_ids_result = matching_ids

    service = SearchService(
        query_engine=FakeQueryEngine([]),
        result_store=store,
        details_reader=FakeDetailsReader(None),
    )

    result = service.complete_event_ids(
        prefix=faker.word(),
        limit=20,
    )

    assert result == matching_ids
