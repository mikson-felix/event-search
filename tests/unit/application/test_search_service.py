from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from event_search.application.search_service import SearchService
from event_search.domain.models import (
    EventDetails,
    EventLocator,
    SearchFilters,
    SearchSummary,
    TimeRange,
)


def make_summary(
    event_id: str = "event-001",
) -> SearchSummary:
    return SearchSummary(
        event_id=event_id,
        user_id="user-001",
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
            parquet_path=Path("/tmp/events.parquet"),
            source_line=1,
            blob_partition="2026/09/09/08",
            blob_name="events.ndjson",
        ),
    )


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


def test_search_queries_engine_and_replaces_latest_results() -> None:
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
        event_name="LOGIN",
    )

    time_range = make_time_range()

    result = service.search(
        time_range=time_range,
        partitions=["2026/09/09/08"],
        filters=filters,
        limit=100,
    )

    assert result == [summary]

    assert store.get("event-001") == summary

    assert len(engine.calls) == 1
    assert engine.calls[0]["limit"] == 100


def test_empty_search_clears_previous_results() -> None:
    store = FakeResultStore()
    store.replace([make_summary()])

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

    assert store.get("event-001") is None


def test_get_from_last_search_reads_exact_locator() -> None:
    summary = make_summary()

    details = EventDetails(
        event_id="event-001",
        blob_partition="2026/09/09/08",
        blob_name="events.ndjson",
        source_line=1,
        raw_json='{"event_id":"event-001"}',
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

    result = service.get_from_last_search("event-001")

    assert result == details

    assert reader.calls == [
        (
            "event-001",
            summary.locator,
        )
    ]


def test_get_from_last_search_does_not_scan_if_event_is_unknown() -> None:
    reader = FakeDetailsReader(
        details=None,
    )

    service = SearchService(
        query_engine=FakeQueryEngine([]),
        result_store=FakeResultStore(),
        details_reader=reader,
    )

    result = service.get_from_last_search("unknown")

    assert result is None
    assert reader.calls == []


def test_complete_event_ids_delegates_to_store() -> None:
    store = FakeResultStore()

    store.find_ids_result = [
        "abc-001",
        "abc-002",
    ]

    service = SearchService(
        query_engine=FakeQueryEngine([]),
        result_store=store,
        details_reader=FakeDetailsReader(None),
    )

    result = service.complete_event_ids(
        prefix="abc",
        limit=20,
    )

    assert result == [
        "abc-001",
        "abc-002",
    ]
