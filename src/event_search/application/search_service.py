from event_search.domain.models import (
    EventDetails,
    SearchFilters,
    SearchSummary,
    TimeRange,
)
from event_search.domain.ports import (
    EventDetailsReader,
    QueryEngine,
    SearchResultStore,
)


class SearchService:
    def __init__(
        self,
        *,
        query_engine: QueryEngine,
        result_store: SearchResultStore,
        details_reader: EventDetailsReader,
    ) -> None:
        self._query_engine = query_engine
        self._result_store = result_store
        self._details_reader = details_reader

    def search(
        self,
        *,
        filters: SearchFilters,
        time_range: TimeRange,
        partitions: list[str],
        limit: int,
    ) -> list[SearchSummary]:
        results = self._query_engine.search(
            filters=filters,
            time_range=time_range,
            partitions=partitions,
            limit=limit,
        )

        self._result_store.replace(results)

        return results

    def get_from_last_search(
        self,
        event_id: str,
    ) -> EventDetails | None:
        summary = self._result_store.get(event_id)

        if summary is None:
            return None

        return self._details_reader.read(
            event_id=summary.event_id,
            locator=summary.locator,
        )

    def complete_event_ids(
        self,
        *,
        prefix: str,
        limit: int = 20,
    ) -> list[str]:
        return self._result_store.find_ids(
            prefix=prefix,
            limit=limit,
        )
