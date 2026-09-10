from pathlib import Path
from typing import Protocol

from .models import (
    BlobObject,
    CacheStatus,
    EventDetails,
    EventLocator,
    ManifestEntry,
    MaterializationResult,
    SearchFilters,
    SearchSummary,
    TimeRange,
)


class BlobSource(Protocol):
    def list_blobs(
        self,
        partition: str,
    ) -> list[BlobObject]: ...

    def download(
        self,
        blob: BlobObject,
        target: Path,
    ) -> None: ...


class ManifestRepository(Protocol):
    def get(
        self,
        blob_name: str,
    ) -> ManifestEntry | None: ...

    def is_materialized(
        self,
        blob: BlobObject,
    ) -> bool: ...

    def save(
        self,
        *,
        blob: BlobObject,
        result: MaterializationResult,
    ) -> None: ...

    def get_status(
        self,
    ) -> CacheStatus: ...


class Materializer(Protocol):
    def materialize(
        self,
        *,
        source_file: Path,
        blob: BlobObject,
    ) -> MaterializationResult: ...


class QueryEngine(Protocol):
    def search(
        self,
        *,
        filters: SearchFilters,
        time_range: TimeRange,
        partitions: list[str],
        limit: int,
    ) -> list[SearchSummary]: ...


class SearchResultStore(Protocol):
    def replace(
        self,
        results: list[SearchSummary],
    ) -> None: ...

    def get(
        self,
        event_id: str,
    ) -> SearchSummary | None: ...

    def find_ids(
        self,
        *,
        prefix: str,
        limit: int = 20,
    ) -> list[str]: ...

    def clear(
        self,
    ) -> None: ...


class EventDetailsReader(Protocol):
    def read(
        self,
        *,
        event_id: str,
        locator: EventLocator,
    ) -> EventDetails | None: ...
