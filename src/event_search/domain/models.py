from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class TimeRange:
    local_from: datetime
    local_to: datetime

    utc_from: datetime
    utc_to: datetime


@dataclass(frozen=True)
class BlobObject:
    name: str
    partition: str
    file_name: str


@dataclass(frozen=True)
class ManifestEntry:
    blob_name: str
    parquet_path: Path
    events_count: int
    materialized_at: datetime


@dataclass(frozen=True)
class MaterializationResult:
    path: Path
    events_count: int


@dataclass(frozen=True)
class SearchFilters:
    event_id: str | None = None
    user_id: str | None = None
    organization_id: str | None = None
    event_name: str | None = None
    category: str | None = None


@dataclass(frozen=True)
class EventLocator:
    parquet_path: Path
    source_line: int

    blob_partition: str
    blob_name: str


@dataclass(frozen=True)
class SearchSummary:
    event_id: str

    user_id: str | None
    organization_id: str | None

    event_name: str | None
    category: str | None

    timestamp: datetime

    locator: EventLocator


@dataclass(frozen=True)
class EventDetails:
    event_id: str

    blob_partition: str
    blob_name: str
    source_line: int

    raw_json: str


@dataclass(frozen=True)
class SyncResult:
    discovered: int
    materialized: int
    skipped: int


@dataclass(frozen=True)
class CacheStatus:
    blobs_count: int
    events_count: int
    parquet_size_bytes: int

    last_materialized_at: datetime | None
