from pathlib import Path

from event_search.domain.models import EventLocator
from event_search.infrastructure.query.parquet_event_reader import (
    DuckDBParquetEventDetailsReader,
)


def test_reads_raw_json_from_exact_parquet_file(
    tmp_path: Path,
    make_parquet,
) -> None:
    raw_json = '{"event_id":"event-001","timestamp":"2026-09-09T08:30:00Z"}'

    parquet_path = make_parquet(
        partition="2026/09/09/08",
        file_name="events.parquet",
        rows=[
            {
                "event_id": "event-001",
                "user_id": "user-001",
                "organization_id": "org-001",
                "event_name": "LOGIN",
                "category": "AUTH",
                "timestamp": "2026-09-09T08:30:00Z",
                "blob_partition": "2026/09/09/08",
                "blob_name": "events.ndjson",
                "source_line": 42,
                "raw_json": raw_json,
            }
        ],
    )

    locator = EventLocator(
        parquet_path=parquet_path,
        source_line=42,
        blob_partition="2026/09/09/08",
        blob_name="events.ndjson",
    )

    reader = DuckDBParquetEventDetailsReader()

    result = reader.read(
        event_id="event-001",
        locator=locator,
    )

    assert result is not None
    assert result.event_id == "event-001"
    assert result.source_line == 42
    assert result.raw_json == raw_json


def test_wrong_event_id_does_not_return_event(
    tmp_path: Path,
    make_parquet,
) -> None:
    parquet_path = make_parquet(
        partition="2026/09/09/08",
        file_name="events.parquet",
        rows=[
            {
                "event_id": "event-001",
                "user_id": None,
                "organization_id": None,
                "event_name": "LOGIN",
                "category": "AUTH",
                "timestamp": "2026-09-09T08:30:00Z",
                "blob_partition": "2026/09/09/08",
                "blob_name": "events.ndjson",
                "source_line": 42,
                "raw_json": '{"event_id":"event-001"}',
            }
        ],
    )

    locator = EventLocator(
        parquet_path=parquet_path,
        source_line=42,
        blob_partition="2026/09/09/08",
        blob_name="events.ndjson",
    )

    reader = DuckDBParquetEventDetailsReader()

    assert (
        reader.read(
            event_id="wrong-event",
            locator=locator,
        )
        is None
    )


def test_wrong_source_line_does_not_return_event(
    tmp_path: Path,
    make_parquet,
) -> None:
    parquet_path = make_parquet(
        partition="2026/09/09/08",
        file_name="events.parquet",
        rows=[
            {
                "event_id": "event-001",
                "user_id": None,
                "organization_id": None,
                "event_name": "LOGIN",
                "category": "AUTH",
                "timestamp": "2026-09-09T08:30:00Z",
                "blob_partition": "2026/09/09/08",
                "blob_name": "events.ndjson",
                "source_line": 42,
                "raw_json": '{"event_id":"event-001"}',
            }
        ],
    )

    locator = EventLocator(
        parquet_path=parquet_path,
        source_line=99,
        blob_partition="2026/09/09/08",
        blob_name="events.ndjson",
    )

    reader = DuckDBParquetEventDetailsReader()

    assert (
        reader.read(
            event_id="event-001",
            locator=locator,
        )
        is None
    )
