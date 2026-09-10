from datetime import datetime
from pathlib import Path

from event_search.domain.models import (
    BlobObject,
    MaterializationResult,
)
from event_search.infrastructure.cache.sqlite_manifest import (
    SQLiteManifestRepository,
)


def make_blob() -> BlobObject:
    return BlobObject(
        name=("activity-logs/year=2026/month=09/day=10/hour=08/events.ndjson"),
        partition="2026/09/10/08",
        file_name="events.ndjson",
    )


def test_get_returns_none_for_unknown_blob(
    tmp_path: Path,
) -> None:
    repository = SQLiteManifestRepository(tmp_path / "event_search.sqlite")

    result = repository.get("unknown.ndjson")

    assert result is None


def test_save_and_get_manifest_entry(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "event_search.sqlite"

    parquet_path = tmp_path / "parquet" / "2026" / "09" / "10" / "08" / "events.parquet"

    parquet_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    parquet_path.write_bytes(b"parquet")

    repository = SQLiteManifestRepository(database_path)

    blob = make_blob()

    result = MaterializationResult(
        path=parquet_path,
        events_count=42,
    )

    repository.save(
        blob=blob,
        result=result,
    )

    entry = repository.get(blob.name)

    assert entry is not None
    assert entry.blob_name == blob.name
    assert entry.parquet_path == parquet_path.resolve()
    assert entry.events_count == 42

    assert isinstance(
        entry.materialized_at,
        datetime,
    )

    assert entry.materialized_at.tzinfo is not None


def test_is_materialized_returns_false_when_entry_missing(
    tmp_path: Path,
) -> None:
    repository = SQLiteManifestRepository(tmp_path / "event_search.sqlite")

    assert repository.is_materialized(make_blob()) is False


def test_is_materialized_returns_true_when_parquet_exists(
    tmp_path: Path,
) -> None:
    repository = SQLiteManifestRepository(tmp_path / "event_search.sqlite")

    parquet_path = tmp_path / "events.parquet"
    parquet_path.write_bytes(b"data")

    blob = make_blob()

    repository.save(
        blob=blob,
        result=MaterializationResult(
            path=parquet_path,
            events_count=10,
        ),
    )

    assert repository.is_materialized(blob) is True


def test_is_materialized_returns_false_when_parquet_was_deleted(
    tmp_path: Path,
) -> None:
    repository = SQLiteManifestRepository(tmp_path / "event_search.sqlite")

    parquet_path = tmp_path / "events.parquet"
    parquet_path.write_bytes(b"data")

    blob = make_blob()

    repository.save(
        blob=blob,
        result=MaterializationResult(
            path=parquet_path,
            events_count=10,
        ),
    )

    parquet_path.unlink()

    assert repository.is_materialized(blob) is False


def test_save_updates_existing_blob(
    tmp_path: Path,
) -> None:
    repository = SQLiteManifestRepository(tmp_path / "event_search.sqlite")

    blob = make_blob()

    first_path = tmp_path / "first.parquet"
    first_path.write_bytes(b"first")

    repository.save(
        blob=blob,
        result=MaterializationResult(
            path=first_path,
            events_count=10,
        ),
    )

    second_path = tmp_path / "second.parquet"
    second_path.write_bytes(b"second")

    repository.save(
        blob=blob,
        result=MaterializationResult(
            path=second_path,
            events_count=25,
        ),
    )

    entry = repository.get(blob.name)

    assert entry is not None
    assert entry.parquet_path == second_path.resolve()
    assert entry.events_count == 25


def test_get_status_returns_empty_status(
    tmp_path: Path,
) -> None:
    repository = SQLiteManifestRepository(tmp_path / "event_search.sqlite")

    status = repository.get_status()

    assert status.blobs_count == 0
    assert status.events_count == 0
    assert status.parquet_size_bytes == 0
    assert status.last_materialized_at is None


def test_get_status_aggregates_existing_parquet_files(
    tmp_path: Path,
) -> None:
    repository = SQLiteManifestRepository(tmp_path / "event_search.sqlite")

    first_blob = make_blob()

    second_blob = BlobObject(
        name=("activity-logs/year=2026/month=09/day=10/hour=08/second.ndjson"),
        partition="2026/09/10/08",
        file_name="second.ndjson",
    )

    first_path = tmp_path / "first.parquet"
    second_path = tmp_path / "second.parquet"

    first_content = b"first"
    second_content = b"second-file"

    first_path.write_bytes(first_content)
    second_path.write_bytes(second_content)

    repository.save(
        blob=first_blob,
        result=MaterializationResult(
            path=first_path,
            events_count=10,
        ),
    )

    repository.save(
        blob=second_blob,
        result=MaterializationResult(
            path=second_path,
            events_count=20,
        ),
    )

    status = repository.get_status()

    assert status.blobs_count == 2
    assert status.events_count == 30

    assert status.parquet_size_bytes == (len(first_content) + len(second_content))

    assert status.last_materialized_at is not None
    assert status.last_materialized_at.tzinfo is not None


def test_get_status_ignores_missing_parquet_files(
    tmp_path: Path,
) -> None:
    repository = SQLiteManifestRepository(tmp_path / "event_search.sqlite")

    blob = make_blob()

    missing_path = tmp_path / "missing.parquet"

    repository.save(
        blob=blob,
        result=MaterializationResult(
            path=missing_path,
            events_count=100,
        ),
    )

    status = repository.get_status()

    assert status.blobs_count == 0
    assert status.events_count == 0
    assert status.parquet_size_bytes == 0
    assert status.last_materialized_at is None
