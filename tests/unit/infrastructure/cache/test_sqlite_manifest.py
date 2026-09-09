from pathlib import Path

import pytest

from event_search.domain.models import (
    BlobObject,
    MaterializationResult,
)
from event_search.infrastructure.cache.sqlite_manifest import (
    SQLiteManifestRepository,
)

PARTITION = "2026/09/10/08"


def make_blob(
    file_name: str = "events.ndjson",
    *,
    partition: str = PARTITION,
) -> BlobObject:
    return BlobObject(
        name=(f"activity-logs/{partition}/{file_name}"),
        partition=partition,
        file_name=file_name,
    )


def make_result(
    *,
    path: Path,
    blobs: tuple[BlobObject, ...],
    events_count: int = 10,
) -> MaterializationResult:
    return MaterializationResult(
        path=path,
        events_count=events_count,
        blobs=blobs,
    )


def test_get_returns_none_for_unknown_blob(
    tmp_path: Path,
) -> None:
    repository = SQLiteManifestRepository(tmp_path / "event_search.sqlite")

    assert repository.get("missing") is None


def test_save_and_get_manifest_entry(
    tmp_path: Path,
) -> None:
    repository = SQLiteManifestRepository(tmp_path / "event_search.sqlite")

    blob = make_blob()

    parquet_path = tmp_path / "part.parquet"
    parquet_path.write_bytes(b"data")

    repository.save(
        make_result(
            path=parquet_path,
            blobs=(blob,),
            events_count=42,
        )
    )

    entry = repository.get(blob.name)

    assert entry is not None
    assert entry.blob_name == blob.name
    assert entry.parquet_path == parquet_path.resolve()
    assert entry.materialized_at.tzinfo is not None


def test_multiple_blobs_reference_same_parquet(
    tmp_path: Path,
) -> None:
    repository = SQLiteManifestRepository(tmp_path / "event_search.sqlite")

    first_blob = make_blob("first.ndjson")
    second_blob = make_blob("second.ndjson")

    parquet_path = tmp_path / "part.parquet"
    parquet_path.write_bytes(b"data")

    repository.save(
        make_result(
            path=parquet_path,
            blobs=(
                first_blob,
                second_blob,
            ),
            events_count=20,
        )
    )

    first = repository.get(first_blob.name)
    second = repository.get(second_blob.name)

    assert first is not None
    assert second is not None

    assert first.parquet_path == parquet_path.resolve()

    assert second.parquet_path == parquet_path.resolve()


def test_is_materialized_returns_false_when_entry_missing(
    tmp_path: Path,
) -> None:
    repository = SQLiteManifestRepository(tmp_path / "event_search.sqlite")

    assert repository.is_materialized(make_blob()) is False


def test_is_materialized_returns_true_when_parquet_exists(
    tmp_path: Path,
) -> None:
    repository = SQLiteManifestRepository(tmp_path / "event_search.sqlite")

    blob = make_blob()

    parquet_path = tmp_path / "part.parquet"
    parquet_path.write_bytes(b"data")

    repository.save(
        make_result(
            path=parquet_path,
            blobs=(blob,),
        )
    )

    assert repository.is_materialized(blob) is True


def test_is_materialized_returns_false_when_parquet_deleted(
    tmp_path: Path,
) -> None:
    repository = SQLiteManifestRepository(tmp_path / "event_search.sqlite")

    blob = make_blob()

    parquet_path = tmp_path / "part.parquet"
    parquet_path.write_bytes(b"data")

    repository.save(
        make_result(
            path=parquet_path,
            blobs=(blob,),
        )
    )

    parquet_path.unlink()

    assert repository.is_materialized(blob) is False


def test_save_updates_existing_blob_to_new_parquet(
    tmp_path: Path,
) -> None:
    repository = SQLiteManifestRepository(tmp_path / "event_search.sqlite")

    blob = make_blob()

    first_path = tmp_path / "first.parquet"
    first_path.write_bytes(b"first")

    repository.save(
        make_result(
            path=first_path,
            blobs=(blob,),
        )
    )

    second_path = tmp_path / "second.parquet"
    second_path.write_bytes(b"second")

    repository.save(
        make_result(
            path=second_path,
            blobs=(blob,),
            events_count=20,
        )
    )

    entry = repository.get(blob.name)

    assert entry is not None
    assert entry.parquet_path == second_path.resolve()


def test_get_status_returns_empty_status(
    tmp_path: Path,
) -> None:
    repository = SQLiteManifestRepository(tmp_path / "event_search.sqlite")

    status = repository.get_status()

    assert status.blobs_count == 0
    assert status.events_count == 0
    assert status.parquet_size_bytes == 0
    assert status.last_materialized_at is None


def test_get_status_counts_shared_parquet_only_once(
    tmp_path: Path,
) -> None:
    repository = SQLiteManifestRepository(tmp_path / "event_search.sqlite")

    first_blob = make_blob("first.ndjson")
    second_blob = make_blob("second.ndjson")

    content = b"parquet-content"

    parquet_path = tmp_path / "part.parquet"
    parquet_path.write_bytes(content)

    repository.save(
        make_result(
            path=parquet_path,
            blobs=(
                first_blob,
                second_blob,
            ),
            events_count=30,
        )
    )

    status = repository.get_status()

    assert status.blobs_count == 2
    assert status.events_count == 30

    assert status.parquet_size_bytes == len(content)

    assert status.last_materialized_at is not None


def test_get_status_aggregates_multiple_parquet_files(
    tmp_path: Path,
) -> None:
    repository = SQLiteManifestRepository(tmp_path / "event_search.sqlite")

    first_blob = make_blob("first.ndjson")
    second_blob = make_blob("second.ndjson")

    first_path = tmp_path / "first.parquet"
    second_path = tmp_path / "second.parquet"

    first_content = b"first"
    second_content = b"second-data"

    first_path.write_bytes(first_content)
    second_path.write_bytes(second_content)

    repository.save(
        make_result(
            path=first_path,
            blobs=(first_blob,),
            events_count=10,
        )
    )

    repository.save(
        make_result(
            path=second_path,
            blobs=(second_blob,),
            events_count=20,
        )
    )

    status = repository.get_status()

    assert status.blobs_count == 2
    assert status.events_count == 30

    assert status.parquet_size_bytes == (len(first_content) + len(second_content))


def test_get_status_ignores_missing_parquet_files(
    tmp_path: Path,
) -> None:
    repository = SQLiteManifestRepository(tmp_path / "event_search.sqlite")

    blob = make_blob()

    repository.save(
        make_result(
            path=(tmp_path / "missing.parquet"),
            blobs=(blob,),
            events_count=100,
        )
    )

    status = repository.get_status()

    assert status.blobs_count == 0
    assert status.events_count == 0
    assert status.parquet_size_bytes == 0
    assert status.last_materialized_at is None


def test_save_rejects_result_without_blobs(
    tmp_path: Path,
) -> None:
    repository = SQLiteManifestRepository(tmp_path / "event_search.sqlite")

    with pytest.raises(
        ValueError,
        match="at least one blob",
    ):
        repository.save(
            make_result(
                path=(tmp_path / "part.parquet"),
                blobs=(),
            )
        )


def test_save_rejects_blobs_from_multiple_partitions(
    tmp_path: Path,
) -> None:
    repository = SQLiteManifestRepository(tmp_path / "event_search.sqlite")

    first_blob = make_blob(
        "first.ndjson",
        partition="2026/09/10/08",
    )

    second_blob = make_blob(
        "second.ndjson",
        partition="2026/09/10/09",
    )

    with pytest.raises(
        ValueError,
        match="one partition",
    ):
        repository.save(
            make_result(
                path=(tmp_path / "part.parquet"),
                blobs=(
                    first_blob,
                    second_blob,
                ),
            )
        )
