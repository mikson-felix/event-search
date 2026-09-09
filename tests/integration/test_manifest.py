from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb

from event_search.domain.models import (
    BlobObject,
    MaterializationResult,
)
from event_search.infrastructure.cache.manifest import (
    DuckDBManifestRepository,
)


def make_blob(
    *,
    partition: str = "2026/09/09/08",
    file_name: str = "events.ndjson",
) -> BlobObject:
    return BlobObject(
        name=f"{partition}/{file_name}",
        partition=partition,
        file_name=file_name,
    )


def make_result(
    *,
    path: Path,
    events_count: int = 10,
) -> MaterializationResult:
    return MaterializationResult(
        path=path,
        events_count=events_count,
    )


def test_get_returns_none_for_unknown_blob(
    tmp_path: Path,
) -> None:
    repository = DuckDBManifestRepository(
        database_path=tmp_path / "events.duckdb",
    )

    result = repository.get(
        "2026/09/09/08/missing.ndjson",
    )

    assert result is None


def test_save_and_get_manifest_entry(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "events.duckdb"

    parquet_path = tmp_path / "events.parquet"
    parquet_path.touch()

    blob = make_blob()

    repository = DuckDBManifestRepository(
        database_path=database_path,
    )

    before = datetime.now(UTC)

    repository.save(
        blob=blob,
        result=make_result(
            path=parquet_path,
            events_count=42,
        ),
    )

    after = datetime.now(UTC)

    entry = repository.get(
        blob.name,
    )

    assert entry is not None

    assert entry.blob_name == blob.name
    assert entry.parquet_path == parquet_path.resolve()
    assert entry.events_count == 42

    assert before <= entry.materialized_at <= after


def test_save_updates_existing_manifest_entry(
    tmp_path: Path,
) -> None:
    repository = DuckDBManifestRepository(
        database_path=tmp_path / "events.duckdb",
    )

    blob = make_blob()

    first_path = tmp_path / "first.parquet"
    second_path = tmp_path / "second.parquet"

    first_path.touch()
    second_path.touch()

    repository.save(
        blob=blob,
        result=make_result(
            path=first_path,
            events_count=10,
        ),
    )

    first_entry = repository.get(
        blob.name,
    )

    assert first_entry is not None

    repository.save(
        blob=blob,
        result=make_result(
            path=second_path,
            events_count=20,
        ),
    )

    second_entry = repository.get(
        blob.name,
    )

    assert second_entry is not None

    assert second_entry.blob_name == blob.name
    assert second_entry.parquet_path == second_path.resolve()
    assert second_entry.events_count == 20

    assert second_entry.materialized_at >= first_entry.materialized_at


def test_is_materialized_returns_false_without_manifest_entry(
    tmp_path: Path,
) -> None:
    repository = DuckDBManifestRepository(
        database_path=tmp_path / "events.duckdb",
    )

    blob = make_blob()

    assert (
        repository.is_materialized(
            blob,
        )
        is False
    )


def test_is_materialized_requires_existing_parquet_file(
    tmp_path: Path,
) -> None:
    repository = DuckDBManifestRepository(
        database_path=tmp_path / "events.duckdb",
    )

    blob = make_blob()

    parquet_path = tmp_path / "events.parquet"
    parquet_path.touch()

    repository.save(
        blob=blob,
        result=make_result(
            path=parquet_path,
        ),
    )

    assert (
        repository.is_materialized(
            blob,
        )
        is True
    )


def test_missing_parquet_is_not_materialized(
    tmp_path: Path,
) -> None:
    repository = DuckDBManifestRepository(
        database_path=tmp_path / "events.duckdb",
    )

    blob = make_blob()

    parquet_path = tmp_path / "events.parquet"

    repository.save(
        blob=blob,
        result=make_result(
            path=parquet_path,
        ),
    )

    assert (
        repository.get(
            blob.name,
        )
        is not None
    )

    assert (
        repository.is_materialized(
            blob,
        )
        is False
    )


def test_is_materialized_becomes_false_after_parquet_is_deleted(
    tmp_path: Path,
) -> None:
    repository = DuckDBManifestRepository(
        database_path=tmp_path / "events.duckdb",
    )

    blob = make_blob()

    parquet_path = tmp_path / "events.parquet"
    parquet_path.touch()

    repository.save(
        blob=blob,
        result=make_result(
            path=parquet_path,
        ),
    )

    assert (
        repository.is_materialized(
            blob,
        )
        is True
    )

    parquet_path.unlink()

    assert (
        repository.is_materialized(
            blob,
        )
        is False
    )


def test_status_counts_only_existing_parquet_files(
    tmp_path: Path,
) -> None:
    repository = DuckDBManifestRepository(
        database_path=tmp_path / "events.duckdb",
    )

    first_blob = make_blob(
        file_name="first.ndjson",
    )

    second_blob = make_blob(
        file_name="second.ndjson",
    )

    first_path = tmp_path / "first.parquet"
    second_path = tmp_path / "second.parquet"

    first_path.write_bytes(
        b"a" * 10,
    )

    second_path.write_bytes(
        b"b" * 20,
    )

    repository.save(
        blob=first_blob,
        result=make_result(
            path=first_path,
            events_count=5,
        ),
    )

    repository.save(
        blob=second_blob,
        result=make_result(
            path=second_path,
            events_count=7,
        ),
    )

    status = repository.get_status()

    assert status.blobs_count == 2
    assert status.events_count == 12
    assert status.parquet_size_bytes == 30

    assert status.last_materialized_at is not None


def test_status_ignores_missing_parquet_files(
    tmp_path: Path,
) -> None:
    repository = DuckDBManifestRepository(
        database_path=tmp_path / "events.duckdb",
    )

    existing_blob = make_blob(
        file_name="existing.ndjson",
    )

    missing_blob = make_blob(
        file_name="missing.ndjson",
    )

    existing_path = tmp_path / "existing.parquet"
    missing_path = tmp_path / "missing.parquet"

    existing_path.write_bytes(
        b"a" * 15,
    )

    repository.save(
        blob=existing_blob,
        result=make_result(
            path=existing_path,
            events_count=3,
        ),
    )

    repository.save(
        blob=missing_blob,
        result=make_result(
            path=missing_path,
            events_count=100,
        ),
    )

    status = repository.get_status()

    assert status.blobs_count == 1
    assert status.events_count == 3
    assert status.parquet_size_bytes == 15

    assert status.last_materialized_at is not None


def test_empty_status(
    tmp_path: Path,
) -> None:
    repository = DuckDBManifestRepository(
        database_path=tmp_path / "events.duckdb",
    )

    status = repository.get_status()

    assert status.blobs_count == 0
    assert status.events_count == 0
    assert status.parquet_size_bytes == 0
    assert status.last_materialized_at is None


def test_status_uses_latest_materialization_time(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "events.duckdb"

    repository = DuckDBManifestRepository(
        database_path=database_path,
    )

    first_blob = make_blob(
        file_name="first.ndjson",
    )

    second_blob = make_blob(
        file_name="second.ndjson",
    )

    first_path = tmp_path / "first.parquet"
    second_path = tmp_path / "second.parquet"

    first_path.touch()
    second_path.touch()

    repository.save(
        blob=first_blob,
        result=make_result(
            path=first_path,
        ),
    )

    repository.save(
        blob=second_blob,
        result=make_result(
            path=second_path,
        ),
    )

    first_time = datetime(
        2026,
        9,
        9,
        8,
        0,
        tzinfo=UTC,
    )

    second_time = first_time + timedelta(
        hours=1,
    )

    with duckdb.connect(
        str(database_path.resolve()),
    ) as connection:
        connection.execute(
            """
            UPDATE cached_blobs
            SET materialized_at = ?
            WHERE blob_name = ?
            """,
            [
                first_time,
                first_blob.name,
            ],
        )

        connection.execute(
            """
            UPDATE cached_blobs
            SET materialized_at = ?
            WHERE blob_name = ?
            """,
            [
                second_time,
                second_blob.name,
            ],
        )

    status = repository.get_status()

    assert status.last_materialized_at == second_time


def test_initialization_creates_expected_schema(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "events.duckdb"

    DuckDBManifestRepository(
        database_path=database_path,
    )

    with duckdb.connect(
        str(database_path.resolve()),
    ) as connection:
        rows = connection.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'cached_blobs'
            """
        ).fetchall()

    columns = {row[0] for row in rows}

    assert columns == {
        "blob_name",
        "parquet_path",
        "events_count",
        "materialized_at",
    }


def test_initialization_recreates_incompatible_schema(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "events.duckdb"

    with duckdb.connect(
        str(database_path),
    ) as connection:
        connection.execute(
            """
            CREATE TABLE cached_blobs (
                blob_name VARCHAR PRIMARY KEY,
                old_column VARCHAR
            )
            """
        )

        connection.execute(
            """
            INSERT INTO cached_blobs (
                blob_name,
                old_column
            )
            VALUES (?, ?)
            """,
            [
                "old.ndjson",
                "obsolete",
            ],
        )

    DuckDBManifestRepository(
        database_path=database_path,
    )

    with duckdb.connect(
        str(database_path.resolve()),
    ) as connection:
        columns = {
            row[0]
            for row in connection.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'cached_blobs'
                """
            ).fetchall()
        }

        rows_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM cached_blobs
            """
        ).fetchone()[0]

    assert columns == {
        "blob_name",
        "parquet_path",
        "events_count",
        "materialized_at",
    }

    assert rows_count == 0
