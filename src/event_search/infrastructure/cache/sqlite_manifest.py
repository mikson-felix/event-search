from datetime import UTC, datetime
from pathlib import Path

from loguru import logger

from event_search.domain.models import (
    BlobObject,
    CacheStatus,
    ManifestEntry,
    MaterializationResult,
)

from .sqlite import SQLiteConnectionFactory


class SQLiteManifestRepository:
    def __init__(
        self,
        database_path: Path,
    ) -> None:
        self._connection_factory = SQLiteConnectionFactory(database_path)

        self._initialize()

    def _initialize(
        self,
    ) -> None:
        with self._connection_factory.connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS parquet_files (
                    parquet_path TEXT PRIMARY KEY,
                    partition TEXT NOT NULL,
                    events_count INTEGER NOT NULL,
                    materialized_at TEXT NOT NULL
                )
                """
            )

            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS processed_blobs (
                    blob_name TEXT PRIMARY KEY,
                    parquet_path TEXT NOT NULL,
                    materialized_at TEXT NOT NULL,

                    FOREIGN KEY (
                        parquet_path
                    )
                    REFERENCES parquet_files (
                        parquet_path
                    )
                    ON DELETE CASCADE
                )
                """
            )

            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS
                    idx_parquet_files_partition
                ON parquet_files (
                    partition
                )
                """
            )

    def get(
        self,
        blob_name: str,
    ) -> ManifestEntry | None:
        with self._connection_factory.connect() as connection:
            row = connection.execute(
                """
                SELECT
                    processed_blobs.blob_name,
                    processed_blobs.parquet_path,
                    processed_blobs.materialized_at
                FROM processed_blobs
                WHERE processed_blobs.blob_name = ?
                """,
                (blob_name,),
            ).fetchone()

        if row is None:
            return None

        return ManifestEntry(
            blob_name=row[0],
            parquet_path=Path(row[1]),
            materialized_at=(datetime.fromisoformat(row[2])),
        )

    def is_materialized(
        self,
        blob: BlobObject,
    ) -> bool:
        entry = self.get(blob.name)

        if entry is None:
            logger.debug(
                "Cache miss: blob={} reason=manifest_entry_missing",
                blob.name,
            )
            return False

        if not entry.parquet_path.exists():
            logger.debug(
                "Cache miss: blob={} reason=parquet_missing path={}",
                blob.name,
                entry.parquet_path,
            )
            return False

        logger.debug(
            "Cache hit: blob={} parquet={}",
            blob.name,
            entry.parquet_path,
        )
        return True

    def save(
        self,
        result: MaterializationResult,
    ) -> None:
        if not result.blobs:
            raise ValueError("Materialization result must contain at least one blob")

        partitions = {blob.partition for blob in result.blobs}

        if len(partitions) != 1:
            raise ValueError("All blobs in one parquet must belong to one partition")

        partition = next(iter(partitions))

        materialized_at = datetime.now(UTC).isoformat()

        parquet_path = str(result.path.resolve())

        with self._connection_factory.connect() as connection:
            connection.execute(
                """
                INSERT INTO parquet_files (
                    parquet_path,
                    partition,
                    events_count,
                    materialized_at
                )
                VALUES (?, ?, ?, ?)

                ON CONFLICT(parquet_path)
                DO UPDATE SET
                    partition =
                        excluded.partition,
                    events_count =
                        excluded.events_count,
                    materialized_at =
                        excluded.materialized_at
                """,
                (
                    parquet_path,
                    partition,
                    result.events_count,
                    materialized_at,
                ),
            )

            connection.executemany(
                """
                INSERT INTO processed_blobs (
                    blob_name,
                    parquet_path,
                    materialized_at
                )
                VALUES (?, ?, ?)

                ON CONFLICT(blob_name)
                DO UPDATE SET
                    parquet_path =
                        excluded.parquet_path,
                    materialized_at =
                        excluded.materialized_at
                """,
                [
                    (
                        blob.name,
                        parquet_path,
                        materialized_at,
                    )
                    for blob in result.blobs
                ],
            )
        logger.debug(
            "Manifest saved: parquet={}, blobs={}, events={}",
            result.path,
            len(result.blobs),
            result.events_count,
        )

    def get_status(
        self,
    ) -> CacheStatus:
        with self._connection_factory.connect() as connection:
            blob_rows = connection.execute(
                """
                SELECT
                    blob_name,
                    parquet_path
                FROM processed_blobs
                """
            ).fetchall()

            parquet_rows = connection.execute(
                """
                SELECT
                    parquet_path,
                    events_count,
                    materialized_at
                FROM parquet_files
                """
            ).fetchall()

        existing_parquet_rows = [row for row in parquet_rows if Path(row[0]).exists()]

        existing_paths = {str(Path(row[0]).resolve()) for row in existing_parquet_rows}

        blobs_count = sum(1 for _, parquet_path in blob_rows if str(Path(parquet_path).resolve()) in existing_paths)

        events_count = sum(row[1] for row in existing_parquet_rows)

        parquet_size_bytes = sum(Path(row[0]).stat().st_size for row in existing_parquet_rows)

        last_materialized_at = max(
            (datetime.fromisoformat(row[2]) for row in existing_parquet_rows),
            default=None,
        )

        return CacheStatus(
            blobs_count=blobs_count,
            events_count=events_count,
            parquet_size_bytes=parquet_size_bytes,
            last_materialized_at=(last_materialized_at),
        )
