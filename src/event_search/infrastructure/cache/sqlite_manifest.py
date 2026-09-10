from datetime import UTC, datetime
from pathlib import Path

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
                CREATE TABLE IF NOT EXISTS cached_blobs (
                    blob_name TEXT PRIMARY KEY,
                    parquet_path TEXT NOT NULL,
                    events_count INTEGER NOT NULL,
                    materialized_at TEXT NOT NULL
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
                    blob_name,
                    parquet_path,
                    events_count,
                    materialized_at
                FROM cached_blobs
                WHERE blob_name = ?
                """,
                (blob_name,),
            ).fetchone()

        if row is None:
            return None

        return ManifestEntry(
            blob_name=row[0],
            parquet_path=Path(row[1]),
            events_count=row[2],
            materialized_at=datetime.fromisoformat(row[3]),
        )

    def is_materialized(
        self,
        blob: BlobObject,
    ) -> bool:
        entry = self.get(blob.name)

        if entry is None:
            return False

        return entry.parquet_path.exists()

    def save(
        self,
        *,
        blob: BlobObject,
        result: MaterializationResult,
    ) -> None:
        materialized_at = datetime.now(UTC)

        with self._connection_factory.connect() as connection:
            connection.execute(
                """
                INSERT INTO cached_blobs (
                    blob_name,
                    parquet_path,
                    events_count,
                    materialized_at
                )
                VALUES (?, ?, ?, ?)

                ON CONFLICT(blob_name)
                DO UPDATE SET
                    parquet_path = excluded.parquet_path,
                    events_count = excluded.events_count,
                    materialized_at = excluded.materialized_at
                """,
                (
                    blob.name,
                    str(result.path.resolve()),
                    result.events_count,
                    materialized_at.isoformat(),
                ),
            )

    def get_status(
        self,
    ) -> CacheStatus:
        with self._connection_factory.connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    parquet_path,
                    events_count,
                    materialized_at
                FROM cached_blobs
                """
            ).fetchall()

        existing_rows = [row for row in rows if Path(row[0]).exists()]

        parquet_size_bytes = sum(Path(row[0]).stat().st_size for row in existing_rows)

        events_count = sum(row[1] for row in existing_rows)

        last_materialized_at = max(
            (datetime.fromisoformat(row[2]) for row in existing_rows),
            default=None,
        )

        return CacheStatus(
            blobs_count=len(existing_rows),
            events_count=events_count,
            parquet_size_bytes=parquet_size_bytes,
            last_materialized_at=(last_materialized_at),
        )
