from datetime import UTC, datetime
from pathlib import Path

import duckdb

from event_search.domain.models import (
    BlobObject,
    CacheStatus,
    ManifestEntry,
    MaterializationResult,
)

_EXPECTED_COLUMNS = {
    "blob_name",
    "parquet_path",
    "events_count",
    "materialized_at",
}


class DuckDBManifestRepository:
    def __init__(
        self,
        database_path: Path,
    ) -> None:
        self._database_path = database_path.resolve()

        self._database_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self._initialize()

    def _connect(
        self,
    ) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self._database_path))

    def _initialize(
        self,
    ) -> None:
        with self._connect() as connection:
            exists = connection.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_name = 'cached_blobs'
                """
            ).fetchone()[0]

            if exists:
                rows = connection.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'cached_blobs'
                    """
                ).fetchall()

                columns = {row[0] for row in rows}

                if columns != _EXPECTED_COLUMNS:
                    connection.execute(
                        """
                        DROP TABLE cached_blobs
                        """
                    )

            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS cached_blobs (
                    blob_name VARCHAR PRIMARY KEY,
                    parquet_path VARCHAR NOT NULL,
                    events_count BIGINT NOT NULL,
                    materialized_at TIMESTAMPTZ NOT NULL
                )
                """
            )

    def get(
        self,
        blob_name: str,
    ) -> ManifestEntry | None:
        with self._connect() as connection:
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
                [blob_name],
            ).fetchone()

        if row is None:
            return None

        return ManifestEntry(
            blob_name=row[0],
            parquet_path=Path(row[1]),
            events_count=row[2],
            materialized_at=row[3],
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

        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO cached_blobs (
                    blob_name,
                    parquet_path,
                    events_count,
                    materialized_at
                )
                VALUES (?, ?, ?, ?)

                ON CONFLICT (blob_name)
                DO UPDATE SET
                    parquet_path = EXCLUDED.parquet_path,
                    events_count = EXCLUDED.events_count,
                    materialized_at = EXCLUDED.materialized_at
                """,
                [
                    blob.name,
                    str(result.path.resolve()),
                    result.events_count,
                    materialized_at,
                ],
            )

    def get_status(
        self,
    ) -> CacheStatus:
        with self._connect() as connection:
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
            (row[2] for row in existing_rows),
            default=None,
        )

        return CacheStatus(
            blobs_count=len(existing_rows),
            events_count=events_count,
            parquet_size_bytes=parquet_size_bytes,
            last_materialized_at=last_materialized_at,
        )
