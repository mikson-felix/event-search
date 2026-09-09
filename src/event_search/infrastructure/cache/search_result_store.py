from pathlib import Path

import duckdb

from event_search.domain.models import (
    EventLocator,
    SearchSummary,
)

_EXPECTED_COLUMNS = {
    "event_id",
    "user_id",
    "organization_id",
    "event_name",
    "category",
    "timestamp",
    "parquet_path",
    "source_line",
    "blob_partition",
    "blob_name",
}


class DuckDBSearchResultStore:
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
                WHERE table_name = 'latest_search_results'
                """
            ).fetchone()[0]

            if exists:
                rows = connection.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'latest_search_results'
                    """
                ).fetchall()

                columns = {row[0] for row in rows}

                if columns != _EXPECTED_COLUMNS:
                    connection.execute(
                        """
                        DROP TABLE latest_search_results
                        """
                    )

            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS latest_search_results (
                    event_id VARCHAR PRIMARY KEY,
                    user_id VARCHAR,
                    organization_id VARCHAR,
                    event_name VARCHAR,
                    category VARCHAR,
                    timestamp TIMESTAMPTZ NOT NULL,
                    parquet_path VARCHAR NOT NULL,
                    source_line BIGINT NOT NULL,
                    blob_partition VARCHAR NOT NULL,
                    blob_name VARCHAR NOT NULL
                )
                """
            )

    def replace(
        self,
        results: list[SearchSummary],
    ) -> None:
        with self._connect() as connection:
            connection.begin()

            try:
                connection.execute(
                    """
                    DELETE FROM latest_search_results
                    """
                )

                if results:
                    connection.executemany(
                        """
                        INSERT INTO latest_search_results (
                            event_id,
                            user_id,
                            organization_id,
                            event_name,
                            category,
                            timestamp,
                            parquet_path,
                            source_line,
                            blob_partition,
                            blob_name
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            (
                                result.event_id,
                                result.user_id,
                                result.organization_id,
                                result.event_name,
                                result.category,
                                result.timestamp,
                                str(result.locator.parquet_path),
                                result.locator.source_line,
                                result.locator.blob_partition,
                                result.locator.blob_name,
                            )
                            for result in results
                        ],
                    )

                connection.commit()

            except Exception:
                connection.rollback()
                raise

    def get(
        self,
        event_id: str,
    ) -> SearchSummary | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    event_id,
                    user_id,
                    organization_id,
                    event_name,
                    category,
                    timestamp,
                    parquet_path,
                    source_line,
                    blob_partition,
                    blob_name
                FROM latest_search_results
                WHERE event_id = ?
                """,
                [event_id],
            ).fetchone()

        if row is None:
            return None

        return SearchSummary(
            event_id=row[0],
            user_id=row[1],
            organization_id=row[2],
            event_name=row[3],
            category=row[4],
            timestamp=row[5],
            locator=EventLocator(
                parquet_path=Path(row[6]),
                source_line=row[7],
                blob_partition=row[8],
                blob_name=row[9],
            ),
        )

    def find_ids(
        self,
        *,
        prefix: str,
        limit: int = 20,
    ) -> list[str]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT event_id
                FROM latest_search_results
                WHERE event_id LIKE ?
                ORDER BY event_id
                LIMIT ?
                """,
                [
                    f"{prefix}%",
                    limit,
                ],
            ).fetchall()

        return [row[0] for row in rows]

    def clear(
        self,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                DELETE FROM latest_search_results
                """
            )
