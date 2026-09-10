from datetime import datetime
from pathlib import Path

from event_search.domain.models import (
    EventLocator,
    SearchSummary,
)

from .sqlite import SQLiteConnectionFactory


class SQLiteSearchResultStore:
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
                CREATE TABLE IF NOT EXISTS latest_search_results (
                    event_id TEXT PRIMARY KEY,

                    user_id TEXT,
                    organization_id TEXT,

                    event_name TEXT,
                    category TEXT,

                    timestamp TEXT NOT NULL,

                    parquet_path TEXT NOT NULL,
                    source_line INTEGER NOT NULL,

                    blob_partition TEXT NOT NULL,
                    blob_name TEXT NOT NULL
                )
                """
            )

    def replace(
        self,
        results: list[SearchSummary],
    ) -> None:
        with self._connection_factory.connect() as connection:
            connection.execute(
                """
                DELETE FROM latest_search_results
                """
            )

            if not results:
                return

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
                        result.timestamp.isoformat(),
                        str(result.locator.parquet_path.resolve()),
                        result.locator.source_line,
                        result.locator.blob_partition,
                        result.locator.blob_name,
                    )
                    for result in results
                ],
            )

    def get(
        self,
        event_id: str,
    ) -> SearchSummary | None:
        with self._connection_factory.connect() as connection:
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
                (event_id,),
            ).fetchone()

        if row is None:
            return None

        return self._to_summary(row)

    def find_ids(
        self,
        *,
        prefix: str,
        limit: int = 20,
    ) -> list[str]:
        with self._connection_factory.connect() as connection:
            rows = connection.execute(
                """
                SELECT event_id
                FROM latest_search_results
                WHERE event_id LIKE ?
                ORDER BY event_id
                LIMIT ?
                """,
                (
                    f"{prefix}%",
                    limit,
                ),
            ).fetchall()

        return [row[0] for row in rows]

    def clear(
        self,
    ) -> None:
        with self._connection_factory.connect() as connection:
            connection.execute(
                """
                DELETE FROM latest_search_results
                """
            )

    @staticmethod
    def _to_summary(
        row: tuple,
    ) -> SearchSummary:
        return SearchSummary(
            event_id=row[0],
            user_id=row[1],
            organization_id=row[2],
            event_name=row[3],
            category=row[4],
            timestamp=datetime.fromisoformat(row[5]),
            locator=EventLocator(
                parquet_path=Path(row[6]),
                source_line=row[7],
                blob_partition=row[8],
                blob_name=row[9],
            ),
        )
