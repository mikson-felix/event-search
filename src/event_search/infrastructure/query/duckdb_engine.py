from pathlib import Path

import duckdb

from event_search.domain.models import (
    EventLocator,
    SearchFilters,
    SearchSummary,
    TimeRange,
)


class DuckDBQueryEngine:
    def __init__(
        self,
        parquet_root: Path,
    ) -> None:
        self._parquet_root = parquet_root.resolve()

    def search(
        self,
        *,
        filters: SearchFilters,
        time_range: TimeRange,
        partitions: list[str],
        limit: int,
    ) -> list[SearchSummary]:
        parquet_files = self._resolve_partition_files(partitions)

        if not parquet_files:
            return []

        with duckdb.connect() as connection:
            self._create_events_view(
                connection,
                parquet_files,
            )

            conditions = [
                "event_id IS NOT NULL",
                "timestamp >= ?",
                "timestamp < ?",
            ]

            params: list[object] = [
                time_range.utc_from,
                time_range.utc_to,
            ]

            if filters.event_id is not None:
                conditions.append("event_id = ?")
                params.append(filters.event_id)

            if filters.user_id is not None:
                conditions.append("user_id = ?")
                params.append(filters.user_id)

            if filters.organization_id is not None:
                conditions.append("organization_id = ?")
                params.append(filters.organization_id)

            if filters.event_name is not None:
                conditions.append("event_name = ?")
                params.append(filters.event_name)

            if filters.category is not None:
                conditions.append("category = ?")
                params.append(filters.category)

            params.append(limit)

            query = f"""
                SELECT
                    event_id,
                    user_id,
                    organization_id,
                    event_name,
                    category,
                    timestamp,
                    blob_partition,
                    blob_name,
                    source_line,
                    filename AS parquet_path
                FROM events
                WHERE {" AND ".join(conditions)}
                ORDER BY timestamp DESC
                LIMIT ?
            """

            rows = connection.execute(
                query,
                params,
            ).fetchall()

        return [self._to_summary(row) for row in rows]

    def _resolve_partition_files(
        self,
        partitions: list[str],
    ) -> list[Path]:
        files: list[Path] = []

        for partition in partitions:
            directory = self._parquet_root / partition

            if not directory.exists():
                continue

            files.extend(path.resolve() for path in directory.glob("*.parquet"))

        return sorted(files)

    @staticmethod
    def _create_events_view(
        connection: duckdb.DuckDBPyConnection,
        files: list[Path],
    ) -> None:
        paths = [str(path) for path in files]

        connection.read_parquet(
            paths,
            filename=True,
        ).create_view("events")

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
            timestamp=row[5],
            locator=EventLocator(
                blob_partition=row[6],
                blob_name=row[7],
                source_line=row[8],
                parquet_path=Path(row[9]),
            ),
        )
