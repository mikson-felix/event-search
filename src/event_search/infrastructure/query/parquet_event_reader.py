import duckdb

from event_search.domain.models import (
    EventDetails,
    EventLocator,
)


class DuckDBParquetEventDetailsReader:
    def read(
        self,
        *,
        event_id: str,
        locator: EventLocator,
    ) -> EventDetails | None:
        if not locator.parquet_path.exists():
            return None

        with duckdb.connect() as connection:
            connection.read_parquet(str(locator.parquet_path)).create_view("event_source")

            row = connection.execute(
                """
                SELECT
                    event_id,
                    blob_partition,
                    blob_name,
                    source_line,
                    raw_json
                FROM event_source
                WHERE source_line = ?
                  AND event_id = ?
                LIMIT 1
                """,
                [
                    locator.source_line,
                    event_id,
                ],
            ).fetchone()

        if row is None:
            return None

        return EventDetails(
            event_id=row[0],
            blob_partition=row[1],
            blob_name=row[2],
            source_line=row[3],
            raw_json=row[4],
        )
