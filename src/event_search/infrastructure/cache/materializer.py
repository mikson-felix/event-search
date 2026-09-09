from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from event_search.domain.errors import MaterializationError
from event_search.domain.models import (
    BlobObject,
    MaterializationResult,
)

from .extractor import extract_indexed_event

EVENT_SCHEMA = pa.schema(
    [
        pa.field(
            "event_id",
            pa.string(),
        ),
        pa.field(
            "user_id",
            pa.string(),
        ),
        pa.field(
            "organization_id",
            pa.string(),
        ),
        pa.field(
            "event_name",
            pa.string(),
        ),
        pa.field(
            "category",
            pa.string(),
        ),
        pa.field(
            "timestamp",
            pa.timestamp(
                "us",
                tz="UTC",
            ),
        ),
        pa.field(
            "blob_partition",
            pa.string(),
        ),
        pa.field(
            "blob_name",
            pa.string(),
        ),
        pa.field(
            "source_line",
            pa.int64(),
        ),
        pa.field(
            "raw_json",
            pa.string(),
        ),
    ]
)


class ParquetMaterializer:
    def __init__(
        self,
        parquet_root: Path,
    ) -> None:
        self._parquet_root = parquet_root.resolve()

    def materialize(
        self,
        *,
        source_file: Path,
        blob: BlobObject,
    ) -> MaterializationResult:
        rows: list[dict] = []

        with source_file.open("rb") as stream:
            for source_line, raw_line in enumerate(
                stream,
                start=1,
            ):
                if not raw_line.strip():
                    continue

                try:
                    row = extract_indexed_event(
                        raw_line,
                        blob_partition=blob.partition,
                        blob_name=blob.file_name,
                        source_line=source_line,
                    )
                except Exception as exc:
                    raise MaterializationError(f"Failed to parse {blob.name} at line {source_line}") from exc

                rows.append(row)

        output_dir = self._parquet_root / blob.partition

        output_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        source_name = Path(blob.file_name)

        output_file = output_dir / source_name.with_suffix(".parquet").name

        temp_file = output_file.with_suffix(".parquet.tmp")

        table = pa.Table.from_pylist(
            rows,
            schema=EVENT_SCHEMA,
        )

        try:
            pq.write_table(
                table,
                temp_file,
                compression="zstd",
            )

            temp_file.replace(output_file)

        finally:
            temp_file.unlink(missing_ok=True)

        return MaterializationResult(
            path=output_file,
            events_count=len(rows),
        )
