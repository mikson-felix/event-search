from pathlib import Path
from uuid import uuid4

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


_WRITE_BATCH_SIZE = 10_000


class ParquetMaterializer:
    def __init__(
        self,
        parquet_root: Path,
        *,
        target_size_mb: int,
    ) -> None:
        if target_size_mb < 1:
            raise ValueError("target_size_mb must be greater than zero")

        self._parquet_root = parquet_root.resolve()

        self._target_size_bytes = target_size_mb * 1024 * 1024

    def materialize(
        self,
        *,
        partition: str,
        sources: list[tuple[Path, BlobObject]],
    ) -> list[MaterializationResult]:
        if not sources:
            return []

        self._validate_sources(
            partition=partition,
            sources=sources,
        )

        groups = self._group_sources(sources)

        return [
            self._materialize_group(
                partition=partition,
                sources=group,
            )
            for group in groups
        ]

    def _group_sources(
        self,
        sources: list[tuple[Path, BlobObject]],
    ) -> list[list[tuple[Path, BlobObject]]]:
        groups: list[list[tuple[Path, BlobObject]]] = []

        current_group: list[tuple[Path, BlobObject]] = []

        current_size = 0

        for source_file, blob in sources:
            source_size = source_file.stat().st_size

            if current_group and current_size + source_size > self._target_size_bytes:
                groups.append(current_group)

                current_group = []
                current_size = 0

            current_group.append(
                (
                    source_file,
                    blob,
                )
            )

            current_size += source_size

        if current_group:
            groups.append(current_group)

        return groups

    def _materialize_group(
        self,
        *,
        partition: str,
        sources: list[tuple[Path, BlobObject]],
    ) -> MaterializationResult:
        output_dir = self._parquet_root / partition

        output_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        output_file = output_dir / f"part-{uuid4()}.parquet"

        temp_file = Path(f"{output_file}.tmp")

        events_count = 0
        writer: pq.ParquetWriter | None = None
        rows: list[dict] = []

        try:
            for source_file, blob in sources:
                with source_file.open("rb") as stream:
                    for (
                        source_line,
                        raw_line,
                    ) in enumerate(
                        stream,
                        start=1,
                    ):
                        if not raw_line.strip():
                            continue

                        try:
                            row = extract_indexed_event(
                                raw_line,
                                blob_partition=(blob.partition),
                                blob_name=(blob.file_name),
                                source_line=(source_line),
                            )
                        except Exception as exc:
                            raise MaterializationError(f"Failed to parse {blob.name} at line {source_line}") from exc

                        rows.append(row)
                        events_count += 1

                        if len(rows) >= _WRITE_BATCH_SIZE:
                            writer = self._write_rows(
                                writer=writer,
                                rows=rows,
                                target=temp_file,
                            )

                            rows = []

            if rows:
                writer = self._write_rows(
                    writer=writer,
                    rows=rows,
                    target=temp_file,
                )

            if writer is None:
                empty_table = pa.Table.from_pylist(
                    [],
                    schema=EVENT_SCHEMA,
                )

                pq.write_table(
                    empty_table,
                    temp_file,
                    compression="zstd",
                )

            else:
                writer.close()
                writer = None

            temp_file.replace(output_file)

        except Exception:
            if writer is not None:
                writer.close()

            raise

        finally:
            temp_file.unlink(missing_ok=True)

        return MaterializationResult(
            path=output_file,
            events_count=events_count,
            blobs=tuple(blob for _, blob in sources),
        )

    @staticmethod
    def _write_rows(
        *,
        writer: pq.ParquetWriter | None,
        rows: list[dict],
        target: Path,
    ) -> pq.ParquetWriter:
        table = pa.Table.from_pylist(
            rows,
            schema=EVENT_SCHEMA,
        )

        if writer is None:
            writer = pq.ParquetWriter(
                target,
                EVENT_SCHEMA,
                compression="zstd",
            )

        writer.write_table(table)

        return writer

    @staticmethod
    def _validate_sources(
        *,
        partition: str,
        sources: list[tuple[Path, BlobObject]],
    ) -> None:
        for _, blob in sources:
            if blob.partition != partition:
                raise ValueError("All blobs must belong to the requested partition")
