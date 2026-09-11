from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.json as pa_json
import pyarrow.parquet as pq
from loguru import logger

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

# Shape of the incoming NDJSON events, only the fields this application reads.
# Fields absent from a given line simply parse as null; unrecognized fields are
# dropped by ParseOptions.unexpected_field_behavior="ignore" rather than erroring.
_RAW_EVENT_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string()),
        pa.field("timestamp", pa.timestamp("us", tz="UTC")),
        pa.field(
            "event",
            pa.struct(
                [
                    pa.field("name", pa.string()),
                    pa.field("category", pa.string()),
                ]
            ),
        ),
        pa.field("event_name", pa.string()),
        pa.field("category", pa.string()),
        pa.field(
            "actor",
            pa.struct(
                [
                    pa.field("user_id", pa.string()),
                    pa.field("organization_id", pa.string()),
                ]
            ),
        ),
        pa.field(
            "attributes",
            pa.struct(
                [
                    pa.field("user_id", pa.string()),
                    pa.field("organization_id", pa.string()),
                ]
            ),
        ),
    ]
)

_RAW_PARSE_OPTIONS = pa_json.ParseOptions(
    explicit_schema=_RAW_EVENT_SCHEMA,
    unexpected_field_behavior="ignore",
)


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
        logger.debug(
            ("Materialization plan: partition={}, source_blobs={}, groups={}"),
            partition,
            len(sources),
            len(groups),
        )

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

        try:
            for source_file, blob in sources:
                table = self._read_source_table(
                    source_file=source_file,
                    blob=blob,
                )

                if table.num_rows == 0:
                    continue

                events_count += table.num_rows

                if writer is None:
                    writer = pq.ParquetWriter(
                        temp_file,
                        EVENT_SCHEMA,
                        compression="zstd",
                    )

                writer.write_table(table)

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
            logger.debug(
                ("Parquet created: path={}, blobs={}, events={}, parquet_size_bytes={}"),
                output_file,
                len(sources),
                events_count,
                output_file.stat().st_size,
            )
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

    def _read_source_table(
        self,
        *,
        source_file: Path,
        blob: BlobObject,
    ) -> pa.Table:
        content = source_file.read_bytes()

        raw_lines: list[bytes] = []
        source_lines: list[int] = []

        for line_number, segment in enumerate(content.split(b"\n"), start=1):
            if not segment.strip():
                continue

            raw_lines.append(segment)
            source_lines.append(line_number)

        if not raw_lines:
            return pa.Table.from_pylist([], schema=EVENT_SCHEMA)

        try:
            return self._parse_vectorized(
                raw_lines=raw_lines,
                source_lines=source_lines,
                blob=blob,
            )
        except pa.lib.ArrowInvalid as exc:
            logger.debug(
                ("Vectorized parse failed, falling back to line-by-line parsing: blob={}, reason={}"),
                blob.name,
                exc,
            )

            return self._parse_fallback(
                raw_lines=raw_lines,
                source_lines=source_lines,
                blob=blob,
            )

    @staticmethod
    def _parse_vectorized(
        *,
        raw_lines: list[bytes],
        source_lines: list[int],
        blob: BlobObject,
    ) -> pa.Table:
        buffer = pa.py_buffer(b"\n".join(raw_lines))

        parsed = pa_json.read_json(
            pa.BufferReader(buffer),
            parse_options=_RAW_PARSE_OPTIONS,
        )

        user_id = pc.coalesce(
            pc.struct_field(parsed["actor"], "user_id"),
            pc.struct_field(parsed["attributes"], "user_id"),
        )

        organization_id = pc.coalesce(
            pc.struct_field(parsed["actor"], "organization_id"),
            pc.struct_field(parsed["attributes"], "organization_id"),
        )

        event_name = pc.coalesce(
            pc.struct_field(parsed["event"], "name"),
            parsed["event_name"],
        )

        category = pc.coalesce(
            pc.struct_field(parsed["event"], "category"),
            parsed["category"],
        )

        row_count = parsed.num_rows

        raw_json_values = pc.utf8_rtrim(
            pa.array(raw_lines, type=pa.binary()).cast(pa.string()),
            characters="\r",
        )

        return pa.Table.from_arrays(
            [
                parsed["event_id"],
                user_id,
                organization_id,
                event_name,
                category,
                parsed["timestamp"],
                pa.array([blob.partition] * row_count, type=pa.string()),
                pa.array([blob.file_name] * row_count, type=pa.string()),
                pa.array(source_lines, type=pa.int64()),
                raw_json_values,
            ],
            schema=EVENT_SCHEMA,
        )

    @staticmethod
    def _parse_fallback(
        *,
        raw_lines: list[bytes],
        source_lines: list[int],
        blob: BlobObject,
    ) -> pa.Table:
        rows: list[dict] = []

        for raw_line, source_line in zip(raw_lines, source_lines, strict=True):
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

        return pa.Table.from_pylist(rows, schema=EVENT_SCHEMA)

    @staticmethod
    def _validate_sources(
        *,
        partition: str,
        sources: list[tuple[Path, BlobObject]],
    ) -> None:
        for _, blob in sources:
            if blob.partition != partition:
                raise ValueError("All blobs must belong to the requested partition")
