from collections.abc import Iterable
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


class _MaterializationGroup:
    """Accumulates parsed sources into a single Parquet output file.

    Sources are added one at a time as they become available, so a group can
    be filled while earlier sources in the same partition are still being
    downloaded - callers are not required to have every source in hand
    upfront.
    """

    def __init__(
        self,
        output_file: Path,
        temp_file: Path,
    ) -> None:
        self.output_file = output_file
        self.temp_file = temp_file
        self.size_bytes = 0

        self._writer: pq.ParquetWriter | None = None
        self._events_count = 0
        self._blobs: list[BlobObject] = []

    def add(
        self,
        table: pa.Table,
        blob: BlobObject,
        source_size: int,
    ) -> None:
        if table.num_rows > 0:
            if self._writer is None:
                self._writer = pq.ParquetWriter(
                    self.temp_file,
                    EVENT_SCHEMA,
                    compression="zstd",
                )

            self._writer.write_table(table)
            self._events_count += table.num_rows

        self._blobs.append(blob)
        self.size_bytes += source_size

    def finalize(self) -> MaterializationResult:
        if self._writer is None:
            empty_table = pa.Table.from_pylist(
                [],
                schema=EVENT_SCHEMA,
            )

            pq.write_table(
                empty_table,
                self.temp_file,
                compression="zstd",
            )
        else:
            self._writer.close()
            self._writer = None

        self.temp_file.replace(self.output_file)

        logger.debug(
            ("Parquet created: path={}, blobs={}, events={}, parquet_size_bytes={}"),
            self.output_file,
            len(self._blobs),
            self._events_count,
            self.output_file.stat().st_size,
        )

        return MaterializationResult(
            path=self.output_file,
            events_count=self._events_count,
            blobs=tuple(self._blobs),
        )

    def abort(self) -> None:
        if self._writer is not None:
            self._writer.close()

        self.temp_file.unlink(missing_ok=True)


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
        sources: Iterable[tuple[bytes, BlobObject]],
    ) -> list[MaterializationResult]:
        results: list[MaterializationResult] = []
        group: _MaterializationGroup | None = None

        try:
            for content, blob in sources:
                if blob.partition != partition:
                    raise ValueError("All blobs must belong to the requested partition")

                source_size = len(content)

                if group is not None and group.size_bytes + source_size > self._target_size_bytes:
                    results.append(group.finalize())
                    group = None

                if group is None:
                    group = self._start_group(partition)

                table = self._read_source_table(
                    content=content,
                    blob=blob,
                )

                group.add(table, blob, source_size)

            if group is not None:
                results.append(group.finalize())

        except Exception:
            if group is not None:
                group.abort()

            raise

        logger.debug(
            "Materialization complete: partition={}, groups={}",
            partition,
            len(results),
        )

        return results

    def _start_group(
        self,
        partition: str,
    ) -> _MaterializationGroup:
        output_dir = self._parquet_root / partition

        output_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        output_file = output_dir / f"part-{uuid4()}.parquet"

        temp_file = Path(f"{output_file}.tmp")

        return _MaterializationGroup(output_file, temp_file)

    def _read_source_table(
        self,
        *,
        content: bytes,
        blob: BlobObject,
    ) -> pa.Table:
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
                content=content,
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
        content: bytes,
        raw_lines: list[bytes],
        source_lines: list[int],
        blob: BlobObject,
    ) -> pa.Table:
        parsed = pa_json.read_json(
            pa.BufferReader(content),
            parse_options=_RAW_PARSE_OPTIONS,
        )

        if parsed.num_rows != len(raw_lines):
            raise pa.lib.ArrowInvalid(
                f"Parsed row count mismatch for {blob.name}: "
                f"expected {len(raw_lines)} non-blank lines, got {parsed.num_rows}"
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
