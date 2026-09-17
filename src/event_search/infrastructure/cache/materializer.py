from collections.abc import Iterable, Iterator
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
            "application",
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
        pa.field("application", pa.string()),
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


def _split_non_blank_lines(content: bytes) -> tuple[list[bytes], list[int]]:
    raw_lines: list[bytes] = []
    source_lines: list[int] = []

    for line_number, segment in enumerate(content.split(b"\n"), start=1):
        if not segment.strip():
            continue

        raw_lines.append(segment)
        source_lines.append(line_number)

    return raw_lines, source_lines


def _batch_by_size(
    sources: Iterable[tuple[bytes, BlobObject]],
    batch_size_bytes: int,
) -> Iterator[list[tuple[bytes, BlobObject]]]:
    """Accumulates sources until their combined content size crosses batch_size_bytes.

    Lets ParquetMaterializer parse/write many small blobs with far fewer
    read_json()/write_table() calls than one per blob, while still consuming
    `sources` lazily as it becomes available.
    """
    batch: list[tuple[bytes, BlobObject]] = []
    batch_bytes = 0

    for content, blob in sources:
        if batch and batch_bytes + len(content) > batch_size_bytes:
            yield batch
            batch = []
            batch_bytes = 0

        batch.append((content, blob))
        batch_bytes += len(content)

    if batch:
        yield batch


class _MaterializationGroup:
    """Accumulates parsed sources into a single Parquet output file.

    Sources are added one at a time as they become available, so a group can
    be filled while earlier sources in the same partition are still being
    downloaded - callers are not required to have every source in hand
    upfront. Tables are buffered internally and written in write_batch_size_bytes
    chunks rather than one write_table() call per source, so a partition made
    of many small blobs still ends up with a small number of larger row groups.
    """

    def __init__(
        self,
        output_file: Path,
        temp_file: Path,
        *,
        write_batch_size_bytes: int,
    ) -> None:
        self.output_file = output_file
        self.temp_file = temp_file
        self.size_bytes = 0

        self._write_batch_size_bytes = write_batch_size_bytes
        self._writer: pq.ParquetWriter | None = None
        self._events_count = 0
        self._blobs: list[BlobObject] = []

        self._pending_tables: list[pa.Table] = []
        self._pending_bytes = 0

    def add(
        self,
        table: pa.Table,
        blob: BlobObject,
        source_size: int,
    ) -> None:
        if table.num_rows > 0:
            self._pending_tables.append(table)
            self._pending_bytes += source_size
            self._events_count += table.num_rows

            if self._pending_bytes > self._write_batch_size_bytes:
                self._flush_pending()

        self._blobs.append(blob)
        self.size_bytes += source_size

    def _flush_pending(self) -> None:
        if not self._pending_tables:
            return

        if self._writer is None:
            self._writer = pq.ParquetWriter(
                self.temp_file,
                EVENT_SCHEMA,
                compression="zstd",
            )

        self._writer.write_table(pa.concat_tables(self._pending_tables))
        self._pending_tables = []
        self._pending_bytes = 0

    def finalize(self) -> MaterializationResult:
        self._flush_pending()

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
        parse_batch_size_mb: int = 4,
        write_batch_size_mb: int = 4,
    ) -> None:
        if target_size_mb < 1:
            raise ValueError("target_size_mb must be greater than zero")

        if parse_batch_size_mb < 1:
            raise ValueError("parse_batch_size_mb must be greater than zero")

        if write_batch_size_mb < 1:
            raise ValueError("write_batch_size_mb must be greater than zero")

        self._parquet_root = parquet_root.resolve()
        self._target_size_bytes = target_size_mb * 1024 * 1024
        self._parse_batch_size_bytes = parse_batch_size_mb * 1024 * 1024
        self._write_batch_size_bytes = write_batch_size_mb * 1024 * 1024

    def materialize(
        self,
        *,
        partition: str,
        sources: Iterable[tuple[bytes, BlobObject]],
    ) -> list[MaterializationResult]:
        results: list[MaterializationResult] = []
        group: _MaterializationGroup | None = None

        try:
            for content, blob, table in self._iter_parsed_sources(sources, partition):
                source_size = len(content)

                if group is not None and group.size_bytes + source_size > self._target_size_bytes:
                    results.append(group.finalize())
                    group = None

                if group is None:
                    group = self._start_group(partition)

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

    def _iter_parsed_sources(
        self,
        sources: Iterable[tuple[bytes, BlobObject]],
        partition: str,
    ) -> Iterator[tuple[bytes, BlobObject, pa.Table]]:
        for parse_batch in _batch_by_size(sources, self._parse_batch_size_bytes):
            if any(blob.partition != partition for _, blob in parse_batch):
                raise ValueError("All blobs must belong to the requested partition")

            tables = self._read_source_tables(parse_batch)

            yield from ((content, blob, table) for (content, blob), table in zip(parse_batch, tables, strict=True))

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

        return _MaterializationGroup(
            output_file,
            temp_file,
            write_batch_size_bytes=self._write_batch_size_bytes,
        )

    def _read_source_tables(
        self,
        batch: list[tuple[bytes, BlobObject]],
    ) -> list[pa.Table]:
        tables = self._parse_batch(batch)

        if tables is not None:
            return tables

        return [self._read_source_table(content=content, blob=blob) for content, blob in batch]

    def _read_source_table(
        self,
        *,
        content: bytes,
        blob: BlobObject,
    ) -> pa.Table:
        tables = self._parse_batch([(content, blob)])

        if tables is not None:
            return tables[0]

        raw_lines, source_lines = _split_non_blank_lines(content)

        logger.debug(
            "Vectorized parse failed, falling back to line-by-line parsing: blob={}",
            blob.name,
        )

        return self._parse_fallback(
            raw_lines=raw_lines,
            source_lines=source_lines,
            blob=blob,
        )

    @staticmethod
    def _parse_batch(
        batch: list[tuple[bytes, BlobObject]],
    ) -> list[pa.Table] | None:
        """Parses every blob in the batch with a single read_json() call, returning
        one EVENT_SCHEMA table per blob in batch order - or None if the combined
        buffer failed to parse, signalling the caller to fall back per blob.
        """
        per_blob_lines = [_split_non_blank_lines(content) for content, _ in batch]
        all_lines = [line for raw_lines, _ in per_blob_lines for line in raw_lines]

        if not all_lines:
            return [pa.Table.from_pylist([], schema=EVENT_SCHEMA) for _ in batch]

        try:
            parsed = pa_json.read_json(
                pa.BufferReader(b"\n".join(all_lines)),
                parse_options=_RAW_PARSE_OPTIONS,
            )
        except pa.lib.ArrowInvalid:
            return None

        if parsed.num_rows != len(all_lines):
            return None

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

        tables: list[pa.Table] = []
        offset = 0

        for (_, blob), (raw_lines, source_lines) in zip(batch, per_blob_lines, strict=True):
            row_count = len(raw_lines)

            if row_count == 0:
                tables.append(pa.Table.from_pylist([], schema=EVENT_SCHEMA))
                continue

            raw_json_values = pc.utf8_rtrim(
                pa.array(raw_lines, type=pa.binary()).cast(pa.string()),
                characters="\r",
            )

            tables.append(
                pa.Table.from_arrays(
                    [
                        parsed["event_id"].slice(offset, row_count),
                        parsed["application"].slice(offset, row_count),
                        user_id.slice(offset, row_count),
                        organization_id.slice(offset, row_count),
                        event_name.slice(offset, row_count),
                        category.slice(offset, row_count),
                        parsed["timestamp"].slice(offset, row_count),
                        pa.array([blob.partition] * row_count, type=pa.string()),
                        pa.array([blob.file_name] * row_count, type=pa.string()),
                        pa.array(source_lines, type=pa.int64()),
                        raw_json_values,
                    ],
                    schema=EVENT_SCHEMA,
                )
            )
            offset += row_count

        return tables

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
