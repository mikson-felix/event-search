from __future__ import annotations

from pathlib import Path

import duckdb
import orjson
import pyarrow.parquet as pq
import pytest

from event_search.domain.errors import MaterializationError
from event_search.domain.models import BlobObject
from event_search.infrastructure.cache.materializer import ParquetMaterializer


def make_blob(
    *,
    partition: str = "2026/09/09/08",
    file_name: str = "events.ndjson",
) -> BlobObject:
    return BlobObject(
        name=f"{partition}/{file_name}",
        partition=partition,
        file_name=file_name,
    )


def test_materializes_entire_ndjson_file(
    tmp_path: Path,
    event_payload: dict,
) -> None:
    source_file = tmp_path / "events.ndjson"

    first = dict(event_payload)
    first["event_id"] = "event-001"

    second = dict(event_payload)
    second["event_id"] = "event-002"

    source_file.write_bytes(orjson.dumps(first) + b"\n" + orjson.dumps(second) + b"\n")

    blob = make_blob()

    parquet_root = tmp_path / "parquet"

    materializer = ParquetMaterializer(
        parquet_root=parquet_root,
    )

    result = materializer.materialize(
        source_file=source_file,
        blob=blob,
    )

    expected_path = (parquet_root / "2026/09/09/08" / "events.parquet").resolve()

    assert result.path == expected_path
    assert result.events_count == 2
    assert result.path.exists()

    table = pq.read_table(
        result.path,
    )

    assert table.num_rows == 2

    rows = table.to_pylist()

    assert [row["event_id"] for row in rows] == [
        "event-001",
        "event-002",
    ]


def test_materializes_indexed_fields(
    tmp_path: Path,
    event_payload: dict,
) -> None:
    source_file = tmp_path / "events.ndjson"

    source_file.write_bytes(orjson.dumps(event_payload) + b"\n")

    blob = make_blob()

    materializer = ParquetMaterializer(
        parquet_root=tmp_path / "parquet",
    )

    result = materializer.materialize(
        source_file=source_file,
        blob=blob,
    )

    table = pq.read_table(
        result.path,
    )

    rows = table.to_pylist()

    assert len(rows) == 1

    row = rows[0]

    assert row["event_id"] == "event-001"
    assert row["user_id"] == "user-001"
    assert row["organization_id"] == "org-001"
    assert row["event_name"] == "LOGIN"
    assert row["category"] == "AUTH"


def test_materializes_provenance_fields(
    tmp_path: Path,
    event_payload: dict,
) -> None:
    source_file = tmp_path / "source.ndjson"

    first = dict(event_payload)
    first["event_id"] = "event-001"

    second = dict(event_payload)
    second["event_id"] = "event-002"

    source_file.write_bytes(orjson.dumps(first) + b"\n" + orjson.dumps(second) + b"\n")

    blob = make_blob(
        partition="2026/09/09/08",
        file_name="source.ndjson",
    )

    materializer = ParquetMaterializer(
        parquet_root=tmp_path / "parquet",
    )

    result = materializer.materialize(
        source_file=source_file,
        blob=blob,
    )

    rows = pq.read_table(
        result.path,
    ).to_pylist()

    assert rows[0]["blob_partition"] == "2026/09/09/08"
    assert rows[0]["blob_name"] == "source.ndjson"
    assert rows[0]["source_line"] == 1

    assert rows[1]["blob_partition"] == "2026/09/09/08"
    assert rows[1]["blob_name"] == "source.ndjson"
    assert rows[1]["source_line"] == 2


def test_raw_json_is_preserved(
    tmp_path: Path,
    event_payload: dict,
) -> None:
    source_file = tmp_path / "events.ndjson"

    raw_json = orjson.dumps(
        event_payload,
    )

    source_file.write_bytes(
        raw_json + b"\n",
    )

    blob = make_blob()

    materializer = ParquetMaterializer(
        parquet_root=tmp_path / "parquet",
    )

    result = materializer.materialize(
        source_file=source_file,
        blob=blob,
    )

    table = pq.read_table(
        result.path,
    )

    row = table.to_pylist()[0]

    assert row["raw_json"] == raw_json.decode(
        "utf-8",
    )


def test_blank_lines_are_ignored(
    tmp_path: Path,
    event_payload: dict,
) -> None:
    source_file = tmp_path / "events.ndjson"

    raw_json = orjson.dumps(
        event_payload,
    )

    source_file.write_bytes(b"\n" + raw_json + b"\n\n")

    blob = make_blob()

    materializer = ParquetMaterializer(
        parquet_root=tmp_path / "parquet",
    )

    result = materializer.materialize(
        source_file=source_file,
        blob=blob,
    )

    assert result.events_count == 1

    rows = pq.read_table(
        result.path,
    ).to_pylist()

    assert len(rows) == 1
    assert rows[0]["event_id"] == "event-001"

    # source_line is the original NDJSON physical line number,
    # including ignored empty lines.
    assert rows[0]["source_line"] == 2


def test_empty_file_creates_empty_parquet(
    tmp_path: Path,
) -> None:
    source_file = tmp_path / "events.ndjson"

    source_file.write_bytes(
        b"",
    )

    blob = make_blob()

    materializer = ParquetMaterializer(
        parquet_root=tmp_path / "parquet",
    )

    result = materializer.materialize(
        source_file=source_file,
        blob=blob,
    )

    assert result.events_count == 0
    assert result.path.exists()

    table = pq.read_table(
        result.path,
    )

    assert table.num_rows == 0


def test_output_file_uses_source_file_name(
    tmp_path: Path,
    event_payload: dict,
) -> None:
    source_file = tmp_path / "downloaded.tmp"

    source_file.write_bytes(orjson.dumps(event_payload) + b"\n")

    blob = make_blob(
        partition="2026/09/09/08",
        file_name="audit-events.ndjson",
    )

    parquet_root = tmp_path / "parquet"

    materializer = ParquetMaterializer(
        parquet_root=parquet_root,
    )

    result = materializer.materialize(
        source_file=source_file,
        blob=blob,
    )

    assert result.path == (parquet_root / blob.partition / "audit-events.parquet").resolve()


def test_invalid_ndjson_raises_materialization_error(
    tmp_path: Path,
    event_payload: dict,
) -> None:
    source_file = tmp_path / "broken.ndjson"

    source_file.write_bytes(orjson.dumps(event_payload) + b"\n" + b"{broken-json\n")

    blob = make_blob(
        file_name="broken.ndjson",
    )

    materializer = ParquetMaterializer(
        parquet_root=tmp_path / "parquet",
    )

    with pytest.raises(
        MaterializationError,
        match=(
            r"Failed to parse "
            r"2026/09/09/08/broken\.ndjson "
            r"at line 2"
        ),
    ):
        materializer.materialize(
            source_file=source_file,
            blob=blob,
        )


def test_invalid_timestamp_raises_materialization_error(
    tmp_path: Path,
    event_payload: dict,
) -> None:
    source_file = tmp_path / "events.ndjson"

    event_payload["timestamp"] = 123

    source_file.write_bytes(orjson.dumps(event_payload) + b"\n")

    blob = make_blob()

    materializer = ParquetMaterializer(
        parquet_root=tmp_path / "parquet",
    )

    with pytest.raises(
        MaterializationError,
        match=(
            r"Failed to parse "
            r"2026/09/09/08/events\.ndjson "
            r"at line 1"
        ),
    ):
        materializer.materialize(
            source_file=source_file,
            blob=blob,
        )


def test_failed_materialization_does_not_create_final_parquet(
    tmp_path: Path,
) -> None:
    source_file = tmp_path / "broken.ndjson"

    source_file.write_bytes(
        b"{broken-json\n",
    )

    blob = make_blob(
        file_name="broken.ndjson",
    )

    parquet_root = tmp_path / "parquet"

    materializer = ParquetMaterializer(
        parquet_root=parquet_root,
    )

    expected_path = (parquet_root / blob.partition / "broken.parquet").resolve()

    with pytest.raises(
        MaterializationError,
    ):
        materializer.materialize(
            source_file=source_file,
            blob=blob,
        )

    assert not expected_path.exists()


def test_temporary_parquet_file_is_removed_after_success(
    tmp_path: Path,
    event_payload: dict,
) -> None:
    source_file = tmp_path / "events.ndjson"

    source_file.write_bytes(orjson.dumps(event_payload) + b"\n")

    blob = make_blob()

    parquet_root = tmp_path / "parquet"

    materializer = ParquetMaterializer(
        parquet_root=parquet_root,
    )

    result = materializer.materialize(
        source_file=source_file,
        blob=blob,
    )

    temp_file = result.path.with_suffix(
        ".parquet.tmp",
    )

    assert result.path.exists()
    assert not temp_file.exists()


def test_parquet_can_be_queried_by_duckdb(
    tmp_path: Path,
    event_payload: dict,
) -> None:
    source_file = tmp_path / "events.ndjson"

    source_file.write_bytes(orjson.dumps(event_payload) + b"\n")

    blob = make_blob()

    materializer = ParquetMaterializer(
        parquet_root=tmp_path / "parquet",
    )

    result = materializer.materialize(
        source_file=source_file,
        blob=blob,
    )

    with duckdb.connect() as connection:
        row = connection.execute(
            """
            SELECT
                event_id,
                user_id,
                organization_id,
                event_name,
                category,
                blob_partition,
                blob_name,
                source_line
            FROM read_parquet(?)
            """,
            [
                str(result.path),
            ],
        ).fetchone()

    assert row == (
        "event-001",
        "user-001",
        "org-001",
        "LOGIN",
        "AUTH",
        "2026/09/09/08",
        "events.ndjson",
        1,
    )
