import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

from event_search.domain.errors import MaterializationError
from event_search.domain.models import BlobObject
from event_search.infrastructure.cache.materializer import (
    ParquetMaterializer,
)

PARTITION = "2026/09/10/08"


def make_blob(
    file_name: str = "events.ndjson",
) -> BlobObject:
    return BlobObject(
        name=(f"activity-logs/year=2026/month=09/day=10/hour=08/{file_name}"),
        partition=PARTITION,
        file_name=file_name,
    )


def make_event(
    *,
    event_id: str = "event-001",
    user_id: str = "user-001",
    organization_id: str = "org-001",
    event_name: str = "document.opened",
    category: str = "document",
    timestamp: str = "2026-09-10T08:30:00Z",
) -> dict:
    return {
        "event_id": event_id,
        "timestamp": timestamp,
        "event": {
            "name": event_name,
            "category": category,
        },
        "actor": {
            "user_id": user_id,
            "organization_id": organization_id,
        },
    }


def write_ndjson(
    path: Path,
    rows: list[dict],
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with path.open(
        "w",
        encoding="utf-8",
    ) as stream:
        for row in rows:
            stream.write(json.dumps(row))
            stream.write("\n")


def make_materializer(
    tmp_path: Path,
    *,
    target_size_mb: int = 128,
) -> ParquetMaterializer:
    return ParquetMaterializer(
        tmp_path / "parquet",
        target_size_mb=target_size_mb,
    )


def materialize_one(
    *,
    materializer: ParquetMaterializer,
    source_file: Path,
    blob: BlobObject,
):
    results = materializer.materialize(
        partition=blob.partition,
        sources=[
            (
                source_file,
                blob,
            ),
        ],
    )

    assert len(results) == 1

    return results[0]


def test_materializes_entire_ndjson_file(
    tmp_path: Path,
) -> None:
    source_file = tmp_path / "events.ndjson"

    write_ndjson(
        source_file,
        [
            make_event(event_id="event-001"),
            make_event(event_id="event-002"),
            make_event(event_id="event-003"),
        ],
    )

    blob = make_blob()

    result = materialize_one(
        materializer=make_materializer(tmp_path),
        source_file=source_file,
        blob=blob,
    )

    assert result.events_count == 3
    assert result.path.exists()
    assert result.blobs == (blob,)

    table = pq.read_table(result.path)

    assert table.num_rows == 3


def test_materializes_indexed_fields(
    tmp_path: Path,
) -> None:
    source_file = tmp_path / "events.ndjson"

    write_ndjson(
        source_file,
        [
            make_event(
                event_id="event-001",
                user_id="user-123",
                organization_id="org-456",
                event_name="file.downloaded",
                category="files",
            )
        ],
    )

    result = materialize_one(
        materializer=make_materializer(tmp_path),
        source_file=source_file,
        blob=make_blob(),
    )

    table = pq.read_table(result.path)

    row = table.to_pylist()[0]

    assert row["event_id"] == "event-001"
    assert row["user_id"] == "user-123"
    assert row["organization_id"] == "org-456"
    assert row["event_name"] == "file.downloaded"
    assert row["category"] == "files"
    assert row["timestamp"] == datetime(
        2026,
        9,
        10,
        8,
        30,
        tzinfo=UTC,
    )


def test_materializes_provenance_fields(
    tmp_path: Path,
) -> None:
    source_file = tmp_path / "events.ndjson"

    write_ndjson(
        source_file,
        [
            make_event(event_id="event-001"),
            make_event(event_id="event-002"),
        ],
    )

    blob = make_blob()

    result = materialize_one(
        materializer=make_materializer(tmp_path),
        source_file=source_file,
        blob=blob,
    )

    rows = pq.read_table(result.path).to_pylist()

    assert rows[0]["blob_partition"] == PARTITION
    assert rows[0]["blob_name"] == "events.ndjson"
    assert rows[0]["source_line"] == 1

    assert rows[1]["blob_partition"] == PARTITION
    assert rows[1]["blob_name"] == "events.ndjson"
    assert rows[1]["source_line"] == 2


def test_raw_json_is_preserved(
    tmp_path: Path,
) -> None:
    source_file = tmp_path / "events.ndjson"

    event = make_event(event_id="event-001")

    write_ndjson(
        source_file,
        [event],
    )

    result = materialize_one(
        materializer=make_materializer(tmp_path),
        source_file=source_file,
        blob=make_blob(),
    )

    row = pq.read_table(result.path).to_pylist()[0]

    assert json.loads(row["raw_json"]) == event


def test_blank_lines_are_ignored(
    tmp_path: Path,
) -> None:
    source_file = tmp_path / "events.ndjson"

    source_file.write_text(
        (
            "\n"
            + json.dumps(make_event(event_id="event-001"))
            + "\n\n"
            + json.dumps(make_event(event_id="event-002"))
            + "\n"
        ),
        encoding="utf-8",
    )

    result = materialize_one(
        materializer=make_materializer(tmp_path),
        source_file=source_file,
        blob=make_blob(),
    )

    assert result.events_count == 2


def test_empty_file_creates_empty_parquet(
    tmp_path: Path,
) -> None:
    source_file = tmp_path / "events.ndjson"

    source_file.write_bytes(b"")

    result = materialize_one(
        materializer=make_materializer(tmp_path),
        source_file=source_file,
        blob=make_blob(),
    )

    assert result.path.exists()
    assert result.events_count == 0

    table = pq.read_table(result.path)

    assert table.num_rows == 0


def test_output_file_uses_generated_part_name(
    tmp_path: Path,
) -> None:
    source_file = tmp_path / "source.ndjson"

    write_ndjson(
        source_file,
        [make_event()],
    )

    result = materialize_one(
        materializer=make_materializer(tmp_path),
        source_file=source_file,
        blob=make_blob("source.ndjson"),
    )

    assert result.path.parent == (tmp_path / "parquet" / PARTITION).resolve()

    assert result.path.name.startswith("part-")

    assert result.path.suffix == ".parquet"


def test_invalid_ndjson_raises_materialization_error(
    tmp_path: Path,
) -> None:
    source_file = tmp_path / "invalid.ndjson"

    source_file.write_text(
        "{invalid json}\n",
        encoding="utf-8",
    )

    materializer = make_materializer(tmp_path)

    with pytest.raises(MaterializationError):
        materializer.materialize(
            partition=PARTITION,
            sources=[
                (
                    source_file,
                    make_blob(),
                )
            ],
        )


def test_invalid_timestamp_raises_materialization_error(
    tmp_path: Path,
) -> None:
    source_file = tmp_path / "invalid.ndjson"

    write_ndjson(
        source_file,
        [make_event(timestamp="invalid")],
    )

    materializer = make_materializer(tmp_path)

    with pytest.raises(MaterializationError):
        materializer.materialize(
            partition=PARTITION,
            sources=[
                (
                    source_file,
                    make_blob(),
                )
            ],
        )


def test_failed_materialization_does_not_create_final_parquet(
    tmp_path: Path,
) -> None:
    source_file = tmp_path / "invalid.ndjson"

    source_file.write_text(
        "{invalid json}\n",
        encoding="utf-8",
    )

    parquet_root = tmp_path / "parquet"

    materializer = ParquetMaterializer(
        parquet_root,
        target_size_mb=128,
    )

    with pytest.raises(MaterializationError):
        materializer.materialize(
            partition=PARTITION,
            sources=[
                (
                    source_file,
                    make_blob(),
                )
            ],
        )

    output_dir = parquet_root / PARTITION

    if output_dir.exists():
        assert list(output_dir.glob("*.parquet")) == []


def test_temporary_parquet_file_is_removed_after_success(
    tmp_path: Path,
) -> None:
    source_file = tmp_path / "events.ndjson"

    write_ndjson(
        source_file,
        [make_event()],
    )

    result = materialize_one(
        materializer=make_materializer(tmp_path),
        source_file=source_file,
        blob=make_blob(),
    )

    assert result.path.exists()

    assert list(result.path.parent.glob("*.tmp")) == []


def test_parquet_can_be_queried_by_duckdb(
    tmp_path: Path,
) -> None:
    source_file = tmp_path / "events.ndjson"

    write_ndjson(
        source_file,
        [
            make_event(event_id="event-001"),
            make_event(event_id="event-002"),
        ],
    )

    result = materialize_one(
        materializer=make_materializer(tmp_path),
        source_file=source_file,
        blob=make_blob(),
    )

    with duckdb.connect() as connection:
        rows = connection.execute(
            """
            SELECT event_id
            FROM read_parquet(?)
            ORDER BY event_id
            """,
            [str(result.path)],
        ).fetchall()

    assert rows == [
        ("event-001",),
        ("event-002",),
    ]


def test_materializes_multiple_sources_into_one_parquet(
    tmp_path: Path,
) -> None:
    first_file = tmp_path / "first.ndjson"

    second_file = tmp_path / "second.ndjson"

    first_blob = make_blob("first.ndjson")
    second_blob = make_blob("second.ndjson")

    write_ndjson(
        first_file,
        [
            make_event(event_id="event-001"),
            make_event(event_id="event-002"),
        ],
    )

    write_ndjson(
        second_file,
        [
            make_event(event_id="event-003"),
        ],
    )

    materializer = make_materializer(
        tmp_path,
        target_size_mb=128,
    )

    results = materializer.materialize(
        partition=PARTITION,
        sources=[
            (
                first_file,
                first_blob,
            ),
            (
                second_file,
                second_blob,
            ),
        ],
    )

    assert len(results) == 1

    result = results[0]

    assert result.events_count == 3
    assert result.blobs == (
        first_blob,
        second_blob,
    )

    rows = pq.read_table(result.path).to_pylist()

    assert {row["blob_name"] for row in rows} == {
        "first.ndjson",
        "second.ndjson",
    }


def test_splits_sources_by_target_size(
    tmp_path: Path,
) -> None:
    first_file = tmp_path / "first.ndjson"
    second_file = tmp_path / "second.ndjson"

    first_blob = make_blob("first.ndjson")
    second_blob = make_blob("second.ndjson")

    large_value = "x" * (700 * 1024)

    first_event = make_event(event_id="event-001")
    first_event["payload"] = large_value

    second_event = make_event(event_id="event-002")
    second_event["payload"] = large_value

    write_ndjson(
        first_file,
        [first_event],
    )

    write_ndjson(
        second_file,
        [second_event],
    )

    materializer = make_materializer(
        tmp_path,
        target_size_mb=1,
    )

    results = materializer.materialize(
        partition=PARTITION,
        sources=[
            (
                first_file,
                first_blob,
            ),
            (
                second_file,
                second_blob,
            ),
        ],
    )

    assert len(results) == 2

    assert results[0].blobs == (first_blob,)

    assert results[1].blobs == (second_blob,)


def test_returns_empty_list_for_no_sources(
    tmp_path: Path,
) -> None:
    materializer = make_materializer(tmp_path)

    result = materializer.materialize(
        partition=PARTITION,
        sources=[],
    )

    assert result == []


def test_rejects_sources_from_other_partition(
    tmp_path: Path,
) -> None:
    source_file = tmp_path / "events.ndjson"

    write_ndjson(
        source_file,
        [make_event()],
    )

    blob = BlobObject(
        name="events.ndjson",
        partition="2026/09/10/09",
        file_name="events.ndjson",
    )

    materializer = make_materializer(tmp_path)

    with pytest.raises(
        ValueError,
        match="All blobs must belong",
    ):
        materializer.materialize(
            partition=PARTITION,
            sources=[
                (
                    source_file,
                    blob,
                )
            ],
        )


def test_rejects_invalid_target_size(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        ValueError,
        match="target_size_mb",
    ):
        ParquetMaterializer(
            tmp_path / "parquet",
            target_size_mb=0,
        )
