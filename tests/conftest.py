from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


@pytest.fixture
def event_payload() -> dict[str, Any]:
    return {
        "event_id": "event-001",
        "timestamp": "2026-09-09T08:30:00Z",
        "schema_version": "1.0",
        "application": "test-service",
        "event": {
            "name": "LOGIN",
            "grouping": "authentication",
            "category": "AUTH",
        },
        "actor": {
            "user_id": "user-001",
            "roles": ["admin"],
            "organization_id": "org-001",
        },
        "metadata": {
            "ip_address": "127.0.0.1",
            "user_agent": "pytest",
        },
        "attributes": {},
        "data_affected": [],
        "sensitive_action": False,
        "correlation_id": "correlation-001",
    }


@pytest.fixture
def parquet_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("event_id", pa.string()),
            pa.field("user_id", pa.string()),
            pa.field("organization_id", pa.string()),
            pa.field("event_name", pa.string()),
            pa.field("category", pa.string()),
            pa.field("timestamp", pa.timestamp("us", tz="UTC")),
            pa.field("blob_partition", pa.string()),
            pa.field("blob_name", pa.string()),
            pa.field("source_line", pa.int64()),
            pa.field("raw_json", pa.string()),
        ]
    )


@pytest.fixture
def make_parquet(tmp_path: Path, parquet_schema: pa.Schema):
    def factory(
        *,
        partition: str,
        file_name: str,
        rows: list[dict[str, Any]],
    ) -> Path:
        directory = tmp_path / "parquet" / partition
        directory.mkdir(parents=True, exist_ok=True)

        path = directory / file_name

        normalized_rows = []

        for row in rows:
            normalized = dict(row)

            timestamp = normalized["timestamp"]

            if isinstance(timestamp, str):
                normalized["timestamp"] = datetime.fromisoformat(timestamp.replace("Z", "+00:00")).astimezone(UTC)

            normalized_rows.append(normalized)

        table = pa.Table.from_pylist(
            normalized_rows,
            schema=parquet_schema,
        )

        pq.write_table(
            table,
            path,
            compression="zstd",
        )

        return path

    return factory
