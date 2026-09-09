from __future__ import annotations

from datetime import UTC, datetime

import orjson
import pytest

from event_search.infrastructure.cache.extractor import extract_indexed_event


def test_extracts_indexed_fields(event_payload: dict) -> None:
    raw_line = orjson.dumps(event_payload)

    result = extract_indexed_event(
        raw_line,
        blob_partition="2026/09/09/08",
        blob_name="events.ndjson",
        source_line=42,
    )

    assert result["event_id"] == "event-001"
    assert result["user_id"] == "user-001"
    assert result["organization_id"] == "org-001"
    assert result["event_name"] == "LOGIN"
    assert result["category"] == "AUTH"

    assert result["timestamp"] == datetime(
        2026,
        9,
        9,
        8,
        30,
        tzinfo=UTC,
    )

    assert result["blob_partition"] == "2026/09/09/08"
    assert result["blob_name"] == "events.ndjson"
    assert result["source_line"] == 42

    assert result["raw_json"] == raw_line.decode("utf-8")


def test_user_id_falls_back_to_attributes(
    event_payload: dict,
) -> None:
    event_payload["actor"].pop("user_id")
    event_payload["attributes"]["user_id"] = "fallback-user"

    result = extract_indexed_event(
        orjson.dumps(event_payload),
        blob_partition="2026/09/09/08",
        blob_name="events.ndjson",
        source_line=1,
    )

    assert result["user_id"] == "fallback-user"


def test_organization_id_falls_back_to_attributes(
    event_payload: dict,
) -> None:
    event_payload["actor"].pop("organization_id")
    event_payload["attributes"]["organization_id"] = "fallback-org"

    result = extract_indexed_event(
        orjson.dumps(event_payload),
        blob_partition="2026/09/09/08",
        blob_name="events.ndjson",
        source_line=1,
    )

    assert result["organization_id"] == "fallback-org"


def test_event_name_falls_back_to_top_level(
    event_payload: dict,
) -> None:
    event_payload["event"].pop("name")
    event_payload["event_name"] = "FALLBACK_EVENT"

    result = extract_indexed_event(
        orjson.dumps(event_payload),
        blob_partition="2026/09/09/08",
        blob_name="events.ndjson",
        source_line=1,
    )

    assert result["event_name"] == "FALLBACK_EVENT"


def test_category_falls_back_to_top_level(
    event_payload: dict,
) -> None:
    event_payload["event"].pop("category")
    event_payload["category"] = "FALLBACK_CATEGORY"

    result = extract_indexed_event(
        orjson.dumps(event_payload),
        blob_partition="2026/09/09/08",
        blob_name="events.ndjson",
        source_line=1,
    )

    assert result["category"] == "FALLBACK_CATEGORY"


def test_nested_values_have_priority_over_fallbacks(
    event_payload: dict,
) -> None:
    event_payload["attributes"]["user_id"] = "wrong-user"
    event_payload["attributes"]["organization_id"] = "wrong-org"
    event_payload["event_name"] = "WRONG_EVENT"
    event_payload["category"] = "WRONG_CATEGORY"

    result = extract_indexed_event(
        orjson.dumps(event_payload),
        blob_partition="2026/09/09/08",
        blob_name="events.ndjson",
        source_line=1,
    )

    assert result["user_id"] == "user-001"
    assert result["organization_id"] == "org-001"
    assert result["event_name"] == "LOGIN"
    assert result["category"] == "AUTH"


def test_optional_fields_can_be_missing(
    event_payload: dict,
) -> None:
    event_payload.pop("actor")
    event_payload["event"].pop("name")
    event_payload["event"].pop("category")

    result = extract_indexed_event(
        orjson.dumps(event_payload),
        blob_partition="2026/09/09/08",
        blob_name="events.ndjson",
        source_line=1,
    )

    assert result["user_id"] is None
    assert result["organization_id"] is None
    assert result["event_name"] is None
    assert result["category"] is None


def test_non_string_values_are_converted_to_strings(
    event_payload: dict,
) -> None:
    event_payload["event_id"] = 123
    event_payload["actor"]["user_id"] = 456
    event_payload["actor"]["organization_id"] = 789

    result = extract_indexed_event(
        orjson.dumps(event_payload),
        blob_partition="2026/09/09/08",
        blob_name="events.ndjson",
        source_line=1,
    )

    assert result["event_id"] == "123"
    assert result["user_id"] == "456"
    assert result["organization_id"] == "789"


def test_timestamp_with_z_is_parsed_as_utc(
    event_payload: dict,
) -> None:
    event_payload["timestamp"] = "2026-09-09T08:30:00Z"

    result = extract_indexed_event(
        orjson.dumps(event_payload),
        blob_partition="2026/09/09/08",
        blob_name="events.ndjson",
        source_line=1,
    )

    assert result["timestamp"] == datetime(
        2026,
        9,
        9,
        8,
        30,
        tzinfo=UTC,
    )


def test_timestamp_with_offset_is_converted_to_utc(
    event_payload: dict,
) -> None:
    event_payload["timestamp"] = "2026-09-09T11:30:00+03:00"

    result = extract_indexed_event(
        orjson.dumps(event_payload),
        blob_partition="2026/09/09/08",
        blob_name="events.ndjson",
        source_line=1,
    )

    assert result["timestamp"] == datetime(
        2026,
        9,
        9,
        8,
        30,
        tzinfo=UTC,
    )


def test_naive_timestamp_is_interpreted_as_utc(
    event_payload: dict,
) -> None:
    event_payload["timestamp"] = "2026-09-09T08:30:00"

    result = extract_indexed_event(
        orjson.dumps(event_payload),
        blob_partition="2026/09/09/08",
        blob_name="events.ndjson",
        source_line=1,
    )

    assert result["timestamp"] == datetime(
        2026,
        9,
        9,
        8,
        30,
        tzinfo=UTC,
    )


def test_timestamp_must_be_string(
    event_payload: dict,
) -> None:
    event_payload["timestamp"] = 123

    with pytest.raises(
        ValueError,
        match="Event timestamp must be a string",
    ):
        extract_indexed_event(
            orjson.dumps(event_payload),
            blob_partition="2026/09/09/08",
            blob_name="events.ndjson",
            source_line=1,
        )


def test_json_line_must_be_object() -> None:
    with pytest.raises(
        ValueError,
        match="NDJSON line must contain a JSON object",
    ):
        extract_indexed_event(
            orjson.dumps(["not", "an", "object"]),
            blob_partition="2026/09/09/08",
            blob_name="events.ndjson",
            source_line=1,
        )


def test_invalid_json_raises_decode_error() -> None:
    with pytest.raises(orjson.JSONDecodeError):
        extract_indexed_event(
            b"{broken-json",
            blob_partition="2026/09/09/08",
            blob_name="events.ndjson",
            source_line=1,
        )


def test_raw_json_strips_only_line_endings(
    event_payload: dict,
) -> None:
    raw_json = orjson.dumps(event_payload)
    raw_line = raw_json + b"\r\n"

    result = extract_indexed_event(
        raw_line,
        blob_partition="2026/09/09/08",
        blob_name="events.ndjson",
        source_line=1,
    )

    assert result["raw_json"] == raw_json.decode("utf-8")
