from __future__ import annotations

from datetime import UTC, datetime

import orjson
import pytest
from faker import Faker

from event_search.infrastructure.cache.extractor import extract_indexed_event


def test_extracts_indexed_fields(event_payload: dict) -> None:
    raw_line = orjson.dumps(event_payload)

    result = extract_indexed_event(
        raw_line,
        blob_partition="2026/09/09/08",
        blob_name="events.ndjson",
        source_line=42,
    )

    assert result["event_id"] == event_payload["event_id"]
    assert result["application"] == event_payload["application"]
    assert result["user_id"] == event_payload["actor"]["user_id"]
    assert result["organization_id"] == event_payload["actor"]["organization_id"]
    assert result["event_name"] == event_payload["event"]["name"]
    assert result["category"] == event_payload["event"]["category"]

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
    faker: Faker,
) -> None:
    fallback_user_id = faker.uuid4()

    event_payload["actor"].pop("user_id")
    event_payload["attributes"]["user_id"] = fallback_user_id

    result = extract_indexed_event(
        orjson.dumps(event_payload),
        blob_partition="2026/09/09/08",
        blob_name="events.ndjson",
        source_line=1,
    )

    assert result["user_id"] == fallback_user_id


def test_organization_id_falls_back_to_attributes(
    event_payload: dict,
    faker: Faker,
) -> None:
    fallback_organization_id = faker.uuid4()

    event_payload["actor"].pop("organization_id")
    event_payload["attributes"]["organization_id"] = fallback_organization_id

    result = extract_indexed_event(
        orjson.dumps(event_payload),
        blob_partition="2026/09/09/08",
        blob_name="events.ndjson",
        source_line=1,
    )

    assert result["organization_id"] == fallback_organization_id


def test_event_name_falls_back_to_top_level(
    event_payload: dict,
    faker: Faker,
) -> None:
    fallback_event_name = faker.word()

    event_payload["event"].pop("name")
    event_payload["event_name"] = fallback_event_name

    result = extract_indexed_event(
        orjson.dumps(event_payload),
        blob_partition="2026/09/09/08",
        blob_name="events.ndjson",
        source_line=1,
    )

    assert result["event_name"] == fallback_event_name


def test_category_falls_back_to_top_level(
    event_payload: dict,
    faker: Faker,
) -> None:
    fallback_category = faker.word()

    event_payload["event"].pop("category")
    event_payload["category"] = fallback_category

    result = extract_indexed_event(
        orjson.dumps(event_payload),
        blob_partition="2026/09/09/08",
        blob_name="events.ndjson",
        source_line=1,
    )

    assert result["category"] == fallback_category


def test_nested_values_have_priority_over_fallbacks(
    event_payload: dict,
    faker: Faker,
) -> None:
    event_payload["attributes"]["user_id"] = faker.uuid4()
    event_payload["attributes"]["organization_id"] = faker.uuid4()
    event_payload["event_name"] = faker.word()
    event_payload["category"] = faker.word()

    result = extract_indexed_event(
        orjson.dumps(event_payload),
        blob_partition="2026/09/09/08",
        blob_name="events.ndjson",
        source_line=1,
    )

    assert result["user_id"] == event_payload["actor"]["user_id"]
    assert result["organization_id"] == event_payload["actor"]["organization_id"]
    assert result["event_name"] == event_payload["event"]["name"]
    assert result["category"] == event_payload["event"]["category"]


def test_optional_fields_can_be_missing(
    event_payload: dict,
) -> None:
    event_payload.pop("actor")
    event_payload.pop("application")
    event_payload["event"].pop("name")
    event_payload["event"].pop("category")

    result = extract_indexed_event(
        orjson.dumps(event_payload),
        blob_partition="2026/09/09/08",
        blob_name="events.ndjson",
        source_line=1,
    )

    assert result["application"] is None
    assert result["user_id"] is None
    assert result["organization_id"] is None
    assert result["event_name"] is None
    assert result["category"] is None


def test_non_string_values_are_converted_to_strings(
    event_payload: dict,
    faker: Faker,
) -> None:
    event_id_value = faker.pyint()
    application_value = faker.pyint()
    user_id_value = faker.pyint()
    organization_id_value = faker.pyint()

    event_payload["event_id"] = event_id_value
    event_payload["application"] = application_value
    event_payload["actor"]["user_id"] = user_id_value
    event_payload["actor"]["organization_id"] = organization_id_value

    result = extract_indexed_event(
        orjson.dumps(event_payload),
        blob_partition="2026/09/09/08",
        blob_name="events.ndjson",
        source_line=1,
    )

    assert result["event_id"] == str(event_id_value)
    assert result["application"] == str(application_value)
    assert result["user_id"] == str(user_id_value)
    assert result["organization_id"] == str(organization_id_value)


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

    raw_line = orjson.dumps(event_payload)

    with pytest.raises(
        ValueError,
        match="Event timestamp must be a string",
    ):
        extract_indexed_event(
            raw_line,
            blob_partition="2026/09/09/08",
            blob_name="events.ndjson",
            source_line=1,
        )


def test_json_line_must_be_object() -> None:
    raw_line = orjson.dumps(["not", "an", "object"])

    with pytest.raises(
        ValueError,
        match="NDJSON line must contain a JSON object",
    ):
        extract_indexed_event(
            raw_line,
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
