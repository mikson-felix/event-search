from datetime import UTC, datetime
from typing import Any

import orjson


def _get(
    data: dict[str, Any],
    *path: str,
) -> Any:
    current: Any = data

    for key in path:
        if not isinstance(
            current,
            dict,
        ):
            return None

        current = current.get(key)

        if current is None:
            return None

    return current


def _as_optional_string(
    value: Any,
) -> str | None:
    if value is None:
        return None

    return str(value)


def _parse_timestamp(
    value: Any,
) -> datetime:
    if not isinstance(
        value,
        str,
    ):
        raise ValueError("Event timestamp must be a string")

    normalized = value.strip()

    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"

    parsed = datetime.fromisoformat(normalized)

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)

    return parsed.astimezone(UTC)


def extract_indexed_event(
    raw_line: bytes,
    *,
    blob_partition: str,
    blob_name: str,
    source_line: int,
) -> dict[str, Any]:
    event = orjson.loads(raw_line)

    if not isinstance(
        event,
        dict,
    ):
        raise ValueError("NDJSON line must contain a JSON object")

    user_id = _get(
        event,
        "actor",
        "user_id",
    ) or _get(
        event,
        "attributes",
        "user_id",
    )

    organization_id = _get(
        event,
        "actor",
        "organization_id",
    ) or _get(
        event,
        "attributes",
        "organization_id",
    )

    event_name = _get(
        event,
        "event",
        "name",
    ) or event.get("event_name")

    category = _get(
        event,
        "event",
        "category",
    ) or event.get("category")

    return {
        "event_id": _as_optional_string(event.get("event_id")),
        "user_id": _as_optional_string(user_id),
        "organization_id": _as_optional_string(organization_id),
        "event_name": _as_optional_string(event_name),
        "category": _as_optional_string(category),
        "timestamp": _parse_timestamp(event.get("timestamp")),
        "blob_partition": blob_partition,
        "blob_name": blob_name,
        "source_line": source_line,
        "raw_json": (raw_line.decode("utf-8").rstrip("\r\n")),
    }
