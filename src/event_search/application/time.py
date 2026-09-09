import re
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from tzlocal import get_localzone

from event_search.domain.errors import InvalidTimeRangeError
from event_search.domain.models import TimeRange

_LAST_PATTERN = re.compile(
    r"^last\s+(\d+)\s+"
    r"(minute|minutes|hour|hours|day|days)$"
)


class TimezoneProvider:
    def get(self) -> ZoneInfo:
        return get_localzone()


class TimeRangeResolver:
    def __init__(
        self,
        timezone_provider: TimezoneProvider,
    ) -> None:
        self._timezone_provider = timezone_provider

    def resolve(
        self,
        *,
        shortcut: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> TimeRange:
        if shortcut and (date_from or date_to):
            raise InvalidTimeRangeError("--time cannot be combined with --from or --to")

        local_tz = self._timezone_provider.get()
        now = datetime.now(local_tz)

        if shortcut:
            local_from, local_to = self._resolve_shortcut(
                shortcut=shortcut,
                now=now,
            )
        else:
            local_from, local_to = self._resolve_explicit(
                date_from=date_from,
                date_to=date_to,
                now=now,
                local_tz=local_tz,
            )

        if local_from >= local_to:
            raise InvalidTimeRangeError("Start time must be earlier than end time")

        return TimeRange(
            local_from=local_from,
            local_to=local_to,
            utc_from=local_from.astimezone(UTC),
            utc_to=local_to.astimezone(UTC),
        )

    def _resolve_shortcut(
        self,
        *,
        shortcut: str,
        now: datetime,
    ) -> tuple[datetime, datetime]:
        value = shortcut.strip().lower()

        aliases = {
            "last hour": "last 1 hour",
            "last day": "last 1 day",
            "current date": "today",
        }

        value = aliases.get(
            value,
            value,
        )

        if value == "today":
            start = now.replace(
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            )

            return start, now

        if value == "yesterday":
            today = now.replace(
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            )

            return (
                today - timedelta(days=1),
                today,
            )

        if value == "current hour":
            start = now.replace(
                minute=0,
                second=0,
                microsecond=0,
            )

            return start, now

        match = _LAST_PATTERN.fullmatch(value)

        if match is None:
            raise InvalidTimeRangeError(f"Unsupported time shortcut: {shortcut!r}")

        amount = int(match.group(1))

        if amount <= 0:
            raise InvalidTimeRangeError("Time amount must be greater than zero")

        unit = match.group(2)

        if unit.startswith("minute"):
            delta = timedelta(minutes=amount)

        elif unit.startswith("hour"):
            delta = timedelta(hours=amount)

        else:
            delta = timedelta(days=amount)

        return now - delta, now

    def _resolve_explicit(
        self,
        *,
        date_from: str | None,
        date_to: str | None,
        now: datetime,
        local_tz: ZoneInfo,
    ) -> tuple[datetime, datetime]:
        if date_from is None and date_to is None:
            return (
                now - timedelta(hours=1),
                now,
            )

        if date_from is None:
            raise InvalidTimeRangeError("--to requires --from")

        resolved_from = self._parse_datetime(
            date_from,
            local_tz=local_tz,
        )

        resolved_to = (
            self._parse_datetime(
                date_to,
                local_tz=local_tz,
            )
            if date_to is not None
            else now
        )

        return (
            resolved_from,
            resolved_to,
        )

    @staticmethod
    def _parse_datetime(
        value: str,
        *,
        local_tz: ZoneInfo,
    ) -> datetime:
        normalized = value.strip()

        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"

        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise InvalidTimeRangeError(f"Invalid datetime: {value!r}") from exc

        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=local_tz)

        return parsed.astimezone(local_tz)


class BlobPartitionResolver:
    def resolve(
        self,
        time_range: TimeRange,
    ) -> list[str]:
        cursor = time_range.utc_from.replace(
            minute=0,
            second=0,
            microsecond=0,
        )

        result: list[str] = []

        while cursor < time_range.utc_to:
            result.append(cursor.strftime("%Y/%m/%d/%H"))

            cursor += timedelta(hours=1)

        return result
