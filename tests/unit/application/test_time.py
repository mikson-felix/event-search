from __future__ import annotations

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from event_search.application.time import (
    BlobPartitionResolver,
    TimeRangeResolver,
    TimezoneProvider,
)
from event_search.domain.errors import InvalidTimeRangeError
from event_search.domain.models import TimeRange

LOCAL_TZ = ZoneInfo("Europe/Kyiv")


class FixedTimezoneProvider(TimezoneProvider):
    def get(self) -> ZoneInfo:
        return LOCAL_TZ


@pytest.fixture
def resolver() -> TimeRangeResolver:
    return TimeRangeResolver(
        timezone_provider=FixedTimezoneProvider(),
    )


def assert_duration_close(
    *,
    start: datetime,
    end: datetime,
    expected: timedelta,
    tolerance: timedelta = timedelta(seconds=1),
) -> None:
    actual = end - start

    assert abs(actual - expected) <= tolerance


def test_last_15_minutes(
    resolver: TimeRangeResolver,
) -> None:
    result = resolver.resolve(
        shortcut="last 15 minutes",
    )

    assert_duration_close(
        start=result.local_from,
        end=result.local_to,
        expected=timedelta(minutes=15),
    )


def test_last_hour(
    resolver: TimeRangeResolver,
) -> None:
    result = resolver.resolve(
        shortcut="last hour",
    )

    assert_duration_close(
        start=result.local_from,
        end=result.local_to,
        expected=timedelta(hours=1),
    )


def test_last_two_hours(
    resolver: TimeRangeResolver,
) -> None:
    result = resolver.resolve(
        shortcut="last 2 hours",
    )

    assert_duration_close(
        start=result.local_from,
        end=result.local_to,
        expected=timedelta(hours=2),
    )


def test_last_two_days(
    resolver: TimeRangeResolver,
) -> None:
    result = resolver.resolve(
        shortcut="last 2 days",
    )

    assert_duration_close(
        start=result.local_from,
        end=result.local_to,
        expected=timedelta(days=2),
    )


def test_default_range_is_last_hour(
    resolver: TimeRangeResolver,
) -> None:
    result = resolver.resolve()

    assert_duration_close(
        start=result.local_from,
        end=result.local_to,
        expected=timedelta(hours=1),
    )


def test_today_starts_at_local_midnight(
    resolver: TimeRangeResolver,
) -> None:
    result = resolver.resolve(
        shortcut="today",
    )

    assert result.local_from.hour == 0
    assert result.local_from.minute == 0
    assert result.local_from.second == 0
    assert result.local_from.microsecond == 0

    assert result.local_from.date() == result.local_to.date()

    assert result.local_from.tzinfo == LOCAL_TZ
    assert result.local_to.tzinfo == LOCAL_TZ


def test_current_date_is_alias_for_today(
    resolver: TimeRangeResolver,
) -> None:
    today = resolver.resolve(
        shortcut="today",
    )

    current_date = resolver.resolve(
        shortcut="current date",
    )

    assert current_date.local_from == today.local_from

    assert abs(current_date.local_to - today.local_to) <= timedelta(seconds=1)


def test_yesterday_is_previous_calendar_day(
    resolver: TimeRangeResolver,
) -> None:
    result = resolver.resolve(
        shortcut="yesterday",
    )

    assert result.local_from.hour == 0
    assert result.local_from.minute == 0

    assert result.local_to.hour == 0
    assert result.local_to.minute == 0

    assert result.local_to.date() - result.local_from.date() == timedelta(days=1)


def test_current_hour_starts_at_beginning_of_hour(
    resolver: TimeRangeResolver,
) -> None:
    result = resolver.resolve(
        shortcut="current hour",
    )

    assert result.local_from.minute == 0
    assert result.local_from.second == 0
    assert result.local_from.microsecond == 0

    assert result.local_from.hour == result.local_to.hour

    assert result.local_from.date() == result.local_to.date()


def test_naive_explicit_datetime_uses_local_timezone(
    resolver: TimeRangeResolver,
) -> None:
    result = resolver.resolve(
        date_from="2026-09-09T10:00:00",
        date_to="2026-09-09T12:00:00",
    )

    assert result.local_from == datetime(
        2026,
        9,
        9,
        10,
        0,
        tzinfo=LOCAL_TZ,
    )

    assert result.local_to == datetime(
        2026,
        9,
        9,
        12,
        0,
        tzinfo=LOCAL_TZ,
    )


def test_explicit_utc_datetime_is_converted_to_local_timezone(
    resolver: TimeRangeResolver,
) -> None:
    result = resolver.resolve(
        date_from="2026-09-09T07:00:00Z",
        date_to="2026-09-09T09:00:00Z",
    )

    assert result.local_from == datetime(
        2026,
        9,
        9,
        10,
        0,
        tzinfo=LOCAL_TZ,
    )

    assert result.local_to == datetime(
        2026,
        9,
        9,
        12,
        0,
        tzinfo=LOCAL_TZ,
    )


def test_local_datetime_is_converted_to_utc(
    resolver: TimeRangeResolver,
) -> None:
    result = resolver.resolve(
        date_from="2026-09-09T10:00:00",
        date_to="2026-09-09T12:00:00",
    )

    assert result.utc_from == datetime(
        2026,
        9,
        9,
        7,
        0,
        tzinfo=UTC,
    )

    assert result.utc_to == datetime(
        2026,
        9,
        9,
        9,
        0,
        tzinfo=UTC,
    )


def test_explicit_offset_is_respected(
    resolver: TimeRangeResolver,
) -> None:
    result = resolver.resolve(
        date_from="2026-09-09T11:00:00+03:00",
        date_to="2026-09-09T12:00:00+03:00",
    )

    assert result.utc_from == datetime(
        2026,
        9,
        9,
        8,
        0,
        tzinfo=UTC,
    )

    assert result.utc_to == datetime(
        2026,
        9,
        9,
        9,
        0,
        tzinfo=UTC,
    )


def test_shortcut_and_from_cannot_be_combined(
    resolver: TimeRangeResolver,
) -> None:
    with pytest.raises(
        InvalidTimeRangeError,
        match="--time cannot be combined",
    ):
        resolver.resolve(
            shortcut="last hour",
            date_from="2026-09-09T10:00:00",
        )


def test_shortcut_and_to_cannot_be_combined(
    resolver: TimeRangeResolver,
) -> None:
    with pytest.raises(
        InvalidTimeRangeError,
        match="--time cannot be combined",
    ):
        resolver.resolve(
            shortcut="last hour",
            date_to="2026-09-09T12:00:00",
        )


def test_to_without_from_is_invalid(
    resolver: TimeRangeResolver,
) -> None:
    with pytest.raises(
        InvalidTimeRangeError,
        match="--to requires --from",
    ):
        resolver.resolve(
            date_to="2026-09-09T12:00:00",
        )


def test_from_after_to_is_invalid(
    resolver: TimeRangeResolver,
) -> None:
    with pytest.raises(
        InvalidTimeRangeError,
        match="Start time must be earlier than end time",
    ):
        resolver.resolve(
            date_from="2026-09-09T12:00:00",
            date_to="2026-09-09T10:00:00",
        )


def test_equal_from_and_to_is_invalid(
    resolver: TimeRangeResolver,
) -> None:
    with pytest.raises(
        InvalidTimeRangeError,
        match="Start time must be earlier than end time",
    ):
        resolver.resolve(
            date_from="2026-09-09T10:00:00",
            date_to="2026-09-09T10:00:00",
        )


def test_invalid_datetime_is_rejected(
    resolver: TimeRangeResolver,
) -> None:
    with pytest.raises(
        InvalidTimeRangeError,
        match="Invalid datetime",
    ):
        resolver.resolve(
            date_from="not-a-datetime",
        )


def test_invalid_shortcut_is_rejected(
    resolver: TimeRangeResolver,
) -> None:
    with pytest.raises(
        InvalidTimeRangeError,
        match="Unsupported time shortcut",
    ):
        resolver.resolve(
            shortcut="last potato",
        )


def test_zero_relative_range_is_rejected(
    resolver: TimeRangeResolver,
) -> None:
    with pytest.raises(
        InvalidTimeRangeError,
        match="Time amount must be greater than zero",
    ):
        resolver.resolve(
            shortcut="last 0 hours",
        )


def test_partition_resolver_single_hour() -> None:
    resolver = BlobPartitionResolver()

    time_range = TimeRange(
        local_from=datetime(
            2026,
            9,
            9,
            8,
            10,
            tzinfo=UTC,
        ),
        local_to=datetime(
            2026,
            9,
            9,
            8,
            50,
            tzinfo=UTC,
        ),
        utc_from=datetime(
            2026,
            9,
            9,
            8,
            10,
            tzinfo=UTC,
        ),
        utc_to=datetime(
            2026,
            9,
            9,
            8,
            50,
            tzinfo=UTC,
        ),
    )

    assert resolver.resolve(time_range) == [
        "2026/09/09/08",
    ]


def test_partition_resolver_crosses_multiple_hours() -> None:
    resolver = BlobPartitionResolver()

    time_range = TimeRange(
        local_from=datetime(
            2026,
            9,
            9,
            8,
            55,
            tzinfo=UTC,
        ),
        local_to=datetime(
            2026,
            9,
            9,
            10,
            5,
            tzinfo=UTC,
        ),
        utc_from=datetime(
            2026,
            9,
            9,
            8,
            55,
            tzinfo=UTC,
        ),
        utc_to=datetime(
            2026,
            9,
            9,
            10,
            5,
            tzinfo=UTC,
        ),
    )

    assert resolver.resolve(time_range) == [
        "2026/09/09/08",
        "2026/09/09/09",
        "2026/09/09/10",
    ]


def test_partition_resolver_crosses_year_boundary() -> None:
    resolver = BlobPartitionResolver()

    time_range = TimeRange(
        local_from=datetime(
            2026,
            12,
            31,
            23,
            55,
            tzinfo=UTC,
        ),
        local_to=datetime(
            2027,
            1,
            1,
            0,
            5,
            tzinfo=UTC,
        ),
        utc_from=datetime(
            2026,
            12,
            31,
            23,
            55,
            tzinfo=UTC,
        ),
        utc_to=datetime(
            2027,
            1,
            1,
            0,
            5,
            tzinfo=UTC,
        ),
    )

    assert resolver.resolve(time_range) == [
        "2026/12/31/23",
        "2027/01/01/00",
    ]
