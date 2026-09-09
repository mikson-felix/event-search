class EventSearchError(Exception):
    """Base application error."""


class InvalidTimeRangeError(EventSearchError):
    """Invalid user-provided time range."""


class MaterializationError(EventSearchError):
    """Failed to convert source NDJSON into Parquet."""
