"""Retention policy helpers."""

from datetime import UTC, date, datetime


def parse_date(value: str | date | None) -> date | None:
    """Parse a date-like value into ``datetime.date``."""

    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return datetime.fromisoformat(str(value)).date()


def is_retention_expired(
    event_date: str | date | None,
    *,
    retention_days: int,
    as_of: date | None = None,
) -> bool:
    """Return true when event_date + retention_days is before as_of."""

    parsed_event_date = parse_date(event_date)
    if parsed_event_date is None:
        return False
    as_of = as_of or datetime.now(tz=UTC).date()
    age_days = (as_of - parsed_event_date).days
    return age_days > retention_days
