"""Compare what arrived against the delivery calendar."""

from __future__ import annotations

from datetime import date, timedelta

from ingest.config import Dataset

LOOKBACK_DAYS = 7


def expected_days(dataset: Dataset, start: date, end: date) -> list[date]:
    if dataset.calendar:
        import exchange_calendars as xc

        days = [d.date() for d in xc.get_calendar(dataset.calendar).sessions_in_range(str(start), str(end))]
    else:
        days = [start + timedelta(i) for i in range((end - start).days + 1)]
        days = [d for d in days if d.weekday() < 5]
    return [d for d in days if d not in dataset.extra_holidays]


def missing_days(dataset: Dataset, counts: dict[date, int], today: date) -> list[date]:
    """Business days whose files should have arrived by today but have not."""
    last_due = today - timedelta(days=dataset.expected_lag_days)
    days = expected_days(dataset, last_due - timedelta(days=LOOKBACK_DAYS), last_due)
    return [d for d in days if counts.get(d, 0) < dataset.expected_files_per_day]
