"""Expectations: which business days should have delivered files, and are any missing."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Protocol

LOOKBACK_DAYS = 7


class Expectation(Protocol):
    lag_days: int

    def missing_days(self, counts: dict[date, int], today: date) -> list[date]: ...


@dataclass(frozen=True)
class NoExpectation:
    lag_days: int = 0

    def missing_days(self, counts, today):
        return []


class CalendarExpectation:
    """Shared logic: business days in the look-back window with fewer files than expected."""

    lag_days: int
    files_per_day: int

    def expected_days(self, start: date, end: date) -> list[date]:
        raise NotImplementedError

    def missing_days(self, counts: dict[date, int], today: date) -> list[date]:
        last_due = today - timedelta(days=self.lag_days)
        days = self.expected_days(last_due - timedelta(days=LOOKBACK_DAYS), last_due)
        return [d for d in days if counts.get(d, 0) < self.files_per_day]


@dataclass(frozen=True)
class Weekdays(CalendarExpectation):
    lag_days: int = 1
    files_per_day: int = 1
    holidays: tuple[date, ...] = ()

    def expected_days(self, start, end):
        days = [start + timedelta(i) for i in range((end - start).days + 1)]
        return [d for d in days if d.weekday() < 5 and d not in self.holidays]


@dataclass(frozen=True)
class ExchangeCalendar(CalendarExpectation):
    """Trading sessions of an exchange_calendars calendar, e.g. XLON, XNYS, XFRA."""

    name: str = "XLON"
    lag_days: int = 1
    files_per_day: int = 1
    holidays: tuple[date, ...] = ()

    def expected_days(self, start, end):
        import exchange_calendars as xc

        sessions = xc.get_calendar(self.name).sessions_in_range(str(start), str(end))
        return [d.date() for d in sessions if d.date() not in self.holidays]
