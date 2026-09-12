"""Checks run as Dagster asset checks. A check needs no Dagster knowledge: it gets a CheckContext
and returns a CheckResult. Delivery expectations (calendar + lag + file count) are the built-in check;
attach more with Table.with_check(...).
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date, timedelta
from typing import TYPE_CHECKING, Literal, Protocol

if TYPE_CHECKING:
    from ingest.config import Table
    from ingest.resources import Manifest, Sql

MAX_LOOKBACK_DAYS = 400  # far enough back to find the last occurrences of a yearly calendar


@dataclass(frozen=True)
class CheckContext:
    manifest: Manifest
    sql: Sql
    table: Table
    today: date
    partition_key: str | None = None  # set for checks targeting the sql asset


@dataclass(frozen=True)
class CheckResult:
    passed: bool
    severity: Literal["WARN", "ERROR"] = "ERROR"
    metadata: dict = field(default_factory=dict)


class Check(Protocol):
    name: str
    target: Literal["raw", "sql"]  # which asset the check is attached to (and runs after)

    def evaluate(self, ctx: CheckContext) -> CheckResult: ...


# --- calendars -----------------------------------------------------------------------------------


class Calendar(Protocol):
    def days(self, start: date, end: date) -> list[date]: ...


@dataclass(frozen=True)
class Weekdays:
    holidays: tuple[date, ...] = ()

    def days(self, start, end):
        days = [start + timedelta(i) for i in range((end - start).days + 1)]
        return [d for d in days if d.weekday() < 5 and d not in self.holidays]


@dataclass(frozen=True)
class Weekly:
    """One business date per week on the given weekday (0 = Monday)."""

    weekday: int = 4
    holidays: tuple[date, ...] = ()

    def days(self, start, end):
        days = [start + timedelta(i) for i in range((end - start).days + 1)]
        return [d for d in days if d.weekday() == self.weekday and d not in self.holidays]


@dataclass(frozen=True)
class NthWeekday:
    """E.g. NthWeekday((1, 3), weekday=5): the first and third Saturday of every month."""

    occurrences: tuple[int, ...]
    weekday: int
    holidays: tuple[date, ...] = ()

    def days(self, start, end):
        days = [start + timedelta(i) for i in range((end - start).days + 1)]
        return [
            d
            for d in days
            if d.weekday() == self.weekday and (d.day - 1) // 7 + 1 in self.occurrences and d not in self.holidays
        ]


@dataclass(frozen=True)
class Monthly:
    """One business date per month, e.g. the period start (day=1) of monthly files."""

    day: int = 1

    def days(self, start, end):
        days = [start + timedelta(i) for i in range((end - start).days + 1)]
        return [d for d in days if d.day == self.day]


@dataclass(frozen=True)
class Yearly:
    month: int = 1
    day: int = 1

    def days(self, start, end):
        return [
            date(y, self.month, self.day)
            for y in range(start.year, end.year + 1)
            if start <= date(y, self.month, self.day) <= end
        ]


@dataclass(frozen=True)
class Exchange:
    """Trading sessions of an exchange_calendars calendar, e.g. XLON, XNYS, XFRA."""

    name: str = "XLON"
    holidays: tuple[date, ...] = ()

    def days(self, start, end):
        import exchange_calendars as xc

        sessions = xc.get_calendar(self.name).sessions_in_range(str(start), str(end))
        return [d.date() for d in sessions if d.date() not in self.holidays]


# --- delivery expectation ------------------------------------------------------------------------


@dataclass(frozen=True)
class Delivery:
    """The last `occurrences` calendar days due by today - lag_days must each have between
    min_files and max_files active files."""

    calendar: Calendar = field(default_factory=Exchange)
    lag_days: int = 1
    min_files: int = 1
    max_files: int | None = None
    occurrences: int = 5
    name: str = "delivery"
    target: Literal["raw", "sql"] = "raw"

    def with_files(self, min_files: int | None = None, max_files: int | None = None, exactly: int | None = None):
        if exactly is not None:
            return replace(self, min_files=exactly, max_files=exactly)
        return replace(self, min_files=self.min_files if min_files is None else min_files, max_files=max_files)

    def with_lag(self, lag_days: int) -> Delivery:
        return replace(self, lag_days=lag_days)

    def evaluate(self, ctx: CheckContext) -> CheckResult:
        last_due = ctx.today - timedelta(days=self.lag_days)
        due = self.calendar.days(last_due - timedelta(days=MAX_LOOKBACK_DAYS), last_due)[-self.occurrences :]
        if not due:
            return CheckResult(passed=True, metadata={"violations": {}})
        counts = ctx.manifest.file_counts(ctx.table.key, due[0])
        bad = {
            d: counts.get(d, 0)
            for d in due
            if counts.get(d, 0) < self.min_files or (self.max_files is not None and counts.get(d, 0) > self.max_files)
        }
        # only the newest due occurrence is missing: warn, it may just be late. Anything else: error.
        severity = "WARN" if list(bad) == [due[-1]] and bad[due[-1]] == 0 else "ERROR"
        return CheckResult(
            passed=not bad, severity=severity, metadata={"violations": {str(d): n for d, n in bad.items()}}
        )


def ExchangeCalendar(name: str = "XLON", lag_days: int = 1, holidays: tuple[date, ...] = ()) -> Delivery:
    """Shorthand: Delivery on an exchange calendar. Chain .with_files(...) to constrain the file count."""
    return Delivery(calendar=Exchange(name, holidays), lag_days=lag_days)


def WeekdayCalendar(lag_days: int = 1, holidays: tuple[date, ...] = ()) -> Delivery:
    return Delivery(calendar=Weekdays(holidays), lag_days=lag_days)
