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

LOOKBACK_DAYS = 7


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
    """Every calendar day up to today - lag_days must have between min_files and max_files files."""

    calendar: Calendar = field(default_factory=Exchange)
    lag_days: int = 1
    min_files: int = 1
    max_files: int | None = None
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
        start = last_due - timedelta(days=LOOKBACK_DAYS)
        counts = ctx.manifest.file_counts(ctx.table.key, start)
        bad = {
            d: counts.get(d, 0)
            for d in self.calendar.days(start, last_due)
            if counts.get(d, 0) < self.min_files or (self.max_files is not None and counts.get(d, 0) > self.max_files)
        }
        # only the newest due day is missing: warn, it may just be late. Anything else: error.
        severity = "WARN" if list(bad) == [last_due] and bad[last_due] == 0 else "ERROR"
        return CheckResult(
            passed=not bad, severity=severity, metadata={"violations": {str(d): n for d, n in bad.items()}}
        )


def ExchangeCalendar(name: str = "XLON", lag_days: int = 1, holidays: tuple[date, ...] = ()) -> Delivery:
    """Shorthand: Delivery on an exchange calendar. Chain .with_files(...) to constrain the file count."""
    return Delivery(calendar=Exchange(name, holidays), lag_days=lag_days)


def WeekdayCalendar(lag_days: int = 1, holidays: tuple[date, ...] = ()) -> Delivery:
    return Delivery(calendar=Weekdays(holidays), lag_days=lag_days)
