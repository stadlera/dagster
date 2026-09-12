"""Stage 3, checks: calendars, delivery expectations and their severity."""

from datetime import date

import pytest

from ingest.checks import (
    CheckContext,
    CheckResult,
    Delivery,
    Exchange,
    ExchangeCalendar,
    Monthly,
    NthWeekday,
    Weekdays,
    Weekly,
    Yearly,
)
from ingest.config import Table
from ingest.resources import Sql
from ingest.sources import Patterns

TABLE = Table("tradeweb", "em", Patterns((r"em-(?P<date>\d{4}-\d{2}-\d{2})\.csv$",)))


class FakeManifest:
    def __init__(self, counts):
        self.counts = counts

    def file_counts(self, table, since):
        return {d: n for d, n in self.counts.items() if d >= since}


def evaluate(check, counts, today):
    return check.evaluate(CheckContext(FakeManifest(counts), Sql(url="sqlite://"), TABLE, today))


def test_calendars():
    sept = (date(2026, 9, 1), date(2026, 9, 30))
    assert Weekdays(holidays=(date(2026, 9, 7),)).days(date(2026, 9, 4), date(2026, 9, 8)) == [
        date(2026, 9, 4),
        date(2026, 9, 8),
    ]
    assert Weekly(weekday=5).days(*sept) == [date(2026, 9, d) for d in (5, 12, 19, 26)]
    assert NthWeekday((1, 3), weekday=5).days(*sept) == [date(2026, 9, 5), date(2026, 9, 19)]
    assert Monthly(day=15).days(date(2026, 8, 1), date(2026, 10, 1)) == [date(2026, 8, 15), date(2026, 9, 15)]
    assert Yearly().days(date(2024, 6, 1), date(2026, 6, 1)) == [date(2025, 1, 1), date(2026, 1, 1)]
    assert date(2026, 12, 25) not in Exchange("XLON").days(date(2026, 12, 20), date(2026, 12, 31))  # exchange holiday


def test_missing_days_respect_calendar_and_lag():
    delivery = Delivery(Weekdays(holidays=(date(2026, 9, 7),)), lag_days=1)
    counts = {date(2026, 9, 8): 1, date(2026, 9, 10): 1}
    # Friday 11th: due through the 10th; 7th holiday, 5th/6th weekend; 5 occurrences: 3,4,8,9,10
    result = evaluate(delivery, counts, today=date(2026, 9, 11))
    assert not result.passed and result.severity == "ERROR"
    assert list(result.metadata["violations"]) == ["2026-09-03", "2026-09-04", "2026-09-09"]


def test_only_the_newest_occurrence_missing_is_a_warning():
    delivery = Delivery(Weekdays(), lag_days=1)
    counts = {date(2026, 9, d): 1 for d in (4, 7, 8, 9)}
    result = evaluate(delivery, counts, today=date(2026, 9, 11))
    assert result.metadata["violations"] == {"2026-09-10": 0} and result.severity == "WARN"


def test_file_count_bounds():
    counts = {date(2026, 9, d): 1 for d in (4, 7, 8, 9, 10)} | {date(2026, 9, 9): 2}
    assert evaluate(Delivery(Weekdays()), counts, date(2026, 9, 11)).passed  # default: at least one
    exact = Delivery(Weekdays()).with_files(exactly=1)
    assert evaluate(exact, counts, date(2026, 9, 11)).metadata["violations"] == {"2026-09-09": 2}
    assert evaluate(Delivery(Weekdays()).with_files(min_files=2), counts, date(2026, 9, 11)).passed is False


def test_monthly_delivery_with_lag_and_the_builder_shorthand():
    delivery = Delivery(Monthly(day=1), lag_days=15, occurrences=2)
    result = evaluate(delivery, {date(2026, 7, 1): 1}, today=date(2026, 9, 10))
    assert result.metadata["violations"] == {"2026-08-01": 0} and result.severity == "WARN"
    assert ExchangeCalendar("XNYS", lag_days=2).with_files(exactly=3) == Delivery(Exchange("XNYS"), 2, 3, 3)


def test_table_check_builders():
    class RowCount:
        name, target = "row_count", "sql"

        def evaluate(self, ctx):
            return CheckResult(True)

    table = TABLE.with_expectation(None)
    assert table.checks == ()
    table = table.with_check(RowCount())
    assert [c.name for c in table.checks] == ["row_count"]
    with pytest.raises(ValueError):
        table.with_check(RowCount())
    assert isinstance(TABLE.with_expectation(Delivery(Weekly())).checks[0], Delivery)
