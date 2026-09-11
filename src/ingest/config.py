"""Static description of what we ingest. One Dataset per provider sub-path."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from ingest.resources import Remote


@dataclass(frozen=True)
class Dataset:
    provider: str
    name: str
    remote_path: str
    include: str = r".*"
    exclude: str | None = None
    # regex with one group that captures the business date in the file name
    business_date: str = r"(\d{4}-\d{2}-\d{2})"
    date_format: str = "%Y-%m-%d"
    # exchange_calendars name; None means every weekday
    calendar: str | None = "XLON"
    extra_holidays: tuple[date, ...] = ()
    expected_lag_days: int = 1
    expected_files_per_day: int = 1

    @property
    def key(self) -> str:
        return f"{self.provider}/{self.name}"


@dataclass(frozen=True)
class Provider:
    name: str
    remote: Remote
    cron: str
    datasets: list[Dataset] = field(default_factory=list)
