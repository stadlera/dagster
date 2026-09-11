"""Static description of what we ingest.

Feed  = what we download: one provider connection and the remote paths we mirror. Configured upfront.
Table = what we load: a selection of mirrored files and how they map to one SQL table. Configured
        once real files have been seen in the manifest.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, replace

from ingest.delivery import ExchangeCalendar, Expectation
from ingest.loaders import CsvLoader, Loader
from ingest.resources import Remote


@dataclass(frozen=True)
class Feed:
    name: str
    remote: Remote
    cron: str
    paths: tuple[str, ...]
    maxdepth: int | None = 1
    exclude: str | None = None  # files matching this are never downloaded


@dataclass(frozen=True)
class ReplaceDay:
    """A reload of a business date replaces all rows of that date."""


@dataclass(frozen=True)
class Upsert:
    keys: tuple[str, ...]


@dataclass(frozen=True)
class Table:
    feed: str
    name: str
    # regexes on the remote path. The business date is the named group `date` (or group 1).
    # Other named groups become metadata columns, e.g. (?P<region>emea|apac) -> _region.
    select: tuple[str, ...]
    ignore: tuple[str, ...] = ()  # downloaded, but never loaded into this table
    date_format: str = "%Y-%m-%d"
    start_date: str = "2026-01-01"
    partition: str = "daily"  # daily | monthly: one load run covers all files with a business date in the window
    loader: Loader = field(default_factory=CsvLoader)
    expectation: Expectation = field(default_factory=ExchangeCalendar)
    merge: ReplaceDay | Upsert = field(default_factory=ReplaceDay)

    @property
    def key(self) -> str:
        return f"{self.feed}/{self.name}"

    @property
    def attribute_names(self) -> tuple[str, ...]:
        """Named groups across all select patterns except `date`; each becomes a _<name> column."""
        names = {g for p in self.select for g in re.compile(p).groupindex if g != "date"}
        return tuple(sorted(names))

    def with_loader(self, loader: Loader) -> Table:
        return replace(self, loader=loader)

    def with_expectation(self, expectation: Expectation) -> Table:
        return replace(self, expectation=expectation)

    def with_merge(self, merge: ReplaceDay | Upsert) -> Table:
        return replace(self, merge=merge)
