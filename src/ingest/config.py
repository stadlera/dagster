"""Static description of what we ingest.

Feed    = what we download: one provider connection and the remote paths we mirror. Configured upfront.
Table   = what we load: a selection of mirrored files and how they map to one SQL table. Configured
          once real files have been seen in the manifest.
Dataset = one feed, its tables, its committed schema and any custom Dagster objects, declared in
          one package under ingest.datasets.<name>.

Asset keys and job names produced by the factory are exposed here (Feed.raw_key, Table.asset_key,
Dataset.sync_schedule_name, Dataset.load_sensor_name) so custom definitions can wire into them.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Callable, Literal

from dagster import AssetKey, Definitions

from ingest.checks import Check, Delivery
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

    @property
    def raw_key(self) -> AssetKey:
        return AssetKey(["raw", self.name])


@dataclass(frozen=True)
class ReplaceDay:
    """A reload of a business date replaces all rows of that date."""


@dataclass(frozen=True)
class Upsert:
    """Rows with the same business key are replaced."""

    keys: tuple[str, ...]


@dataclass(frozen=True)
class Table:
    feed: str
    name: str
    # regexes on the remote path. The business date is the named group `date` (or group 1).
    # Other named groups become metadata columns, e.g. (?P<region>emea|apac) -> _region.
    select: tuple[str, ...]
    ignore: tuple[str, ...] = ()  # downloaded, but never loaded into this table
    # archives holding several logical files (e.g. a yearly repack): members are classified individually
    # by the select patterns, so anchor those on the file name, not on the folder
    archives: tuple[str, ...] = ()
    # logical identity of a file: default is business date + named groups. Two files with the same
    # identity are the same file (moved, repacked, restated). Custom: fn(match, remote_path) -> str
    identity: Callable[[re.Match, str], str] | None = None
    on_collision: Literal["latest", "first"] = "latest"  # same identity, different content: which one is active
    date_format: str = "%Y-%m-%d"
    start_date: str = "2026-01-01"
    partition: Literal["daily", "weekly", "monthly", "yearly"] = "daily"  # one load covers the partition window
    max_partitions_per_run: int = 31  # the load sensor groups pending partitions into range runs up to this size
    loader: Loader = field(default_factory=CsvLoader)
    merge: ReplaceDay | Upsert = field(default_factory=ReplaceDay)
    checks: tuple[Check, ...] = (Delivery(),)

    @property
    def key(self) -> str:
        return f"{self.feed}/{self.name}"

    @property
    def asset_key(self) -> AssetKey:
        return AssetKey(["sql", self.feed, self.name])

    @property
    def attribute_names(self) -> tuple[str, ...]:
        """Named groups across all select patterns except `date`; each becomes a _<name> column."""
        names = {g for p in self.select for g in re.compile(p).groupindex if g != "date"}
        return tuple(sorted(names))

    def with_loader(self, loader: Loader) -> Table:
        return replace(self, loader=loader)

    def with_merge(self, merge: ReplaceDay | Upsert) -> Table:
        return replace(self, merge=merge)

    def with_expectation(self, delivery: Delivery | None) -> Table:
        """Replace the delivery expectation; None disables it."""
        others = tuple(c for c in self.checks if not isinstance(c, Delivery))
        return replace(self, checks=others + ((delivery,) if delivery else ()))

    def with_check(self, check: Check) -> Table:
        if any(c.name == check.name for c in self.checks):
            raise ValueError(f"{self.key} already has a check named {check.name}")
        return replace(self, checks=self.checks + (check,))


@dataclass(frozen=True)
class Dataset:
    feed: Feed
    tables: tuple[Table, ...] = ()
    schema_dir: Path = Path("schemas")  # <schema_dir>/import/<feed>.schema.yaml is committed, export/ is generated
    sql_schema: str | None = None  # SQL schema holding all tables of this dataset; defaults to the feed name
    # per-dataset overrides of the shared resources, e.g. EnvVar("X_LANDING_ROOT").get_value()
    landing_root: str | None = None
    sql_url: str | None = None
    extra: Definitions | None = None  # custom assets, sensors or schedules for this dataset

    def __post_init__(self) -> None:
        for t in self.tables:
            if t.feed != self.feed.name:
                raise ValueError(f"table {t.key} does not belong to feed {self.feed.name}")

    @property
    def name(self) -> str:
        return self.feed.name

    @property
    def schema_name(self) -> str:
        """Name of the dlt pipeline and of the committed schema; also the default SQL schema."""
        return self.sql_schema or self.feed.name

    @property
    def sync_schedule_name(self) -> str:
        return f"sync_{self.name}"

    @property
    def load_sensor_name(self) -> str:
        return f"load_{self.name}"
