"""Static description of what we ingest.

Feed    = stage 1, mirroring: one provider connection and the remote paths we copy. Configured upfront.
Table   = one SQL table, composed of one object per stage:
            source        stage 2  which mirrored files, business date, identity, archives, collisions
            checks        stage 3  delivery expectation and any other Check
            partitioning  stage 4  partition granularity and range runs
            reader/writer stage 5  file format in, sink out
Dataset = one feed, its tables, its committed schema and any custom Dagster objects, declared in one
          package under ingest.datasets.<name>.

Asset keys and job names produced by the factory are exposed here (Feed.raw_key, Table.asset_key,
Dataset.sync_schedule_name, Dataset.load_sensor_name) so custom definitions can wire into them.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from pathlib import Path

from dagster import AssetKey, Definitions

from ingest.checks import Check, Delivery
from ingest.partitioning import Daily, Partitioning
from ingest.readers import CsvReader, Reader
from ingest.resources import Remote
from ingest.sources import Source
from ingest.writers import DltWriter, Writer


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
class Table:
    feed: str
    name: str
    source: Source
    checks: tuple[Check, ...] = (Delivery(),)
    partitioning: Partitioning = field(default_factory=Daily)
    reader: Reader = field(default_factory=CsvReader)
    writer: Writer = field(default_factory=DltWriter)

    @property
    def key(self) -> str:
        return f"{self.feed}/{self.name}"

    @property
    def asset_key(self) -> AssetKey:
        return AssetKey(["sql", self.feed, self.name])

    def with_source(self, source: Source) -> Table:
        return replace(self, source=source)

    def with_partitioning(self, partitioning: Partitioning) -> Table:
        return replace(self, partitioning=partitioning)

    def with_reader(self, reader: Reader) -> Table:
        return replace(self, reader=reader)

    def with_writer(self, writer: Writer) -> Table:
        return replace(self, writer=writer)

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
