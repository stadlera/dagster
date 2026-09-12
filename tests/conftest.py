"""Shared fixture: a local directory acts as the provider's remote, sqlite as manifest and warehouse."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path

import fsspec
import pytest
import sqlalchemy as sa

from ingest.classify import classify
from ingest.config import Dataset, Feed, Table
from ingest.load import load
from ingest.resources import Landing, Manifest, Remote, files
from ingest.sources import Patterns
from ingest.sync import sync

DAILY = Patterns((r"/EM/em-(?P<date>\d{4}-\d{2}-\d{2})\.csv(\.zip|\.gz)?$",))
CSV_08 = b"isin,price\nXS1,1.0\n"
CSV_09 = b"isin,price\nXS1,2.0\nXS2,3.0\n"


@dataclass
class Workspace:
    tmp: Path
    remote: Path  # the provider's "EM" directory
    feed: Feed
    landing: Landing
    manifest: Manifest

    @property
    def sql_url(self) -> str:
        return f"sqlite:///{self.tmp}/warehouse.db"

    def table(self, name="em", source=DAILY, **kw) -> Table:
        return Table(self.feed.name, name, source, **kw)

    def dataset(self, *tables: Table, feed: Feed | None = None) -> Dataset:
        return Dataset(feed or self.feed, tables, schema_dir=self.tmp / "schemas")

    def write_remote(self, relative: str, content: bytes, bump_mtime: bool = False) -> Path:
        path = self.remote / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        if bump_mtime:  # same second as the previous write would look unchanged to the sync
            t = time.time() + 10
            os.utime(path, (t, t))
        return path

    def sync(self, feed: Feed | None = None):
        return sync(fsspec.filesystem("file"), feed or self.feed, self.landing, self.manifest)

    def classify(self, *tables: Table) -> int:
        return classify(self.manifest, self.dataset(*tables))

    def load(self, table: Table, start: date, end: date | None = None, load_id: str = "run-1"):
        rows = self.manifest.files_for(table.key, start, end)
        return load(self.dataset(table), table, rows, self.sql_url, load_id)

    def query(self, sql: str, schema: str = "tradeweb") -> list[tuple]:
        # dlt's sqlalchemy destination keeps one sqlite file per SQL schema (= dataset name)
        with sa.create_engine(f"sqlite:///{self.tmp}/warehouse__{schema}.db").connect() as conn:
            return [tuple(r) for r in conn.execute(sa.text(sql)).all()]

    def statuses(self) -> dict[str, str]:
        """status by path relative to the remote dir; archive members appear as '<archive>!<member>'"""
        with self.manifest.engine().connect() as conn:
            rows = conn.execute(sa.select(files.c.remote_path, files.c.status)).all()
        return {p.replace(f"{self.remote}/", ""): st.value for p, st in rows}


@pytest.fixture
def ws(tmp_path) -> Workspace:
    remote = tmp_path / "remote" / "EM"
    remote.mkdir(parents=True)
    (remote / "em-2026-09-08.csv").write_bytes(CSV_08)
    (remote / "em-2026-09-09.csv").write_bytes(CSV_09)
    (remote / "em.csv").write_bytes(CSV_09)  # static "latest" copy, excluded by the feed
    feed = Feed("tradeweb", Remote(protocol="file"), "0 7 * * 1-5", (str(remote),), exclude=r"^em\.csv$")
    return Workspace(
        tmp=tmp_path,
        remote=remote,
        feed=feed,
        landing=Landing(root=str(tmp_path / "landing")),
        manifest=Manifest(url=f"sqlite:///{tmp_path}/manifest.db"),
    )


def deeper(feed: Feed, maxdepth: int = 3) -> Feed:
    return replace(feed, maxdepth=maxdepth)
