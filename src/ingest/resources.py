"""Dagster resources: the remote filesystem, the landing area and the manifest."""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from pathlib import Path

import fsspec
import sqlalchemy as sa
from dagster import ConfigurableResource
from pydantic import PrivateAttr


class Remote(ConfigurableResource):
    """Any fsspec filesystem: sftp, file, s3, ... Options are passed through."""

    protocol: str
    options: dict[str, str] = {}

    def fs(self) -> fsspec.AbstractFileSystem:
        return fsspec.filesystem(self.protocol, **self.options)


class Sql(ConfigurableResource):
    """Target database for loaded tables: any sqlalchemy url, mssql+pyodbc://... in production."""

    url: str


class Landing(ConfigurableResource):
    root: str

    def path(self, feed: str, relative: str, version: int) -> Path:
        """Mirror the remote layout: <root>/<feed>/<path relative to the feed path>[.vN]."""
        suffix = f".v{version}" if version > 1 else ""
        return Path(self.root, feed, relative + suffix)


class FileStatus(str, Enum):
    DOWNLOADED = "downloaded"  # present in the landing area, newest version of its remote path
    SUPERSEDED = "superseded"  # replaced by a newer version of the same remote path
    IGNORED = "ignored"  # seen on the remote, excluded by the feed, never downloaded


metadata = sa.MetaData()

files = sa.Table(
    "files",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("feed", sa.String(100), nullable=False, index=True),
    sa.Column("remote_path", sa.String(1000), nullable=False),
    sa.Column("remote_mtime", sa.String(40)),
    sa.Column("size", sa.BigInteger),
    sa.Column("version", sa.Integer, nullable=False, default=1),
    sa.Column("status", sa.Enum(FileStatus, native_enum=False, length=20), nullable=False),
    sa.Column("local_path", sa.String(1000)),
    sa.Column("sha256", sa.String(64)),
    sa.Column("downloaded_at", sa.DateTime),
    # assigned by classify(), possibly long after download
    sa.Column("table", sa.String(200), index=True),
    sa.Column("business_date", sa.Date, index=True),
    sa.Column("attributes", sa.JSON),  # named groups of the matching select pattern
    sa.Column("loaded_at", sa.DateTime),
    sa.Column("load_id", sa.String(100)),
)


class Manifest(ConfigurableResource):
    """Ground truth of every file we have seen and downloaded. sqlite locally, mssql in prod."""

    url: str
    _engine: sa.Engine = PrivateAttr(default=None)

    def engine(self) -> sa.Engine:
        if self._engine is None:
            self._engine = sa.create_engine(self.url)
            metadata.create_all(self._engine)
        return self._engine

    def latest(self, feed: str) -> dict[str, sa.Row]:
        """Newest known row per remote path (downloaded or ignored)."""
        stmt = sa.select(files).where(files.c.feed == feed, files.c.status != FileStatus.SUPERSEDED)
        with self.engine().connect() as conn:
            return {row.remote_path: row for row in conn.execute(stmt)}

    def record(self, **row) -> None:
        with self.engine().begin() as conn:
            if row.get("version", 1) > 1:
                conn.execute(
                    sa.update(files)
                    .where(files.c.feed == row["feed"], files.c.remote_path == row["remote_path"])
                    .values(status=FileStatus.SUPERSEDED)
                )
            conn.execute(sa.insert(files).values(**row))

    def classify(self, tables) -> int:
        """Assign table, business_date and attributes to downloaded rows matching a table's select patterns.

        Raises AmbiguousMatch (after committing the unambiguous ones) if a file matches more than one table.
        """
        assigned, ambiguous = 0, []
        with self.engine().begin() as conn:
            unassigned = conn.execute(
                sa.select(files.c.id, files.c.feed, files.c.remote_path).where(
                    files.c.status == FileStatus.DOWNLOADED, files.c.table.is_(None)
                )
            ).all()
            for row in unassigned:
                matches = [(t, m) for t in tables if t.feed == row.feed for m in [_match(t, row.remote_path)] if m]
                if len(matches) > 1:
                    ambiguous.append((row.remote_path, [t.key for t, _ in matches]))
                    continue
                if matches:
                    t, m = matches[0]
                    groups = m.groupdict()
                    raw_date = groups.pop("date", None) or m.group(1)
                    day = datetime.strptime(raw_date, t.date_format).date()
                    conn.execute(
                        sa.update(files)
                        .where(files.c.id == row.id)
                        .values(table=t.key, business_date=day, attributes=groups)
                    )
                    assigned += 1
        if ambiguous:
            raise AmbiguousMatch(ambiguous)
        return assigned

    def files_for(self, table: str, start: date, end: date | None = None) -> list[sa.Row]:
        """Downloaded files with start <= business_date < end (end defaults to the day after start)."""
        end = end or start + timedelta(days=1)
        stmt = (
            sa.select(files)
            .where(
                files.c.table == table,
                files.c.business_date >= start,
                files.c.business_date < end,
                files.c.status == FileStatus.DOWNLOADED,
            )
            .order_by(files.c.business_date, files.c.id)
        )
        with self.engine().connect() as conn:
            return conn.execute(stmt).all()

    def pending_days(self, table: str) -> dict[date, int]:
        """Business dates with unloaded files -> highest pending file id (changes when a revision arrives)."""
        stmt = (
            sa.select(files.c.business_date, sa.func.max(files.c.id))
            .where(files.c.table == table, files.c.status == FileStatus.DOWNLOADED, files.c.loaded_at.is_(None))
            .group_by(files.c.business_date)
        )
        with self.engine().connect() as conn:
            return dict(sorted(conn.execute(stmt).all()))

    def mark_loaded(self, ids: list[int], load_id: str) -> None:
        with self.engine().begin() as conn:
            conn.execute(sa.update(files).where(files.c.id.in_(ids)).values(loaded_at=utcnow(), load_id=load_id))

    def file_counts(self, table: str, since: date) -> dict[date, int]:
        stmt = (
            sa.select(files.c.business_date, sa.func.count())
            .where(files.c.table == table, files.c.status == FileStatus.DOWNLOADED, files.c.business_date >= since)
            .group_by(files.c.business_date)
        )
        with self.engine().connect() as conn:
            return dict(conn.execute(stmt).all())


def _match(table, remote_path: str) -> re.Match | None:
    if any(re.search(p, remote_path) for p in table.ignore):
        return None
    return next((m for p in table.select if (m := re.search(p, remote_path))), None)


class AmbiguousMatch(Exception):
    def __init__(self, files: list[tuple[str, list[str]]]):
        super().__init__("files match more than one table: " + "; ".join(f"{p} -> {t}" for p, t in files))
        self.files = files


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)
