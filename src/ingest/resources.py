"""Dagster resources: the remote filesystem, the landing area and the manifest."""

from __future__ import annotations

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
    DOWNLOADED = "downloaded"  # active: in the landing area, newest content of its logical identity
    SUPERSEDED = "superseded"  # replaced by a newer version of the same remote path or logical identity
    DUPLICATE = "duplicate"  # same logical identity and same content as an active row, never loaded
    EXPANDED = "expanded"  # an archive whose members are tracked as their own rows
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
    # archive members: parent is the archive row, member the path inside it, remote_path = "<archive>!<member>"
    sa.Column("parent_id", sa.Integer, sa.ForeignKey("files.id")),
    sa.Column("member", sa.String(1000)),
    # assigned by classify(), possibly long after download
    sa.Column("table", sa.String(200), index=True),
    sa.Column("business_date", sa.Date, index=True),
    sa.Column("attributes", sa.JSON),  # named groups of the matching select pattern
    sa.Column("identity", sa.String(500), index=True),  # logical file: same identity = same file, whatever the path
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

    # --- classification storage (the logic is in classify.py) ---

    def unclassified(self, feed: str, top_level: bool = False) -> list[sa.Row]:
        stmt = sa.select(files).where(
            files.c.feed == feed, files.c.status == FileStatus.DOWNLOADED, files.c.table.is_(None)
        )
        if top_level:
            stmt = stmt.where(files.c.parent_id.is_(None))
        with self.engine().connect() as conn:
            return conn.execute(stmt.order_by(files.c.id)).all()

    def add_members(self, archive: sa.Row, members) -> None:
        """One row per archive member (remote_path '<archive>!<member>'); the archive becomes EXPANDED."""
        with self.engine().begin() as conn:
            for m in members:
                conn.execute(
                    sa.insert(files).values(
                        feed=archive.feed,
                        remote_path=f"{archive.remote_path}!{m.name}",
                        remote_mtime=archive.remote_mtime,
                        size=m.size,
                        version=archive.version,
                        status=FileStatus.DOWNLOADED,
                        local_path=archive.local_path,
                        sha256=m.sha256,
                        downloaded_at=archive.downloaded_at,
                        parent_id=archive.id,
                        member=m.name,
                    )
                )
            conn.execute(sa.update(files).where(files.c.id == archive.id).values(status=FileStatus.EXPANDED))

    def assign(self, row_id: int, table: str, day: date, attributes: dict, identity: str, status: FileStatus) -> None:
        with self.engine().begin() as conn:
            conn.execute(
                sa.update(files)
                .where(files.c.id == row_id)
                .values(table=table, business_date=day, attributes=attributes, identity=identity, status=status)
            )

    def active_with_identity(self, identity: str, exclude_id: int) -> list[sa.Row]:
        stmt = sa.select(files.c.id, files.c.sha256).where(
            files.c.identity == identity, files.c.status == FileStatus.DOWNLOADED, files.c.id != exclude_id
        )
        with self.engine().connect() as conn:
            return conn.execute(stmt).all()

    def set_status(self, ids: list[int], status: FileStatus) -> None:
        with self.engine().begin() as conn:
            conn.execute(sa.update(files).where(files.c.id.in_(ids)).values(status=status))

    # --- loading ---

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


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)
