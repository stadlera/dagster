"""Dagster resources: the remote filesystem, the landing area and the manifest."""

from __future__ import annotations

from datetime import date, datetime, timezone
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


class Landing(ConfigurableResource):
    root: str

    def path(self, dataset_key: str, business_date: date, filename: str, version: int) -> Path:
        suffix = f".v{version}" if version > 1 else ""
        return Path(self.root, dataset_key, f"{business_date:%Y}", f"{business_date:%m}", filename + suffix)


metadata = sa.MetaData()

files = sa.Table(
    "files",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("dataset", sa.String(200), nullable=False, index=True),
    sa.Column("remote_path", sa.String(1000), nullable=False),
    sa.Column("remote_mtime", sa.String(40)),
    sa.Column("size", sa.BigInteger),
    sa.Column("version", sa.Integer, nullable=False, default=1),
    sa.Column("status", sa.String(20), nullable=False),  # downloaded | superseded | ignored
    sa.Column("business_date", sa.Date, index=True),
    sa.Column("local_path", sa.String(1000)),
    sa.Column("sha256", sa.String(64)),
    sa.Column("downloaded_at", sa.DateTime),
    sa.Column("loaded_at", sa.DateTime),
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

    def latest(self, dataset: str) -> dict[str, sa.Row]:
        """Newest known row per remote path (downloaded or ignored)."""
        stmt = sa.select(files).where(files.c.dataset == dataset, files.c.status != "superseded")
        with self.engine().connect() as conn:
            return {row.remote_path: row for row in conn.execute(stmt)}

    def record(self, **row) -> None:
        with self.engine().begin() as conn:
            if row.get("version", 1) > 1:
                conn.execute(
                    sa.update(files)
                    .where(files.c.dataset == row["dataset"], files.c.remote_path == row["remote_path"])
                    .values(status="superseded")
                )
            conn.execute(sa.insert(files).values(**row))

    def file_counts(self, dataset: str, since: date) -> dict[date, int]:
        stmt = (
            sa.select(files.c.business_date, sa.func.count())
            .where(files.c.dataset == dataset, files.c.status == "downloaded", files.c.business_date >= since)
            .group_by(files.c.business_date)
        )
        with self.engine().connect() as conn:
            return dict(conn.execute(stmt).all())

    def pending_loads(self, dataset: str) -> dict[date, list[str]]:
        """Downloaded files not yet loaded, grouped by business date. Used by the load stage."""
        stmt = sa.select(files.c.business_date, files.c.local_path).where(
            files.c.dataset == dataset, files.c.status == "downloaded", files.c.loaded_at.is_(None)
        )
        out: dict[date, list[str]] = {}
        with self.engine().connect() as conn:
            for day, path in conn.execute(stmt):
                out.setdefault(day, []).append(path)
        return out


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)
