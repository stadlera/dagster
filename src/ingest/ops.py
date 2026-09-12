"""Operator helpers on the manifest, also as a CLI:

uv run python -m ingest.ops show tradeweb/em 2026-09-08          files of one business date, with ids
uv run python -m ingest.ops reload tradeweb/em 2026-09-08 [END]  mark a date range as not loaded (sensor reloads it)
uv run python -m ingest.ops ignore 17 18                          take files out of loading (status ignored)
uv run python -m ingest.ops reclassify tradeweb/em                forget the table's assignments; the next sync
                                                                  re-runs the (changed) patterns
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta

import sqlalchemy as sa

from ingest.resources import FileStatus, Manifest, files


def reload(manifest: Manifest, table: str, start: date, end: date | None = None) -> int:
    """Clear loaded_at/load_id of active rows in [start, end): the load sensor requests the range again."""
    end = end or start + timedelta(days=1)
    with manifest.engine().begin() as conn:
        return conn.execute(
            sa.update(files)
            .where(
                files.c.table == table,
                files.c.business_date >= start,
                files.c.business_date < end,
                files.c.status == FileStatus.DOWNLOADED,
            )
            .values(loaded_at=None, load_id=None)
        ).rowcount


def ignore(manifest: Manifest, ids: list[int]) -> int:
    """Take files out of loading. Already loaded data is not removed; reload the date if it should go."""
    with manifest.engine().begin() as conn:
        return conn.execute(sa.update(files).where(files.c.id.in_(ids)).values(status=FileStatus.IGNORED)).rowcount


def reclassify(manifest: Manifest, table: str) -> int:
    """Forget table, business date, identity and collision outcome of the table's rows; superseded and
    duplicate rows become active again so the next classification re-decides them with current rules."""
    with manifest.engine().begin() as conn:
        return conn.execute(
            sa.update(files)
            .where(
                files.c.table == table,
                files.c.status.in_([FileStatus.DOWNLOADED, FileStatus.SUPERSEDED, FileStatus.DUPLICATE]),
            )
            .values(
                table=None,
                business_date=None,
                attributes=None,
                identity=None,
                status=FileStatus.DOWNLOADED,
                loaded_at=None,
                load_id=None,
            )
        ).rowcount


def main(argv: list[str] | None = None) -> None:
    from ingest import definitions

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("reload")
    p.add_argument("table")
    p.add_argument("start", type=date.fromisoformat)
    p.add_argument("end", type=date.fromisoformat, nargs="?")
    p = sub.add_parser("ignore")
    p.add_argument("ids", type=int, nargs="+")
    p = sub.add_parser("reclassify")
    p.add_argument("table")
    args = parser.parse_args(argv)

    manifest: Manifest = definitions.defs.resources["manifest"]
    if args.command == "reload":
        print(f"{reload(manifest, args.table, args.start, args.end)} file(s) marked for reload")
    elif args.command == "ignore":
        print(f"{ignore(manifest, args.ids)} file(s) ignored")
    elif args.command == "reclassify":
        print(f"{reclassify(manifest, args.table)} file(s) reset; run the raw asset to classify again")


if __name__ == "__main__":
    main()
