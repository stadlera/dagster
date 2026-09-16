"""Propose a committed schema from landed files."""

import argparse
import os
from datetime import date

from ingest.datasets import discover
from ingest.resources import Manifest
from ingest_tools.profiling import ProfileOptions, profile, summary, write_report, write_schema

SKIM_FILES, SKIM_ROWS = 5, 200_000


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("table", help="<feed>/<name> as declared in its dataset package")
    parser.add_argument("--manifest-url", default=os.environ.get("INGEST_MANIFEST_URL", "sqlite:///manifest.db"))
    parser.add_argument("--files", type=int, help="most recent files to sample (default: every classified file)")
    parser.add_argument("--rows", type=int, help="rows sampled per file (default: every row)")
    parser.add_argument("--skim", action="store_true", help=f"quick pass: --files {SKIM_FILES} --rows {SKIM_ROWS}")
    parser.add_argument(
        "--decimals", action="store_true", help="fractional numbers become decimal(p,s) instead of double"
    )
    parser.add_argument("--narrow", action="store_true", help="int / smallint when the value range (x10) fits")
    parser.add_argument(
        "--strict-nulls", action="store_true", help="columns without nulls in the sample become NOT NULL"
    )
    parser.add_argument("--text-headroom", type=float, default=1.5, help="length bucket chosen for max length x this")
    parser.add_argument("--date-format", action="append", default=[], help="strptime format for text columns")
    parser.add_argument("--no-report", action="store_true", help="write only the schema YAML")
    args = parser.parse_args(argv)
    if args.skim:
        args.files, args.rows = args.files or SKIM_FILES, args.rows or SKIM_ROWS

    try:
        dataset, table = next((d, t) for d in discover() for t in d.tables if t.key == args.table)
    except StopIteration:
        parser.error(f"unknown table {args.table!r}")
    manifest = Manifest(url=args.manifest_url)
    files = manifest.files_for(table.key, date.min, date.max)
    if args.files:
        files = files[-args.files :]
    if not files:
        raise SystemExit(f"no classified files for {table.key}; run the raw asset first")
    options = ProfileOptions(
        max_rows=args.rows,
        decimals=args.decimals,
        narrow=args.narrow,
        strict_nulls=args.strict_nulls,
        text_headroom=args.text_headroom,
        date_formats=tuple(args.date_format),
    )
    result = profile(dataset, table, files, options)
    print(summary(result))
    path = write_schema(result.schema, dataset)
    print(f"sampled {len(files)} files -> {path}")
    if not args.no_report:
        print(f"evidence -> {write_report(result, dataset, table)}")
    print("review, adjust types, commit.")


if __name__ == "__main__":
    main()
