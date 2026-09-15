"""Propose a committed schema from landed files:  uv run python -m ingest.profile tradeweb/em [--files 5]"""

import argparse
from datetime import date

from ingest import definitions
from ingest.profiling import ProfileOptions, profile, summary, write_report, write_schema
from ingest.resources import Manifest

parser = argparse.ArgumentParser()
parser.add_argument("table", help="<feed>/<name> as declared in its dataset package")
parser.add_argument("--files", type=int, default=5, help="number of most recent files to sample")
parser.add_argument("--rows", type=int, default=200_000, help="rows sampled per file")
parser.add_argument("--decimals", action="store_true", help="fractional numbers become decimal(p,s) instead of double")
parser.add_argument("--narrow", action="store_true", help="int / smallint when the value range (x10) fits")
parser.add_argument("--strict-nulls", action="store_true", help="columns without nulls in the sample become NOT NULL")
parser.add_argument("--text-headroom", type=float, default=1.5, help="length bucket chosen for max length x this")
parser.add_argument("--date-format", action="append", default=[], help="strptime format for text columns, repeatable")
parser.add_argument("--no-report", action="store_true", help="write only the schema YAML")
args = parser.parse_args()

dataset, table = next((d, t) for d in definitions.datasets for t in d.tables if t.key == args.table)
manifest: Manifest = definitions.defs.resources["manifest"]
files = manifest.files_for(table.key, date.min, date.max)[-args.files :]
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
