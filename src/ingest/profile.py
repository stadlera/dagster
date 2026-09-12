"""Propose a committed schema from landed files:  uv run python -m ingest.profile tradeweb/em [--files 5]"""

import argparse
from datetime import date

from ingest import definitions
from ingest.resources import Manifest
from ingest.schema import profile, write_schema

parser = argparse.ArgumentParser()
parser.add_argument("table", help="<feed>/<name> as declared in its dataset package")
parser.add_argument("--files", type=int, default=5, help="number of most recent files to sample")
parser.add_argument("--rows", type=int, default=200_000, help="rows sampled per file")
args = parser.parse_args()

dataset, table = next((d, t) for d in definitions.datasets for t in d.tables if t.key == args.table)
manifest: Manifest = definitions.defs.resources["manifest"]
files = manifest.files_for(table.key, date.min, date.max)[-args.files :]
if not files:
    raise SystemExit(f"no classified files for {table.key}; run the raw asset first")
path = write_schema(profile(dataset, table, files, args.rows), dataset)
print(f"sampled {len(files)} files -> {path}\nreview, adjust types, commit.")
