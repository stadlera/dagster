"""Load the files of one table and one business date into SQL via dlt.

Every row gets _business_date, _source_file (manifest id), _load_id (the Dagster run id) and one
_<name> column per named group in the select pattern. A reload of a business date replaces that
date (delete-insert on _business_date) unless the table merges on a business key (upsert).
"""

from __future__ import annotations

import os
import re
import tempfile
from datetime import date
from pathlib import Path
from typing import BinaryIO, Iterator

import dlt
import fsspec
import pyarrow as pa

from ingest.config import Table, Upsert

# keep provider column names, only fix characters that are illegal in SQL identifiers
os.environ.setdefault("SCHEMA__NAMING", "sql_cs_v1")
SCHEMA_DIR = Path(__file__).parent.parent.parent / "schemas"

# new tables and columns are added automatically, a changed data type fails the load
CONTRACT = {"tables": "evolve", "columns": "evolve", "data_type": "freeze"}

ARCHIVES = {".zip": "zip", ".tar": "tar", ".tar.gz": "tar", ".tgz": "tar"}


def destination(url: str):
    if url.startswith("mssql"):
        return dlt.destinations.mssql(credentials=url)
    return dlt.destinations.sqlalchemy(credentials=url)


def load(table: Table, day: date, files: list, destination_url: str, dataset_name: str, load_id: str, schema_dir: Path = SCHEMA_DIR) -> dict:
    """files: manifest rows (need .id, .local_path, .attributes). Returns dlt load metrics."""
    upsert = isinstance(table.merge, Upsert)

    @dlt.resource(
        name=table.name,
        # delete-insert works on both mssql and sqlite: rows matching the key of incoming rows are replaced
        write_disposition={"disposition": "merge", "strategy": "delete-insert"},
        primary_key=list(table.merge.keys) if upsert else None,
        merge_key=None if upsert else "_business_date",
        schema_contract=CONTRACT,
    )
    def rows() -> Iterator[pa.Table]:
        for f in files:
            meta = {"_business_date": (day, pa.date32()), "_source_file": (f.id, pa.int64()), "_load_id": (load_id, pa.string())}
            meta |= {f"_{k}": ((f.attributes or {}).get(k), pa.string(), True) for k in table.attribute_names}
            for stream in open_streams(Path(f.local_path)):
                with stream:
                    for batch in table.loader.read(stream):
                        yield _with_metadata(batch, meta)

    pipeline = dlt.pipeline(
        pipeline_name=f"{table.feed}_{table.name}",
        pipelines_dir=tempfile.mkdtemp(prefix="dlt_"),  # state lives in the destination, not on this pod
        destination=destination(destination_url),
        dataset_name=dataset_name,
        import_schema_path=str(schema_dir / "import"),
        export_schema_path=str(schema_dir / "export"),
    )
    info = pipeline.run(rows())
    info.raise_on_failed_jobs()
    counts = pipeline.last_trace.last_normalize_info.row_counts
    return {"load_ids": info.loads_ids, "rows": sum(v for k, v in counts.items() if not k.startswith("_dlt"))}


def open_streams(path: Path) -> Iterator[BinaryIO]:
    """Binary streams for a landed file: archive members via fsspec chaining, compression inferred."""
    name = re.sub(r"\.v\d+$", "", path.name)  # strip our revision suffix
    archive = next((fs for ext, fs in ARCHIVES.items() if name.endswith(ext)), None)
    if archive:
        for f in sorted(fsspec.open_files(f"{archive}://**::file://{path.resolve()}", "rb"), key=lambda f: f.path):
            yield f.open()
    else:
        yield fsspec.open(str(path), "rb", compression=fsspec.utils.infer_compression(name)).open()


def _with_metadata(batch: pa.Table | list[dict], meta: dict) -> pa.Table:
    if not isinstance(batch, pa.Table):
        batch = pa.Table.from_pylist(batch)
    n = len(batch)
    for name, (value, typ, *nullable) in meta.items():
        batch = batch.append_column(pa.field(name, typ, nullable=bool(nullable)), pa.array([value] * n, typ))
    return batch
