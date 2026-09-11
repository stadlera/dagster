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
from ingest.schema import committed_types, schema_name

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


def load(table: Table, files: list, destination_url: str, dataset_name: str, load_id: str, schema_dir: Path = SCHEMA_DIR) -> dict:
    """files: manifest rows (need .id, .local_path, .business_date, .attributes). Returns dlt load metrics."""
    upsert = isinstance(table.merge, Upsert)
    dest = destination(destination_url)
    column_types = committed_types(table, schema_dir, dest.capabilities())

    columns = {
        "_business_date": {"data_type": "date", "nullable": False},
        "_source_file": {"data_type": "bigint", "nullable": False},
        "_load_id": {"data_type": "text", "nullable": False},
        **{f"_{k}": {"data_type": "text", "nullable": True} for k in table.attribute_names},
    }

    @dlt.resource(
        name=table.name,
        # delete-insert works on both mssql and sqlite: rows matching the key of incoming rows are replaced
        write_disposition={"disposition": "merge", "strategy": "delete-insert"},
        primary_key=list(table.merge.keys) if upsert else None,
        merge_key=None if upsert else "_business_date",
        columns=columns,
        schema_contract=CONTRACT,
    )
    def rows() -> Iterator[pa.Table | list[dict]]:
        for f in files:
            meta = {"_business_date": f.business_date, "_source_file": f.id, "_load_id": load_id}
            meta |= {f"_{k}": (f.attributes or {}).get(k) for k in table.attribute_names}
            for stream in open_streams(Path(f.local_path)):
                with stream:
                    for batch in table.loader.read(stream, column_types):
                        yield _with_metadata(batch, meta, columns)

    pipeline = dlt.pipeline(
        pipeline_name=schema_name(table),
        pipelines_dir=tempfile.mkdtemp(prefix="dlt_"),  # state lives in the destination, not on this pod
        destination=dest,
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


ARROW_TYPES = {"date": pa.date32(), "bigint": pa.int64(), "text": pa.string()}


def _with_metadata(batch: pa.Table | list[dict], meta: dict, columns: dict) -> pa.Table | list[dict]:
    """Arrow tables get typed columns appended. Dicts stay dicts so dlt can flatten and unnest them."""
    if not isinstance(batch, pa.Table):
        return [row | meta for row in batch]
    n = len(batch)
    for name, value in meta.items():
        typ = ARROW_TYPES[columns[name]["data_type"]]
        batch = batch.append_column(pa.field(name, typ, nullable=columns[name]["nullable"]), pa.array([value] * n, typ))
    return batch
