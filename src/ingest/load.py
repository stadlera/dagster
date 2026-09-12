"""Load the files of one table and one partition window into SQL via dlt.

Every row gets _business_date, _source_file (manifest id), _load_id (the Dagster run id) and one
_<name> column per named group in the select pattern. A reload replaces the business dates it
contains (delete-insert on _business_date) unless the table merges on a business key (Upsert).
"""

from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path
from typing import BinaryIO, Iterator

import dlt
import fsspec
import pyarrow as pa
from dlt.common.libs.pyarrow import get_py_arrow_datatype

from ingest.config import Dataset, Table, Upsert
from ingest.schema import committed_types

# keep provider column names, only fix characters that are illegal in SQL identifiers
os.environ.setdefault("SCHEMA__NAMING", "sql_cs_v1")

# new tables and columns are added automatically, a changed data type fails the load
CONTRACT = {"tables": "evolve", "columns": "evolve", "data_type": "freeze"}

ARCHIVES = {".zip": "zip", ".tar": "tar", ".tar.gz": "tar", ".tgz": "tar"}


def destination(url: str):
    if url.startswith("mssql"):
        return dlt.destinations.mssql(credentials=url)
    return dlt.destinations.sqlalchemy(credentials=url)


def load(dataset: Dataset, table: Table, files: list, destination_url: str, load_id: str) -> dict:
    """files: manifest rows (need .id, .local_path, .business_date, .attributes). Returns dlt load metrics."""
    upsert = isinstance(table.merge, Upsert)
    dest = destination(destination_url)
    caps = dest.capabilities()
    column_types = committed_types(dataset, table, caps)

    metadata_columns = {
        "_business_date": {"data_type": "date", "nullable": False},
        "_source_file": {"data_type": "bigint", "nullable": False},
        "_load_id": {"data_type": "text", "nullable": False},
        **{f"_{k}": {"data_type": "text", "nullable": True} for k in table.attribute_names},
    }
    metadata_fields = [
        pa.field(name, get_py_arrow_datatype(col, caps, "UTC"), nullable=col["nullable"])
        for name, col in metadata_columns.items()
    ]

    @dlt.resource(
        name=table.name,
        # delete-insert works on both mssql and sqlite: rows matching the key of incoming rows are replaced
        write_disposition={"disposition": "merge", "strategy": "delete-insert"},
        primary_key=list(table.merge.keys) if upsert else None,
        merge_key=None if upsert else "_business_date",
        columns=metadata_columns,
        schema_contract=CONTRACT,
    )
    def rows() -> Iterator[pa.Table | list[dict]]:
        for f in files:
            meta = {"_business_date": f.business_date, "_source_file": f.id, "_load_id": load_id}
            meta |= {f"_{k}": (f.attributes or {}).get(k) for k in table.attribute_names}
            for stream in open_streams(Path(f.local_path)):
                with stream:
                    for batch in table.loader.read(stream, column_types):
                        yield with_metadata(batch, meta, metadata_fields)

    pipeline = dlt.pipeline(
        pipeline_name=dataset.schema_name,
        pipelines_dir=tempfile.mkdtemp(prefix="dlt_"),  # state lives in the destination, not on this pod
        destination=dest,
        dataset_name=dataset.schema_name,
        import_schema_path=str(dataset.schema_dir / "import"),
        export_schema_path=str(dataset.schema_dir / "export"),
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


def with_metadata(batch: pa.Table | list[dict], meta: dict, fields: list[pa.Field]) -> pa.Table | list[dict]:
    """Arrow tables get typed columns appended. Dicts stay dicts so dlt can flatten and unnest them."""
    if not isinstance(batch, pa.Table):
        return [row | meta for row in batch]
    n = len(batch)
    for f in fields:
        batch = batch.append_column(f, pa.array([meta[f.name]] * n, f.type))
    return batch
