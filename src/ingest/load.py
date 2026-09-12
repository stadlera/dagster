"""Stage 5, loading: read the active files of one table and one partition window and hand them to the
table's Writer. Every row carries _business_date, _source_file (manifest id), _load_id (the Dagster run
id) and one _<name> column per source attribute."""

from __future__ import annotations

from pathlib import Path
from typing import Iterator

from ingest.archives import open_streams
from ingest.config import Dataset, Table
from ingest.writers import Batch, WriteContext, WriteResult


def metadata_columns(table: Table) -> dict[str, dict]:
    return {
        "_business_date": {"data_type": "date", "nullable": False},
        "_source_file": {"data_type": "bigint", "nullable": False},
        "_load_id": {"data_type": "text", "nullable": False},
        **{f"_{k}": {"data_type": "text", "nullable": True} for k in table.source.attribute_names},
    }


def load(dataset: Dataset, table: Table, files: list, sql_url: str, load_id: str) -> WriteResult:
    """files: manifest rows (need .id, .local_path, .member, .business_date, .attributes)."""
    column_types = table.writer.column_types(dataset, table)

    def batches() -> Iterator[tuple[dict, Batch]]:
        for f in files:
            meta = {"_business_date": f.business_date, "_source_file": f.id, "_load_id": load_id}
            meta |= {f"_{k}": (f.attributes or {}).get(k) for k in table.source.attribute_names}
            for stream in open_streams(Path(f.local_path), f.member):
                with stream:
                    for batch in table.reader.read(stream, column_types):
                        yield meta, batch

    ctx = WriteContext(dataset, table, sql_url, load_id, metadata_columns(table))
    return table.writer.write(ctx, batches())
