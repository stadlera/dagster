"""Stage 5, writing: a Writer takes (metadata, batch) pairs of one table and one load and persists them.

The default DltWriter merges through dlt with the committed schema as contract. Another writer (e.g.
BULK INSERT via a staging file) only has to implement the same two methods.
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Iterator, Protocol

import dlt
import pyarrow as pa
from dlt.common.libs.pyarrow import get_py_arrow_datatype

from ingest.schema import committed_schema, committed_types, schema_path  # also sets the dlt naming convention

if TYPE_CHECKING:
    from ingest.config import Dataset, Table

Batch = pa.Table | list[dict]


@dataclass(frozen=True)
class WriteContext:
    dataset: Dataset
    table: Table
    sql_url: str
    load_id: str
    metadata_columns: dict[str, dict]  # dlt column definitions of the metadata every row carries


@dataclass(frozen=True)
class WriteResult:
    rows: int
    details: dict = field(default_factory=dict)


class Writer(Protocol):
    def column_types(self, dataset: Dataset, table: Table, sql_url: str) -> dict[str, pa.DataType]:
        """Committed types the reader should read with (empty: infer)."""
        ...

    def write(self, ctx: WriteContext, batches: Iterator[tuple[dict, Batch]]) -> WriteResult: ...


@dataclass(frozen=True)
class ReplaceDay:
    """A reload of a business date replaces all rows of that date."""


@dataclass(frozen=True)
class Upsert:
    """Rows with the same business key are replaced."""

    keys: tuple[str, ...]


@dataclass(frozen=True)
class DltWriter:
    merge: ReplaceDay | Upsert = field(default_factory=ReplaceDay)
    # dict rows (json, avro, custom readers): nested objects are flattened into parent__child columns,
    # lists become child tables <table>__<field>, recursively. max_nesting limits the depth: below it,
    # nested values are stored as json text. 0 keeps every nested value as json (one flat table).
    max_nesting: int | None = None
    # new tables and columns are added automatically, a changed data type fails the load
    contract: dict = field(default_factory=lambda: {"tables": "evolve", "columns": "evolve", "data_type": "freeze"})

    @staticmethod
    def destination(url: str):
        if url.startswith("mssql"):
            return dlt.destinations.mssql(credentials=url)
        return dlt.destinations.sqlalchemy(credentials=url)

    def column_types(self, dataset, table, sql_url):
        return committed_types(dataset, table, self.destination(sql_url).capabilities())

    def write(self, ctx: WriteContext, batches) -> WriteResult:
        with tempfile.TemporaryDirectory(prefix="dlt_") as workdir:
            return self._write(ctx, batches, workdir)

    def _write(self, ctx: WriteContext, batches, workdir: str) -> WriteResult:
        dest = self.destination(ctx.sql_url)
        caps = dest.capabilities()
        fields = [
            pa.field(name, get_py_arrow_datatype(col, caps, "UTC"), nullable=col["nullable"])
            for name, col in ctx.metadata_columns.items()
        ]
        upsert = isinstance(self.merge, Upsert)

        @dlt.resource(
            name=ctx.table.name,
            # delete-insert works on both mssql and sqlite: rows matching the key of incoming rows are replaced
            write_disposition={"disposition": "merge", "strategy": "delete-insert"},
            primary_key=list(self.merge.keys) if upsert else None,
            merge_key=None if upsert else "_business_date",
            columns=ctx.metadata_columns,
            schema_contract=self.contract,
            max_table_nesting=self.max_nesting,
        )
        def rows() -> Iterator[Batch]:
            for meta, batch in batches:
                yield with_metadata(batch, meta, fields)

        committed = committed_schema(ctx.dataset)
        pipeline = dlt.pipeline(
            pipeline_name=ctx.dataset.schema_name,
            pipelines_dir=workdir,  # state lives in the destination, not on this pod
            destination=dest,
            dataset_name=ctx.dataset.schema_name,
            # a committed schema is authoritative; without one dlt infers and the profiler seeds the file later
            import_schema_path=str(schema_path(ctx.dataset).parent) if committed else None,
        )
        info = pipeline.run(rows())
        info.raise_on_failed_jobs()
        counts = pipeline.last_trace.last_normalize_info.row_counts
        rows_loaded = sum(v for k, v in counts.items() if not k.startswith("_dlt"))
        details = {"dlt_load_ids": info.loads_ids}
        if committed and ctx.table.name in committed.tables:
            known = set(committed.tables[ctx.table.name].get("columns", {}))
            loaded = set(pipeline.default_schema.get_table_columns(ctx.table.name))
            if new := sorted(loaded - known - set(ctx.metadata_columns) - {"_dlt_load_id", "_dlt_id"}):
                details["new_columns"] = new  # added by the evolve contract; commit them to the schema
        return WriteResult(rows=rows_loaded, details=details)


def with_metadata(batch: Batch, meta: dict, fields: list[pa.Field]) -> Batch:
    """Arrow tables get typed columns appended. Dicts stay dicts so dlt can flatten and unnest them."""
    if not isinstance(batch, pa.Table):
        return [row | meta for row in batch]
    n = len(batch)
    for f in fields:
        batch = batch.append_column(f, pa.array([meta[f.name]] * n, f.type))
    return batch
