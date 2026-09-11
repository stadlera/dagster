"""Committed schemas: dlt schema YAML in schemas/import/<feed>_<table>.schema.yaml.

`profile` proposes one from landed files (run once, review, commit). `committed_types` turns the
committed columns into arrow types so loaders read with them instead of inferring per file.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import yaml
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.libs.pyarrow import get_py_arrow_datatype, py_arrow_to_table_schema_columns
from dlt.common.schema import Schema
from dlt.common.schema.utils import new_table

from ingest.config import Table

TEXT_BUCKETS = (20, 50, 100, 255, 1000)
INT = re.compile(r"^-?\d{1,18}$")
DECIMAL = re.compile(r"^-?(\d+)\.(\d+)$")
DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?$")


def schema_name(table: Table) -> str:
    return f"{table.feed}_{table.name}"


def schema_path(table: Table, schema_dir: Path) -> Path:
    return schema_dir / "import" / f"{schema_name(table)}.schema.yaml"


def committed_types(table: Table, schema_dir: Path, caps: DestinationCapabilitiesContext) -> dict[str, pa.DataType]:
    """Arrow types of the committed columns, as dlt would map them for the target destination."""
    path = schema_path(table, schema_dir)
    if not path.exists():
        return {}
    columns = yaml.safe_load(path.read_text())["tables"].get(table.name, {}).get("columns", {})
    return {
        name: get_py_arrow_datatype(col, caps, "UTC")
        for name, col in columns.items()
        if not name.startswith("_") and "data_type" in col
    }


@dataclass
class ColumnStats:
    arrow_type: pa.DataType | None = None  # set when the loader already delivered a typed column
    max_len: int = 0
    int_digits: int = 0
    frac_digits: int = 0
    kinds: set = field(default_factory=set)  # bigint | decimal | date | timestamp | text seen in values
    rows: int = 0
    nulls: int = 0

    def add(self, column: pa.ChunkedArray) -> None:
        self.rows += len(column)
        self.nulls += column.null_count
        if not pa.types.is_string(column.type) and not pa.types.is_large_string(column.type):
            self.arrow_type = column.type
            return
        values = column.drop_null()
        if len(values) == 0:
            return
        self.max_len = max(self.max_len, pc.max(pc.utf8_length(values)).as_py())
        for v in pc.unique(values).to_pylist():
            if INT.match(v):
                self.kinds.add("bigint")
            elif m := DECIMAL.match(v):
                self.kinds.add("decimal")
                self.int_digits = max(self.int_digits, len(m.group(1).lstrip("0")) or 1)
                self.frac_digits = max(self.frac_digits, len(m.group(2)))
            elif DATE.match(v):
                self.kinds.add("date")
            elif TIMESTAMP.match(v):
                self.kinds.add("timestamp")
            else:
                self.kinds.add("text")

    def column(self, name: str) -> dict:
        col = {"name": name, "nullable": True}
        if self.arrow_type is not None:
            return py_arrow_to_table_schema_columns(pa.schema([pa.field(name, self.arrow_type)]))[name] | col
        if self.kinds <= {"bigint"} and self.kinds:
            return col | {"data_type": "bigint"}
        if self.kinds <= {"bigint", "decimal"} and "decimal" in self.kinds:
            return col | {"data_type": "decimal", "precision": self.int_digits + self.frac_digits, "scale": self.frac_digits}
        if self.kinds == {"date"}:
            return col | {"data_type": "date"}
        if self.kinds == {"timestamp"}:
            return col | {"data_type": "timestamp"}
        precision = next((b for b in TEXT_BUCKETS if self.max_len <= b), None)
        return col | {"data_type": "text"} | ({"precision": precision} if precision else {})


def profile(table: Table, files: list, max_rows: int = 200_000) -> Schema:
    """Read up to max_rows per file through the table's loader (all text for CSV) and propose a schema."""
    from ingest.load import open_streams

    stats: dict[str, ColumnStats] = {}
    for f in files:
        rows = 0
        for stream in open_streams(Path(f.local_path)):
            with stream:
                for batch in table.loader.read(stream, column_types="string"):
                    if not isinstance(batch, pa.Table):
                        batch = pa.Table.from_pylist(batch)
                    for name in batch.column_names:
                        col = batch[name]
                        if pa.types.is_nested(col.type):
                            continue  # dlt normalizes nested data itself
                        stats.setdefault(name, ColumnStats()).add(col)
                    rows += len(batch)
                    if rows >= max_rows:
                        break
    schema = Schema(schema_name(table))
    schema.update_table(new_table(table.name, columns=[s.column(name) for name, s in stats.items()]))
    return schema


def write_schema(schema: Schema, table: Table, schema_dir: Path) -> Path:
    path = schema_path(table, schema_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(schema.to_pretty_yaml())
    return path
