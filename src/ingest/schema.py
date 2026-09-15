"""Committed schemas: one dlt schema YAML per dataset in <dataset>/schemas/import/<feed>.schema.yaml.

`ingest.profiling` proposes one from landed files (run once, review, commit) and writes its evidence to
<dataset>/schemas/profile/<feed>.<table>.profile.json. `committed_types` turns the committed columns into
arrow types so loaders read with them instead of inferring per file.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import yaml
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.libs.pyarrow import get_py_arrow_datatype
from dlt.common.schema import Schema

if TYPE_CHECKING:
    from ingest.config import Dataset, Table

# keep provider column names, only fix characters that are illegal in SQL identifiers. Loads and the
# profiler's denesting share this so column and child table names cannot drift apart.
os.environ.setdefault("SCHEMA__NAMING", "sql_cs_v1")


def schema_path(dataset: Dataset) -> Path:
    return dataset.schema_dir / "import" / f"{dataset.schema_name}.schema.yaml"


def report_path(dataset: Dataset, table: Table) -> Path:
    return dataset.schema_dir / "profile" / f"{dataset.schema_name}.{table.name}.profile.json"


def committed_schema(dataset: Dataset) -> Schema | None:
    path = schema_path(dataset)
    return Schema.from_dict(yaml.safe_load(path.read_text())) if path.exists() else None


def committed_types(dataset: Dataset, table: Table, caps: DestinationCapabilitiesContext) -> dict[str, pa.DataType]:
    """Arrow types of the committed columns, as dlt would map them for the target destination."""
    schema = committed_schema(dataset)
    if schema is None or table.name not in schema.tables:
        return {}
    columns = schema.tables[table.name].get("columns", {})
    return {
        name: get_py_arrow_datatype(col, caps, "UTC")
        for name, col in columns.items()
        if not name.startswith("_") and "data_type" in col
    }


def write_schema(schema: Schema, dataset: Dataset) -> Path:
    path = schema_path(dataset)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(schema.to_pretty_yaml())
    return path
