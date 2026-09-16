"""Value types shared by the profiling stages: the kinds a value can have, a dlt column, the typing result
of one column in one batch, the file a batch came from and the options every rule reads."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Literal

import pyarrow as pa
from dlt.common.libs.pyarrow import py_arrow_to_table_schema_columns

Kind = Literal["bigint", "double", "decimal", "date", "timestamp", "bool", "text", "json"]
NUMERIC: frozenset[str] = frozenset({"bigint", "double", "decimal"})
TEMPORAL: frozenset[str] = frozenset({"date", "timestamp"})
KEYABLE: frozenset[str] = frozenset({"bigint", "text", "date", "timestamp", "bool"})  # measures and json never key


@dataclass(frozen=True)
class Column:
    """A dlt column definition: what the profiler proposes and what a typed file (parquet, avro) declares."""

    name: str
    data_type: str
    nullable: bool = True
    precision: int | None = None
    scale: int | None = None
    timezone: bool | None = None
    description: str | None = None

    def dlt(self) -> dict:
        return {k: v for k, v in asdict(self).items() if v is not None}

    @classmethod
    def from_arrow(cls, name: str, arrow_type: pa.DataType) -> Column:
        col = py_arrow_to_table_schema_columns(pa.schema([pa.field(name, arrow_type)]))[name]
        return cls(name, col["data_type"], True, col.get("precision"), col.get("scale"), col.get("timezone"))


@dataclass(frozen=True)
class Typed:
    """One column of one batch after typing: the kind its values have, the evidence needed to size that
    kind, and the values as text for every statistic that works on text (lengths, quirks, distinct)."""

    kind: str | None  # None: no values to type
    text: pa.Array | None = None  # the values as text, nulls kept; None for nested columns
    declared: Column | None = None  # the file's own column (parquet, avro, dlt) when it typed the values
    reason: str | None = None  # why a text column stays text, e.g. "leading zeros"
    format: str | None = None  # the date format that parsed the values
    int_digits: int = 0
    frac_digits: int = 0
    timezone: bool | None = None


@dataclass(frozen=True)
class FileInfo:
    path: str  # manifest path: stable across environments, unlike the id
    id: int
    size: int | None
    business_date: date | None
    attributes: dict = field(default_factory=dict)

    @classmethod
    def of(cls, row) -> FileInfo:
        return cls(row.path, row.id, row.size, row.business_date, dict(row.attributes or {}))

    def to_dict(self) -> dict:
        return asdict(self) | {"business_date": str(self.business_date)}


@dataclass(frozen=True)
class ProfileOptions:
    max_rows: int | None = None  # rows sampled per file; None: every row
    decimals: bool = False  # fractional numbers become decimal(p, s) instead of double
    narrow: bool = False  # integers get precision 16 / 32 when their range (times int_headroom) fits
    strict_nulls: bool = False  # columns without nulls in the sample become nullable: false
    text_headroom: float = 1.5  # bucket chosen for max_len * headroom; fixed-length columns get the exact length
    int_headroom: float = 10.0
    decimal_headroom: int = 2  # integer digits added to the largest value seen
    date_formats: tuple[str, ...] = ()  # tried on text columns after ISO 8601, e.g. "%Y%m%d"
    keys: tuple[str, ...] = ()  # never nullable (the table's Upsert keys)
    categorical_max: int = 50  # value counts are kept for columns with at most this many distinct values
    reservoir: int = 10_000  # values per column kept for histograms
    distinct_max: int = 1_000_000  # distinct values tracked per key set; beyond it distinct counts are not reported
    composite_keys: bool = True  # search pairs and triples of columns that are not unique on their own
    key_columns_max: int = 12  # candidate columns considered for composite keys (highest cardinality first)
    key_sets_max: int = 20  # composite key sets followed across files
    key_repeat_threshold: float = 0.5  # share of key values recurring across files that suggests Upsert
