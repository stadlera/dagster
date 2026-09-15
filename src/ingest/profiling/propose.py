"""Turn a column's statistics into a dlt column definition. Every rule is a function of the profile and
the options, so a reviewer can trace the YAML back to the report."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ingest.profiling.stats import ColumnProfile

TEXT_BUCKETS = (20, 50, 100, 255, 1000, 4000)  # nvarchar lengths; above the last one: unbounded
INT_WIDTHS = ((16, 2**15), (32, 2**31))  # dlt bigint precision in bits -> smallint / int on mssql
TIMESTAMP_PRECISION = {0: 0, 3: 3, 6: 6, 9: 7}  # fractional digits seen -> dlt precision (mssql: up to 7)


@dataclass(frozen=True)
class ProfileOptions:
    max_rows: int = 200_000  # rows sampled per file
    decimals: bool = False  # fractional numbers become decimal(p, s) instead of double
    narrow: bool = False  # integers get precision 16 / 32 when their range (times int_headroom) fits
    strict_nulls: bool = False  # columns without nulls in the sample become nullable: false
    text_headroom: float = 1.5  # bucket chosen for max_len * headroom; fixed-length columns get the exact length
    int_headroom: float = 10.0
    decimal_headroom: int = 2  # integer digits added to the largest value seen
    categorical_max: int = 50  # value counts are kept for columns with at most this many distinct values
    reservoir: int = 10_000  # values per column kept for histograms
    date_formats: tuple[str, ...] = ()  # tried on text columns after ISO 8601, e.g. "%Y%m%d"
    keys: tuple[str, ...] = ()  # never nullable (the table's Upsert keys)
    composite_keys: bool = True  # search pairs and triples when no single column is unique within a file
    key_columns_max: int = 12  # candidate columns considered for composite keys (highest distinct ratio first)
    key_repeat_threshold: float = 0.5  # share of key values recurring across files that suggests Upsert


def suggest_merge(keys: dict, files: int, threshold: float = 0.5) -> dict:
    """Which merge fits the sample: Upsert on a key whose values recur across files, otherwise replace-by-day.
    `keys` is TableProfile.keys() output."""
    candidates = keys.get("candidates", [])
    if not candidates:
        return {"merge": "ReplaceDay", "keys": None, "note": "no column set is unique within every file"}
    best = candidates[0]
    if files < 2:
        return {"merge": None, "keys": best["columns"], "note": "one file sampled: recurrence across files unknown"}
    merge = "Upsert" if best["repeat_ratio"] >= threshold else "ReplaceDay"
    return {"merge": merge, "keys": best["columns"], "note": f"{best['repeat_ratio']:.0%} of key values recur"}


def propose_column(name: str, p: ColumnProfile, o: ProfileOptions) -> dict:
    """dlt column dict plus a `description` when the choice is not the obvious one."""
    nullable = name not in o.keys and not (o.strict_nulls and p.rows and p.nulls == 0)
    col = {"name": name, "nullable": nullable}
    kinds = set(p.kinds)
    description = " / ".join(sorted(p.reasons)) or None
    bases = list(p.bases.values())
    if bases and kinds <= {b["data_type"] for b in bases}:  # typed by the file itself (parquet, avro, dlt)
        if len({b["data_type"] for b in bases}) > 1:
            return col | {"data_type": "text", "description": "mixed types across files: " + ", ".join(sorted(p.bases))}
        base = max(bases, key=lambda b: (b.get("precision", 0), b.get("scale", 0)))  # widest arrow width wins
        if base["data_type"] == "bigint":
            base = base | _narrow(p, o)
        if base["data_type"] == "text":
            base = base | _text_length(p, o)
        return col | base | ({"description": description} if description else {})
    if not kinds:
        return col | {"data_type": "text", "description": "no values in the sample"}
    if kinds <= {"bigint"}:
        return col | {"data_type": "bigint"} | _narrow(p, o)
    if kinds <= {"bigint", "decimal"}:
        precision = min(38, p.int_digits + o.decimal_headroom + p.frac_digits)
        if o.decimals:
            return col | {"data_type": "decimal", "precision": precision, "scale": p.frac_digits}
        return col | {"data_type": "double", "description": f"decimal({precision},{p.frac_digits}) with --decimals"}
    if kinds <= {"bigint", "decimal", "double"}:
        return col | {"data_type": "double"}
    if kinds <= {"date"}:
        return col | {"data_type": "date"} | ({"description": description} if description else {})
    if kinds <= {"date", "timestamp"}:
        precision = TIMESTAMP_PRECISION.get(p.fraction_digits, 7)
        ts = {"data_type": "timestamp", "timezone": bool(p.timezone)} | ({"precision": precision} if precision else {})
        return col | ts | ({"description": description} if description else {})
    if kinds <= {"bool"}:
        return col | {"data_type": "bool"}
    if kinds <= {"json"}:
        return col | {"data_type": "json"}
    if not kinds <= {"text"}:
        description = "mixed value kinds: " + ", ".join(sorted(kinds))
    return col | {"data_type": "text"} | _text_length(p, o) | ({"description": description} if description else {})


def _narrow(p: ColumnProfile, o: ProfileOptions) -> dict:
    if not o.narrow or p.min is None:
        return {}
    magnitude = max(abs(p.min), abs(p.max)) * o.int_headroom
    return next(({"precision": bits} for bits, limit in INT_WIDTHS if magnitude < limit), {})


def _text_length(p: ColumnProfile, o: ProfileOptions) -> dict:
    if p.max_len == 0:
        return {"precision": TEXT_BUCKETS[0]}
    if p.min_len == p.max_len and p.distinct > 1:
        return {"precision": p.max_len}  # fixed-length identifiers: several values, all of one length
    need = math.ceil(p.max_len * o.text_headroom)
    bucket = next((b for b in TEXT_BUCKETS if need <= b), None)
    return {"precision": bucket} if bucket else {}
