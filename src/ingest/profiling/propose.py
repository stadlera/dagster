"""Turn a column's statistics into a dlt column. The data type is the narrowest one whose kinds cover
everything seen in the column (WIDEST), then sized from the statistics and the options (REFINE). Every
step is a function of the profile and the options, so a reviewer can trace the YAML back to the report."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Callable

from ingest.profiling.model import Column, ProfileOptions

if TYPE_CHECKING:
    from ingest.profiling.stats import ColumnProfile

TEXT_BUCKETS = (20, 50, 100, 255, 1000, 4000)  # nvarchar lengths; above the last one: unbounded
INT_WIDTHS = ((16, 2**15), (32, 2**31))  # dlt bigint precision in bits -> smallint / int on mssql
TIMESTAMP_PRECISION = {0: 0, 3: 3, 6: 6, 9: 7}  # fractional digits seen -> dlt precision (mssql: up to 7)

# narrowest data type per set of kinds, in widening order: a column is bigint only if every value was one,
# double if any value was, and so on. Kinds outside every set (bigint next to text) fall back to text.
WIDEST: tuple[tuple[str, frozenset[str]], ...] = (
    ("bigint", frozenset({"bigint"})),
    ("decimal", frozenset({"bigint", "decimal"})),
    ("double", frozenset({"bigint", "decimal", "double"})),
    ("date", frozenset({"date"})),
    ("timestamp", frozenset({"date", "timestamp"})),
    ("bool", frozenset({"bool"})),
    ("json", frozenset({"json"})),
    ("text", frozenset({"text"})),
)


def propose_column(p: ColumnProfile, o: ProfileOptions) -> Column:
    nullable = p.name not in o.keys and not (o.strict_nulls and p.rows and p.nulls == 0)
    kinds = frozenset(p.kinds)
    fields = {"description": " / ".join(sorted(p.reasons))} if p.reasons else {}
    if p.declared and kinds <= {c.data_type for c in p.declared.values()}:
        fields |= _declared(p, o)
    elif not kinds:
        fields |= {"data_type": "text", "description": "no values in the sample"}
    else:
        data_type = next((t for t, covers in WIDEST if kinds <= covers), None)
        if data_type is None:
            fields["description"] = "mixed value kinds: " + ", ".join(sorted(kinds))
        fields |= {"data_type": data_type or "text"}
        fields |= REFINE.get(fields["data_type"], _as_is)(p, o)
    return Column(p.name, nullable=nullable, **fields)


def _declared(p: ColumnProfile, o: ProfileOptions) -> dict:
    """The file typed the column itself (parquet, avro, dlt): keep it, widest arrow width wins."""
    declared = list(p.declared.values())
    if len({c.data_type for c in declared}) > 1:
        return {"data_type": "text", "description": "mixed types across files: " + ", ".join(sorted(p.declared))}
    col = max(declared, key=lambda c: (c.precision or 0, c.scale or 0))
    kept = ("data_type", "precision", "scale", "timezone")
    fields = {k: v for k, v in asdict(col).items() if k in kept and v is not None}
    if col.data_type == "bigint":
        fields |= _integer(p, o)
    if col.data_type == "text":
        fields |= _text(p, o)
    return fields


def _as_is(p: ColumnProfile, o: ProfileOptions) -> dict:
    return {}


def _integer(p: ColumnProfile, o: ProfileOptions) -> dict:
    if not o.narrow or p.numeric is None:
        return {}
    magnitude = max(abs(p.numeric.min), abs(p.numeric.max)) * o.int_headroom
    return next(({"precision": bits} for bits, limit in INT_WIDTHS if magnitude < limit), {})


def _decimal(p: ColumnProfile, o: ProfileOptions) -> dict:
    n = p.numeric
    precision, scale = min(38, n.int_digits + o.decimal_headroom + n.frac_digits), n.frac_digits
    if o.decimals:
        return {"precision": precision, "scale": scale}
    return {"data_type": "double", "description": f"decimal({precision},{scale}) with --decimals"}


def _timestamp(p: ColumnProfile, o: ProfileOptions) -> dict:
    t = p.temporal
    precision = TIMESTAMP_PRECISION.get(t.fraction_digits if t else 0, 7)
    return {"timezone": bool(t and t.timezone), "precision": precision or None}


def _text(p: ColumnProfile, o: ProfileOptions) -> dict:
    t = p.text
    if t is None or t.max_len == 0:
        return {"precision": TEXT_BUCKETS[0]}
    if t.min_len == t.max_len and t.varied:
        return {"precision": t.max_len}  # fixed-length identifiers: several values, all of one length
    need = math.ceil(t.max_len * o.text_headroom)
    bucket = next((b for b in TEXT_BUCKETS if need <= b), None)
    return {"precision": bucket} if bucket else {}


REFINE: dict[str, Callable[[ColumnProfile, ProfileOptions], dict]] = {
    "bigint": _integer,
    "decimal": _decimal,
    "timestamp": _timestamp,
    "text": _text,
}


@dataclass(frozen=True)
class Suggestion:
    merge: str | None  # "Upsert" | "ReplaceDay" | None when the sample cannot tell
    keys: list[str] | None
    note: str


def suggest_merge(keys: dict, files: int, threshold: float = 0.5) -> Suggestion:
    """Which merge fits the sample: Upsert on a key whose values recur across files, otherwise replace-by-day.
    `keys` is KeyTracker.report() output, best candidate first."""
    candidates = keys.get("candidates", [])
    if not candidates:
        return Suggestion("ReplaceDay", None, "no column set is unique within every file")
    best = candidates[0]
    if files < 2 or best["repeat_ratio"] is None:
        return Suggestion(None, best["columns"], "recurrence across files unknown")
    merge = "Upsert" if best["repeat_ratio"] >= threshold else "ReplaceDay"
    return Suggestion(merge, best["columns"], f"{best['repeat_ratio']:.0%} of key values recur")
