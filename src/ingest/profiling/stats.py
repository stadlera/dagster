"""Per-column statistics accumulated over typed arrow batches. Everything is computed with arrow kernels
or numpy on a bounded reservoir; `to_dict` is deterministic so the report can be committed."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from itertools import combinations

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from dlt.common.libs.pyarrow import py_arrow_to_table_schema_columns

NUMERIC = {"bigint", "double", "decimal"}
TEMPORAL = {"date", "timestamp"}
# value quirks worth knowing before a load: counted per column, never changing the proposed type
QUIRKS = {
    "newline": r"[\r\n]",
    "double_quoted": r'^".*"$',
    "quote_doubling": r'""',
    "backslash_escape": r'\\["\\nrt]',
    "surrounding_whitespace": r"^\s|\s$",
    "leading_zeros": r"^-?0\d",
    "thousands_separator": r"^-?\d{1,3}(,\d{3})+(\.\d+)?$",
    "non_ascii": r"[^\x00-\x7F]",
}
# tokens that usually mean "no value" but survive parsing (arrow's csv null list covers "", NA, NULL, n/a, ...)
NULL_LIKE = ("-", "--", ".", "n.a.", "N.A.", "none", "None", "NONE", "#N/A", "N/A", "NA", "null", "NULL", "nan", "NaN")
HISTOGRAM_BINS = 10
COMPACT_EVERY = 32  # unique chunks kept before they are merged


def dlt_column(name: str, arrow_type: pa.DataType) -> dict:
    """dlt column definition for an arrow type (data_type, precision, scale)."""
    col = py_arrow_to_table_schema_columns(pa.schema([pa.field(name, arrow_type)]))[name]
    return {k: v for k, v in col.items() if k not in ("name", "nullable")}


def all_midnight(ts: pa.Array | pa.ChunkedArray) -> bool:
    parts = (pc.hour, pc.minute, pc.second, pc.millisecond, pc.microsecond, pc.nanosecond)
    return all(pc.max(part(ts)).as_py() in (0, None) for part in parts)


def fraction_digits(ts: pa.Array | pa.ChunkedArray) -> int:
    for digits, part in ((9, pc.nanosecond), (6, pc.microsecond), (3, pc.millisecond)):
        if pc.max(part(ts)).as_py():
            return digits
    return 0


@dataclass
class ColumnProfile:
    name: str
    reservoir_size: int = 10_000
    categorical_max: int = 50
    rows: int = 0
    nulls: int = 0
    kinds: Counter = field(default_factory=Counter)  # non-null values per dlt data type
    bases: dict[str, dict] = field(default_factory=dict)  # dlt column per arrow type when the file typed it
    reasons: set[str] = field(default_factory=set)  # from the typing pass, e.g. "leading zeros"
    formats: set[str] = field(default_factory=set)  # date formats that parsed the values
    files: set = field(default_factory=set)
    unique_in_file: dict = field(default_factory=dict)
    counts: Counter | None = field(default_factory=Counter)  # categorical values; None once too many
    int_digits: int = 0
    frac_digits: int = 0
    # numeric
    min: object = None
    max: object = None
    zeros: int = 0
    negatives: int = 0
    # text
    min_len: int | None = None
    max_len: int = 0
    max_bytes: int = 0
    empty: int = 0
    lengths: Counter = field(default_factory=Counter)
    flags: Counter = field(default_factory=Counter)
    null_like: Counter = field(default_factory=Counter)
    # temporal
    timezone: bool | None = None
    fraction_digits: int = 0
    midnight: bool | None = None
    _distinct: list = field(default_factory=list)
    _file: object = None
    _file_chunks: list = field(default_factory=list)
    _file_values: int = 0
    _reservoir: np.ndarray = field(default_factory=lambda: np.empty(0))
    _seen: int = 0
    _rng: np.random.Generator = field(default_factory=lambda: np.random.default_rng(0))

    def add(self, values: pa.Array | pa.ChunkedArray, file, evidence: dict | None, raw=None) -> None:
        """`evidence` comes from the typing pass (kind, reason, digits, format, timezone); None means the
        column was typed by the file itself. `raw` are the original strings of a column typed from text."""
        if file != self._file:
            self._close_file()
            self._file = file
        self.files.add(file)
        self.rows += len(values)
        self.nulls += values.null_count
        if evidence is None:
            base = dlt_column(self.name, values.type)
            self.bases.setdefault(str(values.type), base)
            kind = base["data_type"]
            if kind == "decimal":
                self.int_digits = max(self.int_digits, base["precision"] - base["scale"])
                self.frac_digits = max(self.frac_digits, base["scale"])
        else:
            kind = evidence.get("kind")
            if reason := evidence.get("reason"):
                self.reasons.add(reason)
            if fmt := evidence.get("format"):
                self.formats.add(fmt)
            self.int_digits = max(self.int_digits, evidence.get("int_digits", 0))
            self.frac_digits = max(self.frac_digits, evidence.get("frac_digits", 0))
        non_null = values.drop_null()
        if isinstance(non_null, pa.ChunkedArray):
            non_null = non_null.combine_chunks()
        if kind is None or len(non_null) == 0 or pa.types.is_nested(values.type):
            return
        self.kinds[kind] += len(non_null)
        as_text = non_null if pa.types.is_string(non_null.type) else pc.cast(non_null, pa.string())
        self._track_distinct(as_text)
        if kind in NUMERIC:
            self._numeric(non_null)
        elif kind in TEMPORAL:
            self._temporal(non_null, evidence)
        text = raw.drop_null() if raw is not None else as_text
        if isinstance(text, pa.ChunkedArray):
            text = text.combine_chunks()
        if pa.types.is_large_string(text.type):
            text = pc.cast(text, pa.string())
        self._text(text)

    def _track_distinct(self, text: pa.Array) -> None:
        unique = pc.unique(text)
        self._distinct.append(unique)
        self._file_chunks.append(unique)
        self._file_values += len(text)
        if len(self._distinct) > COMPACT_EVERY:
            self._distinct = [pc.unique(pa.concat_arrays(self._distinct))]
        if self.counts is not None:
            for item in pc.value_counts(text).to_pylist():
                self.counts[item["values"]] += item["counts"]
            if len(self.counts) > self.categorical_max:
                self.counts = None

    def _close_file(self) -> None:
        if self._file is not None and self._file_chunks:
            distinct = pc.count_distinct(pa.concat_arrays(self._file_chunks)).as_py()
            self.unique_in_file[self._file] = distinct == self._file_values
        self._file_chunks, self._file_values = [], 0

    def _numeric(self, v: pa.Array) -> None:
        mm = pc.min_max(v)
        lo, hi = mm["min"].as_py(), mm["max"].as_py()
        self.min = lo if self.min is None else min(self.min, lo)
        self.max = hi if self.max is None else max(self.max, hi)
        as_float = pc.cast(v, pa.float64())
        self.zeros += pc.sum(pc.equal(as_float, 0.0)).as_py() or 0
        self.negatives += pc.sum(pc.less(as_float, 0.0)).as_py() or 0
        self._reserve(as_float.to_numpy(zero_copy_only=False))

    def _reserve(self, arr: np.ndarray) -> None:
        k = self.reservoir_size
        free = k - len(self._reservoir)
        if free > 0:
            self._reservoir = np.concatenate([self._reservoir, arr[:free]])
            self._seen += min(free, len(arr))
            arr = arr[free:]
        if len(arr):  # algorithm R, vectorised per batch
            idx = self._rng.integers(0, self._seen + np.arange(1, len(arr) + 1))
            keep = idx < k
            self._reservoir[idx[keep]] = arr[keep]
            self._seen += len(arr)

    def _temporal(self, v: pa.Array, evidence: dict | None) -> None:
        mm = pc.min_max(v)
        lo, hi = mm["min"].as_py(), mm["max"].as_py()
        self.min = lo if self.min is None else min(self.min, lo)
        self.max = hi if self.max is None else max(self.max, hi)
        if pa.types.is_timestamp(v.type):
            tz = evidence.get("timezone") if evidence else v.type.tz is not None
            self.timezone = bool(tz) if self.timezone is None else self.timezone or bool(tz)
            self.fraction_digits = max(self.fraction_digits, fraction_digits(v))
            midnight = all_midnight(v)
            self.midnight = midnight if self.midnight is None else self.midnight and midnight

    def _text(self, s: pa.Array) -> None:
        lens = pc.utf8_length(s)
        mm = pc.min_max(lens)
        lo, hi = mm["min"].as_py(), mm["max"].as_py()
        self.min_len = lo if self.min_len is None else min(self.min_len, lo)
        self.max_len = max(self.max_len, hi)
        self.max_bytes = max(self.max_bytes, pc.max(pc.binary_length(s)).as_py())
        self.empty += pc.sum(pc.equal(lens, 0)).as_py() or 0
        for item in pc.value_counts(lens).to_pylist():
            self.lengths[item["values"]] += item["counts"]
        for flag, pattern in QUIRKS.items():
            if n := pc.sum(pc.match_substring_regex(s, pattern)).as_py():
                self.flags[flag] += n
        like = s.filter(pc.is_in(s, value_set=pa.array(NULL_LIKE)))
        for item in pc.value_counts(like).to_pylist():
            self.null_like[item["values"]] += item["counts"]

    @property
    def distinct(self) -> int:
        self._close_file()
        return pc.count_distinct(pa.concat_arrays(self._distinct)).as_py() if self._distinct else 0

    def to_dict(self) -> dict:
        values = self.rows - self.nulls
        distinct = self.distinct
        out: dict = {
            "rows": self.rows,
            "nulls": self.nulls,
            "null_ratio": round(self.nulls / self.rows, 4) if self.rows else None,
            "distinct": distinct,
            "unique": distinct == values if values else None,
            "unique_in_file": all(self.unique_in_file.values()) if self.unique_in_file else None,
            "files": len(self.files),
            "kinds": dict(sorted(self.kinds.items())),
        }
        if self.bases:
            out["file_types"] = sorted(self.bases)
        if self.reasons:
            out["reasons"] = sorted(self.reasons)
        if self.formats:
            out["date_formats"] = sorted(self.formats)
        if set(self.kinds) & NUMERIC:
            out["numeric"] = {
                "min": _json(self.min),
                "max": _json(self.max),
                "zeros": self.zeros,
                "negatives": self.negatives,
                "int_digits": self.int_digits,
                "frac_digits": self.frac_digits,
                "histogram": _histogram(self._reservoir),
            }
        if set(self.kinds) & TEMPORAL:
            out["temporal"] = {
                "min": _json(self.min),
                "max": _json(self.max),
                "timezone": self.timezone,
                "fraction_digits": self.fraction_digits,
                "all_midnight": self.midnight,
            }
        if self.lengths:
            out["text"] = {
                "min_len": self.min_len,
                "max_len": self.max_len,
                "max_bytes": self.max_bytes,
                "fixed_length": self.min_len == self.max_len,
                "empty": self.empty,
                "lengths": _length_histogram(self.lengths),
                "flags": dict(sorted(self.flags.items())),
                "null_like": dict(sorted(self.null_like.items())),
            }
        if self.counts is not None and self.counts:
            out["categorical"] = dict(sorted(self.counts.items(), key=lambda kv: (-kv[1], kv[0])))
        return out


KEY_KINDS = {"bigint", "text", "date", "timestamp", "bool"}  # measures (double, decimal) and json never key
KEY_SEPARATOR = "\x1f"
MAX_KEYS_REPORTED = 5


@dataclass
class TableProfile:
    name: str
    parent: str | None = None
    reservoir: int = 10_000
    categorical_max: int = 50
    composite_keys: bool = True
    key_columns_max: int = 12
    rows: int = 0
    files: Counter = field(default_factory=Counter)  # rows per file path, in delivery order
    meta: dict[str, dict] = field(default_factory=dict)  # file path -> business_date, attributes
    columns: dict[str, ColumnProfile] = field(default_factory=dict)
    _retained: dict[str, list[pa.Table]] = field(default_factory=dict)  # key candidate columns per file

    def add(self, batch: pa.Table, file: str, evidence: dict[str, dict], raw: dict[str, pa.Array], meta=None) -> None:
        self.rows += len(batch)
        self.files[file] += len(batch)
        if meta:
            self.meta.setdefault(file, meta)
        for name in batch.column_names:
            if name.startswith("_dlt_"):
                continue
            col = self.columns.setdefault(name, ColumnProfile(name, self.reservoir, self.categorical_max))
            col.add(batch[name], file, evidence.get(name), raw.get(name))
        keyable = [c for c in batch.column_names if not c.startswith("_dlt_") and _keyable(batch[c].type)]
        if keyable:
            self._retained.setdefault(file, []).append(batch.select(keyable))

    # --- keys and volume ---

    def _key_values(self, file: str, cols: tuple[str, ...]) -> pa.Array:
        table = pa.concat_tables(self._retained[file], promote_options="permissive")
        arrays = [pc.cast(table[c].combine_chunks(), pa.string()) for c in cols]
        return arrays[0] if len(arrays) == 1 else pc.binary_join_element_wise(*arrays, KEY_SEPARATOR)

    def _unique_in_every_file(self, cols: tuple[str, ...]) -> bool:
        for file in self.files:
            values = self._key_values(file, cols)
            if pc.count_distinct(values).as_py() != len(values):
                return False
        return True

    def key_candidates(self) -> list[str]:
        """Columns that can take part in a key: no nulls, present in every file, not a measure. A value that
        recurs in every file is not "constant" but a business key seen three times."""
        out = []
        for name, col in self.columns.items():
            kinds = set(col.kinds)
            everywhere = len(col.files) == len(self.files)
            if kinds and kinds <= KEY_KINDS and col.nulls == 0 and everywhere:
                if all(name in t.column_names for parts in self._retained.values() for t in parts):
                    out.append(name)
        out.sort(key=lambda n: (-self.columns[n].distinct, n))  # highest cardinality first
        return out

    def keys(self) -> dict:
        """Column sets unique within every file (file keys), how often their values recur across files
        (business keys) and the churn between consecutive files."""
        candidates = self.key_candidates()
        found: list[tuple[str, ...]] = [(c,) for c in candidates if self._unique_in_every_file((c,))]
        size = 1
        if not found and self.composite_keys and self._retained:
            top = candidates[: self.key_columns_max]
            for size in (2, 3):
                found = [cols for cols in combinations(top, size) if self._unique_in_every_file(cols)]
                if found:
                    break
        report = {"candidate_columns": candidates, "candidates": [], "file_key_metadata": self.file_key_metadata()}
        for cols in found[:MAX_KEYS_REPORTED]:
            per_file = {file: pc.unique(self._key_values(file, cols)) for file in self.files}
            overall = pa.concat_arrays(list(per_file.values()))
            distinct = pc.count_distinct(overall).as_py()
            counts = pc.value_counts(overall)
            recurring = pc.sum(pc.greater(pc.struct_field(counts, "counts"), 1)).as_py() or 0
            churn, previous = [], None
            for file, values in per_file.items():
                entry = {"file": file, "keys": len(values)}
                if previous is not None:
                    entry["new"] = pc.sum(pc.invert(pc.is_in(values, value_set=previous))).as_py() or 0
                    entry["dropped"] = pc.sum(pc.invert(pc.is_in(previous, value_set=values))).as_py() or 0
                churn.append(entry)
                previous = values
            report["candidates"].append(
                {
                    "columns": list(cols),
                    "unique_overall": distinct == len(overall),
                    "repeat_ratio": round(recurring / distinct, 4) if len(self.files) > 1 else None,
                    "churn": churn,
                }
            )
        # the key whose values recur most is the business key; a unique measure (a daily price) comes last
        report["candidates"].sort(key=lambda c: (-(c["repeat_ratio"] or 0), len(c["columns"]), c["columns"]))
        return report

    def file_key_metadata(self) -> list[str]:
        """Metadata columns that tell files of one business date apart: the date itself and every pattern
        attribute that varies within a date."""
        by_date: dict = {}
        for file in self.files:
            m = self.meta.get(file, {})
            by_date.setdefault(str(m.get("business_date")), []).append(m.get("attributes", {}))
        varying = set()
        for attrs in by_date.values():
            if len(attrs) > 1:
                for key in {k for a in attrs for k in a}:
                    if len({a.get(key) for a in attrs}) > 1:
                        varying.add(key)
        return ["_business_date", *(f"_{k}" for k in sorted(varying))]

    def volume(self) -> dict:
        rows = list(self.files.values())
        by_date: dict[str, dict] = {}
        for file, n in self.files.items():
            day = str(self.meta.get(file, {}).get("business_date"))
            entry = by_date.setdefault(day, {"files": 0, "rows": 0})
            entry["files"] += 1
            entry["rows"] += n
        per_date = [d["files"] for d in by_date.values()]
        return {
            "rows_per_file": _spread(rows),
            "files_per_business_date": _spread(per_date),
            "rows_per_business_date": _spread([d["rows"] for d in by_date.values()]),
            "by_business_date": dict(sorted(by_date.items())),
        }

    def to_dict(self, proposed: dict[str, dict]) -> dict:
        columns = {}
        for name, col in self.columns.items():
            data = col.to_dict()
            data["files"] = f"{data['files']}/{len(self.files)}"
            columns[name] = {"proposed": {k: v for k, v in proposed[name].items() if k != "name"}} | data
        out = {
            "rows": self.rows,
            "files": dict(self.files),
            "volume": self.volume(),
            "keys": self.keys(),
            "columns": columns,
        }
        return ({"parent": self.parent} if self.parent else {}) | out


def _keyable(t: pa.DataType) -> bool:
    return not (pa.types.is_floating(t) or pa.types.is_decimal(t) or pa.types.is_nested(t) or pa.types.is_binary(t))


def _spread(values: list[int]) -> dict | None:
    if not values:
        return None
    return {"min": min(values), "max": max(values), "mean": round(sum(values) / len(values), 2), "n": len(values)}


def _json(v):
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, np.generic):
        return v.item()
    return v


def _histogram(reservoir: np.ndarray) -> dict | None:
    if len(reservoir) == 0:
        return None
    counts, edges = np.histogram(reservoir, bins=HISTOGRAM_BINS)
    return {"edges": [round(float(e), 6) for e in edges], "counts": [int(c) for c in counts]}


def _length_histogram(lengths: Counter) -> dict:
    if len(lengths) <= 20:
        return {str(k): v for k, v in sorted(lengths.items())}
    counts, edges = np.histogram(list(lengths.elements()), bins=HISTOGRAM_BINS)
    return {"edges": [int(e) for e in edges], "counts": [int(c) for c in counts]}
