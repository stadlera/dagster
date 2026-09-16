"""Per-column and per-table statistics accumulated over typed batches.

A column profile is the sum of its batches: each kind of statistic is a value computed from one array
(`of`) and merged with `+`. Memory is bounded per column (a value sample for the histogram, value counts
up to categorical_max, distinct values in the key tracker up to distinct_max), so every row of every file
can be profiled. `to_dict` is deterministic so the report can be committed."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import TypeVar

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from ingest_tools.profiling.kernel.arrow import all_midnight, flat, fraction_digits
from ingest_tools.profiling.kernel.keys import KeyTracker
from ingest_tools.profiling.kernel.model import NUMERIC, TEMPORAL, Column, FileInfo, ProfileOptions, Sampled, Typed

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

S = TypeVar("S")


def merge(a: S | None, b: S | None) -> S | None:
    return a + b if a is not None and b is not None else a if b is None else b


@dataclass(frozen=True)
class NumericStats:
    min: float
    max: float
    zeros: int
    negatives: int
    int_digits: int  # of the widest value written, or declared by the file
    frac_digits: int
    sample: np.ndarray  # uniform sample of at most `size` finite values, for the histogram
    seen: int  # finite values the sample was drawn from
    size: int

    @classmethod
    def of(cls, v: pa.Array, typed: Typed, size: int) -> NumericStats | None:
        values = pc.cast(v, pa.float64()).to_numpy(zero_copy_only=False)
        values = values[np.isfinite(values)]
        if len(values) == 0:
            return None
        sample = values if len(values) <= size else _rng(len(values)).choice(values, size, replace=False)
        int_digits, frac_digits = typed.int_digits, typed.frac_digits
        if typed.declared and typed.declared.data_type == "decimal":
            int_digits, frac_digits = typed.declared.precision - typed.declared.scale, typed.declared.scale
        zeros, negatives = int((values == 0).sum()), int((values < 0).sum())
        return cls(values.min(), values.max(), zeros, negatives, int_digits, frac_digits, sample, len(values), size)

    def __add__(self, o: NumericStats) -> NumericStats:
        seen = self.seen + o.seen
        if len(self.sample) + len(o.sample) <= self.size:
            sample = np.concatenate([self.sample, o.sample])
        else:  # keep each side in proportion to the values it stands for
            take = int(np.clip(round(self.size * self.seen / seen), self.size - len(o.sample), len(self.sample)))
            rng = _rng(seen)
            mine = rng.choice(self.sample, take, replace=False)
            sample = np.concatenate([mine, rng.choice(o.sample, self.size - take, replace=False)])
        return NumericStats(
            min(self.min, o.min),
            max(self.max, o.max),
            self.zeros + o.zeros,
            self.negatives + o.negatives,
            max(self.int_digits, o.int_digits),
            max(self.frac_digits, o.frac_digits),
            sample,
            seen,
            self.size,
        )

    def to_dict(self) -> dict:
        return {
            "min": float(self.min),
            "max": float(self.max),
            "zeros": self.zeros,
            "negatives": self.negatives,
            "int_digits": self.int_digits,
            "frac_digits": self.frac_digits,
            "histogram": _histogram(self.sample),
        }


@dataclass(frozen=True)
class TemporalStats:
    min: date | datetime
    max: date | datetime
    formats: frozenset[str] = frozenset()  # date formats that parsed the values
    timezone: bool | None = None  # timestamps only
    fraction_digits: int = 0
    midnight: bool | None = None

    @classmethod
    def of(cls, v: pa.Array, typed: Typed) -> TemporalStats:
        mm = pc.min_max(v)
        lo, hi = mm["min"].as_py(), mm["max"].as_py()
        formats = frozenset([typed.format]) if typed.format else frozenset()
        if not pa.types.is_timestamp(v.type):
            return cls(lo, hi, formats)
        tz = typed.timezone if typed.timezone is not None else v.type.tz is not None
        return cls(lo, hi, formats, tz, fraction_digits(v), all_midnight(v))

    def __add__(self, o: TemporalStats) -> TemporalStats:
        return TemporalStats(
            min(self.min, o.min),
            max(self.max, o.max),
            self.formats | o.formats,
            _any(self.timezone, o.timezone),
            max(self.fraction_digits, o.fraction_digits),
            _all(self.midnight, o.midnight),
        )

    def to_dict(self) -> dict:
        return {
            "min": self.min.isoformat(),
            "max": self.max.isoformat(),
            "formats": sorted(self.formats),
            "timezone": self.timezone,
            "fraction_digits": self.fraction_digits,
            "all_midnight": self.midnight,
        }


@dataclass(frozen=True)
class TextStats:
    min_len: int
    max_len: int
    max_bytes: int
    empty: int
    lengths: Counter
    flags: Counter
    null_like: Counter
    first: str  # with `varied`, tells "one value repeated" from "several values of one length"
    varied: bool

    @classmethod
    def of(cls, s: pa.Array, distinct: int) -> TextStats:
        lens = pc.utf8_length(s)
        mm = pc.min_max(lens)
        flags = Counter()
        for flag, pattern in QUIRKS.items():
            if n := pc.sum(pc.match_substring_regex(s, pattern)).as_py():
                flags[flag] = n
        like = s.filter(pc.is_in(s, value_set=pa.array(NULL_LIKE)))
        return cls(
            mm["min"].as_py(),
            mm["max"].as_py(),
            pc.max(pc.binary_length(s)).as_py(),
            pc.sum(pc.equal(lens, 0)).as_py() or 0,
            _counter(pc.value_counts(lens)),
            flags,
            _counter(pc.value_counts(like)),
            s[0].as_py(),
            distinct > 1,
        )

    def __add__(self, o: TextStats) -> TextStats:
        return TextStats(
            min(self.min_len, o.min_len),
            max(self.max_len, o.max_len),
            max(self.max_bytes, o.max_bytes),
            self.empty + o.empty,
            self.lengths + o.lengths,
            self.flags + o.flags,
            self.null_like + o.null_like,
            self.first,
            self.varied or o.varied or self.first != o.first,
        )

    def to_dict(self) -> dict:
        return {
            "min_len": self.min_len,
            "max_len": self.max_len,
            "max_bytes": self.max_bytes,
            "fixed_length": self.min_len == self.max_len,
            "empty": self.empty,
            "lengths": _length_histogram(self.lengths),
            "flags": dict(sorted(self.flags.items())),
            "null_like": dict(sorted(self.null_like.items())),
        }


@dataclass
class ColumnProfile:
    """One column over every batch: how many non-null values of which kind were seen (`kinds`), what the
    file itself declared when it typed them (`declared`, per arrow type), why text stayed text (`reasons`)
    and the statistics of the values by kind. `propose_column` reads these; nothing here chooses a type."""

    name: str
    options: ProfileOptions
    rows: int = 0
    nulls: int = 0
    files: set[str] = field(default_factory=set)
    kinds: Counter = field(default_factory=Counter)
    declared: dict[str, Column] = field(default_factory=dict)
    reasons: set[str] = field(default_factory=set)
    numeric: NumericStats | None = None
    temporal: TemporalStats | None = None
    text: TextStats | None = None
    categorical: Counter | None = field(default_factory=Counter)  # value counts; None once too many

    def add(self, values: pa.ChunkedArray, typed: Typed, counts: pa.StructArray | None, file: str) -> None:
        """`counts` are the value counts of the batch's text form (None for nested columns)."""
        self.rows += len(values)
        self.nulls += values.null_count
        self.files.add(file)
        if typed.declared is not None:
            self.declared.setdefault(str(values.type), typed.declared)
        if typed.reason:
            self.reasons.add(typed.reason)
        if typed.kind is None or counts is None or len(counts) == 0:
            return
        non_null = flat(values.drop_null())
        self.kinds[typed.kind] += len(non_null)
        if typed.kind in NUMERIC:
            self.numeric = merge(self.numeric, NumericStats.of(non_null, typed, self.options.reservoir))
        elif typed.kind in TEMPORAL:
            self.temporal = merge(self.temporal, TemporalStats.of(non_null, typed))
        self.text = merge(self.text, TextStats.of(typed.text.drop_null(), len(counts)))
        if self.categorical is not None:
            if len(counts) > self.options.categorical_max:
                self.categorical = None
            else:
                self.categorical += _counter(counts)
                if len(self.categorical) > self.options.categorical_max:
                    self.categorical = None

    def to_dict(self) -> dict:
        out: dict = {
            "rows": self.rows,
            "nulls": self.nulls,
            "null_ratio": round(self.nulls / self.rows, 4) if self.rows else None,
            "files": len(self.files),
            "kinds": dict(sorted(self.kinds.items())),
        }
        if self.declared:
            out["file_types"] = sorted(self.declared)
        if self.reasons:
            out["reasons"] = sorted(self.reasons)
        if self.numeric:
            out["numeric"] = self.numeric.to_dict()
        if self.temporal:
            out["temporal"] = self.temporal.to_dict()
        if self.text:
            out["text"] = self.text.to_dict()
        if self.categorical:
            out["categorical"] = dict(sorted(self.categorical.items(), key=lambda kv: (-kv[1], kv[0])))
        return out


@dataclass
class TableProfile:
    name: str
    parent: str | None
    options: ProfileOptions
    rows: int = 0
    files: dict[str, FileInfo] = field(default_factory=dict)  # by path, in delivery order
    rows_per_file: Counter = field(default_factory=Counter)
    columns: dict[str, ColumnProfile] = field(default_factory=dict)
    keys: KeyTracker = field(init=False)

    def __post_init__(self) -> None:
        self.keys = KeyTracker(self.options)

    def add(self, s: Sampled) -> None:
        path = s.file.path
        if path not in self.files:
            self.files[path] = s.file
            self.keys.next_file(path)
        self.rows += len(s.batch)
        self.rows_per_file[path] += len(s.batch)
        uniques: dict[str, pa.Array] = {}  # distinct text values per column, computed once for every consumer
        for name in s.batch.column_names:
            typed = s.typed[name]
            counts = pc.value_counts(typed.text.drop_null()) if typed.text is not None else None
            col = self.columns.setdefault(name, ColumnProfile(name, self.options))
            col.add(s.batch[name], typed, counts, path)
            if counts is not None and typed.kind is not None:
                uniques[name] = pc.struct_field(counts, "values")
        self.keys.add(s, uniques)

    def file_key_metadata(self) -> list[str]:
        """Metadata columns that tell files of one business date apart: the date itself and every pattern
        attribute that varies within a date."""
        by_date: dict[str, list[dict]] = {}
        for f in self.files.values():
            by_date.setdefault(str(f.business_date), []).append(f.attributes)
        varying = set()
        for attrs in by_date.values():
            if len(attrs) > 1:
                for key in {k for a in attrs for k in a}:
                    if len({a.get(key) for a in attrs}) > 1:
                        varying.add(key)
        return ["_business_date", *(f"_{k}" for k in sorted(varying))]

    def volume(self) -> dict:
        by_date: dict[str, dict] = {}
        for path, f in self.files.items():
            entry = by_date.setdefault(str(f.business_date), {"files": 0, "rows": 0})
            entry["files"] += 1
            entry["rows"] += self.rows_per_file[path]
        return {
            "rows_per_file": _spread(list(self.rows_per_file.values())),
            "files_per_business_date": _spread([d["files"] for d in by_date.values()]),
            "rows_per_business_date": _spread([d["rows"] for d in by_date.values()]),
            "by_business_date": dict(sorted(by_date.items())),
        }

    def to_dict(self, proposed: dict[str, Column]) -> dict:
        self.keys.finish()
        columns = {
            name: {"proposed": {k: v for k, v in proposed[name].dlt().items() if k != "name"}}
            | col.to_dict()
            | self.keys.column_facts(name)
            for name, col in self.columns.items()
        }
        out = {
            "rows": self.rows,
            "files": dict(self.rows_per_file),
            "volume": self.volume(),
            "keys": self.keys.report() | {"file_key_metadata": self.file_key_metadata()},
            "columns": columns,
        }
        return ({"parent": self.parent} if self.parent else {}) | out


def _rng(seed: int) -> np.random.Generator:
    return np.random.default_rng(seed)  # seeded by the data itself, so a rerun over the same files matches


def _any(a: bool | None, b: bool | None) -> bool | None:
    return None if a is None and b is None else bool(a) or bool(b)


def _all(a: bool | None, b: bool | None) -> bool | None:
    return None if a is None and b is None else (a is None or a) and (b is None or b)


def _counter(counts: pa.StructArray) -> Counter:
    return Counter({item["values"]: item["counts"] for item in counts.to_pylist()})


def _spread(values: list[int]) -> dict | None:
    if not values:
        return None
    return {"min": min(values), "max": max(values), "mean": round(sum(values) / len(values), 2), "n": len(values)}


def _histogram(sample: np.ndarray) -> dict:
    counts, edges = np.histogram(sample, bins=HISTOGRAM_BINS)
    return {"edges": [round(float(e), 6) for e in edges], "counts": [int(c) for c in counts]}


def _length_histogram(lengths: Counter) -> dict:
    if len(lengths) <= 20:
        return {str(k): v for k, v in sorted(lengths.items())}
    counts, edges = np.histogram(list(lengths.elements()), bins=HISTOGRAM_BINS)
    return {"edges": [int(e) for e in edges], "counts": [int(c) for c in counts]}
