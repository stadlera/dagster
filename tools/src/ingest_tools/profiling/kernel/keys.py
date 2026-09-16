"""Key discovery over the sampled files: column sets that are unique within every file (file keys), how
often their values recur across files (business keys) and the churn between consecutive files.

Files are processed one at a time. Every column is followed as a single-column key set. Composite
candidates (pairs, else triples, of the highest-cardinality columns that are not unique on their own) are
chosen on the first file, the only one retained in memory, and verified on every later file. All key sets
that survive are reported; picking the business key among them is a review decision. Distinct values are
tracked up to `distinct_max` per key set; beyond it distinct counts and recurrence are not reported."""

from __future__ import annotations

from dataclasses import dataclass, field
from itertools import combinations

import pyarrow as pa
import pyarrow.compute as pc

from ingest_tools.profiling.kernel.model import KEYABLE, ProfileOptions, Sampled

SEPARATOR = "\x1f"
COMPACT_EVERY = 32  # unique chunks kept before they are merged


@dataclass
class KeySet:
    columns: tuple[str, ...]
    keyable: bool  # may take part in a key (single columns of measure kinds are followed for `distinct` only)
    distinct_max: int
    alive: bool = True  # unique within every file so far
    files: int = 0  # files the columns were present in
    nulls: int = 0
    seen: pa.Array | None = None  # distinct values so far; None once over distinct_max
    recurring: pa.Array | None = None  # distinct values that appeared in more than one file
    churn: list[dict] = field(default_factory=list)
    _chunks: list[pa.Array] = field(default_factory=list)  # unique values per batch of the current file
    _rows: int = 0  # rows of the current file the columns were present in
    _previous: pa.Array | None = None  # unique values of the previous file

    def add(self, unique: pa.Array, nulls: int, rows: int) -> None:
        self.nulls += nulls
        self._rows += rows
        self._chunks.append(unique)
        if len(self._chunks) > COMPACT_EVERY:
            self._chunks = [pc.unique(pa.concat_arrays(self._chunks))]

    def close_file(self, path: str, rows: int) -> None:
        values = pc.unique(pa.concat_arrays(self._chunks)) if self._chunks else pa.array([], pa.string())
        self.files += bool(self._rows)
        self.alive = self.alive and len(values) == rows  # nulls, absence and duplicates all show as fewer values
        if self.alive:
            entry = {"file": path, "keys": len(values)}
            if self._previous is not None:
                entry["new"] = pc.sum(pc.invert(pc.is_in(values, value_set=self._previous))).as_py() or 0
                entry["dropped"] = pc.sum(pc.invert(pc.is_in(self._previous, value_set=values))).as_py() or 0
            self.churn.append(entry)
        if self.seen is None and self.files == 1:
            self.seen, self.recurring = values, pa.array([], pa.string())
        elif self.seen is not None:
            repeat = values.filter(pc.is_in(values, value_set=self.seen))
            self.recurring = pc.unique(pa.concat_arrays([self.recurring, repeat]))
            self.seen = pc.unique(pa.concat_arrays([self.seen, values]))
        if self.seen is not None and len(self.seen) > self.distinct_max:
            self.seen = self.recurring = None
        self._previous = values if self.alive else None
        self._chunks, self._rows = [], 0

    @property
    def distinct(self) -> int | None:
        return len(self.seen) if self.seen is not None else None

    def repeat_ratio(self) -> float | None:
        if self.seen is None or len(self.seen) == 0 or self.files < 2:
            return None
        return round(len(self.recurring) / len(self.seen), 4)


@dataclass
class KeyTracker:
    options: ProfileOptions
    singles: dict[str, KeySet] = field(default_factory=dict)
    composites: dict[tuple[str, ...], KeySet] = field(default_factory=dict)
    files: int = 0
    _file: str | None = None
    _file_rows: int = 0
    _first: list[pa.Table] = field(default_factory=list)  # keyable columns (as text) of the first file

    def next_file(self, path: str) -> None:
        self.finish()
        self._file, self._file_rows = path, 0

    def add(self, s: Sampled, uniques: dict[str, pa.Array]) -> None:
        """`uniques` are the distinct non-null text values of each typed column in the batch."""
        rows = len(s.batch)
        self._file_rows += rows
        for name, unique in uniques.items():
            keyable = s.typed[name].kind in KEYABLE
            key = self.singles.setdefault(name, KeySet((name,), keyable, self.options.distinct_max))
            key.keyable = key.keyable and keyable
            key.add(unique, s.batch[name].null_count, rows)
        for cols, key in self.composites.items():
            if all(c in uniques for c in cols):
                joined = _joined([s.typed[c].text for c in cols])
                key.add(pc.unique(joined.drop_null()), joined.null_count, rows)
        if self.files == 0 and self.options.composite_keys:
            keyable = [c for c in uniques if self.singles[c].keyable]
            if keyable:
                self._first.append(pa.table({c: s.typed[c].text for c in keyable}))

    def finish(self) -> None:
        if self._file is None:
            return
        for key in [*self.singles.values(), *self.composites.values()]:
            key.close_file(self._file, self._file_rows)
        self.files += 1
        if self.files == 1:
            self._choose_composites()
        self.composites = {cols: key for cols, key in self.composites.items() if key.alive}
        self._file = None

    def _choose_composites(self) -> None:
        """Pairs, else triples, of the highest-cardinality columns that are unique within the first file
        without being unique on their own (a composite containing a key column is not a smaller key)."""
        first, self._first = self._first, []
        if not first:
            return
        table = pa.concat_tables(first, promote_options="permissive")
        candidates = [c for c in table.column_names if not self.singles[c].alive and self.singles[c].nulls == 0]
        rank = lambda c: (-(self.singles[c].distinct or float("inf")), c)  # noqa: E731
        top = sorted(candidates, key=rank)[: self.options.key_columns_max]
        for size in (2, 3):
            found = [cols for cols in combinations(top, size) if _unique(table, cols)]
            if found:
                break
        for cols in found[: self.options.key_sets_max]:
            joined = _joined([table[c] for c in cols])
            key = KeySet(cols, True, self.options.distinct_max)
            key.add(pc.unique(joined), 0, len(table))
            key.close_file(self._file, len(table))
            self.composites[cols] = key

    def column_facts(self, name: str) -> dict:
        key = self.singles.get(name)
        if key is None:
            return {"distinct": None, "unique": None, "unique_in_file": None}
        facts: dict = {"distinct": key.distinct}
        if key.distinct is None:
            facts["distinct_over"] = self.options.distinct_max
        facts["unique"] = key.alive and len(key.recurring) == 0 if key.recurring is not None else None
        facts["unique_in_file"] = key.alive and key.files == self.files
        return facts

    def candidate_columns(self) -> list[str]:
        """Columns that can take part in a key: no nulls, present in every file, not a measure; highest
        cardinality first. A value that recurs in every file is not "constant" but a business key seen n times."""
        out = [n for n, k in self.singles.items() if k.keyable and k.nulls == 0 and k.files == self.files]
        return sorted(out, key=lambda n: (-(self.singles[n].distinct or float("inf")), n))

    def report(self) -> dict:
        tracked = [*self.singles.values(), *self.composites.values()]
        candidates = [
            {
                "columns": list(k.columns),
                "unique_overall": len(k.recurring) == 0 if k.recurring is not None else None,
                "repeat_ratio": k.repeat_ratio(),
                "churn": k.churn,
            }
            for k in tracked
            if k.keyable and k.alive and k.files == self.files
        ]
        # the key whose values recur most is the likeliest business key; a unique measure (a daily price) comes last
        candidates.sort(key=lambda c: (-(c["repeat_ratio"] or 0), len(c["columns"]), c["columns"]))
        return {"candidate_columns": self.candidate_columns(), "candidates": candidates}


def _joined(texts: list[pa.Array | pa.ChunkedArray]) -> pa.Array:
    arrays = [t.combine_chunks() if isinstance(t, pa.ChunkedArray) else t for t in texts]
    return arrays[0] if len(arrays) == 1 else pc.binary_join_element_wise(*arrays, SEPARATOR)


def _unique(table: pa.Table, cols: tuple[str, ...]) -> bool:
    values = _joined([table[c] for c in cols])
    return values.null_count == 0 and pc.count_distinct(values).as_py() == len(values)
