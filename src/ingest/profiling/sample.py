"""Typed arrow batches from landed files, the way a load would see them: CSV text is typed with arrow
kernels, Parquet keeps its types, dict rows (JSON, Avro, custom readers) are denested by dlt into parquet
with the table's nesting settings, then typed the same way. Files are processed one at a time."""

from __future__ import annotations

import codecs
import csv
import io
import tempfile
from collections import Counter
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import TYPE_CHECKING, Iterator

import dlt
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ingest.archives import open_streams
from ingest.profiling.arrow import all_midnight, flat
from ingest.profiling.model import Column, FileInfo, Typed
from ingest.readers import CsvReader

if TYPE_CHECKING:
    from ingest.config import Table
    from ingest.profiling.model import ProfileOptions

INT = r"^-?\d+$"
LEADING_ZEROS = r"^-?0\d"
NUMBER = r"^-?\d+(\.\d+)?$"
INT_PART = r"^-?(?P<i>\d+)"
FRAC_PART = r"\.(?P<f>\d+)$"
THOUSANDS = r"^-?\d{1,3}(,\d{3})+(\.\d+)?$"
FLOAT = r"^[-+]?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?$"
TZ_SUFFIX = r"(Z|[+-]\d{2}:?\d{2})$"
BOOLS = pa.array(["true", "false"])
SNIFF_BYTES = 1 << 18


@dataclass
class Sampled:
    """One typed batch of one table (after denesting) from one file."""

    table: str  # table name after denesting (parent or <table>__<field>)
    file: FileInfo
    batch: pa.Table
    typed: dict[str, Typed] = field(default_factory=dict)  # every column of the batch
    parent: str | None = None


def sample(table: Table, files: list, options: ProfileOptions) -> Iterator[Sampled]:
    with Denester(table) as denester:
        for row in files:
            file = FileInfo.of(row)
            dicts: list[list[dict]] = []
            for batch in read_rows(table, row, options.max_rows):
                if isinstance(batch, pa.Table):
                    yield type_table(table.name, file, batch, options)
                else:
                    dicts.append(batch)
            if dicts:
                yield from denester.run(file, dicts, options)


def read_rows(table: Table, row, max_rows: int | None) -> Iterator[pa.Table | list[dict]]:
    """Batches of one landed file through the table's reader (CSV all text), cut at max_rows."""
    rows = 0
    for stream in open_streams(Path(row.local_path), row.member):
        with stream:
            for batch in table.reader.read(stream, column_types="string"):
                if max_rows is not None:
                    room = max_rows - rows
                    batch = batch.slice(0, room) if isinstance(batch, pa.Table) else batch[:room]
                rows += len(batch)
                yield batch
                if max_rows is not None and rows >= max_rows:
                    return


def type_table(
    name: str,
    file: FileInfo,
    batch: pa.Table,
    options: ProfileOptions,
    parent: str | None = None,
    json_columns: frozenset[str] = frozenset(),  # nested values dlt kept as json text (max_nesting)
) -> Sampled:
    out = Sampled(name, file, batch, parent=parent)
    for i, column in enumerate(batch.column_names):
        values = batch[column]
        array, typed = type_column(column, values, options, column in json_columns)
        out.typed[column] = typed
        if array is not values:
            out.batch = out.batch.set_column(i, column, array)
    return out


def type_column(name: str, values: pa.ChunkedArray, options: ProfileOptions, json: bool) -> tuple[pa.Array, Typed]:
    """Text is typed by the pass below; everything else keeps the file's type (doubles get a second look for
    decimal literals when asked). The values as text are computed here, once, for every later statistic."""
    if pa.types.is_nested(values.type):
        return values, Typed("json", declared=Column.from_arrow(name, values.type))
    if json:
        return values, Typed("json", text=flat(values))
    if pa.types.is_string(values.type) or pa.types.is_large_string(values.type):
        text = flat(values)
        array, typed = type_strings(text, options)
        return array, replace(typed, text=text)
    text = pc.cast(flat(values), pa.string())
    if options.decimals and pa.types.is_floating(values.type):
        # JSON numbers arrive as double; their shortest repr recovers the scale of decimal literals
        _, typed = type_strings(text, options)
        if typed.kind == "decimal":
            return values, replace(typed, text=text)
    declared = Column.from_arrow(name, values.type)
    return values, Typed(declared.data_type, text=text, declared=declared)


def type_strings(s: pa.Array, options: ProfileOptions) -> tuple[pa.Array, Typed]:
    """Vectorised typing of a text column: int, decimal, double, bool, date, timestamp or text, with the
    evidence the proposal needs (leading zeros, digits, format, timezone). Numbers with a fraction become
    double here; decimal(p, s) is a proposal decision made from the digits."""
    v = s.drop_null()
    if len(v) == 0:
        return s, Typed(None)

    def every(pattern: str) -> bool:
        return pc.all(pc.match_substring_regex(v, pattern)).as_py()

    for fmt in options.date_formats:  # opted in explicitly, so they win over "20260908 is an integer"
        parsed = pc.strptime(s, format=fmt, unit="us", error_is_null=True)
        if parsed.null_count == s.null_count:
            if all_midnight(parsed):
                return pc.cast(parsed, pa.date32()), Typed("date", format=fmt)
            return parsed, Typed("timestamp", format=fmt, timezone=False)
    if every(INT):
        if pc.any(pc.match_substring_regex(v, LEADING_ZEROS)).as_py():
            return s, Typed("text", reason="leading zeros")
        try:
            return pc.cast(s, pa.int64()), Typed("bigint")
        except pa.ArrowInvalid:
            return s, Typed("text", reason="integers beyond 64 bit")
    if every(NUMBER):  # at least one value has a fraction, the others are integers
        int_digits = pc.max(pc.utf8_length(pc.struct_field(pc.extract_regex(v, INT_PART), "i"))).as_py()
        frac_digits = pc.max(pc.utf8_length(pc.struct_field(pc.extract_regex(v, FRAC_PART), "f"))).as_py()
        return pc.cast(s, pa.float64()), Typed("decimal", int_digits=int_digits, frac_digits=frac_digits)
    if every(THOUSANDS):
        return s, Typed("text", reason="thousands separators")
    if every(FLOAT):
        return pc.cast(s, pa.float64()), Typed("double")
    if pc.all(pc.is_in(pc.utf8_lower(v), value_set=BOOLS)).as_py():
        return pc.cast(pc.utf8_lower(s), pa.bool_()), Typed("bool")
    try:
        return pc.cast(s, pa.date32()), Typed("date")
    except pa.ArrowInvalid:
        pass
    zoned = every(TZ_SUFFIX)
    try:
        if zoned:
            return pc.cast(s, pa.timestamp("us", "UTC")), Typed("timestamp", timezone=True)
        return pc.cast(s, pa.timestamp("us")), Typed("timestamp", timezone=False)
    except pa.ArrowInvalid:
        pass
    try:  # some values carry an offset, some do not: naive ones are taken as UTC
        return pc.cast(s, pa.timestamp("us", "UTC")), Typed("timestamp", timezone=True)
    except pa.ArrowInvalid:
        return s, Typed("text")


class Denester:
    """One dlt pipeline into a temporary parquet destination; every file is one run and every table it
    produces (the parent and <table>__<field> children) is typed like an arrow batch."""

    def __init__(self, table: Table) -> None:
        self.table = table
        self.pipeline = None

    def __enter__(self) -> Denester:
        self._tmp = tempfile.TemporaryDirectory(prefix="profile_")
        return self

    def __exit__(self, *exc) -> None:
        self._tmp.cleanup()

    def run(self, file: FileInfo, batches: list[list[dict]], options: ProfileOptions) -> Iterator[Sampled]:
        tmp = self._tmp.name
        if self.pipeline is None:
            self.pipeline = dlt.pipeline(
                pipeline_name=f"profile_{self.table.name}",
                pipelines_dir=f"{tmp}/work",
                destination=dlt.destinations.filesystem(bucket_url=f"file://{tmp}/out"),
                dataset_name="profile",
            )
        nesting = getattr(self.table.writer, "max_nesting", None)
        resource = dlt.resource(batches, name=self.table.name, max_table_nesting=nesting)
        info = self.pipeline.run(resource, loader_file_format="parquet")
        info.raise_on_failed_jobs()
        load_id = info.loads_ids[-1]
        schema = self.pipeline.default_schema
        for name in schema.data_table_names():
            spec = schema.tables[name]
            columns = spec.get("columns", {})
            json_columns = frozenset(c for c in columns if columns[c].get("data_type") == "json")
            for path in sorted(Path(f"{tmp}/out/profile/{name}").glob(f"{load_id}.*.parquet")):
                batch = pq.read_table(path)
                batch = batch.drop_columns([c for c in batch.column_names if c.startswith("_dlt_")])
                yield type_table(name, file, batch, options, spec.get("parent"), json_columns)


def sniff_csv(stream, reader: CsvReader) -> dict:
    """Byte-level dialect heuristics on the head of a file, compared with the declared reader."""
    head = stream.read(SNIFF_BYTES)
    text = head.decode(reader.encoding, errors="replace")
    lines = text.splitlines(keepends=True)
    if len(head) == SNIFF_BYTES and len(lines) > 1:
        lines = lines[:-1]  # partial last line
    body = lines[reader.skip_rows :]
    endings = Counter("crlf" if line.endswith("\r\n") else "lf" for line in lines)
    header = body[0] if body and reader.column_names is None else ""
    delimiters = {d: header.count(d) for d in (",", ";", "|", "\t") if header.count(d)}
    stripped = [line.rstrip("\r\n") for line in body]
    dialect: dict = {"delimiter": reader.delimiter, "escapechar": reader.escape_char}
    dialect["doublequote"] = reader.double_quote
    if reader.quote_char:
        dialect["quotechar"] = reader.quote_char
    else:
        dialect["quoting"] = csv.QUOTE_NONE
    counts = Counter(len(row) for row in csv.reader(io.StringIO("".join(body)), **dialect))
    quote = reader.quote_char or '"'
    out = {
        "bom": head.startswith(codecs.BOM_UTF8),
        "line_endings": "mixed" if len(endings) > 1 else next(iter(endings), None),
        "declared_delimiter": reader.delimiter,
        "delimiters_in_header": delimiters,
        "quotes": text.count(quote),
        "quote_doubling": text.count(quote * 2),
        "backslash_escapes": text.count("\\" + quote),
        "lines_with_odd_quotes": sum(line.count(quote) % 2 for line in stripped),
        "field_counts": {str(k): v for k, v in sorted(counts.items())},
        "hints": [],
    }
    if delimiters and (top := max(delimiters, key=delimiters.get)) != reader.delimiter:
        out["hints"].append(f"delimiter {top!r} is more frequent in the header than {reader.delimiter!r}")
    if out["lines_with_odd_quotes"] and not reader.newlines_in_values:
        out["hints"].append("lines with an odd number of quotes: values may contain newlines (newlines_in_values=True)")
    if out["backslash_escapes"] and not reader.escape_char:
        out["hints"].append("backslash before a quote: escape_char='\\\\' may be needed")
    if len(counts) > 1:
        out["hints"].append("ragged rows: field count differs between lines")
    if out["bom"]:
        out["hints"].append("utf-8 BOM: use encoding='utf-8-sig' if the first column name carries it")
    return out


def sniff_files(table: Table, files: list) -> dict[int, dict]:
    if not isinstance(table.reader, CsvReader):
        return {}
    out = {}
    for f in files:
        with next(open_streams(Path(f.local_path), f.member)) as stream:
            out[f.id] = sniff_csv(stream, table.reader)
    return out
