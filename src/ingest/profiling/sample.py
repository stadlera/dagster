"""Typed arrow batches from landed files, the way a load would see them: CSV text is typed with arrow
kernels, Parquet keeps its types, dict rows (JSON, Avro, custom readers) are denested by dlt into parquet
with the table's nesting settings, then typed the same way."""

from __future__ import annotations

import codecs
import csv
import io
import tempfile
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Iterator

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ingest.archives import open_streams
from ingest.profiling.stats import all_midnight
from ingest.readers import CsvReader

if TYPE_CHECKING:
    from ingest.config import Table
    from ingest.profiling.propose import ProfileOptions

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
    table: str  # table name after denesting (parent or <table>__<field>)
    file: str  # manifest path (stable across environments, unlike the id)
    batch: pa.Table
    evidence: dict[str, dict] = field(default_factory=dict)  # column -> typing pass result
    raw: dict[str, pa.Array] = field(default_factory=dict)  # original strings of columns typed from text
    parent: str | None = None
    meta: dict = field(default_factory=dict)  # business_date, attributes of the file


def file_meta(f) -> dict:
    return {"id": f.id, "business_date": f.business_date, "attributes": dict(f.attributes or {})}


def sample(table: Table, files: list, options: ProfileOptions) -> Iterator[Sampled]:
    pending: dict[str, list[list[dict]]] = {}  # dict batches per file, denested at the end
    metas = {f.path: file_meta(f) for f in files}
    for f in files:
        rows = 0
        for stream in open_streams(Path(f.local_path), f.member):
            with stream:
                for batch in table.reader.read(stream, column_types="string"):
                    room = options.max_rows - rows
                    if isinstance(batch, pa.Table):
                        yield type_table(table.name, f.path, batch.slice(0, room), options, meta=metas[f.path])
                    else:
                        pending.setdefault(f.path, []).append(batch[:room])
                    rows += len(batch)
                    if rows >= options.max_rows:
                        break
            if rows >= options.max_rows:
                break
    if pending:
        for s in denest(table, pending, options):
            s.meta = metas[s.file]
            yield s


def type_table(
    name: str,
    file: str,
    batch: pa.Table,
    options: ProfileOptions,
    parent: str | None = None,
    json_columns: frozenset[str] = frozenset(),  # nested values dlt kept as json text (max_nesting)
    meta: dict | None = None,
) -> Sampled:
    out = Sampled(name, file, batch, parent=parent, meta=meta or {})
    for i, column in enumerate(batch.column_names):
        values = batch[column]
        if column in json_columns:
            typed, evidence = values, {"kind": "json"}
        elif pa.types.is_string(values.type) or pa.types.is_large_string(values.type):
            typed, evidence = type_strings(values, options)
        elif options.decimals and pa.types.is_floating(values.type):
            typed, evidence = type_doubles(values, options)
        else:
            continue
        if evidence is None:
            continue
        if not pa.types.is_floating(values.type):
            raw = values.combine_chunks()
            out.raw[column] = raw if pa.types.is_string(raw.type) else pc.cast(raw, pa.string())
        out.evidence[column] = evidence
        out.batch = out.batch.set_column(i, column, typed)
    return out


def type_strings(values: pa.ChunkedArray | pa.Array, options: ProfileOptions) -> tuple[pa.Array, dict]:
    """Vectorised typing of a text column: int, decimal, double, bool, date, timestamp or text, with the
    evidence the proposal needs (leading zeros, digits, format, timezone)."""
    s = values.combine_chunks() if isinstance(values, pa.ChunkedArray) else values
    if pa.types.is_large_string(s.type):
        s = pc.cast(s, pa.string())
    v = s.drop_null()
    if len(v) == 0:
        return s, {"kind": None}

    def every(pattern: str) -> bool:
        return pc.all(pc.match_substring_regex(v, pattern)).as_py()

    for fmt in options.date_formats:  # opted in explicitly, so they win over "20260908 is an integer"
        parsed = pc.strptime(s, format=fmt, unit="us", error_is_null=True)
        if parsed.null_count == s.null_count:
            if all_midnight(parsed):
                return pc.cast(parsed, pa.date32()), {"kind": "date", "format": fmt}
            return parsed, {"kind": "timestamp", "format": fmt, "timezone": False}
    if every(INT):
        if pc.any(pc.match_substring_regex(v, LEADING_ZEROS)).as_py():
            return s, {"kind": "text", "reason": "leading zeros"}
        try:
            return pc.cast(s, pa.int64()), {"kind": "bigint"}
        except pa.ArrowInvalid:
            return s, {"kind": "text", "reason": "integers beyond 64 bit"}
    if every(NUMBER):  # at least one value has a fraction, the others are integers
        int_digits = pc.max(pc.utf8_length(pc.struct_field(pc.extract_regex(v, INT_PART), "i"))).as_py()
        frac_digits = pc.max(pc.utf8_length(pc.struct_field(pc.extract_regex(v, FRAC_PART), "f"))).as_py()
        evidence = {"kind": "decimal", "int_digits": int_digits, "frac_digits": frac_digits}
        if options.decimals:
            return pc.cast(s, pa.decimal128(min(38, int_digits + frac_digits), frac_digits)), evidence
        return pc.cast(s, pa.float64()), evidence
    if every(THOUSANDS):
        return s, {"kind": "text", "reason": "thousands separators"}
    if every(FLOAT):
        return pc.cast(s, pa.float64()), {"kind": "double"}
    if pc.all(pc.is_in(pc.utf8_lower(v), value_set=BOOLS)).as_py():
        return pc.cast(pc.utf8_lower(s), pa.bool_()), {"kind": "bool"}
    try:
        return pc.cast(s, pa.date32()), {"kind": "date"}
    except pa.ArrowInvalid:
        pass
    zoned = every(TZ_SUFFIX)
    try:
        if zoned:
            return pc.cast(s, pa.timestamp("us", "UTC")), {"kind": "timestamp", "timezone": True}
        return pc.cast(s, pa.timestamp("us")), {"kind": "timestamp", "timezone": False}
    except pa.ArrowInvalid:
        pass
    try:  # some values carry an offset, some do not: naive ones are taken as UTC
        return pc.cast(s, pa.timestamp("us", "UTC")), {"kind": "timestamp", "timezone": True}
    except pa.ArrowInvalid:
        return s, {"kind": "text"}


def type_doubles(values: pa.ChunkedArray, options: ProfileOptions) -> tuple[pa.Array, dict | None]:
    """JSON numbers arrive as double; their shortest repr recovers the scale of decimal literals."""
    typed, evidence = type_strings(pc.cast(values, pa.string()), options)
    return (typed, evidence) if evidence.get("kind") == "decimal" else (values, None)


def denest(table: Table, pending: dict[str, list[list[dict]]], options: ProfileOptions) -> Iterator[Sampled]:
    """One dlt run per file into a temporary parquet destination; every produced table is sampled."""
    import dlt

    nesting = getattr(table.writer, "max_nesting", None)
    with tempfile.TemporaryDirectory(prefix="profile_") as tmp:
        pipeline = dlt.pipeline(
            pipeline_name=f"profile_{table.name}",
            pipelines_dir=f"{tmp}/work",
            destination=dlt.destinations.filesystem(bucket_url=f"file://{tmp}/out"),
            dataset_name="profile",
        )
        for file, batches in pending.items():
            resource = dlt.resource(batches, name=table.name, max_table_nesting=nesting)
            info = pipeline.run(resource, loader_file_format="parquet")
            info.raise_on_failed_jobs()
            load_id = info.loads_ids[-1]
            for name in pipeline.default_schema.data_table_names():
                spec = pipeline.default_schema.tables[name]
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
