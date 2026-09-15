"""Stage 5, reading: a Reader turns one binary stream into arrow tables or lists of dicts.

`column_types` are the committed types (arrow) for known columns. CSV reads with them instead of
inferring per file; typed formats (Parquet, Avro) carry their own schema and ignore them; JSON yields
dicts and lets dlt coerce to the committed schema. `column_types="string"` reads everything as text
(used by the profiler).
"""

from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass
from typing import BinaryIO, Callable, Iterator, Protocol

import pyarrow as pa
import pyarrow.csv
import pyarrow.parquet

ColumnTypes = dict[str, pa.DataType] | str | None


class Reader(Protocol):
    def read(self, stream: BinaryIO, column_types: ColumnTypes = None) -> Iterator[pa.Table | list[dict]]: ...


@dataclass(frozen=True)
class CsvReader:
    delimiter: str = ","
    encoding: str = "utf-8"
    skip_rows: int = 0  # preamble lines before the header
    column_names: tuple[str, ...] | None = None  # for files without a header row
    block_size: int = 1 << 24
    # dialect quirks; the profiler's csv sniff reports when a file disagrees with what is declared here
    quote_char: str | None = '"'
    escape_char: str | None = None
    double_quote: bool = True  # "" inside a quoted value is a literal quote
    newlines_in_values: bool = False
    null_values: tuple[str, ...] | None = None  # None: arrow's default list ("", "NA", "NULL", "n/a", "#N/A", ...)
    strings_can_be_null: bool = True  # null tokens are null in text columns too, not only in typed ones
    date_formats: tuple[str, ...] = ()  # strptime formats for date/timestamp columns that are not ISO 8601

    def read(self, stream, column_types=None):
        skip_rows, column_names = self.skip_rows, self.column_names
        if column_types == "string":  # consume preamble + header ourselves, then type every column as text
            for _ in range(skip_rows):
                stream.readline()
            if column_names is None:
                header = stream.readline().decode(self.encoding)
                column_names = tuple(next(csv.reader(io.StringIO(header), delimiter=self.delimiter)))
            skip_rows, column_types = 0, {c: pa.string() for c in column_names}
        convert: dict = {"column_types": column_types or {}, "strings_can_be_null": self.strings_can_be_null}
        if self.null_values is not None:
            convert["null_values"] = list(self.null_values)
        dates: list[str] = []
        if self.date_formats:
            convert["timestamp_parsers"] = [*self.date_formats, pa.csv.ISO8601]
            # arrow applies the parsers to timestamp columns only: read committed dates as timestamps, cast below
            dates = [c for c, t in convert["column_types"].items() if pa.types.is_date(t)]
            convert["column_types"] = convert["column_types"] | {c: pa.timestamp("s") for c in dates}
        reader = pa.csv.open_csv(
            stream,
            read_options=pa.csv.ReadOptions(
                encoding=self.encoding, skip_rows=skip_rows, column_names=column_names, block_size=self.block_size
            ),
            parse_options=pa.csv.ParseOptions(
                delimiter=self.delimiter,
                quote_char=self.quote_char or False,
                escape_char=self.escape_char or False,
                double_quote=self.double_quote,
                newlines_in_values=self.newlines_in_values,
            ),
            convert_options=pa.csv.ConvertOptions(**convert),
        )
        for batch in reader:
            table = pa.Table.from_batches([batch])
            for c in dates:
                table = table.set_column(table.column_names.index(c), c, table[c].cast(column_types[c]))
            yield table


@dataclass(frozen=True)
class ParquetReader:
    batch_size: int = 1 << 16

    def read(self, stream, column_types=None):
        for batch in pa.parquet.ParquetFile(stream).iter_batches(batch_size=self.batch_size):
            yield pa.Table.from_batches([batch])


@dataclass(frozen=True)
class JsonReader:
    """JSON lines, or a single top-level array. Yields dicts: dlt flattens objects and unnests lists."""

    def read(self, stream, column_types=None):
        text = stream.read()
        if text.lstrip().startswith(b"["):
            yield json.loads(text)
        else:
            yield [json.loads(line) for line in text.splitlines() if line.strip()]


@dataclass(frozen=True)
class AvroReader:
    batch_size: int = 1 << 16

    def read(self, stream, column_types=None):
        import fastavro

        rows = []
        for record in fastavro.reader(stream):
            rows.append(record)
            if len(rows) >= self.batch_size:
                yield rows
                rows = []
        if rows:
            yield rows


@dataclass(frozen=True)
class FunctionReader:
    """Wrap any function(stream) -> iterator of arrow tables or lists of dicts, e.g. an XML parser."""

    fn: Callable[[BinaryIO], Iterator[pa.Table | list[dict]]]

    def read(self, stream, column_types=None):
        yield from self.fn(stream)
