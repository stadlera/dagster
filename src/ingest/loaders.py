"""Loaders turn one binary stream into arrow tables. Plug your own by implementing `read`."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import BinaryIO, Callable, Iterator, Protocol

import pyarrow as pa
import pyarrow.csv
import pyarrow.parquet


class Loader(Protocol):
    def read(self, stream: BinaryIO) -> Iterator[pa.Table | list[dict]]: ...


@dataclass(frozen=True)
class CsvLoader:
    delimiter: str = ","
    encoding: str = "utf-8"
    skip_rows: int = 0  # preamble lines before the header
    column_names: tuple[str, ...] | None = None  # for files without a header row
    block_size: int = 1 << 24

    def read(self, stream):
        reader = pa.csv.open_csv(
            stream,
            read_options=pa.csv.ReadOptions(
                encoding=self.encoding, skip_rows=self.skip_rows, column_names=self.column_names, block_size=self.block_size
            ),
            parse_options=pa.csv.ParseOptions(delimiter=self.delimiter),
        )
        for batch in reader:
            yield pa.Table.from_batches([batch])


@dataclass(frozen=True)
class ParquetLoader:
    batch_size: int = 1 << 16

    def read(self, stream):
        for batch in pa.parquet.ParquetFile(stream).iter_batches(batch_size=self.batch_size):
            yield pa.Table.from_batches([batch])


@dataclass(frozen=True)
class JsonLoader:
    """JSON lines, or a single top-level array."""

    def read(self, stream):
        text = stream.read()
        if text.lstrip().startswith(b"["):
            yield json.loads(text)
        else:
            yield [json.loads(line) for line in text.splitlines() if line.strip()]


@dataclass(frozen=True)
class AvroLoader:
    batch_size: int = 1 << 16

    def read(self, stream):
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
class FunctionLoader:
    """Wrap any function(stream) -> iterator of arrow tables or lists of dicts, e.g. an XML parser."""

    fn: Callable[[BinaryIO], Iterator[pa.Table | list[dict]]]

    def read(self, stream):
        yield from self.fn(stream)
