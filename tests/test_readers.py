"""Stage 5, reading: formats, dialects, committed types, archives and compression."""

import gzip
import io
import tarfile
import zipfile

import fastavro
import pyarrow as pa
import pyarrow.parquet
import pytest

from ingest.archives import list_members, open_streams
from ingest.readers import AvroReader, CsvReader, FunctionReader, JsonReader, ParquetReader


def read_all(reader, data: bytes, column_types=None):
    out = []
    for batch in reader.read(io.BytesIO(data), column_types):
        out.append(batch if isinstance(batch, pa.Table) else pa.Table.from_pylist(batch))
    return pa.concat_tables(out)


def test_csv_dialects():
    t = read_all(CsvReader(delimiter=";", skip_rows=2), b"provider x\ngenerated 2026\nisin;price\nXS1;1.5\n")
    assert t.to_pylist() == [{"isin": "XS1", "price": 1.5}]
    t = read_all(CsvReader(column_names=("isin", "price")), b"XS1,1.5\n")
    assert t.column_names == ["isin", "price"]
    t = read_all(CsvReader(encoding="latin-1"), "isin,name\nXS1,Müller\n".encode("latin-1"))
    assert t["name"][0].as_py() == "Müller"


def test_csv_reads_with_committed_types_instead_of_inferring():
    data = b"id,price\n00123,1.50\n"
    assert read_all(CsvReader(), data)["id"].type == pa.int64()  # inference loses the leading zeros
    typed = read_all(CsvReader(), data, {"id": pa.string(), "price": pa.decimal128(5, 2)})
    assert typed["id"][0].as_py() == "00123" and str(typed["price"][0].as_py()) == "1.50"
    all_text = read_all(CsvReader(skip_rows=1), b"preamble\nid,price\n00123,1.50\n", "string")
    assert all_text.to_pylist() == [{"id": "00123", "price": "1.50"}]


def test_json_avro_parquet_and_function_readers():
    assert read_all(JsonReader(), b'{"a": 1}\n{"a": 2}\n').to_pylist() == [{"a": 1}, {"a": 2}]
    assert read_all(JsonReader(), b'[{"a": 1}]').to_pylist() == [{"a": 1}]

    buf = io.BytesIO()
    schema = {"type": "record", "name": "R", "fields": [{"name": "a", "type": "long"}]}
    fastavro.writer(buf, schema, [{"a": 1}, {"a": 2}])
    assert read_all(AvroReader(), buf.getvalue())["a"].to_pylist() == [1, 2]

    buf = io.BytesIO()
    pa.parquet.write_table(pa.table({"a": [1, 2]}), buf)
    assert read_all(ParquetReader(), buf.getvalue())["a"].to_pylist() == [1, 2]

    xml_like = FunctionReader(lambda s: iter([[{"a": int(s.read())}]]))
    assert read_all(xml_like, b"7").to_pylist() == [{"a": 7}]


def test_archives_and_compression_are_read_in_place(tmp_path):
    z = tmp_path / "day.zip"
    with zipfile.ZipFile(z, "w") as f:
        f.writestr("b.csv", "x\n2\n")
        f.writestr("a.csv", "x\n1\n")
    assert [s.read() for s in open_streams(z)] == [b"x\n1\n", b"x\n2\n"]  # members in name order
    assert next(open_streams(z, "b.csv")).read() == b"x\n2\n"
    assert {m.name: m.size for m in list_members(z)} == {"a.csv": 4, "b.csv": 4}

    t = tmp_path / "day.tar.gz"
    with tarfile.open(t, "w:gz") as f:
        info = tarfile.TarInfo("c.csv")
        info.size = 4
        f.addfile(info, io.BytesIO(b"x\n3\n"))
    assert [s.read() for s in open_streams(t)] == [b"x\n3\n"]

    g = tmp_path / "day.csv.gz"
    g.write_bytes(gzip.compress(b"x\n4\n"))
    assert next(open_streams(g)).read() == b"x\n4\n"
    v2 = tmp_path / "day.csv.gz.v2"  # our revision suffix does not confuse the detection
    v2.write_bytes(gzip.compress(b"x\n5\n"))
    assert next(open_streams(v2)).read() == b"x\n5\n"

    with pytest.raises(ValueError):
        list_members(g)
