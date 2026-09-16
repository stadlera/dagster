"""Compatibility between generated schemas and the runtime loader."""

from datetime import date

import dlt
import pyarrow as pa
import yaml

from ingest.schema import committed_types
from ingest_tools.profiling import ProfileOptions, profile, write_schema


def test_profile_proposes_a_schema_and_loads_read_with_the_committed_types(ws):
    ws.write_remote(
        "em-2026-09-08.csv", b"id,isin,price,as_of,note\n00123,XS1,1.50,2026-09-08,hello\n7,XS2,12.345,2026-09-08,\n"
    )
    ws.sync()
    table = ws.table()
    ws.classify(table)
    dataset = ws.dataset(table)

    files = ws.manifest.files_for(table.key, date(2026, 9, 8))
    proposed = profile(dataset, table, files).schema.tables["em"]["columns"]
    assert proposed["price"]["data_type"] == "double"
    path = write_schema(profile(dataset, table, files, ProfileOptions(decimals=True)).schema, dataset)
    columns = yaml.safe_load(path.read_text())["tables"]["em"]["columns"]
    assert path.name == "tradeweb.schema.yaml"
    assert columns["id"] == {
        "nullable": True,
        "data_type": "text",
        "precision": 20,
        "description": "leading zeros",
    }
    assert columns["price"] == {"nullable": True, "data_type": "decimal", "precision": 7, "scale": 3}
    assert columns["as_of"]["data_type"] == "date"
    assert columns["note"] == {"nullable": True, "data_type": "text", "precision": 20}
    caps = dlt.destinations.sqlalchemy(credentials="sqlite://").capabilities()
    assert committed_types(dataset, table, caps)["id"].equals(pa.string())
    assert table.writer.column_types(dataset, table, ws.sql_url)["id"].equals(pa.string())
    ws.load(table, date(2026, 9, 8))
    assert ws.query("select id, price from em order by id") == [("00123", 1.5), ("7", 12.345)]

    ws.write_remote(
        "em-2026-09-09.csv",
        b"id,isin,price,as_of,note,currency\n8,XS3,1.0,2026-09-09,x,EUR\n",
        bump_mtime=True,
    )
    ws.sync()
    ws.classify(table)
    assert ws.load(table, date(2026, 9, 9)).details["new_columns"] == ["currency"]

    other = ws.table("em2")
    write_schema(profile(dataset, other, ws.manifest.files_for(table.key, date(2026, 9, 8))).schema, dataset)
    assert set(yaml.safe_load(path.read_text())["tables"]) >= {"em", "em2"}
