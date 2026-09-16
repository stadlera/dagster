"""Stage 5, writing: metadata columns, idempotent reloads, upserts, nesting, schema contract, profiler."""

from datetime import date

import pytest
from dlt.pipeline.exceptions import PipelineStepFailed

from conftest import deeper
from ingest.load import metadata_columns
from ingest.partitioning import Monthly
from ingest.readers import JsonReader
from ingest.sources import Patterns
from ingest.writers import DltWriter, Upsert

REGIONAL = Patterns(
    (
        r"^EM/em-(?P<date>\d{4}-\d{2}-\d{2})\.csv$",
        r"^EM/(?P<region>apac|emea)/em-(?P<date>\d{4}-\d{2}-\d{2})\.csv$",
    )
)
JSON = Patterns((r"^EM/em-(?P<date>\d{4}-\d{2}-\d{2})\.json$",))


def test_rows_carry_metadata_and_reloads_do_not_duplicate(ws):
    ws.sync()
    table = ws.table()
    ws.classify(table)
    assert ws.load(table, date(2026, 9, 9), load_id="run-1").rows == 2
    ws.load(table, date(2026, 9, 9), load_id="run-2")  # same partition again
    ws.load(table, date(2026, 9, 8), load_id="run-3")
    rows = ws.query("select isin, price, _business_date, _source_file, _load_id from em order by _business_date, isin")
    assert rows == [
        ("XS1", 1.0, "2026-09-08", 1, "run-3"),
        ("XS1", 2.0, "2026-09-09", 2, "run-2"),
        ("XS2", 3.0, "2026-09-09", 2, "run-2"),
    ]
    assert list(metadata_columns(table)) == ["_business_date", "_subset", "_source_file", "_load_id"]


def test_restated_partition_replaces_only_its_business_date(ws):
    ws.sync()
    table = ws.table()
    ws.classify(table)
    ws.load(table, date(2026, 9, 8), end=date(2026, 9, 10), load_id="run-1")
    ws.write_remote("em-2026-09-09.csv", b"isin,price\nXS1,9.9\n", bump_mtime=True)
    ws.sync()
    ws.classify(table)
    ws.load(table, date(2026, 9, 9), load_id="run-2")
    assert ws.query("select isin, price, _load_id from em order by _business_date, isin") == [
        ("XS1", 1.0, "run-1"),
        ("XS1", 9.9, "run-2"),
    ]


def test_attributes_become_columns_and_upsert_merges_on_a_business_key(ws):
    ws.write_remote("apac/em-2026-09-08.csv", b"isin,price\nXS1,9.0\nXS7,7.0\n")
    ws.sync(deeper(ws.feed))
    table = ws.table(source=REGIONAL).with_writer(DltWriter(merge=Upsert(keys=("isin",))))
    ws.classify(table)
    ws.load(table, date(2026, 9, 8))
    rows = ws.query("select isin, price, _region from em order by isin")
    assert len(rows) == 2 and rows[1] == ("XS7", 7.0, "apac")  # XS1 from both files collapsed on the key


def test_monthly_window_loads_every_day_of_the_month(ws):
    ws.sync()
    table = ws.table(partitioning=Monthly())
    ws.classify(table)
    assert ws.load(table, date(2026, 9, 1), end=date(2026, 10, 1)).rows == 3
    assert ws.query("select distinct _business_date from em order by 1") == [("2026-09-08",), ("2026-09-09",)]


NESTED = (
    b'{"isin":"XS1","issuer":{"name":"ACME","country":"DE"},"coupons":[{"date":"2026-01-01","amt":1.5},{"date":"2026-07-01","amt":1.5}]}\n'
    b'{"isin":"XS2","issuer":{"name":"B","country":"FR"},"coupons":[]}\n'
)


def test_nested_json_is_flattened_and_unnested_or_kept_as_json(ws):
    ws.write_remote("em-2026-09-12.json", NESTED)
    ws.sync()
    table = ws.table("bonds", source=JSON).with_reader(JsonReader())
    ws.classify(table)
    ws.load(table, date(2026, 9, 12), load_id="run-1")
    ws.load(table, date(2026, 9, 12), load_id="run-2")  # child rows are replaced too
    assert ws.query("select isin, issuer__name, _load_id from bonds order by isin") == [
        ("XS1", "ACME", "run-2"),
        ("XS2", "B", "run-2"),
    ]
    assert ws.query("select amt from bonds__coupons") == [(1.5,), (1.5,)]

    ws.write_remote("bonds-2026-09-12.json", NESTED)
    ws.sync()
    flat_source = Patterns((r"^EM/bonds-(?P<date>\d{4}-\d{2}-\d{2})\.json$",))
    flat = ws.table("bonds_flat", source=flat_source).with_reader(JsonReader()).with_writer(DltWriter(max_nesting=0))
    ws.classify(table, flat)
    ws.load(flat, date(2026, 9, 12))
    (isin, issuer, coupons), *_ = ws.query("select isin, issuer, coupons from bonds_flat order by isin")
    assert isin == "XS1" and '"ACME"' in issuer and coupons.startswith("[")


def test_schema_contract_adds_columns_but_rejects_type_changes(ws):
    ws.write_remote("em-2026-09-12.json", b'{"isin":"XS1","price":1.5}\n')
    ws.write_remote("em-2026-09-13.json", b'{"isin":"XS2","price":2.5,"currency":"EUR"}\n')
    ws.write_remote("em-2026-09-14.json", b'{"isin":"XS3","price":"n/a"}\n')
    ws.sync()
    table = ws.table("px", source=JSON).with_reader(JsonReader())
    ws.classify(table)
    ws.load(table, date(2026, 9, 12))
    result = ws.load(table, date(2026, 9, 13))  # new column: added, and reported once a schema is committed
    assert ws.query("select isin, currency from px order by isin") == [("XS1", None), ("XS2", "EUR")]
    assert "new_columns" not in result.details
    with pytest.raises(PipelineStepFailed, match="price"):  # float -> text: refused, nothing written
        ws.load(table, date(2026, 9, 14))
    assert ws.query("select count(*) from px") == [(2,)]
