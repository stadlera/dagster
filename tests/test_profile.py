"""Profiling: typed sampling per format, statistics and quirks, proposal rules, the committed report."""

import io
import json
from datetime import date
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet
import yaml

from conftest import deeper
from ingest.profiling import ProfileOptions, load_report, profile, write_report, write_schema
from ingest.profiling.sample import sniff_csv
from ingest.readers import CsvReader, JsonReader, ParquetReader
from ingest.sources import Patterns
from ingest.writers import DltWriter, Upsert

JSON = Patterns((r"^EM/em-(?P<date>\d{4}-\d{2}-\d{2})\.json$",))
PARQUET = Patterns((r"^EM/em-(?P<date>\d{4}-\d{2}-\d{2})\.parquet$",))
REGIONAL = Patterns(
    (
        r"^EM/em-(?P<date>\d{4}-\d{2}-\d{2})\.csv$",
        r"^EM/(?P<region>apac|emea)/em-(?P<date>\d{4}-\d{2}-\d{2})\.csv$",
    )
)


def profiled(ws, files: dict[str, bytes], table=None, options=None, only=True):
    """Write the files to the remote, sync, classify, profile (only these files unless `only=False`):
    (Profile, dataset, table)."""
    for name, content in files.items():
        ws.write_remote(name, content)
    ws.sync()
    table = table or ws.table()
    ws.classify(table)
    dataset = ws.dataset(table)
    rows = ws.manifest.files_for(table.key, date.min, date.max)
    if only:
        rows = [r for r in rows if r.path.split("/")[-1] in files]
    return profile(dataset, table, rows, options), dataset, table


def columns(result, name="em") -> dict:
    return {
        c: {k: v for k, v in col.items() if k != "name"} for c, col in result.schema.tables[name]["columns"].items()
    }


def test_csv_typing_pass_detects_int_decimal_date_timestamp_bool_and_keeps_leading_zeros_as_text(ws):
    csv = (
        b"id,qty,price,as_of,ts,flag,note,big\n"
        b'00123,7,1.50,2026-09-08,2026-09-08T10:00:00Z,true,hello,"1,234"\n'
        b'0007,-3,12.345,2026-09-09,2026-09-08T11:00:00.250Z,False,,"12,345,678"\n'
    )
    result, *_ = profiled(ws, {"em-2026-09-10.csv": csv})
    cols = columns(result)
    assert cols["id"] == {"nullable": True, "data_type": "text", "precision": 20, "description": "leading zeros"}
    assert cols["qty"] == {"nullable": True, "data_type": "bigint"}
    assert cols["price"] == {"nullable": True, "data_type": "double", "description": "decimal(7,3) with --decimals"}
    assert cols["as_of"] == {"nullable": True, "data_type": "date"}
    assert cols["ts"] == {"nullable": True, "data_type": "timestamp", "timezone": True, "precision": 3}
    assert cols["flag"] == {"nullable": True, "data_type": "bool"}
    assert cols["big"] == {
        "nullable": True,
        "data_type": "text",
        "precision": 20,
        "description": "thousands separators",
    }
    report = result.report["tables"]["em"]["columns"]
    assert report["qty"]["numeric"]["min"] == -3 and report["qty"]["numeric"]["negatives"] == 1
    assert report["price"]["numeric"] == {
        "min": 1.5,
        "max": 12.345,
        "zeros": 0,
        "negatives": 0,
        "int_digits": 2,
        "frac_digits": 3,
        "histogram": report["price"]["numeric"]["histogram"],
    }
    assert report["ts"]["temporal"]["fraction_digits"] == 3 and report["ts"]["temporal"]["all_midnight"] is False
    assert report["note"]["nulls"] == 1 and report["note"]["text"]["max_len"] == 5


def test_custom_date_formats_become_dates_or_timestamps_and_the_reader_loads_them(ws):
    csv = b"d,t\n20260908,08/09/2026 10:30\n20260909,09/09/2026 00:00\n"
    formats = ("%Y%m%d", "%d/%m/%Y %H:%M")
    result, dataset, table = profiled(
        ws,
        {"em-2026-09-10.csv": csv},
        ws.table().with_reader(CsvReader(date_formats=formats)),
        ProfileOptions(date_formats=formats),
    )
    cols = columns(result)
    assert cols["d"] == {"nullable": True, "data_type": "date"}
    assert cols["t"] == {"nullable": True, "data_type": "timestamp", "timezone": False}
    assert result.report["tables"]["em"]["columns"]["d"]["date_formats"] == ["%Y%m%d"]
    write_schema(result.schema, dataset)
    ws.load(table, date(2026, 9, 10))
    assert ws.query("select d, t from em order by d") == [
        ("2026-09-08", "2026-09-08 10:30:00.000000"),
        ("2026-09-09", "2026-09-09 00:00:00.000000"),
    ]


def test_fixed_length_identifiers_get_exact_length_and_spread_lengths_get_a_bucket_with_headroom(ws):
    csv = b"isin,name,blob\nXS0000000001,A," + b"x" * 5000 + b"\nXS0000000002," + b"b" * 80 + b",y\n"
    result, *_ = profiled(ws, {"em-2026-09-10.csv": csv})
    cols = columns(result)
    assert cols["isin"]["precision"] == 12  # min == max
    assert cols["name"]["precision"] == 255  # 80 * 1.5 -> next bucket
    assert "precision" not in cols["blob"]  # beyond 4000: unbounded
    text = result.report["tables"]["em"]["columns"]["isin"]["text"]
    assert text["fixed_length"] is True and text["lengths"] == {"12": 2}
    result, *_ = profiled(ws, {"em-2026-09-11.csv": csv}, options=ProfileOptions(text_headroom=1.0))
    assert columns(result)["name"]["precision"] == 100


def test_int_narrowing_and_decimals_are_opt_in(ws):
    csv = b"small,medium,large,price\n1,1000000,9000000000,1.50\n100,2000000,1,12.345\n"
    result, *_ = profiled(ws, {"em-2026-09-10.csv": csv})
    cols = columns(result)
    assert cols["small"] == cols["medium"] == cols["large"] == {"nullable": True, "data_type": "bigint"}
    assert cols["price"]["data_type"] == "double"
    result, *_ = profiled(ws, {"em-2026-09-11.csv": csv}, options=ProfileOptions(narrow=True, decimals=True))
    cols = columns(result)
    assert cols["small"]["precision"] == 16 and cols["medium"]["precision"] == 32 and "precision" not in cols["large"]
    assert cols["price"] == {"nullable": True, "data_type": "decimal", "precision": 7, "scale": 3}


def test_nullability_is_reported_but_only_committed_with_strict_nulls_or_for_upsert_keys(ws):
    csv = b"isin,price,note\nXS1,1.0,\nXS2,2.0,x\n"
    result, *_ = profiled(ws, {"em-2026-09-10.csv": csv})
    cols = columns(result)
    assert cols["isin"]["nullable"] and cols["note"]["nullable"]
    report = result.report["tables"]["em"]["columns"]
    assert report["isin"]["null_ratio"] == 0 and report["note"]["null_ratio"] == 0.5
    result, *_ = profiled(ws, {"em-2026-09-11.csv": csv}, options=ProfileOptions(strict_nulls=True))
    cols = columns(result)
    assert not cols["isin"]["nullable"] and not cols["price"]["nullable"] and cols["note"]["nullable"]
    upsert = ws.table("em3").with_writer(DltWriter(merge=Upsert(keys=("isin",))))
    result, *_ = profiled(ws, {"em-2026-09-12.csv": csv}, table=upsert)
    cols = columns(result, "em3")
    assert not cols["isin"]["nullable"] and cols["price"]["nullable"]


def test_string_quirks_newlines_double_quoting_escapes_and_null_like_tokens_are_flagged(ws):
    csv = b'isin,note\nXS1,"line one\nline two"\nXS2,"""quoted"""\nXS3, padded \nXS4,-\nXS5,"say \\"hi\\""\nXS6,caf\xc3\xa9\n'
    result, *_ = profiled(ws, {"em-2026-09-10.csv": csv}, ws.table().with_reader(CsvReader(newlines_in_values=True)))
    text = result.report["tables"]["em"]["columns"]["note"]["text"]
    assert text["flags"] == {  # arrow without escape_char turns 'say \"hi\"' into 'say \\hi\\""': two flags
        "backslash_escape": 1,
        "double_quoted": 1,
        "newline": 1,
        "non_ascii": 1,
        "quote_doubling": 1,
        "surrounding_whitespace": 1,
    }
    assert text["null_like"] == {"-": 1} and text["max_bytes"] == 17
    assert columns(result)["note"]["data_type"] == "text"  # quirks never change the type

    sniff = sniff_csv(io.BytesIO(csv), CsvReader())
    assert sniff["lines_with_odd_quotes"] == 2 and sniff["backslash_escapes"] == 2 and sniff["quote_doubling"] == 3
    assert [h.split(":")[0] for h in sniff["hints"]] == [
        "lines with an odd number of quotes",
        "backslash before a quote",
    ]
    sniff = sniff_csv(io.BytesIO(b"isin;price\r\nXS1;1\r\n"), CsvReader())
    assert sniff["line_endings"] == "crlf" and sniff["hints"] == [
        "delimiter ';' is more frequent in the header than ','"
    ]
    assert sniff_csv(io.BytesIO(b"a,b\n1,2\n3\n"), CsvReader())["hints"] == [
        "ragged rows: field count differs between lines"
    ]


def test_categorical_domain_and_candidate_keys_are_recorded(ws):
    day1 = b"isin,ccy,seq\nXS1,EUR,1\nXS2,USD,2\nXS3,EUR,3\n"
    day2 = b"isin,ccy,seq\nXS1,EUR,4\nXS2,GBP,5\n"
    result, *_ = profiled(
        ws, {"em-2026-09-10.csv": day1, "em-2026-09-11.csv": day2}, options=ProfileOptions(categorical_max=3)
    )
    cols = result.report["tables"]["em"]["columns"]
    assert cols["ccy"]["categorical"] == {"EUR": 3, "USD": 1, "GBP": 1}
    assert "categorical" not in cols["seq"]  # 5 distinct > categorical_max
    assert cols["isin"]["unique_in_file"] is True and cols["isin"]["unique"] is False  # key per day, not overall
    assert cols["seq"]["unique"] is True and cols["seq"]["distinct"] == 5
    assert cols["ccy"]["files"] == "2/2" and list(result.report["tables"]["em"]["files"].values()) == [3, 2]


def test_json_is_denested_like_the_load_and_child_tables_are_profiled(ws):
    rows = (
        b'{"isin":"XS1","issuer":{"name":"ACME"},"coupons":[{"amt":1.5,"on":"2026-12-01"}],"asOf":"2026-09-08"}\n'
        b'{"isin":"XS2","issuer":{"name":"B"},"coupons":[{"amt":1.25,"on":"2027-06-01"}],"asOf":"2026-09-08"}\n'
    )
    table = ws.table("bonds", source=JSON).with_reader(JsonReader())
    result, dataset, _ = profiled(ws, {"em-2026-09-12.json": rows}, table, ProfileOptions(decimals=True))
    bonds, coupons = columns(result, "bonds"), columns(result, "bonds__coupons")
    assert bonds["issuer__name"]["data_type"] == "text" and bonds["asOf"]["data_type"] == "date"
    assert bonds["isin"] == {"nullable": True, "data_type": "text", "precision": 3}
    assert coupons["amt"] == {"nullable": True, "data_type": "decimal", "precision": 5, "scale": 2}  # from doubles
    assert coupons["on"]["data_type"] == "date"
    assert result.schema.tables["bonds__coupons"]["parent"] == "bonds"
    assert "_dlt_id" not in bonds and "_dlt_parent_id" not in coupons
    assert result.report["tables"]["bonds__coupons"]["parent"] == "bonds"

    write_schema(result.schema, dataset)
    ws.load(table, date(2026, 9, 12))  # the committed shape is the loaded shape
    assert ws.query("select amt from bonds__coupons order by amt") == [(1.25,), (1.5,)]

    flat = ws.table("flat", source=JSON).with_reader(JsonReader()).with_writer(DltWriter(max_nesting=0))
    result, dataset, _ = profiled(ws, {"em-2026-09-13.json": rows}, flat)
    assert set(result.schema.tables) >= {"flat"} and "flat__coupons" not in result.schema.tables
    assert columns(result, "flat")["coupons"]["data_type"] == "json"
    write_schema(result.schema, dataset)
    ws.load(flat, date(2026, 9, 13))
    assert ws.query("select count(*) from flat") == [(2,)]


def test_parquet_keeps_its_types_and_still_gets_text_lengths_and_narrowing(ws):
    buf = io.BytesIO()
    pa.parquet.write_table(
        pa.table(
            {
                "n": pa.array([1, 2], pa.int32()),
                "d": pa.array([date(2026, 9, 8), None], pa.date32()),
                "name": ["ab", "abcd"],
                "amt": pa.array([Decimal("1.00"), Decimal("2.50")], pa.decimal128(10, 2)),
            }
        ),
        buf,
    )
    table = ws.table("px", source=PARQUET).with_reader(ParquetReader())
    result, *_ = profiled(ws, {"em-2026-09-10.parquet": buf.getvalue()}, table)
    cols = columns(result, "px")
    assert cols["n"] == {"nullable": True, "data_type": "bigint", "precision": 32}
    assert cols["d"] == {"nullable": True, "data_type": "date"}
    assert cols["name"] == {"nullable": True, "data_type": "text", "precision": 20}
    assert cols["amt"] == {"nullable": True, "data_type": "decimal", "precision": 10, "scale": 2}
    report = result.report["tables"]["px"]["columns"]
    assert report["n"]["file_types"] == ["int32"] and report["d"]["nulls"] == 1
    assert report["name"]["text"]["lengths"] == {"2": 1, "4": 1}


def test_profile_report_is_deterministic_and_written_next_to_the_schema(ws):
    csv = b"isin,price\nXS1,1.0\nXS2,2.0\n"
    result, dataset, table = profiled(ws, {"em-2026-09-10.csv": csv}, only=False)
    path = write_report(result, dataset, table)
    assert path == dataset.schema_dir / "profile" / "tradeweb.em.profile.json"
    again, *_ = profiled(ws, {}, table, only=False)
    assert write_report(again, dataset, table).read_bytes() == path.read_bytes()
    report = json.loads(path.read_text())
    assert load_report(dataset, table) == report and report["version"] == 1
    assert report["table"] == "em" and report["options"]["decimals"] is False
    assert [f["path"] for f in report["files"]] == [
        "EM/em-2026-09-08.csv",
        "EM/em-2026-09-09.csv",
        "EM/em-2026-09-10.csv",
    ]
    assert report["files"][0]["business_date"] == "2026-09-08" and report["files"][0]["attributes"] == {}
    assert report["files"][0]["csv"]["declared_delimiter"] == ","
    assert list(report["tables"]["em"]["files"]) == [f["path"] for f in report["files"]]  # keyed by path, not id
    assert report["tables"]["em"]["columns"]["isin"]["proposed"] == {
        "nullable": True,
        "data_type": "text",
        "precision": 3,
    }
    schema = yaml.safe_load(write_schema(result.schema, dataset).read_text())
    assert set(schema["tables"]["em"]["columns"]) == {"isin", "price"}


def test_file_keys_business_keys_churn_and_volume_are_derived_from_the_sample(ws):
    day1 = b"isin,leg,px\nXS1,A,1.0\nXS1,B,1.1\nXS2,A,2.0\n"
    day2 = b"isin,leg,px\nXS1,A,1.0\nXS2,A,2.1\nXS3,A,3.0\nXS3,B,3.1\n"
    result, dataset, table = profiled(ws, {"em-2026-09-10.csv": day1, "em-2026-09-11.csv": day2})
    em = result.report["tables"]["em"]
    assert em["keys"]["candidate_columns"] == ["isin", "leg"]  # px is a measure; highest cardinality first
    [key] = em["keys"]["candidates"]  # no single column is unique per file, the pair is
    assert key["columns"] == ["isin", "leg"] and key["unique_overall"] is False
    assert key["repeat_ratio"] == 0.4  # XS1/A and XS2/A recur out of five distinct key values
    assert key["churn"] == [
        {"file": "EM/em-2026-09-10.csv", "keys": 3},
        {"file": "EM/em-2026-09-11.csv", "keys": 4, "new": 2, "dropped": 1},
    ]
    assert em["keys"]["file_key_metadata"] == ["_business_date"]
    assert em["suggested"] == {"merge": "ReplaceDay", "keys": ["isin", "leg"], "note": "40% of key values recur"}
    assert em["volume"]["rows_per_file"] == {"min": 3, "max": 4, "mean": 3.5, "n": 2}
    assert em["volume"]["by_business_date"] == {
        "2026-09-10": {"files": 1, "rows": 3},
        "2026-09-11": {"files": 1, "rows": 4},
    }
    rows = [r for r in ws.manifest.files_for(table.key, date(2026, 9, 10), date(2026, 9, 12))]
    result = profile(dataset, table, rows, ProfileOptions(key_repeat_threshold=0.3))
    assert result.report["tables"]["em"]["suggested"]["merge"] == "Upsert"


def test_files_sharing_a_business_date_need_their_attribute_in_the_file_key(ws):
    ws.write_remote("em-2026-09-10.csv", b"isin,px\nXS1,1\nXS2,2\n")
    ws.write_remote("apac/em-2026-09-10.csv", b"isin,px\nXS1,3\nXS2,4\n")
    ws.sync(deeper(ws.feed))
    table = ws.table(source=REGIONAL)
    ws.classify(table)
    dataset = ws.dataset(table)
    result = profile(dataset, table, ws.manifest.files_for(table.key, date(2026, 9, 10)))
    em = result.report["tables"]["em"]
    assert em["volume"]["files_per_business_date"] == {"min": 2, "max": 2, "mean": 2.0, "n": 1}
    assert em["keys"]["file_key_metadata"] == ["_business_date", "_region"]
    # px is unique too but never recurs: the recurring key comes first and drives the suggestion
    assert [c["columns"] for c in em["keys"]["candidates"]] == [["isin"], ["px"]]
    assert em["suggested"] == {"merge": "Upsert", "keys": ["isin"], "note": "100% of key values recur"}
