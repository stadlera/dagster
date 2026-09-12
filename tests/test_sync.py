import gzip
import os
import time
import zipfile
from datetime import date

import dlt
import fastavro
import fsspec
import pyarrow as pa
import pytest
import sqlalchemy as sa
import yaml
from dagster import AssetKey, Definitions, asset

from ingest.checks import CheckContext, CheckResult, Delivery, Weekdays
from ingest.config import Dataset, Feed, Table, Upsert
from ingest.factory import build_definitions
from ingest.load import load
from ingest.loaders import AvroLoader, JsonLoader
from ingest.resources import AmbiguousMatch, Landing, Manifest, Remote, Sql
from ingest.schema import committed_types, profile, write_schema
from ingest.sync import sync

TABLE = Table("tradeweb", "em", select=(r"/EM/em-(?P<date>\d{4}-\d{2}-\d{2})\.csv(\.zip|\.gz)?$",))
FEED = Feed(name="tradeweb", remote=Remote(protocol="file"), cron="0 7 * * *", paths=())


@pytest.fixture
def env(tmp_path):
    remote = tmp_path / "remote" / "EM"
    remote.mkdir(parents=True)
    for d in ("2026-09-08", "2026-09-09"):
        (remote / f"em-{d}.csv").write_bytes(b"isin,price\nXS1,1.0\n")
    (remote / "em.csv").write_bytes(b"static")
    feed = Feed(name="tradeweb", remote=Remote(protocol="file"), cron="", paths=(str(remote),), exclude=r"^em\.csv$")
    landing = Landing(root=str(tmp_path / "landing"))
    manifest = Manifest(url=f"sqlite:///{tmp_path}/manifest.db")
    return fsspec.filesystem("file"), feed, landing, manifest, remote


def make_dataset(tmp_path, feed, *tables):
    return Dataset(feed, tables, schema_dir=tmp_path / "schemas")


def run_load(tmp_path, manifest, table, day, load_id, end=None):
    dataset = make_dataset(tmp_path, FEED, table)
    return load(dataset, table, manifest.files_for(table.key, day, end), f"sqlite:///{tmp_path}/warehouse.db", load_id)


def query(tmp_path, sql):
    # dlt's sqlalchemy destination keeps one sqlite file per SQL schema (= dataset name)
    with sa.create_engine(f"sqlite:///{tmp_path}/warehouse__tradeweb.db").connect() as conn:
        return conn.execute(sa.text(sql)).all()


def test_sync_is_byte_equivalent_and_idempotent(env):
    fs, feed, landing, manifest, remote = env
    first = sync(fs, feed, landing, manifest)
    assert len(first.downloaded) == 2 and first.ignored == 1
    local = landing.path(feed.name, "EM/em-2026-09-08.csv", 1)
    assert local.read_bytes() == (remote / "em-2026-09-08.csv").read_bytes()

    second = sync(fs, feed, landing, manifest)
    assert second.downloaded == [] and second.unchanged == 2 and second.ignored == 1
    assert manifest.latest(feed.name)[str(remote / "em.csv")].status == "ignored"


def test_classify_assigns_table_and_business_date_after_the_fact(env):
    fs, feed, landing, manifest, remote = env
    sync(fs, feed, landing, manifest)
    assert manifest.file_counts(TABLE.key, date(2026, 1, 1)) == {}
    assert manifest.classify([TABLE]) == 2
    assert manifest.classify([TABLE]) == 0
    assert list(manifest.pending_days(TABLE.key)) == [date(2026, 9, 8), date(2026, 9, 9)]


def test_classify_multiple_selects_named_groups_ignore_and_ambiguity(env):
    fs, feed, landing, manifest, remote = env
    (remote / "apac").mkdir()
    (remote / "apac" / "em-2026-09-08.csv").write_bytes(b"isin,price\nXS9,9.0\n")
    (remote / "em-2026-09-08.tmp.csv").write_bytes(b"")
    feed = Feed(name="tradeweb", remote=feed.remote, cron="", paths=feed.paths, maxdepth=2, exclude=feed.exclude)
    sync(fs, feed, landing, manifest)
    regional = Table(
        "tradeweb",
        "em",
        ignore=(r"\.tmp\.csv$",),
        select=(
            r"/EM/em-(?P<date>\d{4}-\d{2}-\d{2})\.csv$",
            r"/EM/(?P<region>apac|emea)/em-(?P<date>\d{4}-\d{2}-\d{2})\.csv$",
        ),
    )
    assert manifest.classify([regional]) == 3
    rows = manifest.files_for(regional.key, date(2026, 9, 8))
    assert sorted((r.attributes or {} for r in rows), key=len) == [{}, {"region": "apac"}]
    assert list(manifest.pending_days(regional.key)) == [date(2026, 9, 8), date(2026, 9, 9)]

    other = Table("tradeweb", "em_copy", select=(r"/EM/em-(?P<date>\d{4}-\d{2}-\d{2})\.csv$",))
    (remote / "em-2026-09-10.csv").write_bytes(b"isin,price\n")
    sync(fs, feed, landing, manifest)
    with pytest.raises(AmbiguousMatch, match="em-2026-09-10.csv"):
        manifest.classify([regional, other])


def test_revision_is_kept_as_new_version(env):
    fs, feed, landing, manifest, remote = env
    sync(fs, feed, landing, manifest)
    path = remote / "em-2026-09-09.csv"
    path.write_bytes(b"isin,price\nXS1,2.0\n")
    os.utime(path, (time.time() + 10, time.time() + 10))

    result = sync(fs, feed, landing, manifest)
    manifest.classify([TABLE])
    assert len(result.revisions) == 1 and result.revisions[0].endswith(".v2")
    assert manifest.latest(feed.name)[str(path)].version == 2
    assert [f.local_path for f in manifest.files_for(TABLE.key, date(2026, 9, 9))] == [
        str(landing.path(feed.name, "EM/em-2026-09-09.csv", 2))
    ]


def test_load_replaces_business_date_reads_zip_and_gz(env, tmp_path):
    fs, feed, landing, manifest, remote = env
    with zipfile.ZipFile(remote / "em-2026-09-10.csv.zip", "w") as z:
        z.writestr("part1.csv", "isin,price\nXS2,3.0\nXS3,4.0\n")
        z.writestr("part2.csv", "isin,price\nXS4,5.0\n")
    (remote / "em-2026-09-11.csv.gz").write_bytes(gzip.compress(b"isin,price\nXS5,6.0\n"))
    sync(fs, feed, landing, manifest)
    manifest.classify([TABLE])

    assert run_load(tmp_path, manifest, TABLE, date(2026, 9, 10), "run-1")["rows"] == 3
    run_load(tmp_path, manifest, TABLE, date(2026, 9, 10), "run-2")  # same day again: no duplicates
    run_load(tmp_path, manifest, TABLE, date(2026, 9, 9), "run-3")
    run_load(tmp_path, manifest, TABLE, date(2026, 9, 11), "run-4")

    rows = query(tmp_path, "select isin, price, _business_date, _load_id from em order by isin")
    assert [r[0] for r in rows] == ["XS1", "XS2", "XS3", "XS4", "XS5"]
    assert {r[3] for r in rows} == {"run-2", "run-3", "run-4"}
    assert rows[1][1] == 3.0


def test_named_groups_become_columns_and_upsert_merges_on_key(env, tmp_path):
    fs, feed, landing, manifest, remote = env
    (remote / "apac").mkdir()
    (remote / "apac" / "em-2026-09-08.csv").write_bytes(b"isin,price\nXS1,9.0\nXS7,7.0\n")
    feed = Feed(name="tradeweb", remote=feed.remote, cron="", paths=feed.paths, maxdepth=2, exclude=feed.exclude)
    sync(fs, feed, landing, manifest)
    table = Table(
        "tradeweb",
        "em",
        select=(
            r"/EM/em-(?P<date>\d{4}-\d{2}-\d{2})\.csv$",
            r"/EM/(?P<region>apac|emea)/em-(?P<date>\d{4}-\d{2}-\d{2})\.csv$",
        ),
    ).with_merge(Upsert(keys=("isin",)))
    manifest.classify([table])

    run_load(tmp_path, manifest, table, date(2026, 9, 8), "run-1")
    rows = query(tmp_path, "select isin, price, _region from em order by isin")
    assert len(rows) == 2 and {r[0] for r in rows} == {"XS1", "XS7"}
    assert rows[1] == ("XS7", 7.0, "apac")


def test_avro_loader(env, tmp_path):
    fs, feed, landing, manifest, remote = env
    schema = {
        "type": "record",
        "name": "Row",
        "fields": [{"name": "isin", "type": "string"}, {"name": "qty", "type": "long"}],
    }
    with open(remote / "em-2026-09-12.avro", "wb") as f:
        fastavro.writer(f, schema, [{"isin": "XS1", "qty": 5}, {"isin": "XS2", "qty": 6}])
    sync(fs, feed, landing, manifest)
    table = Table("tradeweb", "em_avro", select=(r"/em-(?P<date>\d{4}-\d{2}-\d{2})\.avro$",)).with_loader(AvroLoader())
    manifest.classify([table])
    assert run_load(tmp_path, manifest, table, date(2026, 9, 12), "run-1")["rows"] == 2
    assert query(tmp_path, "select isin, qty from em_avro order by isin") == [("XS1", 5), ("XS2", 6)]


def test_nested_json_is_flattened_and_unnested_by_dlt(env, tmp_path):
    fs, feed, landing, manifest, remote = env
    (remote / "em-2026-09-12.json").write_bytes(
        b'{"isin":"XS1","issuer":{"name":"ACME","country":"DE"},"coupons":[{"date":"2026-01-01","amt":1.5},{"date":"2026-07-01","amt":1.5}]}\n'
        b'{"isin":"XS2","issuer":{"name":"B","country":"FR"},"coupons":[]}\n'
    )
    sync(fs, feed, landing, manifest)
    table = Table("tradeweb", "bonds", select=(r"/em-(?P<date>\d{4}-\d{2}-\d{2})\.json$",)).with_loader(JsonLoader())
    manifest.classify([table])
    run_load(tmp_path, manifest, table, date(2026, 9, 12), "run-1")
    run_load(tmp_path, manifest, table, date(2026, 9, 12), "run-2")  # reload must also replace child rows
    assert query(tmp_path, "select isin, issuer__name, _business_date, _load_id from bonds order by isin") == [
        ("XS1", "ACME", "2026-09-12", "run-2"),
        ("XS2", "B", "2026-09-12", "run-2"),
    ]
    assert query(tmp_path, "select amt from bonds__coupons") == [(1.5,), (1.5,)]


def test_profile_proposes_schema_and_load_reads_with_committed_types(env, tmp_path):
    fs, feed, landing, manifest, remote = env
    (remote / "em-2026-09-08.csv").write_bytes(
        b"id,isin,price,as_of,note\n00123,XS1,1.50,2026-09-08,hello\n7,XS2,12.345,2026-09-08,\n"
    )
    sync(fs, feed, landing, manifest)
    manifest.classify([TABLE])

    dataset = make_dataset(tmp_path, FEED, TABLE)
    path = write_schema(profile(dataset, TABLE, manifest.files_for(TABLE.key, date(2026, 9, 8))), dataset)
    assert path.name == "tradeweb.schema.yaml"
    cols = yaml.safe_load(path.read_text())["tables"]["em"]["columns"]
    assert cols["id"] == {"nullable": True, "data_type": "bigint"}
    assert cols["price"] == {"nullable": True, "data_type": "decimal", "precision": 5, "scale": 3}
    assert cols["as_of"]["data_type"] == "date" and cols["note"] == {
        "nullable": True,
        "data_type": "text",
        "precision": 20,
    }

    # review step: the id has leading zeros, keep it as text
    path.write_text(path.read_text().replace("data_type: bigint", "data_type: text\n        precision: 20"))
    caps = dlt.destinations.sqlalchemy(credentials="sqlite://").capabilities()
    assert committed_types(dataset, TABLE, caps)["id"] == pa.string()

    run_load(tmp_path, manifest, TABLE, date(2026, 9, 8), "run-1")
    assert query(tmp_path, "select id, price from em order by id") == [("00123", 1.5), ("7", 12.345)]


def test_monthly_window_loads_all_days(env, tmp_path):
    fs, feed, landing, manifest, remote = env
    sync(fs, feed, landing, manifest)
    monthly = Table("tradeweb", "em_monthly", select=TABLE.select, partition="monthly")
    manifest.classify([monthly])
    assert run_load(tmp_path, manifest, monthly, date(2026, 9, 1), "run-1", end=date(2026, 10, 1))["rows"] == 2
    assert query(tmp_path, "select distinct _business_date from em_monthly order by 1") == [
        ("2026-09-08",),
        ("2026-09-09",),
    ]


class FakeManifest:
    def __init__(self, counts):
        self.counts = counts

    def file_counts(self, table, since):
        return {d: n for d, n in self.counts.items() if d >= since}


def evaluate(delivery, counts, today):
    ctx = CheckContext(FakeManifest(counts), Sql(url="sqlite://"), TABLE, today)
    return delivery.evaluate(ctx)


def test_delivery_respects_calendar_lag_and_file_count():
    delivery = Delivery(Weekdays(holidays=(date(2026, 9, 7),)), lag_days=1)
    counts = {date(2026, 9, 8): 1, date(2026, 9, 10): 1}
    # Friday 11th: due through the 10th; 7th holiday, 5th/6th weekend
    result = evaluate(delivery, counts, today=date(2026, 9, 11))
    assert not result.passed and result.severity == "ERROR"
    assert list(result.metadata["violations"]) == ["2026-09-03", "2026-09-04", "2026-09-09"]

    # only the newest due day missing is a warning
    result = evaluate(
        delivery,
        {d: 1 for d in map(date(2026, 9, 1).__class__, [])} | {date(2026, 9, d): 1 for d in (1, 2, 3, 4, 8, 9)},
        today=date(2026, 9, 11),
    )
    assert not result.passed and result.severity == "WARN"

    # exact file count: two files on one day violate exactly=1
    exact = delivery.with_files(exactly=1)
    counts = {date(2026, 9, d): 1 for d in (1, 2, 3, 4, 8, 9, 10)} | {date(2026, 9, 9): 2}
    result = evaluate(exact, counts, today=date(2026, 9, 11))
    assert result.metadata["violations"] == {"2026-09-09": 2}
    assert evaluate(delivery, counts, today=date(2026, 9, 11)).passed


class RowCount:
    name, target = "row_count", "sql"

    def evaluate(self, ctx):
        return CheckResult(True)


def test_table_check_builders():
    table = TABLE.with_expectation(None)
    assert table.checks == ()

    table = table.with_check(RowCount())
    assert [c.name for c in table.checks] == ["row_count"]
    with pytest.raises(ValueError):
        table.with_check(RowCount())


def test_extra_definitions_wire_into_factory_keys(tmp_path):
    table = TABLE.with_check(
        type("RowCount", (), {"name": "row_count", "target": "sql", "evaluate": lambda s, c: CheckResult(True)})()
    )

    @asset(deps=[table.asset_key])
    def em_report(): ...

    dataset = Dataset(FEED, (table,), schema_dir=tmp_path, extra=Definitions(assets=[em_report]))
    defs = build_definitions([dataset], Landing(root="x"), Manifest(url="sqlite://"), Sql(url="sqlite://"))
    graph = defs.get_repository_def().asset_graph
    assert FEED.raw_key == AssetKey(["raw", "tradeweb"]) and table.asset_key == AssetKey(["sql", "tradeweb", "em"])
    assert graph.get(AssetKey("em_report")).parent_keys == {table.asset_key}
    assert {c.name for c in graph.asset_check_keys} == {"delivery_em", "row_count"}
    assert defs.get_schedule_def(dataset.sync_schedule_name) and defs.get_sensor_def(dataset.load_sensor_name)
