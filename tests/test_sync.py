import gzip
import os
import time
import zipfile
from datetime import date

import fastavro
import fsspec
import pytest
import sqlalchemy as sa

from ingest.config import Feed, Table, Upsert
from ingest.delivery import Weekdays
from ingest.load import load
from ingest.loaders import AvroLoader
from ingest.resources import AmbiguousMatch, Landing, Manifest, Remote
from ingest.sync import sync

TABLE = Table("tradeweb", "em", select=(r"/EM/em-(?P<date>\d{4}-\d{2}-\d{2})\.csv(\.zip|\.gz)?$",))


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


def run_load(tmp_path, manifest, table, day, load_id):
    return load(table, day, manifest.files_for(table.key, day), f"sqlite:///{tmp_path}/warehouse.db", "raw", load_id, schema_dir=tmp_path / "schemas")


def query(tmp_path, sql):
    with sa.create_engine(f"sqlite:///{tmp_path}/warehouse__raw.db").connect() as conn:  # dlt: one sqlite file per dataset
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
    assert manifest.pending_days(TABLE.key) == [date(2026, 9, 8), date(2026, 9, 9)]


def test_classify_multiple_selects_named_groups_ignore_and_ambiguity(env):
    fs, feed, landing, manifest, remote = env
    (remote / "apac").mkdir()
    (remote / "apac" / "em-2026-09-08.csv").write_bytes(b"isin,price\nXS9,9.0\n")
    (remote / "em-2026-09-08.tmp.csv").write_bytes(b"")
    feed = Feed(name="tradeweb", remote=feed.remote, cron="", paths=feed.paths, maxdepth=2, exclude=feed.exclude)
    sync(fs, feed, landing, manifest)
    regional = Table("tradeweb", "em", ignore=(r"\.tmp\.csv$",), select=(
        r"/EM/em-(?P<date>\d{4}-\d{2}-\d{2})\.csv$",
        r"/EM/(?P<region>apac|emea)/em-(?P<date>\d{4}-\d{2}-\d{2})\.csv$",
    ))
    assert manifest.classify([regional]) == 3
    rows = manifest.files_for(regional.key, date(2026, 9, 8))
    assert sorted((r.attributes or {} for r in rows), key=len) == [{}, {"region": "apac"}]
    assert manifest.pending_days(regional.key) == [date(2026, 9, 8), date(2026, 9, 9)]

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
    assert [f.local_path for f in manifest.files_for(TABLE.key, date(2026, 9, 9))] == [str(landing.path(feed.name, "EM/em-2026-09-09.csv", 2))]


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
    table = Table("tradeweb", "em", select=(
        r"/EM/em-(?P<date>\d{4}-\d{2}-\d{2})\.csv$",
        r"/EM/(?P<region>apac|emea)/em-(?P<date>\d{4}-\d{2}-\d{2})\.csv$",
    )).with_merge(Upsert(keys=("isin",)))
    manifest.classify([table])

    run_load(tmp_path, manifest, table, date(2026, 9, 8), "run-1")
    rows = query(tmp_path, "select isin, price, _region from em order by isin")
    assert len(rows) == 2 and {r[0] for r in rows} == {"XS1", "XS7"}
    assert rows[1] == ("XS7", 7.0, "apac")


def test_avro_loader(env, tmp_path):
    fs, feed, landing, manifest, remote = env
    schema = {"type": "record", "name": "Row", "fields": [{"name": "isin", "type": "string"}, {"name": "qty", "type": "long"}]}
    with open(remote / "em-2026-09-12.avro", "wb") as f:
        fastavro.writer(f, schema, [{"isin": "XS1", "qty": 5}, {"isin": "XS2", "qty": 6}])
    sync(fs, feed, landing, manifest)
    table = Table("tradeweb", "em_avro", select=(r"/em-(?P<date>\d{4}-\d{2}-\d{2})\.avro$",)).with_loader(AvroLoader())
    manifest.classify([table])
    assert run_load(tmp_path, manifest, table, date(2026, 9, 12), "run-1")["rows"] == 2
    assert query(tmp_path, "select isin, qty from em_avro order by isin") == [("XS1", 5), ("XS2", 6)]


def test_missing_days_respects_calendar_and_lag():
    expectation = Weekdays(lag_days=1, holidays=(date(2026, 9, 7),))
    counts = {date(2026, 9, 8): 1, date(2026, 9, 10): 1}
    # Friday 11th: due through the 10th; 7th holiday, 5th/6th weekend
    assert expectation.missing_days(counts, today=date(2026, 9, 11)) == [date(2026, 9, 3), date(2026, 9, 4), date(2026, 9, 9)]
