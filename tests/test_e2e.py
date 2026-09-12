"""End to end through the Dagster objects: a provider with regional sub folders, a static "latest" file,
a yearly history archive, and on the second day a late file, a restatement and a retention move.
Everything runs in-process against temp directories and sqlite."""

import zipfile
from datetime import date

from dagster import DagsterInstance, build_sensor_context, materialize

from ingest.checks import CheckResult
from ingest.config import Dataset, Feed, Table
from ingest.factory import build_definitions
from ingest.partitioning import Daily
from ingest.resources import Landing, Manifest, Remote, Sql
from ingest.sources import Patterns

PRICES = Patterns(
    under=r"/prices",
    select=(r"/(?P<region>apac|emea)/prices-(?P<date>\d{8})\.csv$",),
    archives=(r"/history/prices-\d{4}\.zip$",),
    date_format="%Y%m%d",
)


class HasFiles:
    """Custom check on the raw asset: the newest business date has both regions."""

    name, target = "both_regions", "raw"

    def evaluate(self, ctx):
        newest = max(ctx.manifest.file_counts(ctx.table.key, date.min))
        regions = {(r.attributes or {}).get("region") for r in ctx.manifest.files_for(ctx.table.key, newest)}
        return CheckResult(regions == {"apac", "emea"}, metadata={"newest": str(newest), "regions": sorted(regions)})


def csv(*rows):
    return ("isin,price\n" + "".join(f"{i},{p}\n" for i, p in rows)).encode()


class Provider:
    def __init__(self, tmp):
        self.root = tmp / "sftp" / "out" / "prices"
        self.root.mkdir(parents=True)

    def put(self, relative, content):
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        return path


def test_two_days_in_the_life_of_a_provider(tmp_path):
    provider = Provider(tmp_path)
    # day 1 on the remote: two regions for the 7th and 8th, a static latest copy, last year's archive
    for day in ("20260907", "20260908"):
        provider.put(f"apac/prices-{day}.csv", csv(("XS1", 1.0), ("XS2", 2.0)))
        provider.put(f"emea/prices-{day}.csv", csv(("XS3", 3.0)))
    provider.put("emea/latest.csv", csv(("XS3", 3.0)))
    (provider.root / "history").mkdir()
    with zipfile.ZipFile(provider.root / "history" / "prices-2025.zip", "w") as z:
        z.writestr("apac/prices-20251230.csv", csv(("XS1", 0.5)))
        z.writestr("emea/prices-20251230.csv", csv(("XS3", 0.7)))
        z.writestr("apac/prices-20251231.csv", csv(("XS1", 0.6)))

    feed = Feed(
        "acme", Remote(protocol="file"), "0 7 * * 1-5", (str(provider.root),), maxdepth=3, exclude=r"^latest\.csv$"
    )
    table = (
        Table("acme", "prices", PRICES, partitioning=Daily(start="2025-01-01"))
        .with_expectation(None)
        .with_check(HasFiles())
    )
    dataset = Dataset(feed, (table,), schema_dir=tmp_path / "schemas")
    manifest = Manifest(url=f"sqlite:///{tmp_path}/manifest.db")
    defs = build_definitions(
        [dataset], Landing(root=str(tmp_path / "landing")), manifest, Sql(url=f"sqlite:///{tmp_path}/wh.db")
    )
    raw, sql = defs.get_assets_def(feed.raw_key), defs.get_assets_def(table.asset_key)
    sensor = defs.get_sensor_def(dataset.load_sensor_name)

    def run_raw():
        result = materialize([raw, *defs.asset_checks], resources=defs.resources)
        assert result.success
        return result.asset_materializations_for_node("raw__acme")[0].metadata, result.get_asset_check_evaluations()

    def run_pending():
        ctx = build_sensor_context(instance=DagsterInstance.ephemeral(), resources={"manifest": manifest})
        requests = list(sensor(ctx))
        for r in requests:
            assert materialize([sql], resources=defs.resources, tags=r.tags).success
        return requests

    def query(q):
        import sqlalchemy as sa

        with sa.create_engine(f"sqlite:///{tmp_path}/wh__acme.db").connect() as c:
            return [tuple(r) for r in c.execute(sa.text(q)).all()]

    # --- day 1: mirror, classify, load history and current days in two range runs
    meta, (check,) = run_raw()
    assert meta["new_files"].value == 5 and meta["ignored"].value == 1 and meta["classified"].value == 7
    assert check.passed and check.metadata["newest"].value == "2026-09-08"
    landed = tmp_path / "landing" / "acme" / "prices" / "apac" / "prices-20260907.csv"
    assert landed.read_bytes() == (provider.root / "apac" / "prices-20260907.csv").read_bytes()

    requests = run_pending()
    assert [
        (r.tags["dagster/asset_partition_range_start"], r.tags["dagster/asset_partition_range_end"]) for r in requests
    ] == [
        ("2025-12-30", "2025-12-31"),
        ("2026-09-07", "2026-09-08"),
    ]
    assert query("select _business_date, _region, count(*) from prices group by 1, 2 order by 1, 2") == [
        ("2025-12-30", "apac", 1),
        ("2025-12-30", "emea", 1),
        ("2025-12-31", "apac", 1),
        ("2026-09-07", "apac", 2),
        ("2026-09-07", "emea", 1),
        ("2026-09-08", "apac", 2),
        ("2026-09-08", "emea", 1),
    ]
    assert len({r[0] for r in query("select distinct _load_id from prices")}) == 2
    assert run_pending() == []  # nothing left, and a second sync changes nothing
    assert run_raw()[0]["new_files"].value == 0

    # --- day 2: the 9th arrives for apac only, emea restates the 8th, apac's 7th is moved to an archive folder
    provider.put("apac/prices-20260909.csv", csv(("XS1", 1.1)))
    provider.put("emea/prices-20260908.csv", csv(("XS3", 3.5)))
    import os
    import time

    t = time.time() + 60
    os.utime(provider.root / "emea" / "prices-20260908.csv", (t, t))
    (provider.root / "apac" / "prices-20260907.csv").rename(provider.put("archive/apac/prices-20260907.csv", b""))

    meta, (check,) = run_raw()
    assert meta["new_files"].value == 2 and meta["revisions"].value == 1
    assert not check.passed and check.metadata["regions"].value == ["apac"]  # the 9th is missing emea

    requests = run_pending()
    assert [
        (r.tags["dagster/asset_partition_range_start"], r.tags["dagster/asset_partition_range_end"]) for r in requests
    ] == [("2026-09-08", "2026-09-09")]
    assert query("select isin, price from prices where _business_date = '2026-09-08' and _region = 'emea'") == [
        ("XS3", 3.5)
    ]
    assert query("select count(*) from prices where _business_date = '2026-09-07'") == [
        (3,)
    ]  # move: no duplicates, no reload
    assert query("select isin, price from prices where _business_date = '2026-09-09'") == [("XS1", 1.1)]
    assert run_pending() == []
