"""Dagster wiring: keys, extra definitions, checks, sensor run requests, range runs."""

from dagster import AssetKey, DagsterInstance, Definitions, asset, build_sensor_context, materialize
from dagster._core.storage.tags import ASSET_PARTITION_RANGE_END_TAG, ASSET_PARTITION_RANGE_START_TAG

from conftest import CSV_08
from ingest.checks import CheckResult
from ingest.config import Dataset
from ingest.factory import build_definitions
from ingest.resources import Sql


class RowCount:
    name, target = "row_count", "sql"

    def evaluate(self, ctx):
        return CheckResult(ctx.partition_key is not None, metadata={"partition": ctx.partition_key})


def build(ws, *tables, extra=None):
    dataset = Dataset(ws.feed, tables, schema_dir=ws.tmp / "schemas", extra=extra)
    defs = build_definitions([dataset], ws.landing, ws.manifest, Sql(url=ws.sql_url))
    return dataset, defs


def run_sensor(defs, dataset, manifest):
    sensor = defs.resolve_sensor_def(dataset.load_sensor_name)
    return list(sensor(build_sensor_context(instance=DagsterInstance.ephemeral(), resources={"manifest": manifest})))


def test_keys_and_extra_definitions_wire_into_the_factory(ws):
    table = ws.table().with_check(RowCount())

    @asset(deps=[table.asset_key])
    def em_report(): ...

    dataset, defs = build(ws, table, extra=Definitions(assets=[em_report]))
    graph = defs.get_repository_def().asset_graph
    assert ws.feed.raw_key == AssetKey(["raw", "tradeweb"]) and table.asset_key == AssetKey(["sql", "tradeweb", "em"])
    assert graph.get(AssetKey("em_report")).parent_keys == {table.asset_key}
    assert {c.name for c in graph.asset_check_keys} == {"delivery_em", "row_count"}
    assert defs.resolve_schedule_def(dataset.sync_schedule_name).cron_schedule == "0 7 * * 1-5"
    assert set(defs.resources) == {"landing", "manifest", "sql", "notifier", "remote_tradeweb"}


def test_raw_asset_syncs_classifies_and_runs_checks(ws):
    dataset, defs = build(ws, ws.table())
    result = materialize([defs.get_assets_def(ws.feed.raw_key), *defs.asset_checks], resources=defs.resources)
    assert result.success
    meta = result.asset_materializations_for_node("raw__tradeweb")[0].metadata
    assert meta["new_files"].value == 2 and meta["classified"].value == 2
    (check,) = result.get_asset_check_evaluations()
    assert check.check_name == "delivery_em" and not check.passed  # fixture days are long overdue relative to today


def test_sensor_requests_range_runs_and_dedupes_by_newest_file(ws):
    ws.write_remote("em-2026-09-04.csv", CSV_08)
    dataset, defs = build(ws, ws.table())
    materialize([defs.get_assets_def(ws.feed.raw_key)], resources=defs.resources)
    requests = run_sensor(defs, dataset, ws.manifest)
    ranges = [(r.tags[ASSET_PARTITION_RANGE_START_TAG], r.tags[ASSET_PARTITION_RANGE_END_TAG]) for r in requests]
    assert ranges == [("2026-09-04", "2026-09-04"), ("2026-09-08", "2026-09-09")]
    keys = {r.run_key for r in requests}

    result = materialize(
        [defs.get_assets_def(dataset.tables[0].asset_key)], resources=defs.resources, tags=requests[1].tags
    )
    assert result.success
    assert [r.run_key for r in run_sensor(defs, dataset, ws.manifest)] == [requests[0].run_key]  # loaded range gone

    ws.write_remote("em-2026-09-09.csv", b"isin,price\nXS1,9.9\n", bump_mtime=True)
    materialize([defs.get_assets_def(ws.feed.raw_key)], resources=defs.resources)
    new_keys = {r.run_key for r in run_sensor(defs, dataset, ws.manifest)}
    assert len(new_keys) == 2 and not (new_keys - keys) <= keys  # revision -> a run key never seen before


def test_range_run_loads_all_partitions_and_sql_checks_see_the_partition(ws):
    dataset, defs = build(ws, ws.table().with_check(RowCount()))
    materialize([defs.get_assets_def(ws.feed.raw_key)], resources=defs.resources)
    sql_asset = defs.get_assets_def(dataset.tables[0].asset_key)
    checks = [c for c in defs.asset_checks if any(k.name == "row_count" for k in c.check_keys)]
    result = materialize(
        [sql_asset, *checks],
        resources=defs.resources,
        tags={ASSET_PARTITION_RANGE_START_TAG: "2026-09-07", ASSET_PARTITION_RANGE_END_TAG: "2026-09-10"},
    )
    assert result.success
    assert ws.query("select _business_date from em order by 1") == [("2026-09-08",), ("2026-09-09",), ("2026-09-09",)]
    assert ws.manifest.pending_days("tradeweb/em") == {}
    (check,) = result.get_asset_check_evaluations()
    assert check.passed and check.metadata["partition"].value == "2026-09-07"


def test_empty_partition_materializes_with_zero_rows(ws):
    dataset, defs = build(ws, ws.table())
    result = materialize(
        [defs.get_assets_def(dataset.tables[0].asset_key)], resources=defs.resources, partition_key="2026-09-01"
    )
    assert result.success
    assert result.asset_materializations_for_node("sql__tradeweb__em")[0].metadata["rows"].value == 0
