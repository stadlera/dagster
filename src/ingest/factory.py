"""Turn Dataset config into Dagster assets, checks, schedules and sensors.

Naming is defined in config.py (Feed.raw_key, Table.asset_key, Dataset.sync_schedule_name,
Dataset.load_sensor_name); custom definitions in Dataset.extra can rely on it.
"""

from datetime import date

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    AssetSelection,
    DailyPartitionsDefinition,
    Definitions,
    MaterializeResult,
    MonthlyPartitionsDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    asset,
    asset_check,
    sensor,
)

from ingest.checks import Check, CheckContext
from ingest.config import Dataset, Table
from ingest.load import load
from ingest.resources import Landing, Manifest, Sql
from ingest.sync import sync

PARTITIONS = {"daily": DailyPartitionsDefinition, "monthly": MonthlyPartitionsDefinition}


def partition_key(table: Table, day: date) -> str:
    return str(day.replace(day=1) if table.partition == "monthly" else day)


def build_raw_asset(dataset: Dataset):
    feed, tables = dataset.feed, dataset.tables
    remote_key = f"remote_{feed.name}"

    @asset(
        key=feed.raw_key,
        group_name=feed.name,
        description=f"Byte-equivalent mirror of {', '.join(feed.paths)}",
        required_resource_keys={remote_key, "landing", "manifest"},
        retry_policy=RetryPolicy(max_retries=2, delay=120),
    )
    def raw(context: AssetExecutionContext) -> MaterializeResult:
        manifest: Manifest = context.resources.manifest
        landing = Landing(root=dataset.landing_root) if dataset.landing_root else context.resources.landing
        result = sync(getattr(context.resources, remote_key).fs(), feed, landing, manifest)
        classified = manifest.classify(tables)
        context.log.info(
            "%s: %d new, %d revisions, %d unchanged, %d ignored, %d classified",
            feed.name,
            len(result.downloaded),
            len(result.revisions),
            result.unchanged,
            result.ignored,
            classified,
        )
        return MaterializeResult(
            metadata={
                "new_files": len(result.downloaded),
                "revisions": len(result.revisions),
                "unchanged": result.unchanged,
                "ignored": result.ignored,
                "classified": classified,
                "files": result.downloaded[-20:] + result.revisions[-20:],
            }
        )

    return raw


def build_check(dataset: Dataset, table: Table, check: Check):
    target = dataset.feed.raw_key if check.target == "raw" else table.asset_key
    name = check.name if check.target == "sql" else f"{check.name}_{table.name}"

    @asset_check(asset=target, name=name, description=f"{type(check).__name__} on {table.key}")
    def run_check(context: AssetCheckExecutionContext, manifest: Manifest, sql: Sql) -> AssetCheckResult:
        partition = context.run.tags.get("dagster/partition")
        result = check.evaluate(CheckContext(manifest, sql, table, date.today(), partition))
        return AssetCheckResult(
            passed=result.passed, severity=AssetCheckSeverity[result.severity], metadata=result.metadata
        )

    return run_check


def build_table_asset(dataset: Dataset, table: Table):
    @asset(
        key=table.asset_key,
        deps=[dataset.feed.raw_key],
        group_name=dataset.feed.name,
        partitions_def=PARTITIONS[table.partition](start_date=table.start_date),
        required_resource_keys={"manifest", "sql"},
        description=f"{type(table.loader).__name__} on {', '.join(table.select)}",
        # tables of one dataset share a dlt pipeline; limit this key to 1 in the instance to serialize their loads
        op_tags={"dagster/concurrency_key": f"load_{dataset.name}"},
    )
    def sql_table(context: AssetExecutionContext) -> MaterializeResult:
        manifest: Manifest = context.resources.manifest
        window = context.partition_time_window
        files = manifest.files_for(table.key, window.start.date(), window.end.date())
        if not files:
            return MaterializeResult(metadata={"rows": 0, "files": 0})
        result = load(dataset, table, files, dataset.sql_url or context.resources.sql.url, context.run_id)
        manifest.mark_loaded([f.id for f in files], context.run_id)
        return MaterializeResult(
            metadata={"rows": result["rows"], "files": len(files), "dlt_load_ids": result["load_ids"]}
        )

    return sql_table


def build_load_sensor(dataset: Dataset):
    tables = dataset.tables

    @sensor(
        name=dataset.load_sensor_name,
        target=AssetSelection.assets(*[t.asset_key for t in tables]),
        minimum_interval_seconds=300,
    )
    def load_sensor(manifest: Manifest):
        for t in tables:
            pending: dict[str, int] = {}  # partition key -> newest pending file id
            for day, newest_file in manifest.pending_days(t.key).items():
                key = partition_key(t, day)
                pending[key] = max(pending.get(key, 0), newest_file)
            for key, newest_file in pending.items():
                yield RunRequest(
                    run_key=f"{t.key}/{key}/{newest_file}",  # a later revision yields a new key, so it is reloaded
                    asset_selection=[t.asset_key],
                    partition_key=key,
                )

    return load_sensor


def build_dataset(dataset: Dataset) -> Definitions:
    feed = dataset.feed
    defs = Definitions(
        assets=[build_raw_asset(dataset), *(build_table_asset(dataset, t) for t in dataset.tables)],
        asset_checks=[build_check(dataset, t, c) for t in dataset.tables for c in t.checks],
        schedules=[
            ScheduleDefinition(
                name=dataset.sync_schedule_name, cron_schedule=feed.cron, target=AssetSelection.assets(feed.raw_key)
            )
        ],
        sensors=[build_load_sensor(dataset)] if dataset.tables else [],
        resources={f"remote_{feed.name}": feed.remote},
    )
    return Definitions.merge(defs, dataset.extra) if dataset.extra else defs


def build_definitions(datasets: list[Dataset], landing: Landing, manifest: Manifest, sql: Sql) -> Definitions:
    shared = Definitions(resources={"landing": landing, "manifest": manifest, "sql": sql})
    return Definitions.merge(shared, *(build_dataset(d) for d in datasets))
