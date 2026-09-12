"""Turn Dataset config into Dagster assets, checks, schedules and sensors."""

from datetime import date, timedelta

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    AssetKey,
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

from ingest.config import Dataset, Feed, Table
from ingest.delivery import LOOKBACK_DAYS
from ingest.load import load
from ingest.resources import Landing, Manifest, Sql
from ingest.sync import sync

PARTITIONS = {"daily": DailyPartitionsDefinition, "monthly": MonthlyPartitionsDefinition}


def raw_key(feed: Feed) -> AssetKey:
    return AssetKey(["raw", feed.name])


def table_key(table: Table) -> AssetKey:
    return AssetKey(["sql", table.feed, table.name])


def partition_key(table: Table, day: date) -> str:
    return str(day.replace(day=1) if table.partition == "monthly" else day)


def build_raw_asset(dataset: Dataset):
    feed, tables = dataset.feed, dataset.tables
    remote_key = f"remote_{feed.name}"

    @asset(
        key=raw_key(feed),
        group_name=feed.name,
        description=f"Byte-equivalent mirror of {', '.join(feed.paths)}",
        required_resource_keys={remote_key, "landing", "manifest"},
        retry_policy=RetryPolicy(max_retries=2, delay=120),
    )
    def raw(context: AssetExecutionContext) -> MaterializeResult:
        manifest: Manifest = context.resources.manifest
        result = sync(getattr(context.resources, remote_key).fs(), feed, context.resources.landing, manifest)
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


def build_delivery_check(dataset: Dataset, table: Table):
    @asset_check(
        asset=raw_key(dataset.feed), name=f"delivery_{table.name}", description="Files arrived per delivery calendar"
    )
    def delivery(manifest: Manifest) -> AssetCheckResult:
        today = date.today()
        lag = table.expectation.lag_days
        counts = manifest.file_counts(table.key, today - timedelta(days=LOOKBACK_DAYS + lag))
        missing = table.expectation.missing_days(counts, today)
        last_due = today - timedelta(days=lag)
        # only the newest due day missing: warn, it may just be late. Anything older: error.
        severity = AssetCheckSeverity.WARN if missing == [last_due] else AssetCheckSeverity.ERROR
        return AssetCheckResult(
            passed=not missing, severity=severity, metadata={"missing_days": [str(d) for d in missing]}
        )

    return delivery


def build_table_asset(dataset: Dataset, table: Table):
    @asset(
        key=table_key(table),
        deps=[raw_key(dataset.feed)],
        group_name=dataset.feed.name,
        partitions_def=PARTITIONS[table.partition](start_date=table.start_date),
        required_resource_keys={"manifest", "sql"},
        description=f"{type(table.loader).__name__} on {', '.join(table.select)}",
    )
    def sql_table(context: AssetExecutionContext) -> MaterializeResult:
        manifest: Manifest = context.resources.manifest
        sql: Sql = context.resources.sql
        window = context.partition_time_window
        files = manifest.files_for(table.key, window.start.date(), window.end.date())
        if not files:
            return MaterializeResult(metadata={"rows": 0, "files": 0})
        result = load(table, files, sql.url, sql.dataset_name, context.run_id, dataset.schema_dir)
        manifest.mark_loaded([f.id for f in files], context.run_id)
        return MaterializeResult(
            metadata={"rows": result["rows"], "files": len(files), "dlt_load_ids": result["load_ids"]}
        )

    return sql_table


def build_load_sensor(dataset: Dataset):
    tables = dataset.tables

    @sensor(
        name=f"load_{dataset.feed.name}",
        target=AssetSelection.keys(*[table_key(t) for t in tables]),
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
                    asset_selection=[table_key(t)],
                    partition_key=key,
                )

    return load_sensor


def build_dataset(dataset: Dataset) -> Definitions:
    feed = dataset.feed
    defs = Definitions(
        assets=[build_raw_asset(dataset), *(build_table_asset(dataset, t) for t in dataset.tables)],
        asset_checks=[build_delivery_check(dataset, t) for t in dataset.tables],
        schedules=[
            ScheduleDefinition(
                name=f"sync_{feed.name}", cron_schedule=feed.cron, target=AssetSelection.keys(raw_key(feed))
            )
        ],
        sensors=[build_load_sensor(dataset)] if dataset.tables else [],
        resources={f"remote_{feed.name}": feed.remote},
    )
    return Definitions.merge(defs, dataset.extra) if dataset.extra else defs


def build_definitions(datasets: list[Dataset], landing: Landing, manifest: Manifest, sql: Sql) -> Definitions:
    shared = Definitions(resources={"landing": landing, "manifest": manifest, "sql": sql})
    return Definitions.merge(shared, *(build_dataset(d) for d in datasets))
