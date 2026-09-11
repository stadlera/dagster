"""Turn Feed/Table config into Dagster assets, checks, schedules and sensors."""

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
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SensorEvaluationContext,
    asset,
    asset_check,
    sensor,
)

from ingest.config import Feed, Table
from ingest.delivery import LOOKBACK_DAYS
from ingest.load import load
from ingest.resources import Landing, Manifest
from ingest.sync import sync


def raw_key(feed: Feed) -> AssetKey:
    return AssetKey(["raw", feed.name])


def table_key(table: Table) -> AssetKey:
    return AssetKey(["sql", table.feed, table.name])


def build_raw_asset(feed: Feed, tables: list[Table]):
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
            feed.name, len(result.downloaded), len(result.revisions), result.unchanged, result.ignored, classified,
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


def build_delivery_check(feed: Feed, table: Table):
    @asset_check(asset=raw_key(feed), name=f"delivery_{table.name}", description="Files arrived per delivery calendar")
    def delivery(manifest: Manifest) -> AssetCheckResult:
        today = date.today()
        lag = table.expectation.lag_days
        counts = manifest.file_counts(table.key, today - timedelta(days=LOOKBACK_DAYS + lag))
        missing = table.expectation.missing_days(counts, today)
        last_due = today - timedelta(days=lag)
        # only the newest due day missing: warn, it may just be late. Anything older: error.
        severity = AssetCheckSeverity.WARN if missing == [last_due] else AssetCheckSeverity.ERROR
        return AssetCheckResult(passed=not missing, severity=severity, metadata={"missing_days": [str(d) for d in missing]})

    return delivery


def build_table_asset(feed: Feed, table: Table):
    @asset(
        key=table_key(table),
        deps=[raw_key(feed)],
        group_name=feed.name,
        partitions_def=DailyPartitionsDefinition(start_date=table.start_date),
        required_resource_keys={"manifest", "sql"},
        description=f"{type(table.loader).__name__} on {', '.join(table.select)}",
    )
    def sql_table(context: AssetExecutionContext) -> MaterializeResult:
        manifest: Manifest = context.resources.manifest
        day = date.fromisoformat(context.partition_key)
        files = manifest.files_for(table.key, day)
        if not files:
            return MaterializeResult(metadata={"rows": 0, "files": 0})
        result = load(table, day, files, context.resources.sql.url, context.resources.sql.dataset_name, context.run_id)
        manifest.mark_loaded([f.id for f in files], context.run_id)
        return MaterializeResult(metadata={"rows": result["rows"], "files": len(files), "dlt_load_ids": result["load_ids"]})

    return sql_table


def build_load_sensor(feed: Feed, tables: list[Table]):
    @sensor(name=f"load_{feed.name}", target=AssetSelection.keys(*[table_key(t) for t in tables]), minimum_interval_seconds=300)
    def load_sensor(context: SensorEvaluationContext, manifest: Manifest):
        for t in tables:
            for day in manifest.pending_days(t.key):
                yield RunRequest(
                    run_key=f"{t.key}/{day}/{context.cursor or ''}",
                    asset_selection=[table_key(t)],
                    partition_key=str(day),
                )

    return load_sensor


def build_definitions(feeds: list[Feed], tables: list[Table], landing: Landing, manifest: Manifest, sql) -> Definitions:
    assets, checks, schedules, sensors = [], [], [], []
    resources = {"landing": landing, "manifest": manifest, "sql": sql}
    for feed in feeds:
        feed_tables = [t for t in tables if t.feed == feed.name]
        resources[f"remote_{feed.name}"] = feed.remote
        assets.append(build_raw_asset(feed, feed_tables))
        schedules.append(ScheduleDefinition(name=f"sync_{feed.name}", cron_schedule=feed.cron, target=AssetSelection.keys(raw_key(feed))))
        for table in feed_tables:
            assets.append(build_table_asset(feed, table))
            checks.append(build_delivery_check(feed, table))
        if feed_tables:
            sensors.append(build_load_sensor(feed, feed_tables))
    return Definitions(assets=assets, asset_checks=checks, schedules=schedules, sensors=sensors, resources=resources)
