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
    BackfillPolicy,
    Definitions,
    MaterializeResult,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    asset,
    asset_check,
    sensor,
)
from dagster._core.storage.tags import ASSET_PARTITION_RANGE_END_TAG, ASSET_PARTITION_RANGE_START_TAG

from ingest.alerting import Notifier, build_alerting
from ingest.checks import Check, CheckContext
from ingest.classify import classify
from ingest.config import Dataset, Table
from ingest.load import load
from ingest.resources import Landing, Manifest, Sql
from ingest.sync import sync


def build_raw_asset(dataset: Dataset):
    feed = dataset.feed
    remote_key = f"remote_{feed.name}"

    @asset(
        key=feed.raw_key,
        group_name=feed.name,
        description=f"Byte-equivalent mirror of {', '.join(f'{k}={v}' for k, v in feed.subsets.items())}",
        required_resource_keys={remote_key, "landing", "manifest"},
        retry_policy=RetryPolicy(max_retries=2, delay=120),
    )
    def raw(context: AssetExecutionContext) -> MaterializeResult:
        manifest: Manifest = context.resources.manifest
        landing = Landing(root=dataset.landing_root) if dataset.landing_root else context.resources.landing
        result = sync(getattr(context.resources, remote_key).fs(), feed, landing, manifest)
        classified = classify(manifest, dataset)
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
        tags = context.run.tags  # single partition, or the start of a range run
        partition = tags.get("dagster/partition") or tags.get(ASSET_PARTITION_RANGE_START_TAG)
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
        partitions_def=table.partitioning.definition,
        backfill_policy=BackfillPolicy.single_run(),  # a range of partitions is one load
        required_resource_keys={"manifest", "sql"},
        description=f"{type(table.reader).__name__} via {type(table.writer).__name__}",
        # tables of one dataset share a dlt pipeline; limit this key to 1 in the instance to serialize their loads
        op_tags={"dagster/concurrency_key": f"load_{dataset.name}"},
        retry_policy=RetryPolicy(max_retries=1, delay=60),
    )
    def sql_table(context: AssetExecutionContext) -> MaterializeResult:
        manifest: Manifest = context.resources.manifest
        window = context.partition_time_window
        files = manifest.files_for(table.key, window.start.date(), window.end.date())
        if not files:
            return MaterializeResult(metadata={"rows": 0, "files": 0})
        result = load(dataset, table, files, dataset.sql_url or context.resources.sql.url, context.run.run_id)
        manifest.mark_loaded([f.id for f in files], context.run.run_id)
        if new := result.details.get("new_columns"):
            context.log.warning("%s: new columns %s, add them to the committed schema", table.key, new)
        return MaterializeResult(metadata={"rows": result.rows, "files": len(files), **result.details})

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
                key = t.partitioning.key_for(day)
                pending[key] = max(pending.get(key, 0), newest_file)
            for start, end, newest_file in t.partitioning.ranges(pending):
                yield RunRequest(
                    # a later revision yields a new key, so it is reloaded
                    run_key=f"{t.key}/{start}/{end}/{newest_file}",
                    asset_selection=[t.asset_key],
                    tags={ASSET_PARTITION_RANGE_START_TAG: start, ASSET_PARTITION_RANGE_END_TAG: end},
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


def build_definitions(
    datasets: list[Dataset], landing: Landing, manifest: Manifest, sql: Sql, notifier: Notifier | None = None
) -> Definitions:
    shared = Definitions(
        resources={"landing": landing, "manifest": manifest, "sql": sql, "notifier": notifier or Notifier()},
        sensors=build_alerting(),
    )
    return Definitions.merge(shared, *(build_dataset(d) for d in datasets))
