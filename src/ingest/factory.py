"""Turn Provider/Dataset config into Dagster assets, checks and schedules."""


from datetime import date, timedelta

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    Definitions,
    MaterializeResult,
    RetryPolicy,
    ScheduleDefinition,
    asset,
    asset_check,
)

from ingest.config import Dataset, Provider
from ingest.delivery import LOOKBACK_DAYS, missing_days
from ingest.resources import Landing, Manifest
from ingest.sync import sync


def raw_asset_key(dataset: Dataset) -> AssetKey:
    return AssetKey(["raw", dataset.provider, dataset.name])


def build_raw_asset(dataset: Dataset):
    remote_key = f"remote_{dataset.provider}"

    @asset(
        key=raw_asset_key(dataset),
        group_name=dataset.provider,
        description=f"Byte-equivalent mirror of {dataset.remote_path}",
        required_resource_keys={remote_key, "landing", "manifest"},
        retry_policy=RetryPolicy(max_retries=2, delay=120),
    )
    def raw(context: AssetExecutionContext) -> MaterializeResult:
        fs = getattr(context.resources, remote_key).fs()
        result = sync(fs, dataset, context.resources.landing, context.resources.manifest)
        context.log.info(
            "%s: %d new, %d revisions, %d unchanged, %d ignored",
            dataset.key, len(result.downloaded), len(result.revisions), result.unchanged, result.ignored,
        )
        return MaterializeResult(
            metadata={
                "new_files": len(result.downloaded),
                "revisions": len(result.revisions),
                "unchanged": result.unchanged,
                "ignored": result.ignored,
                "files": result.downloaded[-20:] + result.revisions[-20:],
            }
        )

    @asset_check(asset=raw_asset_key(dataset), name="delivery", description="Files arrived per delivery calendar")
    def delivery(manifest: Manifest) -> AssetCheckResult:
        today = date.today()
        counts = manifest.file_counts(dataset.key, today - timedelta(days=LOOKBACK_DAYS + dataset.expected_lag_days))
        missing = missing_days(dataset, counts, today)
        last_due = today - timedelta(days=dataset.expected_lag_days)
        # only the newest due day missing: warn, it may just be late. Anything older: error.
        severity = AssetCheckSeverity.WARN if missing == [last_due] else AssetCheckSeverity.ERROR
        return AssetCheckResult(
            passed=not missing,
            severity=severity,
            metadata={"missing_days": [str(d) for d in missing]},
        )

    return raw, delivery


def build_definitions(providers: list[Provider], landing: Landing, manifest: Manifest) -> Definitions:
    assets, checks, schedules, resources = [], [], [], {"landing": landing, "manifest": manifest}
    for provider in providers:
        resources[f"remote_{provider.name}"] = provider.remote
        for dataset in provider.datasets:
            raw, delivery = build_raw_asset(dataset)
            assets.append(raw)
            checks.append(delivery)
        schedules.append(
            ScheduleDefinition(
                name=f"sync_{provider.name}",
                cron_schedule=provider.cron,
                target=AssetSelection.groups(provider.name),
            )
        )
    return Definitions(assets=assets, asset_checks=checks, schedules=schedules, resources=resources)
