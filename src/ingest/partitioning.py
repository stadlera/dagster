"""Stage 4, run requests: how a table is partitioned and how pending business dates become runs.

Any Dagster TimeWindowPartitionsDefinition works; Daily/Weekly/Monthly/Yearly build the common ones.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone

from dagster import (
    DailyPartitionsDefinition,
    MonthlyPartitionsDefinition,
    PartitionKeyRange,
    TimeWindowPartitionsDefinition,
    WeeklyPartitionsDefinition,
)


@dataclass(frozen=True)
class Partitioning:
    definition: TimeWindowPartitionsDefinition
    max_per_run: int = 31  # pending partitions are grouped into contiguous range runs of at most this size

    def key_for(self, day: date) -> str:
        timestamp = datetime.combine(day, time.min, timezone.utc).timestamp()
        return self.definition.get_partition_key_for_timestamp(timestamp)

    def ranges(self, pending: dict[str, int]) -> list[tuple[str, str, int]]:
        """Group pending partition keys (-> newest file id) into contiguous ranges of at most max_per_run keys.
        Returns (start_key, end_key, newest_file_id) triples."""
        if not pending:
            return []
        keys = self.definition.get_partition_keys_in_range(PartitionKeyRange(min(pending), max(pending)))
        ranges: list[list[str]] = []
        current: list[str] = []
        for key in keys:
            if key in pending and len(current) < self.max_per_run:
                current.append(key)
            else:
                if current:
                    ranges.append(current)
                current = [key] if key in pending else []
        if current:
            ranges.append(current)
        return [(r[0], r[-1], max(pending[k] for k in r)) for r in ranges]


def Daily(start: str = "2026-01-01", max_per_run: int = 31) -> Partitioning:
    return Partitioning(DailyPartitionsDefinition(start_date=start), max_per_run)


def Weekly(start: str = "2026-01-01", max_per_run: int = 12) -> Partitioning:
    return Partitioning(WeeklyPartitionsDefinition(start_date=start), max_per_run)


def Monthly(start: str = "2026-01-01", max_per_run: int = 12) -> Partitioning:
    return Partitioning(MonthlyPartitionsDefinition(start_date=start), max_per_run)


def Yearly(start: str = "2020-01-01", max_per_run: int = 1) -> Partitioning:
    return Partitioning(
        TimeWindowPartitionsDefinition(start=start, cron_schedule="0 0 1 1 *", fmt="%Y-%m-%d"), max_per_run
    )
