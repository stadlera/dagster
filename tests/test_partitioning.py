"""Stage 4, run requests: partition keys and contiguous range runs."""

from datetime import date

from dagster import TimeWindowPartitionsDefinition

from ingest.partitioning import Daily, Monthly, Partitioning, Weekly, Yearly


def test_business_dates_map_to_partition_keys():
    assert Daily().key_for(date(2026, 9, 8)) == "2026-09-08"
    assert Monthly().key_for(date(2026, 9, 8)) == "2026-09-01"
    assert Weekly().key_for(date(2026, 9, 9)) == "2026-09-06"  # dagster weeks start on Sunday
    assert Yearly(start="2024-01-01").key_for(date(2025, 6, 1)) == "2025-01-01"


def test_pending_partitions_are_grouped_into_contiguous_capped_ranges():
    pending = {"2026-09-01": 1, "2026-09-02": 5, "2026-09-03": 2, "2026-09-07": 9, "2026-09-08": 3}
    assert Daily().ranges(pending) == [("2026-09-01", "2026-09-03", 5), ("2026-09-07", "2026-09-08", 9)]
    assert Daily(max_per_run=2).ranges(pending) == [
        ("2026-09-01", "2026-09-02", 5),
        ("2026-09-03", "2026-09-03", 2),
        ("2026-09-07", "2026-09-08", 9),
    ]
    assert Daily().ranges({}) == []


def test_any_time_window_definition_can_be_used():
    semi_monthly = TimeWindowPartitionsDefinition(start="2026-01-01", cron_schedule="0 0 1,15 * *", fmt="%Y-%m-%d")
    p = Partitioning(semi_monthly, max_per_run=4)
    assert p.key_for(date(2026, 1, 20)) == "2026-01-15"
    assert p.ranges({"2026-01-01": 1, "2026-01-15": 2, "2026-02-15": 3}) == [
        ("2026-01-01", "2026-01-15", 2),
        ("2026-02-15", "2026-02-15", 3),
    ]
