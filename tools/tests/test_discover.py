"""Read-only remote inventory suggestions used before declaring a feed."""

from datetime import datetime, timezone

from ingest_tools.discover import RemoteFile, analyze


def test_analyze_suggests_feed_fields_patterns_and_delivery_evidence():
    files = []
    for folder, prefix in (("EM", "em"), ("Platform", "prices")):
        for day in (8, 9, 10):
            files.append(
                RemoteFile(
                    f"/out/{folder}/{prefix}-2026-09-{day:02}.csv",
                    datetime(2026, 9, day + 1, 6, 35, tzinfo=timezone.utc),
                    100,
                )
            )
    files.append(RemoteFile("/out/EM/em.csv", datetime(2026, 9, 11, 6, 36, tzinfo=timezone.utc), 100))

    report = analyze("/out", files)

    assert report.subsets == {"EM": "/out/EM", "Platform": "/out/Platform"}
    assert report.maxdepth == 1
    assert report.cron == "0 7 * * 1-5"
    assert report.exclude_candidates == (r"^em\.csv$",)
    assert [(item.subset, item.pattern, item.date_format) for item in report.patterns] == [
        ("EM", r"^EM/em\-(?P<date>\d{4}\-\d{2}\-\d{2})\.csv$", "%Y-%m-%d"),
        ("Platform", r"^Platform/prices\-(?P<date>\d{4}\-\d{2}\-\d{2})\.csv$", "%Y-%m-%d"),
    ]
    assert all(
        item.cadence == "daily" and item.weekdays == (2, 3, 4) and item.lag_days == (1, 1) for item in report.patterns
    )
    assert all(item.files_per_date == (1, 1) for item in report.patterns)


def test_analyze_does_not_guess_schedule_or_exclusion_from_too_little_evidence():
    report = analyze(
        "/out",
        [RemoteFile("/out/EM/em-20260908.csv", datetime(2026, 9, 9, tzinfo=timezone.utc), 100)],
    )

    assert report.cron is None
    assert report.exclude_candidates == ()
    assert report.patterns[0].date_format == "%Y%m%d"
    assert report.patterns[0].cadence is None
