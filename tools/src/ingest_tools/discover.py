"""Inspect remote metadata and suggest inputs for Feed and table onboarding."""

from __future__ import annotations

import argparse
import json
import os
import posixpath
import re
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone

import fsspec

from ingest.remote import coerce_option, modification_time

DATE_SHAPES = (
    (re.compile(r"\d{4}-\d{2}-\d{2}"), "%Y-%m-%d", r"\d{4}\-\d{2}\-\d{2}"),
    (re.compile(r"\d{8}"), "%Y%m%d", r"\d{8}"),
    (re.compile(r"\d{6}"), "%Y%m", r"\d{6}"),
    (re.compile(r"\d{4}"), "%Y", r"\d{4}"),
)


@dataclass(frozen=True)
class RemoteFile:
    path: str
    mtime: datetime | None
    size: int | None


@dataclass(frozen=True)
class PatternSuggestion:
    subset: str
    pattern: str
    date_format: str
    examples: tuple[str, ...]
    cadence: str | None
    weekdays: tuple[int, ...]
    lag_days: tuple[int, int] | None
    files_per_date: tuple[int, int]


@dataclass(frozen=True)
class DiscoveryReport:
    root: str
    files: int
    subsets: dict[str, str]
    maxdepth: int
    cron: str | None
    exclude_candidates: tuple[str, ...]
    patterns: tuple[PatternSuggestion, ...]
    caveats: tuple[str, ...]

    def to_dict(self) -> dict:
        return asdict(self)


def inventory(fs: fsspec.AbstractFileSystem, root: str, maxdepth: int | None = None) -> list[RemoteFile]:
    """List metadata only. Discovery never opens or downloads a remote file."""
    found = fs.find(root, maxdepth=maxdepth, detail=True)
    files = []
    for info in found.values():
        if info.get("type") != "file":
            continue
        raw_mtime = modification_time(info)
        files.append(RemoteFile(info["name"], _parse_mtime(raw_mtime), info.get("size")))
    return sorted(files, key=lambda item: item.path)


def analyze(root: str, files: list[RemoteFile]) -> DiscoveryReport:
    root = root.rstrip("/")
    relative = [(item, posixpath.relpath(item.path, root)) for item in files]
    subset_names = sorted({path.split("/", 1)[0] for _, path in relative if "/" in path})
    subsets = {name: f"{root}/{name}" for name in subset_names}
    maxdepth = max((path.count("/") for _, path in relative), default=0)

    grouped: dict[tuple[str, str, str], list[tuple[RemoteFile, str, date]]] = defaultdict(list)
    undated: list[tuple[str, str]] = []
    caveats = []
    for item, path in relative:
        subset, separator, within_subset = path.partition("/")
        if not separator:
            caveats.append(f"{path}: no subset folder below discovery root")
            continue
        matched = _date_match(posixpath.basename(path))
        if matched is None:
            undated.append((subset, posixpath.basename(path)))
            continue
        match, date_format, regex_date = matched
        try:
            business_date = datetime.strptime(match.group(), date_format).date()
        except ValueError:
            caveats.append(f"{path}: date-shaped value {match.group()!r} is invalid")
            continue
        basename = posixpath.basename(within_subset)
        shape = re.escape(basename[: match.start()]) + f"(?P<date>{regex_date})" + re.escape(basename[match.end() :])
        folder = posixpath.dirname(within_subset)
        logical_shape = f"{re.escape(subset)}/{re.escape(folder)}/" if folder else f"{re.escape(subset)}/"
        grouped[(subset, f"^{logical_shape}{shape}$", date_format)].append((item, path, business_date))

    patterns = tuple(
        _pattern_suggestion(subset, pattern, date_format, observations)
        for (subset, pattern, date_format), observations in sorted(grouped.items())
    )
    cron = _cron(patterns, grouped)
    exclusions = _exclude_candidates(undated, grouped)
    if any(item.mtime is None for item in files):
        caveats.append("some files have missing or unparseable mtimes; schedule and lag evidence excludes them")
    if len(files) < 3:
        caveats.append("fewer than three files observed; cadence, lag, cron, and exclusions need more history")
    return DiscoveryReport(root, len(files), subsets, maxdepth, cron, exclusions, patterns, tuple(sorted(set(caveats))))


def _date_match(name: str):
    for pattern, date_format, regex_date in DATE_SHAPES:
        if match := pattern.search(name):
            return match, date_format, regex_date
    return None


def _pattern_suggestion(subset, pattern, date_format, observations) -> PatternSuggestion:
    dates = sorted({business_date for _, _, business_date in observations})
    gaps = [(right - left).days for left, right in zip(dates, dates[1:])]
    cadence = None
    if len(dates) >= 3 and gaps:
        if max(gaps) <= 3:
            cadence = "daily"
        elif all(6 <= gap <= 8 for gap in gaps):
            cadence = "weekly"
        elif all(27 <= gap <= 32 for gap in gaps):
            cadence = "monthly"
    lags = sorted(
        (item.mtime.date() - business_date).days for item, _, business_date in observations if item.mtime is not None
    )
    counts = Counter(business_date for _, _, business_date in observations)
    return PatternSuggestion(
        subset=subset,
        pattern=pattern,
        date_format=date_format,
        examples=tuple(path for _, path, _ in observations[:3]),
        cadence=cadence,
        weekdays=tuple(sorted({business_date.isoweekday() for business_date in dates})),
        lag_days=(min(lags), max(lags)) if len(lags) >= 3 else None,
        files_per_date=(min(counts.values()), max(counts.values())),
    )


def _cron(patterns, grouped) -> str | None:
    if not patterns or any(len(observations) < 3 for observations in grouped.values()):
        return None
    mtimes = [item.mtime for observations in grouped.values() for item, _, _ in observations if item.mtime]
    if len(mtimes) < 3 or any(item.utcoffset() is None for item in mtimes):
        return None
    minute_of_day = [item.hour * 60 + item.minute for item in mtimes]
    if max(minute_of_day) - min(minute_of_day) > 120:
        return None
    run_at = max(mtimes).replace(second=0, microsecond=0) + timedelta(minutes=25)
    run_at = run_at.replace(minute=0) + timedelta(hours=1) if run_at.minute else run_at
    weekdays = "1-5" if all(item.weekday() < 5 for item in mtimes) else "*"
    return f"{run_at.minute} {run_at.hour} * * {weekdays}"


def _exclude_candidates(undated, grouped) -> tuple[str, ...]:
    if not grouped:
        return ()
    dated_by_subset: dict[str, set[tuple[str, str]]] = defaultdict(set)
    for (subset, pattern, _), observations in grouped.items():
        if len(observations) < 3:
            continue
        for _, path, _ in observations:
            name = posixpath.basename(path)
            matched = _date_match(name)
            if matched:
                match = matched[0]
                signature = (name[: match.start()].rstrip("-_."), posixpath.splitext(name)[1])
                dated_by_subset[subset].add(signature)
    candidates = []
    for subset, name in undated:
        signature = (posixpath.splitext(name)[0].rstrip("-_."), posixpath.splitext(name)[1])
        if signature in dated_by_subset[subset]:
            candidates.append(f"^{re.escape(name)}$")
    return tuple(sorted(set(candidates)))


def _parse_mtime(value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", help="Provider-owned root to inspect")
    parser.add_argument("--protocol", default="file", help="fsspec protocol, for example file or sftp")
    parser.add_argument("--option", action="append", default=[], metavar="KEY=VALUE", help="non-secret fsspec option")
    parser.add_argument(
        "--option-env",
        action="append",
        default=[],
        metavar="KEY=ENV",
        help="fsspec option read from an environment variable",
    )
    parser.add_argument("--maxdepth", type=int, help="limit discovery listing depth")
    args = parser.parse_args(argv)
    options = {key: coerce_option(value) for key, value in (option.split("=", 1) for option in args.option)}
    options.update(
        {key: coerce_option(os.environ[env]) for key, env in (option.split("=", 1) for option in args.option_env)}
    )
    fs = fsspec.filesystem(args.protocol, **options)
    print(json.dumps(analyze(args.root, inventory(fs, args.root, args.maxdepth)).to_dict(), indent=2, default=str))


if __name__ == "__main__":
    main()
