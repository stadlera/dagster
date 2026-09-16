"""Propose a committed schema from landed files and keep the evidence.

    profile(dataset, table, files, options) -> Profile(schema, report)
    write_schema(profile.schema, dataset); write_report(profile, dataset, table)
    load_report(dataset, table) -> dict          # for tests, docs, derived checks

model.py holds the shared value types, sample.py turns files into typed arrow batches, stats.py
accumulates per-column statistics and quirks, keys.py finds file and business keys, propose.py maps the
statistics to dlt columns and suggests a merge strategy.

Report contract (REPORT_VERSION): files are identified by manifest path, never by id. Numeric statistics
are JSON numbers (doubles, also for decimal columns); date and timestamp values are ISO strings. Every
statistic is addressable by a plain path, e.g.
`tables.<table>.volume.rows_per_file.min`, `tables.<table>.keys.candidates[0].columns`,
`tables.<table>.columns.<column>.numeric.max`.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING

from dlt.common.schema import Schema
from dlt.common.schema.utils import new_table

from ingest.schema import committed_schema, schema_path
from ingest_tools.profiling.kernel.model import Column, ProfileOptions
from ingest_tools.profiling.kernel.propose import propose_column, suggest_merge
from ingest_tools.profiling.kernel.stats import TableProfile
from ingest_tools.profiling.sample import FileInfo, sample, sniff_files

if TYPE_CHECKING:
    from ingest.config import Dataset, Table

__all__ = ["Column", "Profile", "ProfileOptions", "load_report", "profile", "summary", "write_report", "write_schema"]

REPORT_VERSION = 1


@dataclass
class Profile:
    schema: Schema  # the dataset schema with the profiled tables replaced
    report: dict


def profile(dataset: Dataset, table: Table, files: list, options: ProfileOptions | None = None) -> Profile:
    options = options or ProfileOptions()
    keys = getattr(getattr(table.writer, "merge", None), "keys", None)
    if keys and not options.keys:
        options = replace(options, keys=tuple(keys))
    tables: dict[str, TableProfile] = {}
    for s in sample(table, files, options):
        tables.setdefault(s.table, TableProfile(s.table, s.parent, options)).add(s)
    schema = committed_schema(dataset) or Schema(dataset.schema_name)
    if stale := [name for name in tables if name in schema.tables]:
        schema.drop_tables(stale)
    report_tables = {}
    for name, tp in tables.items():
        columns = {col: propose_column(p, options) for col, p in tp.columns.items()}
        schema.update_table(new_table(name, parent_table_name=tp.parent, columns=[c.dlt() for c in columns.values()]))
        data = tp.to_dict(columns)
        if tp.parent is None:
            data["suggested"] = asdict(suggest_merge(data["keys"], len(tp.files), options.key_repeat_threshold))
        report_tables[name] = data
    sniffed = sniff_files(table, files)
    report = {
        "version": REPORT_VERSION,
        "table": table.name,
        "options": asdict(options),
        "files": [FileInfo.of(f).to_dict() | ({"csv": sniffed[f.id]} if f.id in sniffed else {}) for f in files],
        "tables": report_tables,
    }
    return Profile(schema, report)


def write_report(profile: Profile, dataset: Dataset, table: Table) -> Path:
    path = report_path(dataset, table)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(profile.report, indent=2, default=str) + "\n")
    return path


def load_report(dataset: Dataset, table: Table) -> dict:
    return json.loads(report_path(dataset, table).read_text())


def report_path(dataset: Dataset, table: Table) -> Path:
    return dataset.schema_dir / "profile" / f"{dataset.schema_name}.{table.name}.profile.json"


def write_schema(schema: Schema, dataset: Dataset) -> Path:
    path = schema_path(dataset)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(schema.to_pretty_yaml())
    return path


def summary(profile: Profile) -> str:
    """A few lines per table and one per column for the terminal."""
    lines = []
    for name, tp in profile.report["tables"].items():
        vol = tp["volume"]["rows_per_file"]
        lines.append(f"{name}: {tp['rows']} rows, {len(tp['files'])} files, {vol['min']}..{vol['max']} rows per file")
        for key in tp["keys"]["candidates"]:
            ratio = key["repeat_ratio"]
            recur = f"{ratio:.0%} of values recur" if ratio is not None else "recurrence unknown"
            lines.append(f"  key {'+'.join(key['columns'])}: {recur}")
        if suggested := tp.get("suggested"):
            lines.append(f"  suggested: {suggested['merge']} on {suggested['keys']} ({suggested['note']})")
        for col, data in tp["columns"].items():
            p = data["proposed"]
            size = ",".join(str(p[k]) for k in ("precision", "scale") if k in p)
            typ = p["data_type"] + (f"({size})" if size else "")
            parts = [f"  {col:<32} {typ:<16} nulls {data['nulls']}/{data['rows']}"]
            if "text" in data:
                parts.append(f"len {data['text']['min_len']}..{data['text']['max_len']}")
            if "numeric" in data:
                parts.append(f"range {data['numeric']['min']}..{data['numeric']['max']}")
            if "temporal" in data:
                parts.append(f"range {data['temporal']['min']}..{data['temporal']['max']}")
            if data.get("unique_in_file"):
                parts.append("unique per file" if not data.get("unique") else "unique")
            if flags := data.get("text", {}).get("flags"):
                parts.append("flags " + ",".join(flags))
            if description := p.get("description"):
                parts.append(f"[{description}]")
            lines.append("  ".join(parts))
    return "\n".join(lines)
