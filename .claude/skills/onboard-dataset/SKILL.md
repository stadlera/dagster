---
name: onboard-dataset
description: Onboard a new data provider (SFTP feed) as a dataset package: gather the provider facts, map them to the supported patterns, declare feed and tables, profile the schema, add a scenario test.
---

# Onboard a dataset

Outcome: `src/ingest/datasets/<name>/__init__.py` exposing `dataset`, a committed schema under
`schemas/import/`, and a scenario test in `tests/test_dataset_<name>.py`. Follow README.md
"Adding a dataset", "Semantics worth knowing" and "Provider patterns"; do not re-derive them.

## 1. Facts to collect before writing code

Ask for what is unknown; do not guess naming or cadence.

- Connection: host, port, auth (password or key), root folders. Secrets as `EnvVar("<FEED>_...")`.
- Folder layout: which folders hold distinct data -> one `Feed.subsets` entry each. Depth (`maxdepth`).
- File naming per folder: example names for two consecutive business dates, static or temp files.
- Cadence: daily / weekly / monthly / odd; which calendar; how many files per business date; typical
  delay after the business date (`lag_days`).
- Formats and dialects: csv delimiter, encoding, preamble, header; zip/gz; json nesting; parquet/avro.
- Provider quirks: retention moves, repacks into archives, restatements (same name or new name),
  overwritten "latest" files, files per region or part.
- Target: table names, business key (`Upsert`) or replace-by-day, expected daily volume.

## 2. Map facts to declarations

Use the README "Provider patterns" table row by row. Rules of thumb:

- One subset = one remote root. Tables draw from subsets via `Table(subsets=...)`.
- Patterns are anchored on the logical path: `^<subset>/...`, `date` as named group, other named
  groups only for things that must become columns or must distinguish files of one day.
- Do not declare `archives` for zips that bundle one day; only for containers of many business dates.
- Start with the default identity and `on_collision="latest"`; switch only for a documented quirk.
- Expectation: calendar of business dates + lag; `with_files(exactly=n)` only if the count is stable.

## 3. Deliver in this order

1. Package with `feed` and `tables=()`. `uv run dagster definitions validate`.
2. Sync once against the dev SFTP (materialize `raw/<feed>`), inspect the manifest
   (`select subset, path, status from files where feed = '<feed>'`), confirm the naming facts.
3. Add tables, materialize `raw/<feed>` again (classifies the mirrored files), check that
   `classified` in the asset metadata matches expectations and no `AmbiguousMatch` occurred.
4. `uv run python -m ingest.profile <feed>/<table>`, review the YAML (leading zeros -> text, decimal
   precision with headroom, dates), commit it.
5. Load one partition from the UI, check `_business_date`, `_subset`, attribute columns and row counts.
6. Scenario test: copy the shape of `tests/test_e2e.py` with the provider's real naming, covering at
   least one quirk from step 1 (a move, a restatement, a repack, or a second region). Use small
   inline CSVs; never real data.
7. Turn on `sync_<feed>` and `load_<feed>` in the UI; alerting sensors are shared.

## Checklist before handing over

- [ ] every subset root exists on the remote and lands under `<feed>/<subset>/`
- [ ] each table's patterns match all example names and nothing else (`Patterns.classify(path)` in a test)
- [ ] delivery expectation reflects the real calendar and delay; no ERROR on a normal day
- [ ] schema YAML committed; a second load of the same partition changes nothing
- [ ] README "Provider patterns" gained a row if the provider needed something not listed
