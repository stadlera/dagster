# ingest — working notes for code changes

Dagster + dlt ingestion of provider files (SFTP -> byte-exact landing area -> manifest -> SQL Server).
Read README.md first: "Stages and layout", "Semantics worth knowing", "Provider patterns".

## Commands

    uv run pytest -q                              # ~60 tests, < 10 s, sqlite + temp dirs, no network
    uvx ruff check --fix src tests && uvx ruff format src tests
    uv run dagster definitions validate           # after touching config/factory/datasets
    uv run dagster dev                            # demo dataset in src/ingest/datasets/tradeweb

## Architecture rules

- One object per stage on `Table`: `source`, `checks`, `partitioning`, `reader`, `writer`. Provider
  quirks go into these objects or into `datasets/<name>/`, never into new fields on `Table` or `Feed`.
- Protocols (`Source`, `Check`, `Reader`, `Writer`) get a new method or a new sibling protocol only when
  a second real implementation needs it. Prefer a class implementing an existing protocol.
- `resources.Manifest` is storage: small named queries, no decisions. Decisions live in `classify.py`,
  `load.py`, `factory.py`.
- Asset keys and job names come from `Feed.raw_key`, `Table.asset_key`, `Dataset.*_name`; never build
  key strings by hand.
- The manifest never deletes rows and the landing area is never modified. New behaviour must preserve:
  reruns download nothing new, reloads never duplicate, every row is traceable to a manifest id.
- sqlite locally, SQL Server in production: no dialect-specific SQL in the manifest; dlt handles the sink.
- Nothing generated is committed: `landing/`, `*.db`, dlt work dirs. Committed schemas and profile reports
  live in `datasets/<name>/schemas/import/` and `schemas/profile/` and are written only by `ingest.profile`.
- Profiling is `profiling/`: `model.py` (shared value types: `Column`, `Typed`, `FileInfo`, `ProfileOptions`),
  `sample.py` (files -> typed arrow batches, one path per format, one file at a time), `stats.py` (statistics
  as values: `XStats.of(array)` merged with `+`, no decisions), `keys.py` (file and business keys), `propose.py`
  (statistics -> `Column`, an ordered rule table, every rule reads options). `schema.py` is committed-schema io
  only. A new heuristic is a stat in `stats.py` plus a rule in `propose.py` plus a test. Pass typed objects
  between the stages, not dicts; the report dict is built only in `to_dict` methods. The default run reads every
  file and row, so nothing may retain rows across files (the first file is the one exception, for key candidates).

## When changing behaviour

1. Find the stage (README table) and its test file: `tests/test_<stage>.py`. `tests/conftest.py`
   provides `ws` (a fake remote dir, landing, sqlite manifest and warehouse) with `sync`, `classify`,
   `load`, `query`, `statuses` helpers. `tests/test_e2e.py` is the two-day provider scenario.
2. Add or adjust a test that states the provider behaviour in its name, then change the code.
3. If a semantic changed (statuses, identity, partitions, schema handling), update the matching README
   section in the same change. Keep README sections as the single explanation; do not add doc files.
4. Run the three commands above. Warnings from our own package are errors in pytest.

## Pitfalls seen

- Dagster: `from __future__ import annotations` breaks `@asset` context typing in `factory.py`.
- Dagster config rejects union types on resources (`Remote.options` is `dict[str, str]`, coerced in `fs()`).
- dlt seeds `import_schema_path` itself if the file is missing; we pass it only when a committed schema exists.
- Select patterns match the logical path `<subset>/<relative>`; anchor with `^<subset>/`.
- A failing insert inside a manifest transaction holds the sqlite lock and makes pytest hang.
