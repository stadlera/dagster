# ingest

Dagster ingestion of external finance datasets: mirror provider files byte for byte into a landing
area, track every file in a manifest, load them into SQL Server with dlt.

The repository is a uv workspace with two distributions. `ingest` is the deployable Dagster user-code
package. `ingest-tools` contains human-run discovery, profiling and manifest administration workflows and
depends on `ingest`; runtime code never depends on tools.

## Stages and layout

One table goes through five stages; each stage is configured by one object on the `Table`:

    1 mirroring       Feed.subsets       sync.py         list each subset root, diff against manifest, download new/changed
    2 selection       Table.source       sources.py      Patterns (select/ignore/archives/identity) or your own Source
                                         classify.py     expand archives, match, resolve identity collisions
    3 checks          Table.checks       checks.py       Delivery (calendar + lag + file count) or any Check
    4 run requests    Table.partitioning partitioning.py Daily/Weekly/Monthly/Yearly or any TimeWindowPartitionsDefinition
    5 loading         Table.reader       readers.py      CsvReader, ParquetReader, JsonReader, AvroReader, FunctionReader
                      Table.writer       writers.py      DltWriter(merge=ReplaceDay() | Upsert(keys)) or your own Writer
                                         load.py         open files (archives.py), read, attach metadata, write

    src/ingest/
      config.py            Feed / Table / Dataset and the asset key properties
      resources.py         Remote (any fsspec filesystem), Landing, Manifest (storage only), Sql
      schema.py            committed schema reads: runtime readers use its types
      factory.py           Dataset -> assets, checks, sync schedule, load sensor
      definitions.py       Dagster entry point: shared resources + all discovered datasets
      alerting.py          Notifier resource, run-failure and run-findings (checks, schema changes) sensors
      datasets/<name>/     `dataset = Dataset(...)`, custom Dagster objects, schemas/import/<feed>.schema.yaml,
                           schemas/profile/<feed>.<table>.profile.json

    tools/src/ingest_tools/
      discover.py          remote metadata inventory and onboarding suggestions
      profile.py           schema profiling CLI and application workflow
      profiling/kernel/    reusable value types, statistics, key discovery and proposal rules
      profiling/sample.py  runtime adapters: landed files and readers -> typed batches
      ops.py               operator helpers: reload / ignore / reclassify

Dagster objects per dataset: one unpartitioned `raw/<feed>` asset (mirror + classify), one partitioned
`sql/<feed>/<table>` asset per table, asset checks, a sync schedule and a load sensor.

## Run locally

    uv sync --all-packages --extra sftp
    uv run pytest
    uv run --package ingest-tools pytest tools/tests
    uv run dagster dev        # demo feed reads examples/remote, writes ./landing, manifest.db, warehouse*.db

Environment: `INGEST_LANDING_ROOT`, `INGEST_MANIFEST_URL`, `INGEST_SQL_URL` (`mssql+pyodbc://...` in
production; sqlite by default), plus per-feed secrets via `EnvVar` in the dataset package.
`Dataset(landing_root=..., sql_url=...)` overrides the shared resources for one dataset.

## Adding a dataset

1. Create `src/ingest/datasets/<name>/__init__.py` exposing a module-level `dataset`:

       feed = Feed(name="acme", cron="0 7 * * 1-5", exclude=r"^latest\.csv$",
                   subsets={"prices": "/outgoing/prices", "refdata": "/outgoing/reference"},
                   remote=Remote(protocol="sftp", options={"host": "sftp.acme.com", "port": "22",
                                 "username": "us", "password": EnvVar("ACME_PASSWORD")}))
                   # or key_filename=... ; options are passed to paramiko's SSHClient.connect
       dataset = Dataset(feed, tables=(), schema_dir=Path(__file__).parent / "schemas")

   A subset is one remote folder holding distinct data (a sub dataset, a region, ...). Files are keyed
   as `<subset>/<path below the subset root>` everywhere: in the manifest (`subset`, `path`), in the
   landing area and in the select patterns. Two subsets never collide, whatever their file names; a
   table may draw from several subsets. Start with no tables: the sync runs, the manifest fills, and
   you can look at what actually arrives.
2. Declare tables once the naming is known. A table names its feed, its SQL table and its source; every
   other stage has a default (daily partitions, CSV, dlt replace-by-day, XLON calendar with one day lag
   and at least one file per day) and is swapped with a builder method:

       Table("acme", "prices", Patterns((r"^prices/(?P<region>apac|emea)/prices-(?P<date>\d{8})\.csv$",), date_format="%Y%m%d"),
             subsets=("prices",))  # only files of these subsets are considered; empty = all
           .with_partitioning(Monthly(start="2024-01-01"))
           .with_reader(CsvReader(delimiter=";", skip_rows=2))
           .with_writer(DltWriter(merge=Upsert(keys=("isin", "as_of"))))
           .with_expectation(ExchangeCalendar("XNYS", lag_days=2).with_files(exactly=2))
           .with_check(RowCountCheck(min_rows=1000))

   The next sync classifies all files already in the manifest (lazy sync first, tables later).
3. Profile and commit the schema (below), then let the load sensor pick up the pending partitions.

## Schema workflow

1. Run the raw asset so files are landed and classified.
2. `uv run ingest-profile acme/prices` reads every classified file (`--skim`: the 5 most recent
   files, 200k rows each; `--files N --rows N` for anything in between), proposes the table's columns into
   `datasets/acme/schemas/import/acme.schema.yaml` (other tables in the file are kept) and writes the
   evidence to `datasets/acme/schemas/profile/acme.prices.profile.json`. Both are committed.
   - Files are sampled the way a load sees them, one file at a time. CSV is read as text and typed with
     arrow kernels (bigint, double, bool, date, timestamp; leading zeros or thousands separators keep a
     column text). Parquet keeps its types. JSON, Avro and other dict readers are denested by dlt with the
     table's `max_nesting`, so child tables `<table>__<field>` are profiled and committed too. Memory is
     bounded per column (a value sample for histograms, capped value counts, distinct values up to
     `distinct_max`, default one million; beyond it `distinct` and `unique` are reported as null).
   - Opt-ins: `--decimals` (decimal(p,s) with two integer digits of headroom instead of double), `--narrow`
     (int / smallint when the value range x10 fits), `--strict-nulls` (NOT NULL for columns without nulls in
     the sample; `Upsert` keys are always NOT NULL), `--date-format %Y%m%d` (repeatable; the reader needs
     the same `CsvReader(date_formats=...)`).
   - Text length: several values all of one length -> that exact length; otherwise the smallest bucket
     of 20, 50, 100, 255, 1000, 4000 above max length x 1.5 (`--text-headroom`); longer: unbounded.
     A column whose type is not the obvious one carries the reason in `description`.
   - The report holds per column: nulls, distinct, uniqueness per file and overall, files present in,
     min/max and a histogram, string lengths, quirks (newlines, doubled quotes, backslash escapes,
     surrounding whitespace, non-ASCII, null-like tokens such as `-`), categorical value counts; per CSV
     file a dialect sniff (delimiter, quoting, line endings, ragged rows) with hints when the file
     disagrees with the declared `CsvReader`.
   - Per table: `volume` (rows per file, files and rows per business date) and `keys`: column sets that
     are unique within every file, whether they are unique across the sample, the share of key values
     that recur in later files (`repeat_ratio`) and the churn between consecutive files (new / dropped
     key values). Every column is followed on its own; pairs (else triples) of the highest-cardinality
     columns that are not unique on their own but unique together within the first file are chosen there
     and verified on every later file. Every surviving key set is reported, most recurring first; which
     one is the business key is a review decision. `file_key_metadata` lists `_business_date` plus every
     pattern attribute that varies between files of one date. `suggested` turns the first key set into
     `Upsert(keys)` when values recur (default: at least half) or `ReplaceDay` otherwise; it is a hint,
     the writer stays your declaration.
   - Report contract: `version`, files identified by manifest path (never id), numeric statistics as
     JSON numbers (doubles, also for decimal columns), dates and timestamps as ISO strings.
    `ingest_tools.profiling.load_report(dataset, table)` reads it back, e.g. for a
     scenario test asserting `tables.em.volume.rows_per_file.min` or a range check derived from
     `tables.em.columns.price.numeric`.
3. Review the YAML (widen decimals, drop a fixed length that is a coincidence of the sample, drop `description`
   lines if you like), commit YAML and report.
4. Loads read with the committed types: CSV via pyarrow column types, JSON coerced by dlt,
   Parquet/Avro keep their own schema. New columns are added and reported as `new_columns` in the
   asset metadata plus a warning in the run log; add them to the YAML. A changed type fails the load.
   Widen a column by editing the YAML. Without a committed schema dlt infers freely per load.

CSV null tokens (`""`, `NA`, `NULL`, `n/a`, `#N/A`, ...) are null in text columns as well as typed ones;
`CsvReader(strings_can_be_null=False)` keeps them as text, `null_values=(...)` replaces the list. Quoting,
escaping and newlines inside values are `CsvReader(quote_char, escape_char, double_quote, newlines_in_values)`.

All tables of a dataset share one dlt pipeline and one SQL schema (`Dataset.sql_schema`, default:
feed name). Loaded rows carry `_business_date`, `_subset`, `_source_file` (manifest id), `_load_id` (Dagster
run id) and `_<name>` for each named group of the select pattern. Nested JSON is flattened into
`parent__child` columns and lists into `<table>__<field>` child tables; `DltWriter(max_nesting=N)`
limits the depth, 0 keeps nested values as json text.

## Semantics worth knowing

- **Business date** is a property of the file, taken from its name by the select pattern (`date` group +
  `date_format`). For period files it is the period start (`%Y%m` -> first of month, `%Y` -> Jan 1st).
  It is the partition key of the SQL asset and the `_business_date` of every row, never the delivery day.
- **Delivery expectation** = calendar of business dates + lag + file count. The calendar says which
  business dates exist, `lag_days` how long after a business date its files may take. A monthly file
  arriving mid next month is `Delivery(Monthly(day=1), lag_days=15)`. Only the last `occurrences` dates
  are checked (default 5), so a check never depends on how far back history goes.
- **Reruns are safe everywhere.** Sync downloads only what is new or changed. Classification only
  touches unassigned rows. A load replaces the business dates it contains (or the keys, with `Upsert`).
  Materializing a partition again is a reload; the load sensor only asks for partitions with unloaded
  files and keys its run requests by the newest pending file, so a revision triggers exactly one reload.
- **Two notions of "same file".** A *version* is the same remote path with changed content (mtime or
  size differ): the old row becomes `superseded`, v2 lands next to v1. An *identity* is the logical file
  across paths (see below): moved or repacked copies dedupe by content, restatements supersede.
- **Ambiguity is an error.** A file matching the select patterns of two tables fails the raw asset run
  (after committing everything unambiguous) so a config mistake cannot load data twice.
- **Schema contract**: new columns are accepted and reported, type changes are refused, the load fails
  and nothing is written. Widening is a manual YAML edit. Without a committed schema dlt infers per load.

## Provider patterns

| Provider behaviour | Declaration |
|---|---|
| Daily file per business day | `Patterns((r"^EM/em-(?P<date>\d{4}-\d{2}-\d{2})\.csv$",))` and the defaults |
| Several sub datasets / folders | one `Feed.subsets` entry per folder, one table per data kind, `Table(subsets=(...))` |
| Regions as sibling folders, same schema | subsets per region or one subset with `(?P<region>...)` in the path; `_region` / `_subset` columns |
| Static "latest" copy next to dated files | `Feed(exclude=r"^latest\.csv$")` (never downloaded, recorded once as `ignored`) |
| Several files per day (parts, regions) | give them a named group; identical names on one day need one |
| Daily zip bundling a day's entities | nothing: the zip is one logical file and is read member by member |
| Yearly / monthly repack of daily files | `Patterns(archives=(r"^EM/em-\d{4}\.zip$",))`: members classified individually, identical ones dedupe |
| Files moved to an archive folder inside the subset | nothing: same identity, same content -> `duplicate` |
| Files moved from a shared folder into per-dataset folders | subsets for both, `Patterns(identity=across_subsets)` |
| Restatement under the same name | nothing: new version, `latest` wins, partition reloads |
| Restatement under a new name (`_corrected`) | `Patterns(identity=lambda m, p: m.group("date"))` |
| Provider repacks noisily, corrections impossible | `Patterns(on_collision="first")` |
| Monthly / weekly / yearly period files | `date_format="%Y%m"` etc., `with_partitioning(Monthly())`, `Delivery(Monthly(day=1), lag_days=N)` |
| Odd schedules (15th, every Saturday, 1st and 3rd Saturday) | `Delivery(Monthly(day=15))`, `Delivery(Weekly(5))`, `Delivery(NthWeekday((1, 3), 5))` |
| Full snapshot deliveries on a schedule | business date = snapshot date, daily partitions with gaps, `with_expectation(None)` or the schedule's calendar |
| CSV with preamble, odd delimiter, no header | `CsvReader(skip_rows=2, delimiter=";", column_names=(...))` |
| Nested JSON | `JsonReader()`; dlt flattens and unnests, `DltWriter(max_nesting=0)` keeps json text |
| XML or other formats | `FunctionReader(fn)` yielding dicts or arrow tables |
| Business key instead of replace-by-day | `DltWriter(merge=Upsert(keys=(...)))` |
| Date only inside the file content | a custom `Source` (`classify(path)` may open the landed file) |

## Operating it

- **Manifest** (`files` table) is the ground truth. Statuses: `downloaded` (active), `superseded`
  (newer version of the same remote path or logical identity), `duplicate` (same identity and content
  as an active row), `expanded` (archive whose members are tracked as rows), `ignored` (excluded by the
  feed). `loaded_at` / `load_id` say which run loaded a row.
- **Landing layout** mirrors the remote: `<root>/<feed>/<subset>/<path below the subset root>[.vN]`.
  A changed remote file is downloaded as a new version next to the old one; nothing is ever modified,
  deleted or extracted. The manifest never forgets a file either: rows change status, never disappear.
- **Delivery checks** run after every sync on the raw asset, one per table. The newest due occurrence
  missing is a warning (may be late), anything else an error.
- **Alerting**: two sensors send email through the `Notifier` resource: `alert_run_failures`, and
  `alert_run_findings` for successful runs that carry failed checks (severity ERROR; warnings are only
  logged) or schema changes (new columns accepted by the contract). Configure
  `INGEST_SMTP_HOST`, `INGEST_SMTP_PORT` (25), `INGEST_SMTP_FROM`, `INGEST_ALERT_TO` (comma separated),
  optionally `INGEST_SMTP_USER` / `INGEST_SMTP_PASSWORD` for STARTTLS login. Without a host it only
  logs. Turn the sensors on in the Dagster UI.
- **Manual interventions**: reloading is materializing the partition or range in the UI. For the rest
  `uv run ingest-ops ...`: `reload <table> <start> [<end>]` marks a date range as not loaded
  so the load sensor requests it again; `ignore <id...>` takes files out of loading (ids from the
  manifest); `reclassify <table>` forgets the table's assignments so the next sync re-runs changed
  patterns.
- **Partitions**: `Table.partitioning` is `Daily()` (default), `Weekly()`, `Monthly()`, `Yearly()` or
  `Partitioning(<any TimeWindowPartitionsDefinition>)`. Loads use a single-run backfill policy: the
  load sensor groups pending partitions into contiguous ranges (`max_per_run`) and one run loads the
  whole range. Empty partitions materialize with zero rows. Reloading a partition replaces its business
  dates (delete-insert), `Upsert` replaces rows with the same key instead.
- **Concurrency**: load assets carry the tag `dagster/concurrency_key: load_<feed>`; set that pool
  to 1 in the instance if loads of one dataset should not run in parallel (they share a dlt pipeline).

## Logical files, archives and collisions

Every classified row gets an `identity`: by default subset + file name + business date + named groups,
so a file keeps its identity when it is moved within its subset (retention folders) or repacked into
an archive. `Patterns(identity=across_subsets)` drops the subset for providers that move files between
folders declared as separate subsets (`output/` -> `history-a/`): the moved copy is then a duplicate.
`Patterns(identity=fn(match, path) -> str)` overrides the rule (e.g. to treat `x_corrected.csv` as a
restatement of `x.csv`), or a custom Source decides entirely.

When a new row has the identity of an active row:

- same content hash: the new row becomes `duplicate` and is never loaded (moved file, identical repack)
- different content: with `Patterns(on_collision="latest")` (default) the old row is `superseded` and
  the partition is reloaded (restatement); with `"first"` the new row is superseded and ignored.

Archives are normally one logical file (a day's entities bundled) and are read as one unit. An archive
holding several logical files (e.g. `em-2026.zip` with one member per day) is listed in
`Patterns(archives=...)`; classify then creates one manifest row per member (`<archive>!<member>`, the
archive becomes `expanded`) and matches members on their virtual path `<archive dir>/<member>`, as if
unpacked in place, so the same select patterns apply. Nothing is extracted to disk; members are read
straight from the archive at load time.

## Extension points

The factory names everything through properties, use them instead of string literals:

    feed.raw_key                 AssetKey(["raw", "<feed>"])            one per dataset, unpartitioned
    table.asset_key              AssetKey(["sql", "<feed>", "<table>"])  partitioned per Table.partitioning
    dataset.sync_schedule_name   "sync_<feed>"
    dataset.load_sensor_name     "load_<feed>"
    resource keys                landing, manifest, sql, remote_<feed>

Custom Dagster objects go into the dataset package and are merged by the factory:

    @asset(deps=[tables[0].asset_key], group_name="acme")
    def prices_report(sql: Sql): ...

    dataset = Dataset(feed, tables, schema_dir=..., extra=Definitions(assets=[prices_report]))

Stage protocols, each a small class you can replace per table:

    Source   expands(path) -> bool, classify(path) -> Classified | None, on_collision   naming rules
    Check    name, target ("raw" | "sql"), evaluate(CheckContext) -> CheckResult        validations
    Reader   read(stream, column_types) -> arrow tables or lists of dicts               file formats
    Writer   column_types(dataset, table), write(ctx, (meta, batch) pairs)             sinks

Checks receive the manifest, the sql resource, the table and the date; no Dagster imports needed.
