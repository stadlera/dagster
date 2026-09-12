# ingest

Dagster ingestion of external finance datasets: mirror provider files byte for byte into a landing
area, track every file in a manifest, load them into SQL Server with dlt.

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
      schema.py            committed schema: profiler proposes a dlt schema YAML, readers read with its types
      factory.py           Dataset -> assets, checks, sync schedule, load sensor
      definitions.py       Dagster entry point: shared resources + all discovered datasets
      profile.py           CLI: uv run python -m ingest.profile <feed>/<table>
      alerting.py          Notifier resource, run-failure and check-failure sensors
      ops.py               operator helpers: show / reload / ignore / reclassify (also a CLI)
      datasets/<name>/     `dataset = Dataset(...)`, custom Dagster objects, schemas/import/<feed>.schema.yaml

Dagster objects per dataset: one unpartitioned `raw/<feed>` asset (mirror + classify), one partitioned
`sql/<feed>/<table>` asset per table, asset checks, a sync schedule and a load sensor.

## Run locally

    uv sync --extra sftp
    uv run pytest
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

   A subset is a remote folder holding distinct data (a sub dataset, a region, ...). Files are keyed
   as `<subset>/<path below the subset root>` everywhere: in the manifest (`subset`, `path`), in the
   landing area and in the select patterns. Two subsets never collide, whatever their file names.
       dataset = Dataset(feed, tables=(), schema_dir=Path(__file__).parent / "schemas")

   Start with no tables: the sync runs, the manifest fills, and you can look at what actually arrives.
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
2. `uv run python -m ingest.profile acme/prices` samples recent files and writes the table into
   `datasets/acme/schemas/import/acme.schema.yaml` (other tables in the file are kept):
   bigint / decimal(p,s) / date / timestamp are detected from the values, text gets a length bucket
   (20, 50, 100, 255, 1000, else max).
3. Review the YAML (e.g. keep identifiers with leading zeros as text, widen decimals), commit it.
4. Loads read with the committed types: CSV via pyarrow column types, JSON coerced by dlt,
   Parquet/Avro keep their own schema. New columns are added and reported as `new_columns` in the
   asset metadata plus a warning in the run log; add them to the YAML. A changed type fails the load.
   Widen a column by editing the YAML. Without a committed schema dlt infers freely per load.

All tables of a dataset share one dlt pipeline and one SQL schema (`Dataset.sql_schema`, default:
feed name). Loaded rows carry `_business_date`, `_subset`, `_source_file` (manifest id), `_load_id` (Dagster
run id) and `_<name>` for each named group of the select pattern. Nested JSON is flattened into
`parent__child` columns and lists into `<table>__<field>` child tables; `DltWriter(max_nesting=N)`
limits the depth, 0 keeps nested values as json text.

## Operating it

- **Manifest** (`files` table) is the ground truth. Statuses: `downloaded` (active), `superseded`
  (newer version of the same remote path or logical identity), `duplicate` (same identity and content
  as an active row), `expanded` (archive whose members are tracked as rows), `ignored` (excluded by the
  feed). `loaded_at` / `load_id` say which run loaded a row.
- **Landing layout** mirrors the remote: `<root>/<feed>/<last path component>/<relative path>[.vN]`.
  A changed remote file is downloaded as a new version; nothing is ever modified or extracted.
- **Delivery checks** run after every sync on the raw asset, one per table. The newest due occurrence
  missing is a warning (may be late), anything else an error.
- **Alerting**: two sensors send email through the `Notifier` resource: `alert_run_failures`, and
  `alert_run_findings` for successful runs that carry failed checks (severity ERROR; warnings are only
  logged) or schema changes (new columns accepted by the contract). Configure
  `INGEST_SMTP_HOST`, `INGEST_SMTP_PORT` (25), `INGEST_SMTP_FROM`, `INGEST_ALERT_TO` (comma separated),
  optionally `INGEST_SMTP_USER` / `INGEST_SMTP_PASSWORD` for STARTTLS login. Without a host it only
  logs. Turn the sensors on in the Dagster UI.
- **Manual interventions**: reloading is materializing the partition or range in the UI. For the rest
  `uv run python -m ingest.ops ...`: `reload <table> <start> [<end>]` marks a date range as not loaded
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
