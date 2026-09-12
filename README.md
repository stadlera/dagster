# ingest

Dagster ingestion of external finance datasets.

    Feed  (download)  raw/<feed>          mirror remote paths byte for byte, record in manifest, classify
    Table (load)      sql/<feed>/<table>  daily partitions by business_date, loaded with dlt

## Layout

    src/ingest/
      config.py            Feed / Table / Dataset declarations and merge strategies
      resources.py         Remote (any fsspec filesystem), Landing, Manifest (SQLAlchemy, one table), Sql
      sync.py              list remote, diff against manifest, download new/changed files
      loaders.py           CsvLoader, ParquetLoader, JsonLoader, AvroLoader, FunctionLoader (stream -> arrow/dicts)
      load.py              open landed files (zip/tar members and gz via fsspec), add metadata, dlt merge
      schema.py            committed schema: profiler proposes a dlt schema YAML, loaders read with its types
      checks.py            Check protocol, calendars, Delivery expectation builder
      factory.py           Dataset -> assets, delivery checks, sync schedule, load sensor
      definitions.py       Dagster entry point: shared resources + all discovered datasets
      profile.py           CLI: uv run python -m ingest.profile <feed>/<table>
      datasets/
        tradeweb/
          __init__.py      feed, tables, optional custom Dagster objects  -> `dataset = Dataset(...)`
          schemas/import/  committed dlt schema <feed>.schema.yaml, all tables (export/ is generated)

Adding a dataset means adding one package under `datasets/` that exposes a module-level
`dataset`. Custom assets, sensors or schedules for that provider go into `Dataset(extra=Definitions(...))`
in the same package, next to its schemas.

Landing layout mirrors the remote: `<root>/<feed>/<last path component>/<relative path>[.vN]`.
A changed remote file is downloaded as a new version; the old manifest row becomes `superseded`.

Manifest rows get `table` and `business_date` from `Manifest.classify()`, which runs after every
sync and also backfills files downloaded before their table was defined (lazy sync first, define tables later).

A table is declared with defaults (CSV, XLON calendar with one day lag and at least one file per
day, replace by business date) and adjusted with builder methods:

    Table("tradeweb", "em", select=(r"/EM/(?P<region>apac|emea)/em-(?P<date>\d{4}-\d{2}-\d{2})\.csv$",))
        .with_loader(CsvLoader(delimiter=";", skip_rows=2))
        .with_expectation(ExchangeCalendar("XNYS", lag_days=2).with_files(exactly=2))
        .with_merge(Upsert(keys=("isin", "as_of")))
        .with_check(RowCountCheck(min_rows=1000))

`with_expectation` replaces the delivery expectation (`None` disables it); `with_check` adds any
object implementing `Check` (`name`, `target` = raw | sql, `evaluate(CheckContext) -> CheckResult`).
Checks get the manifest, the sql resource, the table and the date, no Dagster knowledge needed.

## Extension points

The factory names everything through properties, use them instead of string literals:

    feed.raw_key                 AssetKey(["raw", "<feed>"])          one per dataset, unpartitioned
    table.asset_key              AssetKey(["sql", "<feed>", "<table>"])  daily or monthly partitions
    dataset.sync_schedule_name   "sync_<feed>"
    dataset.load_sensor_name     "load_<feed>"
    resource keys                landing, manifest, sql, remote_<feed>

Custom Dagster objects go into the dataset package and are merged by the factory:

    @asset(deps=[tables[0].asset_key], group_name="tradeweb")
    def em_report(sql: Sql): ...

    dataset = Dataset(feed, tables, schema_dir=..., extra=Definitions(assets=[em_report]))

All tables of a dataset share one dlt pipeline and one SQL schema (`Dataset.sql_schema`, default:
feed name). Load assets carry the tag `dagster/concurrency_key: load_<feed>`; set that pool to 1 in
the instance if loads of one dataset should not run in parallel.

Several `select` patterns may feed one table; `ignore` patterns exclude files from it. A file
matching two tables fails classification. The named group `date` is the business date, every
other named group becomes a `_<name>` column.

`partition="monthly"` on a Table makes one load run cover all business dates of the month.
Empty partitions materialize with zero rows; missing deliveries are reported by the delivery check.

## Schema workflow

1. Run the raw asset so files are landed and classified.
2. `uv run python -m ingest.profile tradeweb/em` samples recent files and writes the table into
   `datasets/tradeweb/schemas/import/tradeweb.schema.yaml` (other tables in the file are kept): bigint / decimal(p,s) / date / timestamp are detected
   from the values, text gets a length bucket (20, 50, 100, 255, 1000, else max).
3. Review the YAML (e.g. keep identifiers with leading zeros as text), commit it.
4. Loads read with the committed types: CSV via pyarrow column types, JSON coerced by dlt,
   Parquet/Avro keep their own schema. New columns are added (warning), a changed type fails.

Loaded rows carry `_business_date`, `_source_file` (manifest id) and `_load_id` (Dagster run id).
Reloading a partition replaces that business date; `Upsert` replaces rows with the same key
instead. Schema contract: new columns are added, a changed type fails the load.

## Run locally

    uv sync --extra sftp
    uv run pytest
    uv run dagster dev        # demo feed reads examples/remote, writes ./landing, manifest.db, warehouse*.db

Environment: `INGEST_LANDING_ROOT`, `INGEST_MANIFEST_URL`, `INGEST_SQL_URL`
(`mssql+pyodbc://...` in production; sqlite by default), plus per-feed secrets via `EnvVar`.
