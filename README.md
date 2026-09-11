# ingest

Dagster ingestion of external finance datasets.

    Feed  (download)  raw/<feed>          mirror remote paths byte for byte, record in manifest, classify
    Table (load)      sql/<feed>/<table>  daily partitions by business_date, loaded with dlt

## Layout

    src/ingest/config.py       Feed / Table declarations
    src/ingest/resources.py    Remote (any fsspec filesystem), Landing, Manifest (SQLAlchemy, one table)
    src/ingest/sync.py         list remote, diff against manifest, download new/changed files
    src/ingest/loaders.py      CsvLoader, ParquetLoader, JsonLoader, AvroLoader, FunctionLoader (stream -> arrow)
    src/ingest/load.py         open landed files (zip/tar members and gz via fsspec), add metadata, dlt merge
    src/ingest/delivery.py     expectations: ExchangeCalendar, Weekdays, NoExpectation
    src/ingest/factory.py      config -> assets, delivery checks, sync schedule, load sensor
    src/ingest/definitions.py  Dagster entry point: declare feeds and tables here
    schemas/import/            dlt schemas, reviewed and committed; schemas/export/ is generated

Landing layout mirrors the remote: `<root>/<feed>/<last path component>/<relative path>[.vN]`.
A changed remote file is downloaded as a new version; the old manifest row becomes `superseded`.

Manifest rows get `table` and `business_date` from `Manifest.classify()`, which runs after every
sync and also backfills files downloaded before their table was defined (lazy sync first, define tables later).

A table is declared with defaults (CSV, XLON calendar with one day lag, replace by business date)
and adjusted with builder methods:

    Table("tradeweb", "em", select=(r"/EM/(?P<region>apac|emea)/em-(?P<date>\d{4}-\d{2}-\d{2})\.csv$",))
        .with_loader(CsvLoader(delimiter=";", skip_rows=2))
        .with_expectation(ExchangeCalendar("XNYS", lag_days=2))
        .with_merge(Upsert(keys=("isin", "as_of")))

Several `select` patterns may feed one table; `ignore` patterns exclude files from it. A file
matching two tables fails classification. The named group `date` is the business date, every
other named group becomes a `_<name>` column.

Loaded rows carry `_business_date`, `_source_file` (manifest id) and `_load_id` (Dagster run id).
Reloading a partition replaces that business date; `Upsert` replaces rows with the same key
instead. Schema contract: new columns are added, a changed type fails the load.

## Run locally

    uv sync --extra sftp
    uv run pytest
    uv run dagster dev        # demo feed reads examples/remote, writes ./landing, manifest.db, warehouse*.db

Environment: `INGEST_LANDING_ROOT`, `INGEST_MANIFEST_URL`, `INGEST_SQL_URL`
(`mssql+pyodbc://...` in production; sqlite by default), plus per-feed secrets via `EnvVar`.
