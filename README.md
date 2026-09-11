# ingest

Dagster ingestion of external finance datasets. Stage 1: mirror provider files
byte for byte into the landing area and record them in a manifest.

## Layout

    src/ingest/config.py       Dataset / Provider declarations (the only config)
    src/ingest/resources.py    Remote (any fsspec filesystem), Landing, Manifest (SQLAlchemy)
    src/ingest/sync.py         the mirror: list remote, diff against manifest, download new/changed
    src/ingest/delivery.py     expected business days vs. arrivals
    src/ingest/factory.py      config -> raw asset + delivery check + schedule per provider
    src/ingest/definitions.py  Dagster entry point, declare providers here

Landing layout: `<root>/<provider>/<dataset>/<YYYY>/<MM>/<original filename>[.vN]`.
A changed remote file is downloaded as a new version, the old row becomes `superseded`.

## Run locally

    uv sync --extra sftp
    uv run pytest
    uv run dagster dev            # demo provider reads examples/remote, writes ./landing and ./manifest.db

Environment: `INGEST_LANDING_ROOT`, `INGEST_MANIFEST_URL` (e.g. `mssql+pyodbc://...`),
plus per-provider secrets referenced via `EnvVar` in definitions.py.

## Adding a dataset

Add a `Dataset(...)` to the provider in `definitions.py`. Filename patterns
(`include`, `exclude`, `business_date`) and the delivery expectation
(`calendar`, `expected_lag_days`, `expected_files_per_day`) are all it needs.
