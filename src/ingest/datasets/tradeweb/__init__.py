"""Tradeweb: daily EM csv files. Local demo reads examples/remote; in production set TRADEWEB_* env vars."""

from pathlib import Path

from dagster import EnvVar

from ingest.checks import ExchangeCalendar
from ingest.config import Dataset, Feed, Table
from ingest.resources import Remote

REMOTE_ROOT = EnvVar("TRADEWEB_REMOTE_ROOT").get_value("examples/remote/tradeweb")

feed = Feed(
    name="tradeweb",
    cron="0 7 * * 1-5",
    # production:
    # Remote(protocol="sftp", options={"host": ..., "username": ..., "password": EnvVar("TRADEWEB_PASSWORD")})
    remote=Remote(protocol="file"),
    paths=(f"{REMOTE_ROOT}/EM",),
    exclude=r"^em\.csv$",  # static copy of the latest file, would duplicate the dated one
)

tables = (
    Table(
        "tradeweb", "em", select=(r"/EM/em-(?P<date>\d{4}-\d{2}-\d{2})\.csv$",), start_date="2026-09-01"
    ).with_expectation(ExchangeCalendar("XLON", lag_days=1).with_files(exactly=1)),
)

dataset = Dataset(feed, tables, schema_dir=Path(__file__).parent / "schemas")
