"""Entry point for Dagster. Feeds and tables are declared here, secrets come from the environment."""

from dagster import ConfigurableResource, EnvVar

from ingest.config import Feed, Table
from ingest.delivery import ExchangeCalendar
from ingest.factory import build_definitions
from ingest.resources import Landing, Manifest, Remote


class Sql(ConfigurableResource):
    """Target database for loaded tables. sqlalchemy url; mssql+pyodbc://... in production."""

    url: str
    dataset_name: str = "raw"


feeds = [
    Feed(
        name="tradeweb",
        cron="0 7 * * 1-5",
        # local demo: the "remote" is a directory in this repo. In prod:
        # Remote(protocol="sftp", options={"host": ..., "username": ..., "password": EnvVar("TRADEWEB_PASSWORD")})
        remote=Remote(protocol="file"),
        paths=(EnvVar("TRADEWEB_REMOTE_ROOT").get_value("examples/remote/tradeweb") + "/EM",),
        exclude=r"^em\.csv$",
    ),
]

tables = [
    Table("tradeweb", "em", select=(r"/EM/em-(?P<date>\d{4}-\d{2}-\d{2})\.csv$",), start_date="2026-09-01")
    .with_expectation(ExchangeCalendar("XLON", lag_days=1)),
]

defs = build_definitions(
    feeds,
    tables,
    landing=Landing(root=EnvVar("INGEST_LANDING_ROOT").get_value("landing")),
    manifest=Manifest(url=EnvVar("INGEST_MANIFEST_URL").get_value("sqlite:///manifest.db")),
    sql=Sql(url=EnvVar("INGEST_SQL_URL").get_value("sqlite:///warehouse.db")),
)
