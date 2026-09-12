"""Entry point for Dagster: shared resources from the environment plus every dataset package."""

from dagster import EnvVar

from ingest.alerting import Notifier
from ingest.datasets import discover
from ingest.factory import build_definitions
from ingest.resources import Landing, Manifest, Sql

datasets = discover()

defs = build_definitions(
    datasets,
    landing=Landing(root=EnvVar("INGEST_LANDING_ROOT").get_value("landing")),
    manifest=Manifest(url=EnvVar("INGEST_MANIFEST_URL").get_value("sqlite:///manifest.db")),
    sql=Sql(url=EnvVar("INGEST_SQL_URL").get_value("sqlite:///warehouse.db")),
    notifier=Notifier(
        smtp_host=EnvVar("INGEST_SMTP_HOST").get_value(),
        smtp_port=int(EnvVar("INGEST_SMTP_PORT").get_value("25")),
        smtp_from=EnvVar("INGEST_SMTP_FROM").get_value("ingest@localhost"),
        smtp_to=EnvVar("INGEST_ALERT_TO").get_value(""),
        smtp_user=EnvVar("INGEST_SMTP_USER").get_value(),
        smtp_password=EnvVar("INGEST_SMTP_PASSWORD").get_value(),
    ),
)
