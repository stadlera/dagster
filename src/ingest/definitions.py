"""Entry point for Dagster: shared resources from the environment plus every dataset package."""

from dagster import EnvVar

from ingest.datasets import discover
from ingest.factory import build_definitions
from ingest.resources import Landing, Manifest, Sql

datasets = discover()

defs = build_definitions(
    datasets,
    landing=Landing(root=EnvVar("INGEST_LANDING_ROOT").get_value("landing")),
    manifest=Manifest(url=EnvVar("INGEST_MANIFEST_URL").get_value("sqlite:///manifest.db")),
    sql=Sql(url=EnvVar("INGEST_SQL_URL").get_value("sqlite:///warehouse.db")),
)
