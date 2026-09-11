"""Entry point for Dagster. Providers and datasets are declared here, secrets come from the environment."""

from dagster import EnvVar

from ingest.config import Dataset, Provider
from ingest.factory import build_definitions
from ingest.resources import Landing, Manifest, Remote

providers = [
    Provider(
        name="tradeweb",
        cron="0 7 * * 1-5",
        # local demo: the "remote" is a directory in this repo. In prod:
        # Remote(protocol="sftp", options={"host": ..., "username": ..., "password": EnvVar("TRADEWEB_PASSWORD")})
        remote=Remote(protocol="file"),
        datasets=[
            Dataset(
                provider="tradeweb",
                name="em",
                remote_path=EnvVar("TRADEWEB_REMOTE_ROOT").get_value("examples/remote/tradeweb") + "/EM",
                include=r"^em-\d{4}-\d{2}-\d{2}\.csv$",
                exclude=r"^em\.csv$",
            ),
        ],
    ),
]

defs = build_definitions(
    providers,
    landing=Landing(root=EnvVar("INGEST_LANDING_ROOT").get_value("landing")),
    manifest=Manifest(url=EnvVar("INGEST_MANIFEST_URL").get_value("sqlite:///manifest.db")),
)
