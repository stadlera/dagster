import os
import time
from datetime import date

import fsspec
import pytest

from ingest.config import Dataset
from ingest.delivery import missing_days
from ingest.resources import Landing, Manifest
from ingest.sync import sync


@pytest.fixture
def env(tmp_path):
    remote = tmp_path / "remote" / "EM"
    remote.mkdir(parents=True)
    for d in ("2026-09-08", "2026-09-09"):
        (remote / f"em-{d}.csv").write_bytes(b"isin,price\nXS1,1.0\n")
    (remote / "em.csv").write_bytes(b"static")
    dataset = Dataset(
        provider="tradeweb", name="em", remote_path=str(remote),
        include=r"^em-\d{4}-\d{2}-\d{2}\.csv$", exclude=r"^em\.csv$",
    )
    landing = Landing(root=str(tmp_path / "landing"))
    manifest = Manifest(url=f"sqlite:///{tmp_path}/manifest.db")
    return fsspec.filesystem("file"), dataset, landing, manifest, remote


def test_sync_is_byte_equivalent_and_idempotent(env):
    fs, dataset, landing, manifest, remote = env
    first = sync(fs, dataset, landing, manifest)
    assert len(first.downloaded) == 2 and first.ignored == 1
    local = landing.path(dataset.key, date(2026, 9, 8), "em-2026-09-08.csv", 1)
    assert local.read_bytes() == (remote / "em-2026-09-08.csv").read_bytes()

    second = sync(fs, dataset, landing, manifest)
    assert second.downloaded == [] and second.unchanged == 2 and second.ignored == 1
    assert manifest.latest(dataset.key)[str(remote / "em.csv")].status == "ignored"


def test_revision_is_kept_as_new_version(env):
    fs, dataset, landing, manifest, remote = env
    sync(fs, dataset, landing, manifest)
    path = remote / "em-2026-09-09.csv"
    path.write_bytes(b"isin,price\nXS1,2.0\n")
    os.utime(path, (time.time() + 10, time.time() + 10))

    result = sync(fs, dataset, landing, manifest)
    assert len(result.revisions) == 1 and result.revisions[0].endswith(".v2")
    assert manifest.latest(dataset.key)[str(path)].version == 2
    assert manifest.file_counts(dataset.key, date(2026, 9, 1)) == {date(2026, 9, 8): 1, date(2026, 9, 9): 1}
    assert manifest.pending_loads(dataset.key)[date(2026, 9, 9)] == [str(landing.path(dataset.key, date(2026, 9, 9), "em-2026-09-09.csv", 2))]


def test_missing_days_respects_calendar_and_lag():
    dataset = Dataset(provider="p", name="d", remote_path="/", calendar=None, extra_holidays=(date(2026, 9, 7),))
    counts = {date(2026, 9, 8): 1, date(2026, 9, 10): 1}
    # Friday 11th: due through the 10th; 7th holiday, 5th/6th weekend
    assert missing_days(dataset, counts, today=date(2026, 9, 11)) == [date(2026, 9, 3), date(2026, 9, 4), date(2026, 9, 9)]
