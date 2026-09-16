"""Stage 1, mirroring: byte-equivalent copies, idempotent reruns, versions, exclusions."""

import gzip
from dataclasses import replace
from datetime import date

from conftest import CSV_08, CSV_09, deeper
from ingest.resources import FileStatus


def test_files_are_copied_byte_for_byte_and_recorded(ws):
    result = ws.sync()
    assert len(result.downloaded) == 2 and result.ignored == 1
    local = ws.landing.path("tradeweb", "EM/em-2026-09-08.csv", 1)
    assert local.read_bytes() == CSV_08
    assert not list(local.parent.glob("*.part"))  # temp names are renamed on success
    row = ws.manifest.latest("tradeweb")[str(ws.remote / "em-2026-09-08.csv")]
    assert row.sha256 and row.size == len(CSV_08) and row.downloaded_at and row.status == FileStatus.DOWNLOADED


def test_rerun_downloads_nothing(ws):
    ws.sync()
    again = ws.sync()
    assert again.downloaded == [] and again.revisions == [] and again.unchanged == 2 and again.ignored == 1


def test_excluded_file_is_recorded_once_but_never_downloaded(ws):
    ws.sync()
    ws.sync()
    assert ws.statuses()["em.csv"] == "ignored"
    assert not (ws.landing.path("tradeweb", "EM/em.csv", 1)).exists()


def test_changed_remote_file_becomes_a_new_version_next_to_the_old_one(ws):
    ws.sync()
    ws.write_remote("em-2026-09-09.csv", b"isin,price\nXS1,9.9\n", bump_mtime=True)
    result = ws.sync()
    assert len(result.revisions) == 1 and result.revisions[0].endswith(".v2")
    assert ws.landing.path("tradeweb", "EM/em-2026-09-09.csv", 1).read_bytes() == CSV_09  # v1 untouched
    assert ws.manifest.latest("tradeweb")[str(ws.remote / "em-2026-09-09.csv")].version == 2


def test_late_file_is_picked_up_by_the_next_sync(ws):
    ws.sync()
    ws.write_remote("em-2026-09-07.csv", CSV_08)
    assert len(ws.sync().downloaded) == 1
    ws.classify(ws.table())
    assert date(2026, 9, 7) in ws.manifest.pending_days("tradeweb/em")


def test_subfolders_need_maxdepth_and_compressed_files_stay_compressed(ws):
    ws.write_remote("apac/em-2026-09-08.csv", CSV_08)
    ws.write_remote("em-2026-09-10.csv.gz", gzip.compress(CSV_08))
    assert len(ws.sync().downloaded) == 3  # maxdepth=1: the sub folder is not visited
    assert len(ws.sync(deeper(ws.feed)).downloaded) == 1
    assert ws.landing.path("tradeweb", "EM/em-2026-09-10.csv.gz", 1).read_bytes() == gzip.compress(CSV_08)


def test_subsets_land_under_their_name_and_are_recorded(ws):
    other = ws.remote.parent / "some" / "deep" / "folder"
    other.mkdir(parents=True)
    (other / "pf-2026-09-08.csv").write_bytes(CSV_08)
    feed = replace(ws.feed, subsets={"EM": str(ws.remote), "platform": str(other)})
    assert len(ws.sync(feed).downloaded) == 3
    assert ws.landing.path("tradeweb", "platform/pf-2026-09-08.csv", 1).exists()
    rows = ws.manifest.unclassified("tradeweb")
    assert {(r.subset, r.path) for r in rows} >= {
        ("platform", "platform/pf-2026-09-08.csv"),
        ("EM", "EM/em-2026-09-08.csv"),
    }


def test_subset_names_are_validated_and_remote_options_are_coerced():
    import pytest

    from ingest.config import Feed
    from ingest.remote import coerce_option
    from ingest.resources import Remote

    with pytest.raises(ValueError, match="subset names"):
        Feed("x", Remote(protocol="file"), "0 7 * * *", {"a/b": "/out"})
    assert (
        coerce_option("22") == 22
        and coerce_option("true") is True
        and coerce_option("sftp.acme.com") == "sftp.acme.com"
    )
