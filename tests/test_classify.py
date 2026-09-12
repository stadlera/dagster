"""Stage 2, classification: lazy assignment, ambiguity, archives, identity collisions."""

from datetime import date

import pytest

from conftest import CSV_08, CSV_09, deeper
from ingest.classify import AmbiguousMatch
from ingest.sources import Patterns

BY_NAME = Patterns((r"em-(?P<date>\d{4}-\d{2}-\d{2})\.csv$",))  # any folder, also archive members
REGIONAL = Patterns(
    (
        r"^EM/em-(?P<date>\d{4}-\d{2}-\d{2})\.csv$",
        r"^EM/(?P<region>apac|emea)/em-(?P<date>\d{4}-\d{2}-\d{2})\.csv$",
    )
)


def test_tables_only_see_their_subsets(ws):
    other = ws.remote.parent / "Platform"
    other.mkdir()
    (other / "em-2026-09-08.csv").write_bytes(CSV_08)  # same name as in EM, distinct data
    from dataclasses import replace

    feed = replace(ws.feed, subsets={"EM": str(ws.remote), "Platform": str(other)})
    ws.sync(feed)
    em = ws.table(source=BY_NAME, subsets=("EM",))
    platform = ws.table("platform", source=BY_NAME, subsets=("Platform",))
    from ingest.classify import classify
    from ingest.config import Dataset

    assert classify(ws.manifest, Dataset(feed, (em, platform), schema_dir=ws.tmp)) == 3
    assert len(ws.manifest.files_for(em.key, date(2026, 9, 8))) == 1
    assert len(ws.manifest.files_for(platform.key, date(2026, 9, 8))) == 1  # same name: no collision across subsets


def test_tables_can_be_defined_after_the_files_were_mirrored(ws):
    ws.sync()
    table = ws.table()
    assert ws.manifest.pending_days(table.key) == {}
    assert ws.classify(table) == 2
    assert ws.classify(table) == 0  # idempotent
    assert list(ws.manifest.pending_days(table.key)) == [date(2026, 9, 8), date(2026, 9, 9)]


def test_several_select_patterns_feed_one_table_and_keep_their_attributes(ws):
    ws.write_remote("apac/em-2026-09-08.csv", CSV_09)
    ws.sync(deeper(ws.feed))
    table = ws.table(source=REGIONAL)
    assert ws.classify(table) == 3
    rows = ws.manifest.files_for(table.key, date(2026, 9, 8))
    assert sorted((r.attributes or {}).get("region", "") for r in rows) == ["", "apac"]


def test_a_file_matching_two_tables_fails_after_committing_the_rest(ws):
    ws.sync()
    with pytest.raises(AmbiguousMatch, match="em-2026-09-08.csv"):
        ws.classify(ws.table("em"), ws.table("em_copy"))
    assert ws.statuses()["em-2026-09-08.csv"] == "downloaded"  # still unassigned, not lost


def test_yearly_archive_members_fill_gaps_dedupe_and_restate(ws):
    import zipfile

    with zipfile.ZipFile(ws.remote / "em-2026.zip", "w") as z:
        z.writestr("em-2026-09-07.csv", CSV_08)  # never delivered as daily file: fills the gap
        z.writestr("em-2026-09-08.csv", CSV_08)  # identical repack of the daily file
        z.writestr("em-2026-09-09.csv", b"isin,price\nXS1,9.9\n")  # restated
    ws.sync()
    table = ws.table(source=Patterns((r"^EM/em-(?P<date>\d{4}-\d{2}-\d{2})\.csv$",), archives=(r"^EM/em-\d{4}\.zip$",)))
    assert ws.classify(table) == 5

    st = ws.statuses()
    assert st["em-2026.zip"] == "expanded"
    assert st["em-2026-09-08.csv"] == "downloaded" and st["em-2026.zip!em-2026-09-08.csv"] == "duplicate"
    assert st["em-2026-09-09.csv"] == "superseded" and st["em-2026.zip!em-2026-09-09.csv"] == "downloaded"
    assert st["em-2026.zip!em-2026-09-07.csv"] == "downloaded"
    assert ws.manifest.files_for(table.key, date(2026, 9, 9))[0].member == "em-2026-09-09.csv"
    assert ws.classify(table) == 0


def test_moved_file_with_same_content_is_a_duplicate(ws):
    ws.sync()
    table = ws.table(source=BY_NAME)
    ws.classify(table)
    ws.write_remote("archive/em-2026-09-08.csv", CSV_08)
    ws.sync(deeper(ws.feed))
    ws.classify(table)
    assert ws.statuses()["archive/em-2026-09-08.csv"] == "duplicate"
    assert len(ws.manifest.files_for(table.key, date(2026, 9, 8))) == 1


def test_restated_file_under_a_new_name_needs_a_custom_identity(ws):
    ws.write_remote("em-2026-09-08_corrected.csv", b"isin,price\nXS1,1.5\n")
    ws.sync()
    src = Patterns((r"em-(?P<date>\d{4}-\d{2}-\d{2})(_corrected)?\.csv$",), identity=lambda m, p: m.group("date"))
    table = ws.table(source=src)
    ws.classify(table)
    active = ws.manifest.files_for(table.key, date(2026, 9, 8))
    assert [a.remote_path.rsplit("/", 1)[1] for a in active] == ["em-2026-09-08_corrected.csv"]  # latest wins
    assert ws.statuses()["em-2026-09-08.csv"] == "superseded"


def test_keep_first_policy_ignores_later_restatements(ws):
    ws.write_remote("em-2026-09-08_corrected.csv", b"isin,price\nXS1,1.5\n")
    ws.sync()
    src = Patterns(
        (r"em-(?P<date>\d{4}-\d{2}-\d{2})(_corrected)?\.csv$",),
        identity=lambda m, p: m.group("date"),
        on_collision="first",
    )
    table = ws.table(source=src)
    ws.classify(table)
    assert ws.statuses()["em-2026-09-08_corrected.csv"] == "superseded"


def test_two_differently_named_files_of_one_day_are_both_active(ws):
    ws.write_remote("em-2026-09-08_part2.csv", CSV_09)
    ws.sync()
    table = ws.table(source=Patterns((r"em-(?P<date>\d{4}-\d{2}-\d{2})(_part2)?\.csv$",)))
    ws.classify(table)
    assert len(ws.manifest.files_for(table.key, date(2026, 9, 8))) == 2


def test_revision_of_the_same_remote_path_supersedes_and_becomes_pending_again(ws):
    ws.sync()
    table = ws.table()
    ws.classify(table)
    ws.manifest.mark_loaded([r.id for r in ws.manifest.files_for(table.key, date(2026, 9, 9))], "run-1")
    assert ws.manifest.pending_days(table.key) == {date(2026, 9, 8): 1}

    ws.write_remote("em-2026-09-09.csv", b"isin,price\nXS1,9.9\n", bump_mtime=True)
    ws.sync()
    ws.classify(table)
    pending = ws.manifest.pending_days(table.key)
    assert set(pending) == {date(2026, 9, 8), date(2026, 9, 9)} and pending[date(2026, 9, 9)] > 2


def test_files_moved_between_subsets_dedupe_with_the_across_subsets_identity(ws):
    from dataclasses import replace

    from ingest.classify import classify
    from ingest.config import Dataset
    from ingest.sources import across_subsets

    output, history = ws.remote.parent / "output", ws.remote.parent / "history-a"
    output.mkdir(), history.mkdir()
    (output / "a-2026-09-08.csv").write_bytes(CSV_08)
    feed = replace(ws.feed, subsets={"output": str(output), "history-a": str(history)})
    patterns = Patterns((r"^(output|history-a)/a-(?P<date>\d{4}-\d{2}-\d{2})\.csv$",), identity=across_subsets)
    table = ws.table("a", source=patterns, subsets=("output", "history-a"))
    dataset = Dataset(feed, (table,), schema_dir=ws.tmp)
    ws.sync(feed)
    classify(ws.manifest, dataset)

    (output / "a-2026-09-08.csv").rename(history / "a-2026-09-08.csv")  # provider moves it after retention
    ws.sync(feed)
    classify(ws.manifest, dataset)
    st = {p.rsplit("/", 2)[-2]: status for p, status in ws.statuses().items() if p.endswith("a-2026-09-08.csv")}
    assert st == {"output": "downloaded", "history-a": "duplicate"}
    assert len(ws.manifest.files_for(table.key, date(2026, 9, 8))) == 1
