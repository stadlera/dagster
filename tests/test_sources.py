"""Stage 2, selection rules: what a Patterns source makes of a path."""

import re
from datetime import date

from ingest.sources import Patterns, default_identity


def test_business_date_from_named_group_or_group_one_and_date_formats():
    assert Patterns((r"em-(?P<date>\d{4}-\d{2}-\d{2})\.csv$",)).classify("/x/em-2026-09-08.csv").business_date == date(
        2026, 9, 8
    )
    assert Patterns((r"em-(\d{8})\.csv$",), date_format="%Y%m%d").classify("/x/em-20260908.csv").business_date == date(
        2026, 9, 8
    )
    monthly = Patterns((r"em-(?P<date>\d{6})\.csv$",), date_format="%Y%m")
    assert monthly.classify("/x/em-202609.csv").business_date == date(2026, 9, 1)  # period start
    yearly = Patterns((r"em-(?P<date>\d{4})\.zip$",), date_format="%Y")
    assert yearly.classify("/x/em-2026.zip").business_date == date(2026, 1, 1)


def test_named_groups_become_attributes_and_identity_includes_name_date_and_attributes():
    src = Patterns((r"/(?P<region>apac|emea)/em-(?P<date>\d{4}-\d{2}-\d{2})\.csv$",))
    c = src.classify("/out/EM/apac/em-2026-09-08.csv")
    assert c.attributes == {"region": "apac"}
    assert c.identity == "em-2026-09-08.csv|2026-09-08|region=apac"
    assert src.attribute_names == ("region",)
    assert default_identity("/a/b.csv", date(2026, 1, 1), {}) == "b.csv|2026-01-01"


def test_under_ignore_and_no_match():
    src = Patterns((r"em-(?P<date>\d{4}-\d{2}-\d{2})\.csv$",), under=r"/EM$", ignore=(r"\.tmp\.",))
    assert src.classify("/out/EM/em-2026-09-08.csv")
    assert src.classify("/out/EM/archive/em-2026-09-08.csv") is None  # narrowed away by `under`
    assert src.classify("/out/EM/em-2026-09-08.tmp.csv") is None
    assert src.classify("/out/EM/other.csv") is None


def test_custom_identity_and_archive_detection():
    src = Patterns(
        (r"em-(?P<date>\d{4}-\d{2}-\d{2})(?P<suffix>_corrected)?\.csv$",),
        identity=lambda m, path: m.group("date"),
        archives=(r"/em-\d{4}\.zip$",),
    )
    assert src.classify("/x/em-2026-09-08.csv").identity == src.classify("/x/em-2026-09-08_corrected.csv").identity
    assert src.expands("/x/em-2026.zip") and not src.expands("/x/em-2026-09-08.csv")


class ContentDateSource:
    """A Source that does not use patterns at all (e.g. the date only lives in the file content)."""

    on_collision = "latest"
    attribute_names = ()

    def expands(self, path):
        return False

    def classify(self, path):
        from ingest.sources import Classified

        m = re.search(r"snapshot_(\d+)\.csv$", path)
        return Classified(date(2026, 1, 1), {}, f"snapshot-{m.group(1)}") if m else None


def test_protocol_allows_a_custom_source():
    assert ContentDateSource().classify("/x/snapshot_7.csv").identity == "snapshot-7"
