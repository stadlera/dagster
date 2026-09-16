"""Operator helpers and CLI."""

from datetime import date

from ingest_tools import ops


def test_reload_ignore_and_reclassify(ws):
    ws.sync()
    table = ws.table()
    ws.classify(table)
    rows = ws.manifest.files_for(table.key, date(2026, 9, 8), date(2026, 9, 10))
    ws.manifest.mark_loaded([r.id for r in rows], "run-1")
    assert ws.manifest.pending_days(table.key) == {}

    assert ops.reload(ws.manifest, table.key, date(2026, 9, 9)) == 1
    assert list(ws.manifest.pending_days(table.key)) == [date(2026, 9, 9)]

    assert ops.ignore(ws.manifest, [rows[1].id]) == 1
    assert ws.manifest.pending_days(table.key) == {} and ws.statuses()["em-2026-09-09.csv"] == "ignored"

    assert ops.reclassify(ws.manifest, table.key) == 1  # the ignored one stays ignored
    assert ws.manifest.unclassified("tradeweb")[0].remote_path.endswith("em-2026-09-08.csv")
    assert ws.classify(table) == 1 and list(ws.manifest.pending_days(table.key)) == [date(2026, 9, 8)]


def test_ops_cli(ws, capsys):
    ws.sync()
    table = ws.table()
    ws.classify(table)
    ops.main(["--manifest-url", ws.manifest.url, "reclassify", "tradeweb/em"])
    assert "2 file(s) reset" in capsys.readouterr().out
