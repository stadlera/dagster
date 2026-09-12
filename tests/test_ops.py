"""Operator helpers and alerting sensors."""

from datetime import date

import pytest
from dagster import DagsterEventType, DagsterInstance, asset, build_run_status_sensor_context, materialize

from ingest import ops
from ingest.alerting import Notifier, build_alerting
from ingest.config import Dataset
from ingest.factory import build_definitions
from ingest.resources import Sql


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


def test_ops_cli(ws, monkeypatch, capsys):
    ws.sync()
    table = ws.table()
    ws.classify(table)
    dataset = Dataset(ws.feed, (table,), schema_dir=ws.tmp)
    defs = build_definitions([dataset], ws.landing, ws.manifest, Sql(url=ws.sql_url))
    monkeypatch.setattr("ingest.definitions.defs", defs, raising=False)
    ops.main(["reclassify", "tradeweb/em"])
    assert "2 file(s) reset" in capsys.readouterr().out


class FakeSmtp:
    """Stands in for smtplib.SMTP and records what would have been sent."""

    sent: list = []

    def __init__(self, host, port, timeout):
        self.host = host

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def send_message(self, msg):
        FakeSmtp.sent.append((msg["To"], msg["Subject"], msg.get_content()))


@pytest.fixture
def mailbox(monkeypatch):
    FakeSmtp.sent = []
    monkeypatch.setattr("ingest.alerting.smtplib.SMTP", FakeSmtp)
    return FakeSmtp.sent


NOTIFIER = Notifier(smtp_host="mail.local", smtp_to="ops@example.com")


def status_context(name, instance, run, event_type):
    event = next(e.dagster_event for e in instance.all_logs(run.run_id, of_type=event_type))
    return build_run_status_sensor_context(name, event, instance, run, resources={"notifier": NOTIFIER})


def test_failed_checks_are_emailed(ws, mailbox):
    dataset = Dataset(ws.feed, (ws.table(),), schema_dir=ws.tmp)
    defs = build_definitions([dataset], ws.landing, ws.manifest, Sql(url=ws.sql_url))
    instance = DagsterInstance.ephemeral()
    result = materialize(
        [defs.get_assets_def(ws.feed.raw_key), *defs.asset_checks], resources=defs.resources, instance=instance
    )
    _, on_check_failure = build_alerting()
    ctx = status_context("alert_check_failures", instance, result.dagster_run, DagsterEventType.RUN_SUCCESS)
    list(on_check_failure(ctx) or [])
    ((to, subject, body),) = mailbox  # the fixture days are long overdue: severity ERROR
    assert to == "ops@example.com" and "1 check(s) failed" in subject
    assert "raw/tradeweb / delivery_em" in body and "violations=" in body


def test_passing_checks_send_nothing(ws, mailbox):
    dataset = Dataset(ws.feed, (ws.table().with_expectation(None),), schema_dir=ws.tmp)
    defs = build_definitions([dataset], ws.landing, ws.manifest, Sql(url=ws.sql_url))
    instance = DagsterInstance.ephemeral()
    result = materialize([defs.get_assets_def(ws.feed.raw_key)], resources=defs.resources, instance=instance)
    _, on_check_failure = build_alerting()
    ctx = status_context("alert_check_failures", instance, result.dagster_run, DagsterEventType.RUN_SUCCESS)
    list(on_check_failure(ctx) or [])
    assert mailbox == []


def test_run_failures_are_emailed(mailbox):
    @asset
    def broken():
        raise RuntimeError("sftp unreachable")

    instance = DagsterInstance.ephemeral()
    result = materialize([broken], instance=instance, raise_on_error=False)
    assert not result.success
    on_run_failure, _ = build_alerting()
    ctx = status_context("alert_run_failures", instance, result.dagster_run, DagsterEventType.RUN_FAILURE)
    list(on_run_failure(ctx) or [])
    ((_, subject, body),) = mailbox
    assert "run failed" in subject and "sftp unreachable" in body


def test_notifier_without_host_only_logs(mailbox, caplog):
    Notifier().send("subject", "body")
    assert mailbox == [] and "subject" in caplog.text
