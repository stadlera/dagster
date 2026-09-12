"""Alerting: a run-failure sensor and a check-failure sensor, both sending email through one Notifier resource.

Without an SMTP host the Notifier only logs (local development).
"""

from __future__ import annotations

import logging
import smtplib
from email.message import EmailMessage

from dagster import (
    ConfigurableResource,
    DagsterEventType,
    DagsterRunStatus,
    RunStatusSensorContext,
    SensorDefinition,
    run_failure_sensor,
    run_status_sensor,
)

log = logging.getLogger("ingest.alerting")


class Notifier(ConfigurableResource):
    """Sends alert emails. Logs only when no smtp_host is configured."""

    smtp_host: str | None = None
    smtp_port: int = 25
    smtp_from: str = "ingest@localhost"
    smtp_to: str = ""  # comma separated recipients
    smtp_user: str | None = None  # with smtp_password: STARTTLS + login
    smtp_password: str | None = None

    def send(self, subject: str, body: str) -> None:
        log.error("%s\n%s", subject, body)
        if not self.smtp_host:
            return
        msg = EmailMessage()
        msg["Subject"], msg["From"], msg["To"] = subject, self.smtp_from, self.smtp_to
        msg.set_content(body)
        with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=30) as smtp:
            if self.smtp_user:
                smtp.starttls()
                smtp.login(self.smtp_user, self.smtp_password or "")
            smtp.send_message(msg)


def build_alerting() -> list[SensorDefinition]:
    @run_failure_sensor(name="alert_run_failures", minimum_interval_seconds=60)
    def on_run_failure(context: RunStatusSensorContext, notifier: Notifier):
        run = context.dagster_run
        steps = context.instance.all_logs(run.run_id, of_type=DagsterEventType.STEP_FAILURE)
        errors = [f"{e.step_key}: {_root_cause(e.dagster_event.event_specific_data.error)}" for e in steps]
        notifier.send(
            f"[ingest] run failed: {run.job_name}",
            f"run {run.run_id}\nassets: {', '.join(k.to_user_string() for k in run.asset_selection or [])}\n"
            f"partition: {run.tags.get('dagster/partition') or run.tags.get('dagster/asset_partition_range_start')}\n"
            + "\n".join(errors or [context.failure_event.message]),
        )

    @run_status_sensor(run_status=DagsterRunStatus.SUCCESS, name="alert_run_findings", minimum_interval_seconds=60)
    def on_run_findings(context: RunStatusSensorContext, notifier: Notifier):
        """Successful runs can still carry findings: failed asset checks (they do not fail the run) and
        schema changes (new columns accepted by the evolve contract)."""
        run_id = context.dagster_run.run_id
        instance = context.instance
        checks = instance.all_logs(run_id, of_type=DagsterEventType.ASSET_CHECK_EVALUATION)
        failed = [e.dagster_event.event_specific_data for e in checks if not e.dagster_event.event_specific_data.passed]
        errors = [f for f in failed if f.severity.value == "ERROR"]  # WARN: it may just be late, log only
        if errors:
            lines = [f"{f.asset_key.to_user_string()} / {f.check_name}: {_plain(f.metadata)}" for f in errors]
            notifier.send(f"[ingest] {len(errors)} check(s) failed", f"run {run_id}\n" + "\n".join(lines))
        materializations = instance.all_logs(run_id, of_type=DagsterEventType.ASSET_MATERIALIZATION)
        changed = [
            (m.asset_key.to_user_string(), m.metadata["new_columns"].value)
            for m in (e.dagster_event.event_specific_data.materialization for e in materializations)
            if "new_columns" in m.metadata
        ]
        if changed:
            lines = [
                f"{asset}: new columns {cols} (loaded; add them to the committed schema)" for asset, cols in changed
            ]
            notifier.send("[ingest] schema change", f"run {run_id}\n" + "\n".join(lines))

    return [on_run_failure, on_run_findings]


def _root_cause(error) -> str:
    """Dagster wraps user exceptions; the original message is at the end of the cause chain."""
    while error.cause is not None:
        error = error.cause
    return error.message.strip()


def _plain(metadata: dict) -> str:
    return ", ".join(f"{k}={getattr(v, 'value', v)}" for k, v in metadata.items())
