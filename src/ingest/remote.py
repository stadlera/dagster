"""Normalize filesystem options and metadata across fsspec backends."""

from __future__ import annotations

from datetime import datetime, timezone


def coerce_option(value: str):
    if value.isdigit():
        return int(value)
    if value.lower() in ("true", "false"):
        return value.lower() == "true"
    return value


def modification_time(info: dict) -> str | None:
    """Return an fsspec file's modification time as normalized ISO text."""
    value = info.get("mtime") or info.get("LastModified") or info.get("modified")
    if value is None:
        return None
    if isinstance(value, (int, float)):
        value = datetime.fromtimestamp(value, tz=timezone.utc)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).replace(microsecond=0).isoformat()
    return str(value)
