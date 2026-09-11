"""Mirror a remote directory into the landing area, byte for byte, recording every file in the manifest."""

from __future__ import annotations

import hashlib
import posixpath
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone

import fsspec

from ingest.config import Dataset
from ingest.resources import Landing, Manifest, utcnow


@dataclass
class SyncResult:
    downloaded: list[str] = field(default_factory=list)
    revisions: list[str] = field(default_factory=list)
    ignored: int = 0
    unchanged: int = 0


def sync(fs: fsspec.AbstractFileSystem, dataset: Dataset, landing: Landing, manifest: Manifest) -> SyncResult:
    result = SyncResult()
    known = manifest.latest(dataset.key)

    for info in fs.find(dataset.remote_path, detail=True).values():
        if info.get("type") != "file":
            continue
        remote_path = info["name"]
        filename = posixpath.basename(remote_path)
        mtime = _mtime(info)
        size = info.get("size")
        prev = known.get(remote_path)

        if not re.search(dataset.include, filename) or (dataset.exclude and re.search(dataset.exclude, filename)):
            result.ignored += 1
            if prev is None:
                manifest.record(dataset=dataset.key, remote_path=remote_path, remote_mtime=mtime, size=size, status="ignored")
            continue

        if prev is not None and prev.remote_mtime == mtime and prev.size == size:
            result.unchanged += 1
            continue

        version = prev.version + 1 if prev is not None else 1
        business_date = _business_date(dataset, filename, mtime)
        local = landing.path(dataset.key, business_date, filename, version)
        local.parent.mkdir(parents=True, exist_ok=True)
        tmp = local.with_name(local.name + ".part")
        fs.get_file(remote_path, str(tmp))
        tmp.replace(local)

        manifest.record(
            dataset=dataset.key,
            remote_path=remote_path,
            remote_mtime=mtime,
            size=size,
            version=version,
            status="downloaded",
            business_date=business_date,
            local_path=str(local),
            sha256=_sha256(local),
            downloaded_at=utcnow(),
        )
        (result.revisions if version > 1 else result.downloaded).append(str(local))

    return result


def _mtime(info: dict) -> str | None:
    """fsspec backends disagree on the key and type of the modification time. Normalise to ISO text."""
    value = info.get("mtime") or info.get("LastModified") or info.get("modified")
    if value is None:
        return None
    if isinstance(value, (int, float)):
        value = datetime.fromtimestamp(value, tz=timezone.utc)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).replace(microsecond=0).isoformat()
    return str(value)


def _business_date(dataset: Dataset, filename: str, mtime: str | None) -> date:
    m = re.search(dataset.business_date, filename)
    if m:
        return datetime.strptime(m.group(1), dataset.date_format).date()
    if mtime:
        return datetime.fromisoformat(mtime).date()
    return date.today()


def _sha256(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()
