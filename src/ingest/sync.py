"""Mirror remote directories into the landing area, byte for byte, recording every file in the manifest."""

from __future__ import annotations

import hashlib
import posixpath
import re
from dataclasses import dataclass, field

import fsspec

from ingest.config import Feed
from ingest.remote import modification_time
from ingest.resources import FileStatus, Landing, Manifest, utcnow


@dataclass
class SyncResult:
    downloaded: list[str] = field(default_factory=list)
    revisions: list[str] = field(default_factory=list)
    ignored: int = 0
    unchanged: int = 0


def sync(fs: fsspec.AbstractFileSystem, feed: Feed, landing: Landing, manifest: Manifest) -> SyncResult:
    result = SyncResult()
    known = manifest.latest(feed.name)

    for subset, base in feed.subsets.items():
        base = base.rstrip("/")
        for info in fs.find(base, maxdepth=feed.maxdepth, detail=True).values():
            if info.get("type") != "file":
                continue
            remote_path = info["name"]
            mtime, size = modification_time(info), info.get("size")
            prev = known.get(remote_path)

            if feed.exclude and re.search(feed.exclude, posixpath.basename(remote_path)):
                result.ignored += 1
                if prev is None:
                    manifest.record(
                        feed=feed.name,
                        subset=subset,
                        remote_path=remote_path,
                        path=f"{subset}/{posixpath.relpath(remote_path, base)}",
                        remote_mtime=mtime,
                        size=size,
                        status=FileStatus.IGNORED,
                    )
                continue

            if prev is not None and prev.remote_mtime == mtime and prev.size == size:
                result.unchanged += 1
                continue

            version = prev.version + 1 if prev is not None else 1
            logical = f"{subset}/{posixpath.relpath(remote_path, base)}"
            local = landing.path(feed.name, logical, version)
            local.parent.mkdir(parents=True, exist_ok=True)
            tmp = local.with_name(local.name + ".part")
            fs.get_file(remote_path, str(tmp))
            tmp.replace(local)

            manifest.record(
                feed=feed.name,
                subset=subset,
                remote_path=remote_path,
                path=logical,
                remote_mtime=mtime,
                size=size,
                version=version,
                status=FileStatus.DOWNLOADED,
                local_path=str(local),
                sha256=_sha256(local),
                downloaded_at=utcnow(),
            )
            (result.revisions if version > 1 else result.downloaded).append(str(local))

    return result


def _sha256(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()
