"""Archive members without extraction: listing (with content hashes) and opening one member as a stream."""

from __future__ import annotations

import hashlib
import re
import tarfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Iterator

import fsspec

ARCHIVES = {".zip": "zip", ".tar": "tar", ".tar.gz": "tar", ".tgz": "tar"}


@dataclass(frozen=True)
class Member:
    name: str
    size: int
    sha256: str


def archive_type(path: Path) -> str | None:
    name = re.sub(r"\.v\d+$", "", path.name)  # strip our revision suffix
    return next((fs for ext, fs in ARCHIVES.items() if name.endswith(ext)), None)


def list_members(path: str | Path) -> list[Member]:
    path = Path(path)
    kind = archive_type(path)
    if kind == "zip":
        with zipfile.ZipFile(path) as z:
            infos = [i for i in z.infolist() if not i.is_dir()]
            return [Member(i.filename, i.file_size, _sha256(z.open(i))) for i in infos]
    if kind == "tar":
        with tarfile.open(path) as t:
            infos = [i for i in t.getmembers() if i.isfile()]
            return [Member(i.name, i.size, _sha256(t.extractfile(i))) for i in infos]
    raise ValueError(f"{path} is not a supported archive")


def open_streams(path: Path, member: str | None = None) -> Iterator[BinaryIO]:
    """Binary streams for a landed file: one member, all members of an archive, or the file itself
    with compression inferred from its name."""
    kind = archive_type(path)
    if member:
        archive = fsspec.filesystem(kind, fo=str(path.resolve()))  # kept alive while the member stream is used
        yield archive.open(member, "rb")
    elif kind:
        for f in sorted(fsspec.open_files(f"{kind}://**::file://{path.resolve()}", "rb"), key=lambda f: f.path):
            yield f.open()
    else:
        name = re.sub(r"\.v\d+$", "", path.name)
        yield fsspec.open(str(path), "rb", compression=fsspec.utils.infer_compression(name)).open()


def _sha256(stream) -> str:
    h = hashlib.sha256()
    with stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()
