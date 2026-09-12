"""Stage 2, classification: run every table's Source over the unclassified rows of a dataset.

1. archives a Source wants expanded are listed into member rows (nothing is extracted)
2. each row is classified by exactly one table (more than one is an error, none is left for later)
3. identity collisions are resolved: same content -> DUPLICATE, else the Source's on_collision policy
"""

from __future__ import annotations

import posixpath

from ingest.archives import list_members
from ingest.config import Dataset, Table
from ingest.resources import FileStatus, Manifest


class AmbiguousMatch(Exception):
    def __init__(self, files: list[tuple[str, list[str]]]):
        super().__init__("files match more than one table: " + "; ".join(f"{p} -> {t}" for p, t in files))
        self.files = files


def classify(manifest: Manifest, dataset: Dataset) -> int:
    """Returns the number of newly classified rows. Raises AmbiguousMatch after committing the rest."""
    expand_archives(manifest, dataset)
    assigned, ambiguous = 0, []
    for row in manifest.unclassified(dataset.name):
        path = match_path(row.path)
        candidates = [t for t in dataset.tables if not t.subsets or row.subset in t.subsets]
        matches = [(t, c) for t in candidates if (c := t.source.classify(path))]
        if len(matches) > 1:
            ambiguous.append((row.remote_path, [t.key for t, _ in matches]))
            continue
        if not matches:
            continue
        table, classified = matches[0]
        identity = f"{table.key}|{classified.identity}"
        status = resolve_collision(manifest, table, row, identity)
        manifest.assign(row.id, table.key, classified.business_date, classified.attributes, identity, status)
        assigned += 1
    if ambiguous:
        raise AmbiguousMatch(ambiguous)
    return assigned


def expand_archives(manifest: Manifest, dataset: Dataset) -> None:
    for row in manifest.unclassified(dataset.name, top_level=True):
        tables = [t for t in dataset.tables if not t.subsets or row.subset in t.subsets]
        if any(t.source.expands(row.path) for t in tables):
            manifest.add_members(row, list_members(row.local_path))


def resolve_collision(manifest: Manifest, table: Table, row, identity: str) -> FileStatus:
    active = manifest.active_with_identity(identity, exclude_id=row.id)
    if not active:
        return FileStatus.DOWNLOADED
    if any(a.sha256 == row.sha256 for a in active):
        return FileStatus.DUPLICATE
    if table.source.on_collision == "first":
        return FileStatus.SUPERSEDED
    manifest.set_status([a.id for a in active], FileStatus.SUPERSEDED)
    return FileStatus.DOWNLOADED


def match_path(path: str) -> str:
    """'<archive>!<member>' is matched as '<archive dir>/<member>', as if unpacked in place."""
    if "!" not in path:
        return path
    archive, member = path.split("!", 1)
    return posixpath.join(posixpath.dirname(archive), member)
