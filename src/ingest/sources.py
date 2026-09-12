"""Stage 2, selection: which mirrored files belong to a table, their business date and logical identity.

A Source is asked two questions per file path: `expands(path)` for archives that hold several logical
files (members are then classified individually), and `classify(path)` which returns a Classified or
None. Archive members are classified on their virtual path, the member name placed next to the archive
(<archive dir>/<member>), so the same select patterns match plain files and repacked ones.
Implement the protocol for providers whose naming cannot be expressed as patterns.
"""

from __future__ import annotations

import posixpath
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Callable, Literal, Protocol


@dataclass(frozen=True)
class Classified:
    business_date: date
    attributes: dict[str, str]  # become _<name> metadata columns
    identity: str  # same identity = same logical file, whatever the path (moved, repacked, restated)


class Source(Protocol):
    on_collision: Literal["latest", "first"]  # same identity, different content: which one stays active

    @property
    def attribute_names(self) -> tuple[str, ...]: ...

    def expands(self, path: str) -> bool: ...

    def classify(self, path: str) -> Classified | None: ...


@dataclass(frozen=True)
class Patterns:
    """Default source: regexes on the remote path. The business date is the named group `date` (or group 1),
    every other named group is an attribute. `under` narrows by directory before the select patterns run."""

    select: tuple[str, ...]
    under: str | None = None  # regex on the directory part, e.g. r"/EM(/|$)"
    ignore: tuple[str, ...] = ()
    archives: tuple[str, ...] = ()  # archives holding several logical files; members are classified individually
    date_format: str = "%Y-%m-%d"
    identity: Callable[[re.Match, str], str] | None = None  # fn(match, path) -> str; default: name + date + attributes
    on_collision: Literal["latest", "first"] = "latest"

    @property
    def attribute_names(self) -> tuple[str, ...]:
        names = {g for p in self.select for g in re.compile(p).groupindex if g != "date"}
        return tuple(sorted(names))

    def expands(self, path: str) -> bool:
        return self._under(path) and any(re.search(p, path) for p in self.archives)

    def classify(self, path: str) -> Classified | None:
        if not self._under(path) or any(re.search(p, path) for p in self.ignore):
            return None
        match = next((m for p in self.select if (m := re.search(p, path))), None)
        if match is None:
            return None
        groups = match.groupdict()
        raw_date = groups.pop("date", None) or match.group(1)
        day = datetime.strptime(raw_date, self.date_format).date()
        identity = self.identity(match, path) if self.identity else default_identity(path, day, groups)
        return Classified(day, groups, identity)

    def _under(self, path: str) -> bool:
        return self.under is None or re.search(self.under, posixpath.dirname(path)) is not None


def default_identity(path: str, day: date, attributes: dict[str, str]) -> str:
    """File name + business date + attributes: survives moves and repacks, keeps differently named
    files of one day apart. A restatement under a new name needs a custom identity function."""
    return "|".join([posixpath.basename(path), str(day), *(f"{k}={v}" for k, v in sorted(attributes.items()))])
