"""One package per dataset. Each exposes a module-level `dataset: Dataset` and is discovered by definitions.py."""

from __future__ import annotations

import importlib
import pkgutil

from ingest.config import Dataset


def discover() -> list[Dataset]:
    return [importlib.import_module(f"{__name__}.{m.name}").dataset for m in pkgutil.iter_modules(__path__)]
