"""Small arrow helpers shared by sampling and statistics."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc


def flat(values: pa.Array | pa.ChunkedArray) -> pa.Array:
    """One contiguous array; large_string becomes string so every kernel sees one text type."""
    if isinstance(values, pa.ChunkedArray):
        values = values.combine_chunks()
    return pc.cast(values, pa.string()) if pa.types.is_large_string(values.type) else values


def all_midnight(ts: pa.Array | pa.ChunkedArray) -> bool:
    parts = (pc.hour, pc.minute, pc.second, pc.millisecond, pc.microsecond, pc.nanosecond)
    return all(pc.max(part(ts)).as_py() in (0, None) for part in parts)


def fraction_digits(ts: pa.Array | pa.ChunkedArray) -> int:
    for digits, part in ((9, pc.nanosecond), (6, pc.microsecond), (3, pc.millisecond)):
        if pc.max(part(ts)).as_py():
            return digits
    return 0
