"""Shared statistics utilities for historical corpus exploration."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import polars as pl


# ---------------------------------------------------------------------------
# Word-count helpers
# ---------------------------------------------------------------------------

_WHITESPACE_RE = re.compile(r'\s+')


def count_words(text: str) -> int:
    """Count whitespace-delimited tokens in a string.

    Args:
        text: Raw or lightly pre-processed text.

    Returns:
        Number of whitespace-delimited tokens.
    """
    if not text or not text.strip():
        return 0
    return len(_WHITESPACE_RE.split(text.strip()))


def count_words_pos_tagged(text: str) -> int:
    """Count words in a POS-tagged string of the form ``word_TAG word_TAG``.

    Only the surface form (left of ``_``) is counted.  Punctuation-only tokens
    whose surface form contains no alphabetic character are excluded.

    Args:
        text: POS-tagged text in ``word_TAG`` format.

    Returns:
        Number of word tokens (excluding punctuation-only tokens).
    """
    tokens = _WHITESPACE_RE.split(text.strip())
    n = 0
    for tok in tokens:
        surface = tok.split('_')[0] if '_' in tok else tok
        if any(c.isalpha() for c in surface):
            n += 1
    return n


# ---------------------------------------------------------------------------
# Parquet I/O helpers
# ---------------------------------------------------------------------------

def save_stats(df: pl.DataFrame, path: Path | str) -> None:
    """Write a Polars DataFrame to Parquet, creating parent directories.

    Args:
        df: DataFrame to write.
        path: Output file path (must end in ``.parquet``).
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def append_stats(rows: list[dict[str, Any]], path: Path | str, schema: dict[str, pl.DataType] | None = None) -> None:
    """Append a list of record dicts to a Parquet file, creating it if absent.

    Because Parquet is not an append-friendly format, this helper reads the
    existing file (if any), concatenates, and rewrites it.  Use only for
    moderate-sized accumulator files (statistics, not raw text).

    Args:
        rows: New rows to append.
        path: Parquet file path.
        schema: Optional Polars schema mapping column names to dtypes.
    """
    path = Path(path)
    new_df = pl.DataFrame(rows, schema=schema)
    if path.exists():
        existing = pl.read_parquet(path)
        combined = pl.concat([existing, new_df], how='diagonal_relaxed')
    else:
        combined = new_df
    save_stats(combined, path)


# ---------------------------------------------------------------------------
# MARC date helpers (Institutional Books dataset)
# ---------------------------------------------------------------------------

_MARC_U_RE = re.compile(r'u', re.IGNORECASE)


def marc_year_max(value: str | int | None) -> int | None:
    """Parse a MARC 21 date value into the maximum possible integer year.

    Parse a MARC 21 date value into the maximum possible integer year.

    Treats unknown digits ``'u'`` as ``9``, giving the latest plausible year
    for uncertain dates.  E.g. ``"19uu"`` → 1999, ``"191u"`` → 1919.

    Use this to conservatively reject entries whose true year might exceed
    a cutoff even though their minimum-parsed year does not.

    Args:
        value: Raw MARC date value (string or int).

    Returns:
        Maximum possible integer year, or ``None`` if unparseable or outside
        the plausible range 1000–2100.
    """
    if value is None:
        return None
    s = _MARC_U_RE.sub('9', str(value).strip()[:4])
    try:
        year = int(s)
        return year if 1000 <= year <= 2100 else None
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Period helpers
# ---------------------------------------------------------------------------

def decade(year: int) -> int:
    """Return the decade floor of a year (e.g. 1887 → 1880).

    Args:
        year: Four-digit year integer.

    Returns:
        Decade floor (year rounded down to nearest 10).
    """
    return (year // 10) * 10


def century(year: int) -> int:
    """Return the century floor of a year (e.g. 1887 → 1800).

    Args:
        year: Four-digit year integer.

    Returns:
        Century floor (year rounded down to nearest 100).
    """
    return (year // 100) * 100
