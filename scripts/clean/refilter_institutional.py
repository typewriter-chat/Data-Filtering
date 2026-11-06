"""Re-filter already-downloaded institutional_clean parquets with the v2 date rules.

Reads every shard from ``--input-dir``, applies two additional date filters that
were missing from the original download, and writes passing rows to ``--output-dir``.
No HuggingFace re-download is needed.

New filters applied on top of the original OCR / language / year checks:

  Fix 1 — uncertain dates
      Reject books where the *maximum* possible year (treating MARC 'u' as '9')
      exceeds ``--year-max``.  E.g. date1='19uu' → max=1999 → rejected.

  Fix 2 — continuing resources (serials, journals, government series)
      For entries with date_types_src in the continuing-resource set, the date1
      field is the *founding year* of the series, not the volume year.  These
      entries are accepted only if their *end year* (date2_src) is also ≤
      year_max.  Entries with date2='9999' (ongoing) or date2='uuuu' (unknown)
      are rejected.

Usage:
    uv run python scripts/clean/refilter_institutional.py
"""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

from historical_lm.stats import marc_year_max

_REPO = Path(__file__).resolve().parents[2]
_DEFAULT_INPUT = _REPO / 'data' / 'clean' / 'institutional_clean'
_DEFAULT_OUTPUT = _REPO / 'data' / 'clean' / 'institutional_clean_v2'

_DATE2_BOUNDED_TYPES: frozenset[str] = frozenset({
    'Continuing resource currently published',
    'Continuing resource ceased publication (Dead status)',
    'Continuing resource status unknown',
    'Multiple dates',
    'Questionable date',
    'Inclusive dates of collection',
    'Range of years of bulk of collection',
})


def _passes(row: dict, year_max: int) -> bool:
    date1 = row.get('date1_src')
    date2 = row.get('date2_src')
    date_type = (row.get('date_types_src') or '').strip()

    # Fix 1: uncertain date1 — reject if max possible year exceeds cutoff
    y1_max = marc_year_max(date1)
    if y1_max is not None and y1_max > year_max:
        return False

    # Fix 2: types where date2 is an end/upper-bound year — require date2 ≤ year_max.
    # Use marc_year_max so uncertain values like '19uu' (max=1999) are rejected.
    if date_type in _DATE2_BOUNDED_TYPES:
        y2_max = marc_year_max(date2)
        if y2_max is None or y2_max > year_max:
            return False

    return True


def refilter(
    input_dir: Path,
    output_dir: Path,
    year_max: int,
    shard_start: int,
    shard_end: int | None,
) -> None:
    parquets = sorted(input_dir.glob('*.parquet'))[shard_start:shard_end]
    output_dir.mkdir(parents=True, exist_ok=True)

    total_in = total_out = 0
    for shard_path in parquets:
        df = pl.read_parquet(shard_path)
        total_in += len(df)

        keep_mask = [
            _passes(row, year_max)
            for row in df.select(
                ['date1_src', 'date2_src', 'date_types_src']
            ).iter_rows(named=True)
        ]
        df_out = df.filter(keep_mask)
        total_out += len(df_out)

        out_path = output_dir / shard_path.name
        df_out.write_parquet(out_path, compression='zstd')
        print(
            f'{shard_path.name}: {len(df)} → {len(df_out)} '
            f'(removed {len(df) - len(df_out)})',
            flush=True,
        )

    removed = total_in - total_out
    print(
        f'\nTotal: {total_in:,} → {total_out:,} books '
        f'({removed:,} removed, {removed/total_in*100:.1f}%)'
    )


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument('--input-dir',  default=_DEFAULT_INPUT,  type=Path)
    p.add_argument('--output-dir', default=_DEFAULT_OUTPUT, type=Path)
    p.add_argument('--year-max',   default=1913, type=int)
    p.add_argument('--shard-start', default=0,    type=int)
    p.add_argument('--shard-end',   default=None, type=int)
    return p


if __name__ == '__main__':
    args = _build_parser().parse_args()
    refilter(args.input_dir, args.output_dir, args.year_max,
             args.shard_start, args.shard_end)
