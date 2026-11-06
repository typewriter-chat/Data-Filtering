"""Process the historic Hansard JSONL files (1802–1920).

Dataset : Historic Hansard (UK Parliament debates)
Coverage: 1802–1920, downloaded via download_hansard.py.
Format  : JSONL — one file per year (``hansard_<year>.jsonl``).
          Each line is a JSON object with fields:
          ``date``, ``year``, ``month``, ``day``, ``house``, ``title``,
          ``slug``, ``url``, ``paragraphs`` (list of strings).

Unit of output: one row per debate section.  The ``paragraphs`` field is
the list of paragraph strings already present in the source JSONL, after
filtering paragraphs shorter than ``--min-para-words`` words.

Usage
-----
    # Stats only
    uv run python scripts/process/process_hansard.py

    # Full extraction
    uv run python scripts/process/process_hansard.py \\
        --output-dir   data/processed/hansard \\
        --preview-path data/samples/hansard_preview.jsonl \\
        --year-min 1802 --year-max 1938
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from historical_lm.stats import count_words, save_stats

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_JSONL_DIR = _REPO_ROOT / 'data' / 'raw' / 'hansard'
_DEFAULT_OUTPUT = _REPO_ROOT / 'data_test' / 'stats' / 'hansard_stats.parquet'


def _flush_shard(rows: list[dict], output_dir: Path, shard_idx: int) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f'part-{shard_idx:05d}.parquet'
    pq.write_table(pa.Table.from_pylist(rows), path, compression='zstd')
    print(f'  Wrote shard {shard_idx:05d} ({len(rows)} sections) -> {path}')


def collect_stats(
    jsonl_dir: Path,
    year_min: int,
    year_max: int,
    houses: set[str],
    min_para_words: int,
    max_sections: int,
    output_dir: Path | None,
    preview_path: Path | None,
    shard_size: int,
    preview_paragraphs: int,
) -> pl.DataFrame:
    """Process all Hansard JSONL files, filter, and optionally save text.

    Args:
        jsonl_dir: Directory containing ``hansard_<year>.jsonl`` files.
        year_min: Keep only sections with year >= this value.
        year_max: Keep only sections with year <= this value.
        houses: Parliamentary houses to keep (empty = keep all).
        min_para_words: Minimum words per paragraph to keep.
        max_sections: Stop after this many accepted sections (0 = no limit).
        output_dir: If given, write Parquet text shards here.
        preview_path: If given, write a JSONL preview file here.
        shard_size: Sections per Parquet shard.
        preview_paragraphs: Paragraphs per section in the JSONL preview.

    Returns:
        Polars DataFrame with one row per accepted section (no text).
    """
    files = sorted(jsonl_dir.glob('hansard_*.jsonl'))
    if not files:
        print(f'ERROR: no hansard_*.jsonl files found in {jsonl_dir}')
        return pl.DataFrame()

    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
    if preview_path is not None:
        preview_path.parent.mkdir(parents=True, exist_ok=True)

    stat_rows: list[dict] = []
    shard_buf: list[dict] = []
    shard_idx = 0
    n_year_skip = 0
    n_house_skip = 0
    n_empty_skip = 0

    preview_fh = (
        preview_path.open('w', encoding='utf-8') if preview_path is not None else None
    )
    try:
        for jsonl_path in tqdm(files, desc='Hansard files', unit='file'):
            with jsonl_path.open(encoding='utf-8') as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    year = rec.get('year')
                    if year is None or year < year_min or year > year_max:
                        n_year_skip += 1
                        continue

                    house = (rec.get('house') or '').strip().lower()
                    if houses and house not in houses:
                        n_house_skip += 1
                        continue

                    raw_paras = rec.get('paragraphs') or []
                    paragraphs = [
                        p for p in raw_paras
                        if isinstance(p, str) and count_words(p) >= min_para_words
                    ]
                    if not paragraphs:
                        n_empty_skip += 1
                        continue

                    stat_row = {
                        'date': rec.get('date', ''),
                        'year': year,
                        'month': rec.get('month', ''),
                        'day': rec.get('day'),
                        'house': house,
                        'title': (rec.get('title') or '').strip(),
                        'slug': (rec.get('slug') or '').strip(),
                        'n_paragraphs': len(paragraphs),
                        'word_count': count_words(' '.join(paragraphs)),
                    }
                    stat_rows.append(stat_row)

                    if output_dir is not None:
                        shard_buf.append({**stat_row, 'paragraphs': paragraphs})
                        if len(shard_buf) >= shard_size:
                            _flush_shard(shard_buf, output_dir, shard_idx)
                            shard_buf = []
                            shard_idx += 1

                    if preview_fh is not None:
                        preview_fh.write(
                            json.dumps(
                                {
                                    **stat_row,
                                    'text_preview': paragraphs[:preview_paragraphs],
                                },
                                ensure_ascii=False,
                            ) + '\n'
                        )

                    if max_sections > 0 and len(stat_rows) >= max_sections:
                        break

            if max_sections > 0 and len(stat_rows) >= max_sections:
                break

    finally:
        if preview_fh is not None:
            preview_fh.close()

    if output_dir is not None and shard_buf:
        _flush_shard(shard_buf, output_dir, shard_idx)

    print(f'\n--- Filter diagnostics ---')
    print(f'  Files found       : {len(files):,}')
    print(f'  Year out of range : {n_year_skip:,}')
    print(f'  House skipped     : {n_house_skip:,}')
    print(f'  Empty after filter: {n_empty_skip:,}')
    print(f'  Sections accepted : {len(stat_rows):,}')

    if output_dir is not None:
        print(f'Text saved -> {output_dir}')
    if preview_path is not None:
        print(f'Preview written -> {preview_path}')

    return pl.DataFrame(stat_rows)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Process historic Hansard JSONL files into Parquet shards.'
    )
    parser.add_argument(
        '--jsonl-dir',
        default=_DEFAULT_JSONL_DIR,
        type=Path,
        help='Directory containing the Hansard JSONL files.',
    )
    parser.add_argument(
        '--output',
        default=_DEFAULT_OUTPUT,
        type=Path,
        help='Output Parquet file for per-section statistics.',
    )
    parser.add_argument(
        '--output-dir',
        default=None,
        type=Path,
        help='Directory for Parquet text shards.',
    )
    parser.add_argument(
        '--preview-path',
        default=None,
        type=Path,
        help='Path for JSONL preview file.',
    )
    parser.add_argument(
        '--year-min',
        default=1802,
        type=int,
        help='Minimum year (default: 1802).',
    )
    parser.add_argument(
        '--year-max',
        default=1920,
        type=int,
        help='Maximum year inclusive (default: 1920).',
    )
    parser.add_argument(
        '--houses',
        default=['commons', 'lords'],
        nargs='+',
        metavar='HOUSE',
        help='Parliamentary houses to keep (default: commons lords). '
             'Pass empty string to keep all.',
    )
    parser.add_argument(
        '--min-para-words',
        default=5,
        type=int,
        help='Minimum words per paragraph to keep (default: 5).',
    )
    parser.add_argument(
        '--shard-size',
        default=1000,
        type=int,
        help=f'Sections per Parquet shard (default: 1000).',
    )
    parser.add_argument(
        '--preview-paragraphs',
        default=3,
        type=int,
        help='Paragraphs per section in JSONL preview (default: 3).',
    )
    parser.add_argument(
        '--max-sections',
        default=0,
        type=int,
        help='Stop after this many accepted sections (0 = no limit).',
    )
    return parser


def main() -> None:
    """Entry point for Hansard processing."""
    args = _build_parser().parse_args()

    if not args.jsonl_dir.is_dir():
        print(f'ERROR: directory not found: {args.jsonl_dir}')
        return

    houses = set(args.houses) if args.houses != [''] else set()

    print(f'Processing {args.jsonl_dir} ...')
    df = collect_stats(
        args.jsonl_dir,
        args.year_min,
        args.year_max,
        houses,
        args.min_para_words,
        args.max_sections,
        args.output_dir,
        args.preview_path,
        args.shard_size,
        args.preview_paragraphs,
    )

    save_stats(df, args.output)
    print(f'\nSaved {len(df):,} section records -> {args.output}')

    if df.is_empty():
        return

    print('\n--- Summary ---')
    print(f'Total sections  : {len(df):,}')
    print(f'Total paragraphs: {df["n_paragraphs"].sum():,}')
    print(f'Total words     : {df["word_count"].sum():,}')
    year_col = df['year'].drop_nulls()
    if year_col.len() > 0:
        print(f'Year range      : {year_col.min()} - {year_col.max()}')
    print(
        '\nBy decade:\n'
        + str(
            df.with_columns((pl.col('year') // 10 * 10).alias('decade'))
            .group_by('decade')
            .agg(pl.len().alias('n_sections'), pl.col('word_count').sum())
            .sort('decade')
        )
    )
    print(
        '\nBy house:\n'
        + str(
            df.group_by('house')
            .agg(pl.len().alias('n_sections'), pl.col('word_count').sum())
            .sort('n_sections', descending=True)
        )
    )


if __name__ == '__main__':
    main()
