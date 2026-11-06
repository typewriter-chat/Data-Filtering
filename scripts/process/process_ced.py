"""Process the Corpus of English Dialogues 1560-1760 (CED).

Dataset : Corpus of English Dialogues 1560-1760
Coverage: 1560-1760, prose dialogues and trial records.
Homepage: https://www.engelska.uu.se/research/english-language/electronic-resources/ced/
Format  : Helsinki/ICAME style — one file per text with single-letter header
          tags (``<B>``, ``<N>``, ``<A>``, ``<C>``, ``<O>``, ...) followed by
          ``<P N>`` paragraph markers and plain text content.
          Speaker labels are in ``[$ ... $]`` blocks.

Unit of output: one row per text file.  The ``paragraphs`` field contains cleaned paragraph strings (speaker labels and markup stripped), after filtering paragraphs shorter than ``--min-para-words`` words.

Usage
-----
    # Stats only
    uv run python scripts/process/process_ced.py

    # Full extraction
    uv run python scripts/process/process_ced.py --output-dir data/processed/ced --preview-path data/samples/ced_preview.jsonl --min-para-words 0 --year-min 1700 --year-max 1920
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from historical_lm.corpus_header import parse_corpus_file
from historical_lm.stats import count_words, save_stats

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_TXT_DIR = _REPO_ROOT / 'data' / 'raw' / 'ced_dialogues'
_DEFAULT_OUTPUT = _REPO_ROOT / 'data' / 'stats' / 'ced_stats.parquet'
_DEFAULT_SHARD_SIZE = 50


def _flush_shard(rows: list[dict], output_dir: Path, shard_idx: int) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f'part-{shard_idx:05d}.parquet'
    pq.write_table(pa.Table.from_pylist(rows), path, compression='zstd')
    print(f'  Wrote shard {shard_idx:05d} ({len(rows)} texts) -> {path}')


def collect_stats(
    txt_dir: Path,
    year_min: int,
    year_max: int,
    min_para_words: int,
    max_texts: int,
    output_dir: Path | None,
    preview_path: Path | None,
    shard_size: int,
    preview_paragraphs: int,
) -> pl.DataFrame:
    """Process all CED text files, filter by year, and optionally save text.

    Args:
        txt_dir: Directory containing the CED text files (no extension).
        year_min: Keep only texts with year >= this value.
        year_max: Keep only texts with year <= this value.
        min_para_words: Minimum word count for a paragraph to be kept.
        max_texts: Stop after this many accepted texts (0 = no limit).
        output_dir: If given, write Parquet text shards here.
        preview_path: If given, write a JSONL preview file here.
        shard_size: Texts per Parquet shard.
        preview_paragraphs: Paragraphs per text in the JSONL preview.

    Returns:
        Polars DataFrame with one row per accepted text (no text content).
    """
    # CED files have no extension
    files = [f for f in sorted(txt_dir.iterdir()) if f.is_file() and f.suffix == '']
    if not files:
        # Fallback: any file that is not a directory or known non-text type
        files = [
            f for f in sorted(txt_dir.iterdir())
            if f.is_file() and f.suffix not in {'.pdf', '.zip', '.md'}
        ]
    if not files:
        print(f'ERROR: no text files found in {txt_dir}')
        return pl.DataFrame()

    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
    if preview_path is not None:
        preview_path.parent.mkdir(parents=True, exist_ok=True)

    stat_rows: list[dict] = []
    shard_buf: list[dict] = []
    shard_idx = 0
    n_year_skip = 0
    n_empty_skip = 0
    n_parse_err = 0

    preview_fh = (
        preview_path.open('w', encoding='utf-8') if preview_path is not None else None
    )
    try:
        for path in tqdm(files, desc='CED texts', unit='file'):
            result = parse_corpus_file(path, min_para_words)
            if result is None:
                n_parse_err += 1
                continue
            meta, paragraphs = result

            year = meta.get('year')
            if year is None or year < year_min or year > year_max:
                n_year_skip += 1
                print(f'  Skipping {meta["text_id"]} due to year {year}')
                continue

            if not paragraphs:
                n_empty_skip += 1
                continue

            stat_row = {
                'text_id': meta['text_id'],
                'title': meta['title'],
                'author': meta['author'],
                'year': year,
                'period_code': meta['period_code'],
                'date_orig': meta['date_orig'],
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
                        {**stat_row, 'text_preview': paragraphs[:preview_paragraphs]},
                        ensure_ascii=False,
                    ) + '\n'
                )

            if max_texts > 0 and len(stat_rows) >= max_texts:
                break

    finally:
        if preview_fh is not None:
            preview_fh.close()

    if output_dir is not None and shard_buf:
        _flush_shard(shard_buf, output_dir, shard_idx)

    print(f'\n--- Filter diagnostics ---')
    print(f'  Files found       : {len(files):,}')
    print(f'  Parse errors      : {n_parse_err:,}')
    print(f'  Year out of range : {n_year_skip:,}')
    print(f'  Empty after filter: {n_empty_skip:,}')
    print(f'  Texts accepted    : {len(stat_rows):,}')

    if output_dir is not None:
        print(f'Text saved -> {output_dir}')
    if preview_path is not None:
        print(f'Preview written -> {preview_path}')

    return pl.DataFrame(stat_rows)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Process CED text files into Parquet shards.'
    )
    parser.add_argument(
        '--txt-dir',
        default=_DEFAULT_TXT_DIR,
        type=Path,
        help='Directory containing the CED text files.',
    )
    parser.add_argument(
        '--output',
        default=_DEFAULT_OUTPUT,
        type=Path,
        help='Output Parquet file for per-text statistics.',
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
        default=1560,
        type=int,
        help='Minimum year (default: 1560).',
    )
    parser.add_argument(
        '--year-max',
        default=1760,
        type=int,
        help='Maximum year inclusive (default: 1760).',
    )
    parser.add_argument(
        '--min-para-words',
        default=5,
        type=int,
        help='Minimum words per paragraph to keep (default: 5).',
    )
    parser.add_argument(
        '--shard-size',
        default=_DEFAULT_SHARD_SIZE,
        type=int,
        help=f'Texts per Parquet shard (default: {_DEFAULT_SHARD_SIZE}).',
    )
    parser.add_argument(
        '--preview-paragraphs',
        default=3,
        type=int,
        help='Paragraphs per text in JSONL preview (default: 3).',
    )
    parser.add_argument(
        '--max-texts',
        default=0,
        type=int,
        help='Stop after this many accepted texts (0 = no limit).',
    )
    return parser


def main() -> None:
    """Entry point for CED processing."""
    args = _build_parser().parse_args()

    if not args.txt_dir.is_dir():
        print(f'ERROR: directory not found: {args.txt_dir}')
        return

    print(f'Processing {args.txt_dir} ...')
    df = collect_stats(
        args.txt_dir,
        args.year_min,
        args.year_max,
        args.min_para_words,
        args.max_texts,
        args.output_dir,
        args.preview_path,
        args.shard_size,
        args.preview_paragraphs,
    )

    save_stats(df, args.output)
    print(f'\nSaved {len(df):,} text records -> {args.output}')

    if df.is_empty():
        return

    print('\n--- Summary ---')
    print(f'Total texts     : {len(df):,}')
    print(f'Total paragraphs: {df["n_paragraphs"].sum():,}')
    print(f'Total words     : {df["word_count"].sum():,}')
    year_col = df['year'].drop_nulls()
    if year_col.len() > 0:
        print(f'Year range      : {year_col.min()} - {year_col.max()}')
    print(
        '\nBy period:\n'
        + str(
            df.group_by('period_code')
            .agg(pl.len().alias('n_texts'), pl.col('word_count').sum())
            .sort('period_code')
        )
    )


if __name__ == '__main__':
    main()
