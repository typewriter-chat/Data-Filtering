"""Explore and collect statistics from the British Library Books dataset.

Dataset  : TheBritishLibrary/blbooks (HuggingFace)
Homepage : https://huggingface.co/datasets/TheBritishLibrary/blbooks
License  : CC0 1.0 (Public Domain)
Coverage : c. 1510–1900, ~25M pages across 49 K books.
Format   : JSONL — one record per *page*.

Schema (key fields)
-------------------
record_id       : unique book identifier
date            : normalised publication date (datetime or null)
raw_date        : raw date string from catalogue
title           : book title
author          : author name(s)
place           : place of publication
country         : country of publication
text            : OCR text of the page (may be empty)
mean_wc_ocr     : mean OCR word-confidence score [0–1] for the page
std_wc_ocr      : std dev of OCR word-confidence score
Language_1…4    : detected languages (string codes, e.g. ``English``)

Each accepted *page* becomes one output row.  A page is accepted when its
year, language, and per-page OCR score all pass the configured thresholds,
so a book may contribute zero, some, or all of its pages.

Usage
-----
    uv run python scripts/explore_blbooks.py \\
        --output data/stats/blbooks_stats.parquet \\
        --year-min 1700 \\
        --year-max 1920 \\
        --min-ocr-score 0.8 \\
        --language English \\
        --max-books 0

Set ``--max-books`` to a positive integer during development to limit the
number of books processed (0 = no limit).
"""

from __future__ import annotations

import argparse
import json
from itertools import chain
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from datasets import load_dataset
from tqdm import tqdm

from historical_lm.stats import count_words, save_stats

# Fields to pull from each page record
_PAGE_FIELDS = (
    'record_id', 'date', 'raw_date', 'title', 'author', 'place',
    'country', 'text', 'mean_wc_ocr', 'std_wc_ocr',
    'Language_1', 'Language_2', 'Language_3', 'Language_4',
)

# HuggingFace dataset identifier
_HF_DATASET = 'TheBritishLibrary/blbooks'

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_OUTPUT = _REPO_ROOT / 'data' / 'stats' / 'blbooks_stats.parquet'
_DEFAULT_OUTPUT_DIR = _REPO_ROOT / 'data' / 'processed' / 'blbooks'
_DEFAULT_PREVIEW = _REPO_ROOT / 'data' / 'samples' / 'blbooks_preview.jsonl'

# Available named configs and their approximate year ranges.
# We select only the configs that overlap with [year_min, year_max] at load
# time, skipping shards that cannot contain any matching pages.
_CONFIGS: list[tuple[str, int, int]] = [
    ('1510_1699', 1510, 1699),
    ('1700_1799', 1700, 1799),
    ('1800_1899', 1800, 1899),
]


def _is_english(row: dict) -> bool:
    """Return True if any of the language fields indicates English.

    Args:
        row: A page-level record dict.

    Returns:
        True if at least one of Language_1…4 contains 'English'.
    """
    for key in ('Language_1', 'Language_2', 'Language_3', 'Language_4'):
        val = row.get(key) or ''
        if 'english' in val.lower():
            return True
    return False


def _flush_shard(rows: list[dict], output_dir: Path, shard_idx: int) -> None:
    """Write a batch of page rows to a Parquet shard.

    Args:
        rows: List of page dicts, each including a ``text`` field.
        output_dir: Directory to write the shard into.
        shard_idx: Zero-based shard index used to form the filename.
    """
    table = pa.Table.from_pylist(rows)
    shard_path = output_dir / f'part-{shard_idx:05d}.parquet'
    pq.write_table(table, shard_path, compression='zstd')


def collect_stats(
    year_min: int,
    year_max: int,
    min_ocr_score: float,
    language_filter: str | None,
    max_books: int,
    output_dir: Path | None,
    preview_path: Path | None,
    shard_size: int,
    preview_pages: int,
    local_dir: Path | None = None,
    hf_cache_dir: Path | None = None,
) -> pl.DataFrame:
    """Stream the BL Books dataset, aggregate per-book statistics, and optionally save text.

    Loads the dataset in streaming mode.  Only configs whose year range
    overlaps ``[year_min, year_max]`` are requested from HuggingFace,
    skipping irrelevant shards.  Pages are grouped by ``record_id``; texts
    are accumulated per book.  After all pages of a book are seen, books
    that pass the OCR threshold are written to Parquet shards and a JSONL
    preview file.

    Args:
        year_min: Keep only books with year >= this value (inclusive).
        year_max: Keep only books with year <= this value (inclusive).
        min_ocr_score: Minimum mean OCR confidence [0–1] required per book.
            Books with no OCR data are excluded.
        language_filter: If given (e.g. ``'English'``), keep only books whose
            primary language field contains this string (case-insensitive).
        max_books: Stop after this many *books* (0 = process all).
        output_dir: If given, write filtered books as Parquet shards here.
        preview_path: If given, write a JSONL file with the first
            ``preview_pages`` pages per book for manual inspection.
        shard_size: Number of books per Parquet shard.
        preview_pages: Number of pages to include per book in the JSONL preview.
        local_dir: If given, load from this local snapshot directory instead of
            streaming from HuggingFace.  Produced by ``download_blbooks_raw.py``.
        hf_cache_dir: Directory for HuggingFace Arrow cache when loading locally
            (``streaming=False``).  Defaults to ``<repo_root>/.hf_cache/datasets``
            to avoid writing to the home directory quota.

    Returns:
        Polars DataFrame with one row per accepted book (stats only, no text).
    """
    # Select only configs whose range overlaps [year_min, year_max]
    configs = [name for name, lo, hi in _CONFIGS if lo <= year_max and hi >= year_min]
    if not configs:
        print('No dataset configs overlap the requested year range.')
        return pl.DataFrame()

    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
    if preview_path is not None:
        preview_path.parent.mkdir(parents=True, exist_ok=True)

    source = str(local_dir) if local_dir is not None else _HF_DATASET
    streaming = local_dir is None
    if local_dir is not None:
        _cache_dir = str(hf_cache_dir) if hf_cache_dir is not None else str(_REPO_ROOT / '.hf_cache' / 'datasets')
        Path(_cache_dir).mkdir(parents=True, exist_ok=True)
        print(f'Loading from local directory: {local_dir}, configs: {configs}')
        print(f'Arrow cache → {_cache_dir}')
    else:
        _cache_dir = None
        print(f'Streaming from {_HF_DATASET}, configs: {configs}')

    streams = [
        load_dataset(source, name=cfg, split='train', streaming=streaming,
                     trust_remote_code=True, cache_dir=_cache_dir)
        for cfg in configs
    ]
    ds = chain(*streams)

    # record_id -> accumulated book dict (stats + page texts)
    books: dict[str, dict] = {}
    books_seen: int = 0
    shard_buf: list[dict] = []
    shard_idx: int = 0
    stat_rows: list[dict] = []

    # Rejection counters for diagnostics
    n_pages_total: int = 0
    n_no_date: int = 0
    n_year_out: int = 0
    n_lang_skip: int = 0
    n_ocr_skip: int = 0

    preview_fh = preview_path.open('w', encoding='utf-8') if preview_path is not None else None

    try:
        for page in tqdm(ds, desc='BL Books pages', unit='page'):
            rid = page.get('record_id') or ''
            if not rid:
                continue

            n_pages_total += 1
            year_raw = page.get('date')
            year = year_raw.year if year_raw is not None else None
            if year is None:
                n_no_date += 1
                continue
            if year < year_min or year > year_max:
                n_year_out += 1
                continue

            if language_filter and not any(
                language_filter.lower() in (page.get(k) or '').lower()
                for k in ('Language_1', 'Language_2', 'Language_3', 'Language_4')
            ):
                n_lang_skip += 1
                continue

            if page.get('mean_wc_ocr') is None or page.get('mean_wc_ocr') < min_ocr_score:
                n_ocr_skip += 1
                continue

            text = page.get('text') or ''
            page_words = count_words(text)
            if rid not in books:
                if max_books > 0 and books_seen >= max_books:
                    break
                books[rid] = {
                    'record_id': rid,
                    'title': page.get('title') or '',
                    'name': page.get('name') or '',
                    'year': year,
                    'raw_date': page.get('raw_date') or '',
                    'place': page.get('place') or '',
                    'country': page.get('country') or '',
                    'language': page.get('Language_1') or '',
                    'page_count': 0,
                    'word_count': 0,
                    'text_by_page': {},
                }
                books_seen += 1

            entry = books[rid]
            entry['page_count'] += 1
            entry['word_count'] += page_words
            entry['text_by_page'][page['pg']] = text

    finally:
        if preview_fh is not None:
            preview_fh.close()

    print(f'\n--- Page-level filter diagnostics ---')
    print(f'  Pages seen        : {n_pages_total:,}')
    print(f'  No date (skipped) : {n_no_date:,}')
    print(f'  Year out of range : {n_year_out:,}')
    print(f'  Language skipped  : {n_lang_skip:,}')
    print(f'  OCR skipped       : {n_ocr_skip:,}')
    print(f'  Books accumulated : {books_seen:,}')

    # save results
    preview_fh = preview_path.open('w', encoding='utf-8') if preview_path is not None else None
    try:
        for entry in books.values():
            pages = entry.pop('text_by_page')
            stat_rows.append(entry)
            if output_dir is not None:
                # Store pages as a list sorted by page number for Parquet compatibility
                sorted_pages = [text for _, text in sorted(pages.items())]
                shard_buf.append({**entry, 'text_by_page': sorted_pages})
                if len(shard_buf) >= shard_size:
                    _flush_shard(shard_buf, output_dir, shard_idx)
                    print(f'  Wrote shard {shard_idx:05d} ({len(shard_buf)} books)')
                    shard_buf = []
                    shard_idx += 1

            if preview_fh is not None:
                preview_record = {**entry, 'text_preview': list(pages.values())[:preview_pages]}
                preview_fh.write(json.dumps(preview_record, ensure_ascii=False) + '\n')

        if output_dir is not None and shard_buf:
            _flush_shard(shard_buf, output_dir, shard_idx)
            print(f'  Wrote shard {shard_idx:05d} ({len(shard_buf)} books)')

    finally:
        if preview_fh is not None:
            preview_fh.close()

    print(f'  Books accepted    : {len(stat_rows):,}')

    if output_dir is not None:
        print(f'Text saved → {output_dir}')
    if preview_path is not None:
        print(f'Preview written → {preview_path}')

    return pl.DataFrame(stat_rows)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Collect per-book statistics from the BL Books HuggingFace dataset '
                    'and optionally save filtered text locally.'
    )
    parser.add_argument(
        '--output',
        default=_DEFAULT_OUTPUT,
        type=Path,
        help='Output Parquet file for per-book statistics.',
    )
    parser.add_argument(
        '--output-dir',
        default=None,
        type=Path,
        help=f'If set, save filtered book text as Parquet shards here. '
        f'Suggested: {_DEFAULT_OUTPUT_DIR}',
    )
    parser.add_argument(
        '--preview-path',
        default=None,
        type=Path,
        help=f'If set, write a JSONL preview file with the first --preview-pages pages per book. '
        f'Suggested: {_DEFAULT_PREVIEW}',
    )
    parser.add_argument(
        '--year-min',
        default=1700,
        type=int,
        metavar='YEAR',
        help='Keep only books with year >= YEAR (default: 1700).',
    )
    parser.add_argument(
        '--year-max',
        default=1920,
        type=int,
        metavar='YEAR',
        help='Keep only books with year <= YEAR (default: 1920).',
    )
    parser.add_argument(
        '--min-ocr-score',
        default=0.8,
        type=float,
        metavar='SCORE',
        help='Minimum mean OCR confidence per book [0–1] (default: 0.8).',
    )
    parser.add_argument(
        '--language',
        default='English',
        type=str,
        help='Language filter matched against Language_1…4 fields. '
             'Pass empty string to disable filtering.',
    )
    parser.add_argument(
        '--shard-size',
        default=1000,
        type=int,
        metavar='N',
        help='Number of books per Parquet shard (default: 1000).',
    )
    parser.add_argument(
        '--preview-pages',
        default=3,
        type=int,
        metavar='N',
        help='Number of pages to include per book in the JSONL preview (default: 3).',
    )
    parser.add_argument(
        '--max-books',
        default=0,
        type=int,
        metavar='N',
        help='Stop after N books (0 = no limit). Useful for development.',
    )
    parser.add_argument(
        '--local-dir',
        default=None,
        type=Path,
        help='Load from a local snapshot directory (produced by download_blbooks_raw.py) '
             'instead of streaming from HuggingFace. Avoids network disconnections.',
    )
    parser.add_argument(
        '--hf-cache-dir',
        default=None,
        type=Path,
        help='Directory for the HuggingFace Arrow cache when loading locally '
             f'(default: <repo_root>/.hf_cache/datasets).',
    )
    return parser


def main() -> None:
    """Entry point for BL Books statistics collection and optional text download."""
    args = _build_parser().parse_args()

    language_filter = args.language.strip() if args.language else None
    df = collect_stats(
        args.year_min,
        args.year_max,
        args.min_ocr_score,
        language_filter,
        args.max_books,
        args.output_dir,
        args.preview_path,
        args.shard_size,
        args.preview_pages,
        args.local_dir,
        args.hf_cache_dir,
    )

    save_stats(df, args.output)
    print(f'\nSaved {len(df):,} book records -> {args.output}')

    print('\n--- Summary ---')
    print(f'Total books   : {len(df):,}')
    if df.is_empty():
        return
    print(f'Total pages   : {df["page_count"].sum():,}')
    print(f'Total words   : {df["word_count"].sum():,}')
    if df['year'].drop_nulls().len() > 0:
        print(f'Year range    : {df["year"].drop_nulls().min()} – {df["year"].drop_nulls().max()}')
    print(df.describe())


if __name__ == '__main__':
    main()
