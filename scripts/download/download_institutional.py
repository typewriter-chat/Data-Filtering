"""Download and locally save a filtered subset of Institutional Books 1.0.

Applies filters (year range, OCR
quality, language) but also saves the full per-page text so the dataset can
be used for training or further cleaning without re-downloading from HuggingFace.

Output
------
Parquet shards  : ``--output-dir`` / part-XXXXX.parquet
    One Parquet file per ``--shard-size`` books.  Each row is one book.
    Includes all metadata fields plus ``text_by_page_gen`` (post-processed
    OCR text, one string per page).

JSONL preview   : ``--preview-path``
    One JSON object per book, containing metadata and the first
    ``--preview-pages`` pages of text.  Intended for manual quality review.

Usage
-----
    # Dry-run: first 200 books
    uv run python scripts/download/download_institutional.py \\
        --output-dir data/processed/institutional \\
        --preview-path data/samples/institutional_preview.jsonl \\
        --max-books 200

    # Full filtered download
    uv run python scripts/download/download_institutional.py --output-dir data_test/processed/institutional --preview-path data_test/samples/institutional_preview.jsonl --random-sample-path data_test/samples/institutional_random_sample.parquet --year-min 1700 --year-max 1920 --min-ocr-score 80 
"""

from __future__ import annotations

import argparse
import json
import random
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from datasets import load_dataset
from tqdm import tqdm

from historical_lm.stats import marc_year_max

_HF_DATASET = 'institutional/institutional-books-1.0'

# MARC date_types_src values for which date2_src represents an *end* year that
# must also be within the cutoff.  Two families:
#
#   Continuing resources (c/d/u): date1 = series founding year, date2 = end year.
#     date2='9999' (ongoing) and date2='uuuu' (unknown) parse to None → rejected.
#
#   Span types (m/q): date1 = start/earliest, date2 = end/latest possible year.
#     'Multiple dates' (m): multi-volume works whose later volumes may be post-1913.
#     'Questionable date' (q): date2 is the latest plausible year; item could be
#     post-1913 if date2 > year_max.
_DATE2_BOUNDED_TYPES: frozenset[str] = frozenset({
    'Continuing resource currently published',
    'Continuing resource ceased publication (Dead status)',
    'Continuing resource status unknown',
    'Multiple dates',
    'Questionable date',
    'Inclusive dates of collection',
    'Range of years of bulk of collection',
})
_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_OUTPUT_DIR = _REPO_ROOT / 'data_test' / 'processed' / 'institutional'
_DEFAULT_PREVIEW = _REPO_ROOT / 'data_test' / 'samples' / 'institutional_preview.jsonl'
# _DEFAULT_RANDOM_SAMPLE = _REPO_ROOT / 'data_test' / 'samples' / 'institutional_random_sample.parquet'
_DEFAULT_RANDOM_SAMPLE = None

# Metadata fields to include in the saved Parquet (text fields added separately)
_META_FIELDS = [
    'title_src',
    'author_src',
    'date1_src',
    'date2_src',
    'date_types_src',
    'page_count_src',
    'token_count_o200k_base_gen',
    'language_src',
    'language_gen',
    'topic_or_subject_src',
    'topic_or_subject_gen',
    'topic_or_subject_score_gen',
    'genre_or_form_src',
    'ocr_score_src',
    'ocr_score_gen',
]


def _passes_filters(
    book: dict,
    year_min: int,
    year_max: int,
    min_ocr_score: float,
    language_filter: str | None,
) -> tuple[bool, int | None]:
    """Check whether a book record passes all selection filters.

    Args:
        book: Raw record dict from the HuggingFace dataset.
        year_min: Minimum publication year (inclusive).
        year_max: Maximum publication year (inclusive).
        min_ocr_score: Minimum required value for both OCR score fields.
        language_filter: ISO 639-3 code to match against language_src / language_gen.

    Returns:
        Tuple of (passes, resolved_year).  ``passes`` is False and year may
        be None when the book is rejected.
    """
    date1 = book.get('date1_src')
    date2 = book.get('date2_src')
    date_type = (book.get('date_types_src') or '').strip()

    # Treat unknown MARC digits as '9' (worst case) so that uncertain dates
    # like "19uu" (max 1999) are rejected rather than accepted as 1900.
    year = marc_year_max(date1)
    if year is None:
        year = marc_year_max(date2)
    if year is None or year < year_min or year > year_max:
        return False, year

    # For types where date2 is an end/upper-bound year, also require date2 ≤ year_max.
    if date_type in _DATE2_BOUNDED_TYPES:
        year2_max = marc_year_max(date2)
        if year2_max is None or year2_max > year_max:
            return False, year

    ocr_src = book.get('ocr_score_src')
    ocr_gen = book.get('ocr_score_gen')
    if ocr_src is None or ocr_gen is None:
        return False, year
    if float(ocr_src) < min_ocr_score or float(ocr_gen) < min_ocr_score:
        return False, year

    if language_filter:
        lang_src = (book.get('language_src') or '').strip().lower()
        lang_gen = (book.get('language_gen') or '').strip().lower()
        if language_filter.lower() not in (lang_src, lang_gen):
            return False, year

    return True, year


def _build_row(book: dict, year: int) -> dict:
    """Flatten one book record into a dict suitable for Parquet serialisation.

    Args:
        book: Raw record dict.
        year: Pre-resolved publication year.

    Returns:
        Flat dict with metadata fields and ``text_by_page_gen``.
    """
    row: dict = {'year': year}
    for field in _META_FIELDS:
        val = book.get(field)
        if isinstance(val, (dict, list)) and field != 'text_by_page_gen':
            val = str(val)
        row[field] = val
    # Include post-processed text (list of page strings)
    pages = book.get('text_by_page_gen') or []
    row['text_by_page_gen'] = pages if isinstance(pages, list) else list(pages)
    return row


def _flush_shard(rows: list[dict], output_dir: Path, shard_idx: int) -> None:
    """Write a batch of book rows to a Parquet shard file.

    Args:
        rows: List of flattened book dicts.
        output_dir: Directory to write the shard into.
        shard_idx: Zero-based shard index, used to form the filename.
    """
    table = pa.Table.from_pylist(rows)
    shard_path = output_dir / f'part-{shard_idx:05d}.parquet'
    pq.write_table(table, shard_path, compression='zstd')


def _maybe_collect_random_sample(
    row: dict,
    sample_buf: list[dict],
    sample_prob: float,
    rng: random.Random,
) -> bool:
    """Append ``row`` to the random-sample buffer with probability ``sample_prob``.

    Args:
        row: Flattened book record ready for serialisation.
        sample_buf: In-memory buffer of sampled rows.
        sample_prob: Bernoulli sampling probability in ``[0, 1]``.
        rng: Random-number generator used for reproducible sampling.

    Returns:
        True if the row was sampled, otherwise False.
    """
    if sample_prob <= 0:
        return False
    if rng.random() >= sample_prob:
        return False
    sample_buf.append(row)
    return True


def download(
    output_dir: Path,
    preview_path: Path,
    random_sample_path: Path | None,
    year_min: int,
    year_max: int,
    min_ocr_score: float,
    language_filter: str | None,
    shard_size: int,
    preview_pages: int,
    max_books: int,
    sample_prob: float,
    sample_seed: int,
) -> None:
    """Stream, filter, and save the Institutional Books dataset locally.

    Args:
        output_dir: Directory for Parquet shard files.
        preview_path: Output path for the JSONL spot-check file.
        random_sample_path: Optional Parquet path for a small random sample of accepted books.
        year_min: Minimum publication year (inclusive).
        year_max: Maximum publication year (inclusive).
        min_ocr_score: Minimum OCR score threshold for both score fields.
        language_filter: ISO 639-3 language code filter, or None.
        shard_size: Number of books per Parquet shard.
        preview_pages: Number of pages to include in the JSONL preview.
        max_books: Maximum number of books to accept (0 = no limit).
        sample_prob: Probability of keeping any accepted book in the random sample.
        sample_seed: Seed used for reproducible random sampling.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    preview_path.parent.mkdir(parents=True, exist_ok=True)
    # if random_sample_path is not None:
    #     random_sample_path.parent.mkdir(parents=True, exist_ok=True)

    print(f'Loading {_HF_DATASET} in streaming mode …')
    ds = load_dataset(
        _HF_DATASET,
        split='train',
        streaming=True,
        trust_remote_code=True,
    )

    shard_buf: list[dict] = []
    sample_buf: list[dict] = []
    shard_idx = 0
    total_accepted = 0
    total_sampled = 0
    rng = random.Random(sample_seed)

    with preview_path.open('w', encoding='utf-8') as preview_fh:
        for book in tqdm(ds, desc='Institutional books', unit='book'):
            if max_books > 0 and total_accepted >= max_books:
                break

            passes, year = _passes_filters(book, year_min, year_max, min_ocr_score, language_filter)
            if not passes:
                continue

            row = _build_row(book, year)
            shard_buf.append(row)
            total_accepted += 1
            if _maybe_collect_random_sample(row, sample_buf, sample_prob, rng):
                total_sampled += 1
            # Write JSONL preview: metadata + first N pages
            preview_record = {k: v for k, v in row.items() if k != 'text_by_page_gen'}
            preview_record['text_preview'] = row['text_by_page_gen'][:preview_pages]
            preview_fh.write(json.dumps(preview_record, ensure_ascii=False) + '\n')

            # Flush shard when full
            if len(shard_buf) >= shard_size:
                _flush_shard(shard_buf, output_dir, shard_idx)
                print(f'  Wrote shard {shard_idx:05d} ({len(shard_buf)} books)')
                shard_buf = []
                shard_idx += 1

        # Flush remaining books
        if shard_buf:
            _flush_shard(shard_buf, output_dir, shard_idx)
            print(f'  Wrote shard {shard_idx:05d} ({len(shard_buf)} books)')

    # if random_sample_path is not None and sample_buf:
    #     table = pa.Table.from_pylist(sample_buf)
    #     pq.write_table(table, random_sample_path, compression='zstd')

    print(f'\nDone. {total_accepted:,} books saved across {shard_idx + 1} shards -> {output_dir}')
    print(f'Preview written -> {preview_path}')
    # if random_sample_path is not None:
    #     print(
    #         f'Random sample written -> {random_sample_path} '
    #         f'({total_sampled:,} books, p={sample_prob})'
    #     )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Download a filtered subset of Institutional Books 1.0 to local Parquet shards.'
    )
    parser.add_argument('--output-dir', default=_DEFAULT_OUTPUT_DIR, type=Path,
                        help='Directory for output Parquet shard files.')
    parser.add_argument('--preview-path', default=_DEFAULT_PREVIEW, type=Path,
                        help='Output JSONL file for manual quality inspection.')
    parser.add_argument('--random-sample-path', default=_DEFAULT_RANDOM_SAMPLE, type=Path,
                        help='Output Parquet file for a small random sample of accepted books.')
    parser.add_argument('--year-min', default=1700, type=int, metavar='YEAR',
                        help='Minimum publication year, inclusive (default: 1700).')
    parser.add_argument('--year-max', default=1920, type=int, metavar='YEAR',
                        help='Maximum publication year, inclusive (default: 1920).')
    parser.add_argument('--min-ocr-score', default=80, type=float, metavar='SCORE',
                        help='Minimum required value for ocr_score_src and ocr_score_gen (default: 80).')
    parser.add_argument('--language', default='eng', type=str,
                        help='ISO 639-3 language code (default: eng). Empty string disables filtering.')
    parser.add_argument('--shard-size', default=1000, type=int, metavar='N',
                        help='Number of books per Parquet shard (default: 1000).')
    parser.add_argument('--preview-pages', default=10, type=int, metavar='N',
                        help='Number of pages to include in the JSONL preview per book (default: 10).')
    parser.add_argument('--max-books', default=0, type=int, metavar='N',
                        help='Stop after N accepted books (0 = no limit).')
    parser.add_argument('--sample-prob', default=1 / 10000, type=float, metavar='P',
                        help='Probability of saving an accepted book into the random sample (default: 1/10000).')
    parser.add_argument('--sample-seed', default=42, type=int, metavar='N',
                        help='Random seed for reproducible sampling (default: 42).')
    return parser


def main() -> None:
    """Entry point for the Institutional Books download script."""
    args = _build_parser().parse_args()
    language_filter = args.language.strip() if args.language else None
    download(
        output_dir=args.output_dir,
        preview_path=args.preview_path,
        random_sample_path=args.random_sample_path,
        year_min=args.year_min,
        year_max=args.year_max,
        min_ocr_score=args.min_ocr_score,
        language_filter=language_filter,
        shard_size=args.shard_size,
        preview_pages=args.preview_pages,
        max_books=args.max_books,
        sample_prob=args.sample_prob,
        sample_seed=args.sample_seed,
    )


if __name__ == '__main__':
    main()
