"""Combined OCR-filter + clean pipeline for Institutional Books.

Reads raw Parquet shards from ``download_institutional.py`` (one row per book,
``text_by_page_gen`` list) and applies two stages in a single pass:

  Stage 0 - OCR quality filter
      Drop books where ``ocr_score_src / 100 < min_ocr_score`` or
      ``ocr_score_gen / 100 < min_ocr_score``.  Scores are stored as integers
      on a 0-100 scale; the threshold is expressed as a fraction (default 0.9).

  Stage 1 - Paragraph extraction and cleaning
      - ``_normalise_block``: whitespace normalisation + strip leading bare
        page numbers + strip leading digit-table prefixes (D2) + strip OCR
        66/99 open/close-quote artifacts (D1).
      - ``_looks_caps_fragment``: detects short multi-line all-caps layout
        blocks AND longer caps-heavy structural blocks (chapter headings,
        edition notices, TOC summaries) (D3).
      - ``_drop_reason``: drops institution stamps, barcodes, provenance
        blocks, symbol garbage, TOC dot-leaders, caps fragments.
      - Merge continuation fragments across paragraph boundaries.
      - Drop paragraphs shorter than _MIN_PARA_CHARS characters.

Usage
-----
    # Test on one shard
    uv run python scripts/clean/clean_institutional.py --max-shards 1

    # Process a single shard
    uv run python scripts/clean/clean_institutional.py --shard-id 0

    # Full sequential run with stricter OCR filter
    uv run python scripts/clean/clean_institutional.py --year-max 1938 --min-ocr-score 90 --shard-id 100
"""

from __future__ import annotations

import argparse
import json
import random
import re
from collections import Counter
from pathlib import Path
from typing import Callable

import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from historical_lm.stats import count_words

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_INPUT_DIR = _REPO_ROOT / 'data_test' / 'processed' / 'institutional'
_DEFAULT_OUTPUT_DIR = _REPO_ROOT / 'data_test' / 'clean' / 'institutional_clean'
# _DEFAULT_SAMPLE = _REPO_ROOT / 'data' / 'clean' / 'institutional_final_sample.jsonl'
# _DEFAULT_REPORT = _REPO_ROOT / 'data' / 'clean' / 'institutional_final_report.json'


def _passes_ocr_filter(row: dict, min_ocr_score: float) -> bool:
    s = row.get('ocr_score_src')
    g = row.get('ocr_score_gen')
    if s is None or g is None:
        return False
    return float(s) >= min_ocr_score and float(g) >= min_ocr_score


# ── Stage 1: paragraph extraction and cleaning ────────────────────────────────

_PARA_SPLIT_RE = re.compile(r'\n\s*\n+')
_WHITESPACE_RE = re.compile(r'\s+')

# Heuristic thresholds.
_IDENTIFIER_MAX_WORDS = 35
_IDENTIFIER_ALPHA_RATIO_MAX = 0.55
_IDENTIFIER_DIGIT_RATIO_MIN = 0.18
_IDENTIFIER_STRONG_DIGIT_RATIO_MIN = 0.3
_SYMBOL_NOISE_MAX_WORDS = 40
_SYMBOL_NOISE_RATIO_MIN = 0.12
_NON_ASCII_NOISE_MAX_WORDS = 30
_NON_ASCII_NOISE_RATIO_MIN = 0.08
_CAPS_FRAGMENT_MAX_WORDS = 25
_CAPS_FRAGMENT_UPPER_RATIO_MIN = 0.45
_CAPS_STRUCTURAL_RATIO = 0.55   # caps ratio for caps+keyword structural blocks
_CAPS_HARD_RATIO = 0.70         # caps ratio for unconditional drop (no keyword needed)
_CAPS_STRUCTURAL_MIN_CHARS = 100
_INSTITUTION_MAX_WORDS = 60
_MOTTO_MAX_WORDS = 20
_PROVENANCE_MAX_WORDS = 70
_MERGE_SHORT_PARAGRAPH_MAX_WORDS = 8
_TOC_ENTRY_MAX_WORDS = 15
_MIN_PARA_CHARS = 100

# Noise-family regexes.
_INSTITUTION_RE = re.compile(
    r'\b('
    r'HARVARD|WIDENER|COUNTWAY|LITTAUER|ANDOVER-HARVARD|'
    r'SCIENCE\s+CENTER|SCIENCE\s+LIBRARY|HARVARD\s+LAW|'
    r'LIBRARY|COLLEGE|UNIVERSITY|NEDL\s+TRANSFER|'
    r'GIFT\s+OF|BEQUEST|FROM\s+THE\s+LIBRARY\s+OF|'
    r'TRANSFERRED\s+TO|FROM\s+BY\s+EXCHANGE'
    r')\b',
    re.IGNORECASE,
)
_MOTTO_RE = re.compile(r'\b(VERI\s*TAS|VERO\s*TAS|CHRISTO|ACADEMIAE|HARVARDIAN)\b', re.IGNORECASE)
_CALLNO_RE = re.compile(r'\b[A-Z]{1,3}\s*\d{1,4}(?:\.\d+)?(?:\s+[A-Z0-9.]+){0,4}\b')
_BARCODE_RE = re.compile(r'\b(?:\d[\d\s]{7,}\d|[A-Z]{2,}\d{4,})\b')
_PROVENANCE_RE = re.compile(
    r'\b(GIFT\s+OF|BEQUEST|FROM\s+THE\s+LIBRARY\s+OF|TRANSFERRED\s+TO|FROM\s+BY\s+EXCHANGE|IN\s+MEMORY\s+OF)\b',
    re.IGNORECASE,
)
_LOWER_START_RE = re.compile(r'^[a-z]')
_END_PUNCT_RE = re.compile(r'[.!?:"\')\]]$')
_PUNCT_ONLY_RE = re.compile(r'^[^\w]+$')
# Strip a short leading page-number bleed (1-4 digits + whitespace).
_LEADING_NUMBER_RE = re.compile(r'^\d{1,4}\s+')
# D2: strip leading digit-table prefix (3+ numeric tokens before any alpha word).
_LEADING_TABLE_RE = re.compile(r'^\s*(?:\d[\d,]*|www)(?:\s+(?:\d[\d,]*|www)){2,}\s+')
# D1: OCR open/close-quote artifacts — runs of "66" or "99" not adjacent to other digits.
_OCR_QUOTE_RE = re.compile(r'(?<!\d)(?:66|99)(?:\s+(?:66|99))+(?!\d)')
# Table-of-contents dot-leader entries.
_TOC_ENTRY_RE = re.compile(r'(?:\.\s*){2,}\s*$')
# D3: structural keyword set for caps-heavy blocks.
_STRUCTURAL_KW_RE = re.compile(
    r'\b(CHAPTER|BOOK|VOLUME|PART|EDITION|SECTION|APPENDIX|CONTENTS|PREFACE|'
    r'FOREWORD|INTRODUCTION|BIBLIOGRAPHY)\b',
    re.IGNORECASE,
)

_INVISIBLE_CHARS = frozenset({0x200b, 0x200c, 0x200d, 0xfeff, 0x00ad, 0x2028, 0x2029})


def _normalise_block(block: str, counters: Counter) -> tuple[str, list[str]]:
    """Normalise one OCR block: whitespace, page-number bleed, D2, D1."""
    lines = [line.strip() for line in block.splitlines() if line.strip()]
    text = _WHITESPACE_RE.sub(' ', ' '.join(lines)).strip()

    # Strip leading bare page-number bleed.
    text = _LEADING_NUMBER_RE.sub('', text).strip()

    # D2: strip leading digit-table prefix (e.g. "12 34 56 Some text…").
    cleaned, n = _LEADING_TABLE_RE.subn('', text)
    if n:
        counters['stripped_leading_table'] += 1
        text = cleaned.strip()

    # D1: strip OCR open/close-quote artifacts ("66 66 word 99 99").
    cleaned, n = _OCR_QUOTE_RE.subn('', text)
    if n:
        counters['stripped_ocr_quotes'] += n
        text = _WHITESPACE_RE.sub(' ', cleaned).strip()

    return text, lines


def _char_ratio(text: str, pred: Callable[[str], bool]) -> float:
    if not text:
        return 0.0
    return sum(1 for ch in text if pred(ch)) / len(text)


def _looks_identifier_heavy(text: str) -> bool:
    words = count_words(text)
    digit_ratio = _char_ratio(text, str.isdigit)
    alpha_ratio = _char_ratio(text, str.isalpha)
    code_hits = len(_CALLNO_RE.findall(text)) + len(_BARCODE_RE.findall(text))
    return (
        code_hits >= 2
        or (digit_ratio > _IDENTIFIER_DIGIT_RATIO_MIN and alpha_ratio < _IDENTIFIER_ALPHA_RATIO_MAX and words <= _IDENTIFIER_MAX_WORDS)
        or (digit_ratio > _IDENTIFIER_STRONG_DIGIT_RATIO_MIN and words <= 100)
    )


def _looks_symbol_noisy(text: str) -> bool:
    words = count_words(text)
    weird_ratio = _char_ratio(text, lambda ch: not (ch.isalnum() or ch.isspace() or ch in ".,;:!?'-\"()[]"))
    non_ascii_ratio = _char_ratio(text, lambda ch: ord(ch) > 127)
    has_invisible = any(ord(ch) in _INVISIBLE_CHARS for ch in text)
    return (
        (words <= _SYMBOL_NOISE_MAX_WORDS and weird_ratio > _SYMBOL_NOISE_RATIO_MIN)
        or (words <= _NON_ASCII_NOISE_MAX_WORDS and non_ascii_ratio > _NON_ASCII_NOISE_RATIO_MIN)
        or (has_invisible and words <= _NON_ASCII_NOISE_MAX_WORDS)
    )


def _looks_caps_fragment(text: str, lines: list[str]) -> bool:
    """Return True for all-caps layout blocks — short multi-line fragments and
    longer caps-heavy structural blocks (D3: chapter headings, edition notices,
    TOC summaries)."""
    words = count_words(text)
    upper_ratio = _char_ratio(text, str.isupper)
    caps_lines = sum(1 for line in lines if line and line == line.upper())

    # Short multi-line all-caps blocks (original heuristic).
    if (words <= _CAPS_FRAGMENT_MAX_WORDS and len(lines) >= 2
            and (upper_ratio > _CAPS_FRAGMENT_UPPER_RATIO_MIN or caps_lines >= 2)):
        return True

    # D3: longer caps-heavy structural blocks.
    if len(text) >= _CAPS_STRUCTURAL_MIN_CHARS:
        if upper_ratio > _CAPS_HARD_RATIO:
            return True
        if upper_ratio > _CAPS_STRUCTURAL_RATIO and _STRUCTURAL_KW_RE.search(text):
            return True

    return False


def _drop_reason(text: str, lines: list[str]) -> str | None:
    if not text:
        return 'empty'
    words = count_words(text)
    if words == 0:
        return 'empty'
    if _PUNCT_ONLY_RE.match(text):
        return 'punct_only'
    if _INSTITUTION_RE.search(text) and (_looks_caps_fragment(text, lines) or words <= _INSTITUTION_MAX_WORDS):
        return 'institution_block'
    if _MOTTO_RE.search(text) and (_looks_caps_fragment(text, lines) or words <= _MOTTO_MAX_WORDS):
        return 'motto_block'
    if _PROVENANCE_RE.search(text) and words <= _PROVENANCE_MAX_WORDS:
        return 'provenance_block'
    if _looks_identifier_heavy(text):
        return 'identifier_block'
    if _looks_symbol_noisy(text):
        return 'symbol_noise'
    if _looks_caps_fragment(text, lines):
        return 'caps_fragment'
    if _TOC_ENTRY_RE.search(text) and words <= _TOC_ENTRY_MAX_WORDS:
        return 'toc_entry'
    return None


def _should_merge(prev_para: str, para: str) -> bool:
    if not prev_para or not para:
        return False
    if prev_para.endswith('-') and _LOWER_START_RE.match(para):
        return True
    if _END_PUNCT_RE.search(prev_para):
        return False
    if _LOWER_START_RE.match(para):
        return True
    return count_words(para) <= _MERGE_SHORT_PARAGRAPH_MAX_WORDS and para[0].islower()


def _merge_paragraphs(paragraphs: list[str]) -> tuple[list[str], int]:
    merged: list[str] = []
    merge_count = 0
    for para in paragraphs:
        if merged and _should_merge(merged[-1], para):
            if merged[-1].endswith('-') and para[0].islower():
                merged[-1] = merged[-1][:-1] + para
            else:
                merged[-1] = f'{merged[-1]} {para}'
            merge_count += 1
            continue
        merged.append(para)
    return merged, merge_count


def _extract_clean_paragraphs(pages: list[str], counters: Counter) -> list[str]:
    """Split page OCR into paragraphs, normalise, drop noise, merge, length-filter."""
    paragraphs: list[str] = []
    for page in pages:
        if not page or not page.strip():
            counters['empty_pages'] += 1
            continue
        for raw_block in _PARA_SPLIT_RE.split(page):
            text, lines = _normalise_block(raw_block, counters)
            counters['paragraph_candidates'] += 1
            reason = _drop_reason(text, lines)
            if reason is not None:
                counters[f'dropped_{reason}'] += 1
                continue
            if text:
                paragraphs.append(text)

    merged, merge_count = _merge_paragraphs(paragraphs)
    counters['merged_paragraphs'] += merge_count

    final: list[str] = []
    for para in merged:
        if len(para) < _MIN_PARA_CHARS:
            counters['dropped_too_short'] += 1
        else:
            final.append(para)
    counters['kept_paragraphs'] += len(final)
    counters['words_kept'] += count_words(' '.join(final))
    return final


def _build_output_row(row: dict, paragraphs: list[str]) -> dict:
    meta = {k: v for k, v in row.items() if k != 'text_by_page_gen'}
    meta['source_page_count'] = len(row.get('text_by_page_gen') or [])
    meta['n_paragraphs'] = len(paragraphs)
    meta['word_count'] = count_words(' '.join(paragraphs))
    meta['paragraphs'] = paragraphs
    return meta


def _write_parquet(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path, compression='zstd')


def _maybe_write_sample(row, sample_fh, sample_prob, rng, counters) -> None:
    if sample_fh is None or sample_prob <= 0:
        return
    if rng.random() >= sample_prob:
        return
    sample_fh.write(json.dumps(row, ensure_ascii=False) + '\n')
    counters['sampled_books'] += 1


def clean_institutional_pipeline(
    input_dir: Path,
    output_dir: Path,
    sample_path: Path | None,
    report_path: Path | None,
    min_ocr_score: float,
    sample_prob: float,
    sample_seed: int,
    max_shards: int,
    shard_id: int,
    overwrite: bool,
    batch_size: int,
    year_min: int,
    year_max: int
) -> None:
    """Run the combined OCR-filter + clean pipeline."""
    files = sorted(input_dir.glob('part-*.parquet'))
    if shard_id >= 0:
        if shard_id >= len(files):
            print(f'shard_id {shard_id} out of range (only {len(files)} shards found)')
            return
        files = [files[shard_id]]
    elif max_shards > 0:
        files = files[:max_shards]

    output_dir.mkdir(parents=True, exist_ok=True)
    if sample_path is not None:
        sample_path.parent.mkdir(parents=True, exist_ok=True)
    if report_path is not None:
        report_path.parent.mkdir(parents=True, exist_ok=True)

    counters: Counter = Counter()
    rng = random.Random(sample_seed)
    sample_fh = sample_path.open('w', encoding='utf-8') if sample_path is not None else None

    try:
        for path in tqdm(files, desc='Institutional pipeline', unit='shard'):
            out_path = output_dir / path.name
            if out_path.exists() and not overwrite:
                counters['skipped_existing_shards'] += 1
                continue

            out_rows: list[dict] = []
            for batch in pq.ParquetFile(path).iter_batches(batch_size=batch_size):
                for row in batch.to_pylist():
                    counters['books_seen'] += 1
                    year = row.get('year')
                    if year is None or not (year_min <= year <= year_max):
                        continue

                    if not _passes_ocr_filter(row, min_ocr_score):
                        counters['books_dropped_ocr'] += 1
                        continue

                    paragraphs = _extract_clean_paragraphs(
                        row.get('text_by_page_gen') or [], counters
                    )
                    if not paragraphs:
                        counters['books_dropped_empty'] += 1
                        continue

                    out_row = _build_output_row(row, paragraphs)
                    out_rows.append(out_row)
                    counters['books_kept'] += 1
                    _maybe_write_sample(out_row, sample_fh, sample_prob, rng, counters)

            if out_rows:
                _write_parquet(out_rows, out_path)
                counters['shards_written'] += 1
            else:
                counters['empty_output_shards'] += 1
    finally:
        if sample_fh is not None:
            sample_fh.close()

    if report_path is not None:
        report_path.write_text(
            json.dumps(dict(sorted(counters.items())), indent=2), encoding='utf-8'
        )

    print('\n--- Pipeline summary ---')
    for key, value in sorted(counters.items()):
        print(f'{key}: {value:,}')
    print(f'OCR threshold: both scores >= {min_ocr_score:.2f} (0-1 scale, stored as 0-100)')
    print(f'Output written -> {output_dir}')
    if sample_path is not None:
        print(f'Sample written -> {sample_path}')
    if report_path is not None:
        print(f'Report written -> {report_path}')


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Combined OCR-filter + clean pipeline for Institutional Books.'
    )
    parser.add_argument('--input-dir', default=_DEFAULT_INPUT_DIR, type=Path,
                        help='Directory with raw processed institutional Parquet shards.')
    parser.add_argument('--output-dir', default=_DEFAULT_OUTPUT_DIR, type=Path,
                        help='Directory for final output shards.')
    parser.add_argument('--sample-path', default=None, type=Path,
                        help='Optional JSONL path for random full-book samples.')
    parser.add_argument('--report-path', default=None, type=Path,
                        help='Optional JSON report for aggregate cleaning counts.')
    parser.add_argument('--min-ocr-score', default=90, type=float,
                        metavar='SCORE',
                        help='Minimum OCR quality (0-1). Both ocr_score_src and '
                             'ocr_score_gen must meet this threshold (default: 90).')
    parser.add_argument('--sample-prob', default=1/10000, type=float, metavar='P')
    parser.add_argument('--sample-seed', default=42, type=int, metavar='N')
    parser.add_argument('--max-shards', default=0, type=int, metavar='N')
    parser.add_argument('--shard-id', default=-1, type=int, metavar='N',
                        help='Process exactly shard N (0-indexed).')
    parser.add_argument('--batch-size', default=16, type=int, metavar='N')
    parser.add_argument('--year-min', default=1700, type=int, metavar='YEAR',
                        help='Minimum publication year, inclusive (default: 1700).')
    parser.add_argument('--year-max', default=1913, type=int, metavar='YEAR',
                        help='Maximum publication year, inclusive (default: 1913).')
    parser.add_argument('--overwrite', action='store_true')
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    clean_institutional_pipeline(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        sample_path=args.sample_path,
        report_path=args.report_path,
        min_ocr_score=args.min_ocr_score,
        sample_prob=args.sample_prob,
        sample_seed=args.sample_seed,
        max_shards=args.max_shards,
        shard_id=args.shard_id,
        overwrite=args.overwrite,
        batch_size=args.batch_size,
        year_max=args.year_max,
        year_min=args.year_min
    )


if __name__ == '__main__':
    main()
