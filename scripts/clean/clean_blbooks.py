"""Clean processed BL Books shards into paragraph-level text.

Reads Parquet shards produced by ``explore_blbooks.py`` where each row is one
book with a ``text_by_page`` list. The cleaner:
- drops pages that are only numbers or tiny OCR scraps
- strips page-number and running-head prefixes at page starts
- normalises obvious OCR word-split and hyphenation artifacts
- splits cleaned pages into paragraph candidates
- writes cleaned Parquet shards with one row per book and a ``paragraphs`` field

Usage
-----
    # Test on one shard
    uv run python scripts/clean/clean_blbooks.py --max-shards 1

    # Full cleaning
    uv run python scripts/clean/clean_blbooks.py
"""

from __future__ import annotations

import argparse
import json
import random
import re
from collections import Counter
from pathlib import Path
from typing import IO

import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from historical_lm.stats import count_words

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_INPUT_DIR = _REPO_ROOT / 'data' / 'processed' / 'blbooks'
_DEFAULT_OUTPUT_DIR = _REPO_ROOT / 'data' / 'clean' / 'blbooks_clean'
_DEFAULT_SAMPLE = _REPO_ROOT / 'data' / 'clean' / 'blbooks_clean_sample.jsonl'
_DEFAULT_REPORT = _REPO_ROOT / 'data' / 'clean' / 'blbooks_clean_report.json'

# Regexes for detecting page-level layout artifacts.
# BL Books is page-oriented OCR, so many cleaning decisions happen before any
# paragraph splitting: page numbers, running heads, and tiny header-only pages.
_PAGE_NUMBER_ONLY_RE = re.compile(r'^\s*\d{1,4}\s*$')
_PARA_SPLIT_RE = re.compile(r'\n\s*\n+')
_WHITESPACE_RE = re.compile(r'\s+')
_LEADING_NUMBER_RE = re.compile(r'^\s*\d{1,4}\s+')
_LEADING_PAGE_LABEL_RE = re.compile(r'^\s*\d{1,4}\s+Page\s+\d+\.?\s*', re.IGNORECASE)
_LEADING_HEADER_WITH_NUMBER_RE = re.compile(
    r'^\s*(?:'
    r'NOTES?|APPENDIX|PART\s+[IVXLC]+|BOOK\s+[IVXLC]+|'
    r'POEMS?\.?|MISCELLANEOUS\s+PIECES\.?(?:\s+[IVXLC]+\.?)?|'
    r'LORD\s+[A-Z\' -]+|[A-Z][A-Z\'&., -]{3,60}'
    r')\s+\d{1,4}\s+'
)
_PUNCT_ONLY_RE = re.compile(r'^[^\w]+$')
# Inline page-reference citations embedded mid-paragraph by scholarly commentary
# books: "... Page 277. LuciNA If ever ..." → strip "Page 277. ".
# Capital-P Page followed by a 1–4 digit number and a period is distinctive enough
# that it does not occur as normal prose.
_INLINE_PAGE_REF_RE = re.compile(r'\bPage\s+\d{1,4}\.\s+', re.IGNORECASE)
_LOWER_START_RE = re.compile(r'^[a-z]')
_END_PUNCT_RE = re.compile(r'[.!?:"\')\]]$')
# After stripping a leading page number, the remaining all-caps running head (e.g.
# "NOTES. ", "HISTORY OF ENGLAND. ") can be removed when it is followed by lowercase
# prose.  The period (.) is intentionally excluded from the character class so the
# pattern does not greedily match across heading segments like "MISCELLANEOUS PIECES. II.".
_REMAINING_HEAD_RE = re.compile(r'^([A-Z][A-Z\'&,\- ]{2,60})\.\s+(?=[a-z])')
_HEADER_ONLY_RE = re.compile(
    r'^(?:'
    r'NOTES?|APPENDIX|PART\s+[IVXLC]+|BOOK\s+[IVXLC]+|'
    r'POEMS?\.?|TO\s+MY\s+[A-Z][a-z]+|'
    r'CATALOGUE(?:\s+OF.*)?'
    r')$',
    re.IGNORECASE,
)

# Lexical hints for conservative word-join repair.
# We only join split words when the left token looks like a common OCR-broken prefix and is not itself a normal standalone function word.
_WORD_SPLIT_PREFIXES = (
    'ad', 'ap', 'com', 'con', 'de', 'dis', 'em', 'ex', 'im', 'in', 'inter',
    'mis', 'pre', 'pro', 're', 'sub', 'sur', 'theo', 'trans', 'un'
)
_LEFT_STOPWORDS = {
    'a', 'an', 'and', 'as', 'at', 'be', 'by', 'for', 'he', 'her', 'his', 'i',
    'in', 'is', 'it', 'me', 'my', 'nor', 'of', 'on', 'or', 'so', 'the', 'their',
    'them', 'they', 'thou', 'to', 'we', 'who', 'with', 'you', 'your'
}

# Length thresholds for dropping trivial pages and keeping paragraph candidates.
# These keep the first pass conservative: we remove only obviously empty or
# fragmentary material and preserve most real page content.
_MIN_PAGE_CHARS = 8
_MIN_PAGE_WORDS = 2
_HEADER_MAX_WORDS = 8
_OCR_SPLIT_LEFT_MAX = 5
_OCR_SPLIT_RIGHT_MIN = 3
_MIN_PARAGRAPH_WORDS = 3
# Hard minimum paragraph length applied after the merge pass.  Drops chapter
# titles, copyright notices, and spaced-letter OCR headings (≈10 % of kept blocks).
# Applied post-merge so short continuation fragments are rejoined before filtering.
_MIN_PARAGRAPH_CHARS = 100
_MERGE_SHORT_PARAGRAPH_MAX_WORDS = 8


def _strip_prefixes(text: str, counters: Counter[str]) -> str:
    """Strip common page-start page numbers and running heads.

    Args:
        text: Raw page text.
        counters: Mutable counter dict updated with strip event counts.

    Returns:
        Page text with leading page-number and running-head prefixes removed.
    """
    original = text
    text = _LEADING_PAGE_LABEL_RE.sub('', text)
    if text != original:
        counters['stripped_page_label_prefix'] += 1
        original = text

    text = _LEADING_HEADER_WITH_NUMBER_RE.sub('', text)
    if text != original:
        counters['stripped_header_number_prefix'] += 1
        original = text

    before_number = text
    text = _LEADING_NUMBER_RE.sub('', text)
    if text != before_number:
        counters['stripped_leading_number'] += 1
        # Secondary pass: when the page began with "NUMBER RUNNING_HEAD. prose...",
        # stripping the number leaves "RUNNING_HEAD. prose...".  Remove the remaining
        # all-caps heading before the first lowercase-starting sentence.
        stripped = text.strip()
        normalised = _REMAINING_HEAD_RE.sub('', stripped)
        if normalised != stripped:
            counters['stripped_remaining_running_head'] += 1
        text = normalised

    return text.strip()


def _join_split_words(text: str, counters: Counter[str]) -> str:
    """Join conservative OCR split-word patterns.

    Args:
        text: Normalised page text possibly containing OCR-broken words.
        counters: Mutable counter dict incremented for each joined pair.

    Returns:
        Text with qualifying split-word pairs rejoined.
    """
    tokens = text.split()
    if len(tokens) < 2:
        return text

    out: list[str] = []
    i = 0
    while i < len(tokens):
        if i + 1 < len(tokens):
            left = tokens[i]
            right = tokens[i + 1]
            left_core = re.sub(r'^[^A-Za-z]+|[^A-Za-z]+$', '', left)
            right_core = re.sub(r'^[^A-Za-z]+|[^A-Za-z]+$', '', right)
            if (
                left_core
                and right_core
                and len(left_core) <= _OCR_SPLIT_LEFT_MAX
                and len(right_core) >= _OCR_SPLIT_RIGHT_MIN
                and left_core.lower() not in _LEFT_STOPWORDS
                and right_core[0].islower()
                and left_core[0].isalpha()
                and any(left_core.lower().startswith(prefix) for prefix in _WORD_SPLIT_PREFIXES)
            ):
                left_prefix = left[:left.find(left_core)] if left_core in left else ''
                left_suffix = left[left.find(left_core) + len(left_core):] if left_core in left else ''
                right_prefix = right[:right.find(right_core)] if right_core in right else ''
                right_suffix = right[right.find(right_core) + len(right_core):] if right_core in right else ''
                if not left_suffix and not right_prefix:
                    out.append(f'{left_prefix}{left_core}{right_core}{right_suffix}')
                    i += 2
                    continue
        out.append(tokens[i])
        i += 1

    return ' '.join(out)


def _normalise_text(text: str, counters: Counter[str]) -> str:
    """Apply conservative page-level OCR normalization.

    Args:
        text: Raw page text after prefix stripping.
        counters: Mutable counter dict updated with normalisation event counts.

    Returns:
        Normalised text with soft hyphens resolved, inline page refs removed,
        line-break hyphens joined, and OCR split words repaired.
    """
    text = text.replace('\u00ad', '-')
    refs_removed = len(_INLINE_PAGE_REF_RE.findall(text))
    if refs_removed:
        text = _INLINE_PAGE_REF_RE.sub('', text)
        counters['stripped_inline_page_refs'] += refs_removed
    text = re.sub(r'([A-Za-z])-\s+([a-z])', r'\1\2', text)
    text = re.sub(r'\s+', ' ', text).strip()
    text = _join_split_words(text, counters)
    text = _WHITESPACE_RE.sub(' ', text).strip()
    return text


def _should_merge(prev_para: str, para: str) -> bool:
    """Return True when ``para`` is likely a continuation of ``prev_para``.

    Args:
        prev_para: The preceding paragraph candidate.
        para: The current paragraph candidate.

    Returns:
        True if ``para`` should be appended to ``prev_para``.
    """
    if not prev_para or not para:
        return False
    if prev_para.endswith('-') and _LOWER_START_RE.match(para):
        return True
    if _END_PUNCT_RE.search(prev_para):
        return False
    if _LOWER_START_RE.match(para):
        return True
    return count_words(para) <= _MERGE_SHORT_PARAGRAPH_MAX_WORDS and para[:1].islower()


def _merge_paragraphs(paragraphs: list[str]) -> tuple[list[str], int]:
    """Merge adjacent continuation fragments in a single left-to-right pass.

    Args:
        paragraphs: Ordered list of paragraph candidate strings.

    Returns:
        Tuple of (merged paragraph list, number of merges performed).
    """
    merged: list[str] = []
    merge_count = 0
    for para in paragraphs:
        if merged and _should_merge(merged[-1], para):
            if merged[-1].endswith('-') and para[:1].islower():
                merged[-1] = merged[-1][:-1] + para
            else:
                merged[-1] = f'{merged[-1]} {para}'
            merge_count += 1
            continue
        merged.append(para)
    return merged, merge_count


def _drop_page(text: str) -> str | None:
    """Return a drop reason for a page, or None when it should be kept.

    Args:
        text: Stripped page text to classify.

    Returns:
        A short reason string if the page should be dropped, else None.
    """
    if not text:
        return 'empty_page'
    if _PAGE_NUMBER_ONLY_RE.fullmatch(text):
        return 'page_number_only'
    if _PUNCT_ONLY_RE.fullmatch(text):
        return 'punctuation_only'
    if len(text) < _MIN_PAGE_CHARS:
        return 'tiny_page'
    if count_words(text) < _MIN_PAGE_WORDS:
        return 'too_short'
    if _HEADER_ONLY_RE.fullmatch(text) and count_words(text) <= _HEADER_MAX_WORDS:
        return 'header_only'
    return None


def _extract_paragraphs(pages: list[str], counters: Counter[str]) -> list[str]:
    """Turn raw OCR pages into cleaned paragraph strings.

    Three-stage pipeline:
    1. Strip page-level noise and split into paragraph candidates; keep short
       fragments that pass the word-count floor so they can be rejoined.
    2. Merge adjacent blocks that look like OCR-split continuations (hyphen-break
       or lowercase-start following a non-terminal previous block).
    3. Drop paragraphs shorter than _MIN_PARAGRAPH_CHARS characters.

    Args:
        pages: List of raw OCR page strings for one book.
        counters: Mutable counter dict updated throughout all three stages.

    Returns:
        List of cleaned paragraph strings ready for pretraining.
    """
    # Stage 1: page-level cleaning and paragraph candidate collection.
    paragraphs: list[str] = []
    for raw_page in pages:
        counters['pages_seen'] += 1
        page = (raw_page or '').strip()
        reason = _drop_page(page)
        if reason is not None:
            counters[f'dropped_{reason}'] += 1
            continue

        page = _strip_prefixes(page, counters)
        page = _normalise_text(page, counters)
        reason = _drop_page(page)
        if reason is not None:
            counters[f'dropped_{reason}'] += 1
            continue

        blocks = _PARA_SPLIT_RE.split(page)
        kept_from_page = 0
        for block in blocks:
            para = _WHITESPACE_RE.sub(' ', block).strip()
            if count_words(para) < _MIN_PARAGRAPH_WORDS:
                counters['dropped_short_paragraph'] += 1
                continue
            paragraphs.append(para)
            kept_from_page += 1
        if kept_from_page == 0:
            counters['pages_without_paragraphs'] += 1

    # # Stage 2: merge continuation fragments across paragraph and page boundaries.
    # merged, merge_count = _merge_paragraphs(paragraphs)
    # counters['merged_paragraphs'] += merge_count

    # Stage 3: minimum character-length filter applied after merging.
    final: list[str] = []
    # for para in merged:
    for para in paragraphs:
        if len(para) < _MIN_PARAGRAPH_CHARS:
            counters['dropped_too_short'] += 1
        else:
            final.append(para)
    counters['kept_paragraphs'] += len(final)
    counters['words_kept'] += count_words(' '.join(final))
    return final


def _build_output_row(row: dict, paragraphs: list[str]) -> dict:
    """Convert one BL Books row into the cleaned output schema.

    Args:
        row: Raw input row dict from the processed Parquet shard.
        paragraphs: Cleaned paragraph strings for this book.

    Returns:
        Output row dict with metadata fields and a ``paragraphs`` list.
    """
    meta = {key: value for key, value in row.items() if key != 'text_by_page'}
    meta['source_page_count'] = len(row.get('text_by_page') or [])
    meta['n_paragraphs'] = len(paragraphs)
    meta['clean_word_count'] = count_words(' '.join(paragraphs))
    meta['paragraphs'] = paragraphs
    return meta


def _write_parquet(rows: list[dict], path: Path) -> None:
    """Write cleaned rows to one Parquet shard.

    Args:
        rows: List of output row dicts to serialise.
        path: Destination Parquet file path (parent directories created).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path, compression='zstd')


def _maybe_write_sample(
    row: dict,
    sample_fh: IO[str] | None,
    sample_prob: float,
    rng: random.Random,
    counters: Counter[str],
) -> None:
    """Write one full cleaned book to the sample file with small probability.

    Args:
        row: Cleaned output row dict for one book.
        sample_fh: Open writable file handle for the sample JSONL, or None.
        sample_prob: Probability in [0, 1] of writing this book.
        rng: Random number generator for the sampling decision.
        counters: Mutable counter dict incremented when a book is sampled.
    """
    if sample_fh is None or sample_prob <= 0:
        return
    if rng.random() >= sample_prob:
        return
    sample_fh.write(json.dumps(row, ensure_ascii=False) + '\n')
    counters['sampled_books'] += 1


def clean_blbooks(
    input_dir: Path,
    output_dir: Path,
    sample_path: Path | None,
    report_path: Path | None,
    sample_prob: float,
    sample_seed: int,
    max_shards: int,
    overwrite: bool,
    batch_size: int,
) -> None:
    """Clean BL Books processed shards into paragraph-level output.

    Args:
        input_dir: Directory containing processed BL Books Parquet shards.
        output_dir: Directory for cleaned output shards.
        sample_path: Optional JSONL path for random full-book samples.
        report_path: Optional JSON path for aggregate cleaning counts.
        sample_prob: Probability of writing a cleaned book to the sample file.
        sample_seed: Random seed for reproducible sampling.
        max_shards: Maximum number of input shards to process (0 = all).
        overwrite: If True, overwrite existing output shards.
        batch_size: Parquet row batch size per read step.
    """
    files = sorted(input_dir.glob('part-*.parquet'))
    if max_shards > 0:
        files = files[:max_shards]

    output_dir.mkdir(parents=True, exist_ok=True)
    if sample_path is not None:
        sample_path.parent.mkdir(parents=True, exist_ok=True)
    if report_path is not None:
        report_path.parent.mkdir(parents=True, exist_ok=True)

    counters: Counter[str] = Counter()
    rng = random.Random(sample_seed)
    sample_fh = sample_path.open('w', encoding='utf-8') if sample_path is not None else None

    try:
        for path in tqdm(files, desc='BL Books clean shards', unit='shard'):
            out_path = output_dir / path.name
            if out_path.exists() and not overwrite:
                counters['skipped_existing_shards'] += 1
                continue

            out_rows: list[dict] = []
            pf = pq.ParquetFile(path)
            for batch in pf.iter_batches(batch_size=batch_size):
                for row in batch.to_pylist():
                    counters['books_seen'] += 1
                    paragraphs = _extract_paragraphs(row.get('text_by_page') or [], counters)
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
        report_path.write_text(json.dumps(dict(sorted(counters.items())), indent=2), encoding='utf-8')

    print('\n--- Cleaning summary ---')
    for key, value in sorted(counters.items()):
        print(f'{key}: {value:,}')
    print(f'Output written -> {output_dir}')
    if sample_path is not None:
        print(f'Sample written -> {sample_path}')
    if report_path is not None:
        print(f'Report written -> {report_path}')


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Clean processed BL Books shards into paragraph-level Parquet.'
    )
    parser.add_argument('--input-dir', default=_DEFAULT_INPUT_DIR, type=Path, help='Directory containing processed BL Books Parquet shards.')
    parser.add_argument('--output-dir', default=_DEFAULT_OUTPUT_DIR, type=Path, help='Directory for cleaned output shards.')
    parser.add_argument('--sample-path', default=_DEFAULT_SAMPLE, type=Path, help='Optional JSONL path for random full-book samples.')
    parser.add_argument('--report-path', default=_DEFAULT_REPORT, type=Path, help='Optional JSON report for aggregate cleaning counts.')
    parser.add_argument('--sample-prob', default=5 / 1000, type=float, metavar='P', help='Probability of saving a cleaned book to the sample file.')
    parser.add_argument('--sample-seed', default=42, type=int, metavar='N', help='Random seed for reproducible sampling.')
    parser.add_argument('--max-shards', default=0, type=int, metavar='N', help='Process at most N input shards (0 = all).')
    parser.add_argument('--batch-size', default=64, type=int, metavar='N', help='Parquet batch size per read step.')
    parser.add_argument('--overwrite', action='store_true', help='Overwrite output shards if they already exist.')
    return parser


def main() -> None:
    """Entry point for the BL Books cleaner."""
    args = _build_parser().parse_args()
    clean_blbooks(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        sample_path=args.sample_path,
        report_path=args.report_path,
        sample_prob=args.sample_prob,
        sample_seed=args.sample_seed,
        max_shards=args.max_shards,
        overwrite=args.overwrite,
        batch_size=args.batch_size,
    )


if __name__ == '__main__':
    main()
