"""Clean processed Hansard shards with light normalization.

Reads Parquet shards produced by ``explore_hansard.py`` where each row is one
debate section with a ``paragraphs`` list. The cleaner:
- decodes HTML entities in titles and paragraphs
- strips editorial section markers such as ``§`` at paragraph start
- normalizes obvious punctuation-spacing artifacts
- repairs a few clear line-wrap/page-number word breaks
- writes cleaned Parquet shards plus a random sample of full cleaned sections

Usage
-----
    # Test on one shard
    uv run python scripts/clean/clean_hansard.py --max-shards 1 
    # Full cleaning
    uv run python scripts/clean/clean_hansard.py
"""

from __future__ import annotations

import argparse
import html
import json
import random
import re
from collections import Counter
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from historical_lm.stats import count_words

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_INPUT_DIR = _REPO_ROOT / 'data_test' / 'processed' / 'hansard'
_DEFAULT_OUTPUT_DIR = _REPO_ROOT / 'data_test' / 'clean' / 'hansard_clean'
_DEFAULT_SAMPLE = _REPO_ROOT / 'data_test' / 'clean' / 'hansard_clean_sample.jsonl'
_DEFAULT_REPORT = _REPO_ROOT / 'data_test' / 'clean' / 'hansard_clean_report.json'

# Regexes for Hansard-specific normalization.
# These are intentionally light-touch because the corpus is mostly clean and we
# want to normalize editorial markup rather than rewrite the debates.
_WHITESPACE_RE = re.compile(r'\s+')
_LEADING_SECTION_RE = re.compile(r'^\s*§\s*')
_SPACE_BEFORE_PUNCT_RE = re.compile(r'\s+([,.;:?!])')
_OPEN_QUOTE_SPACE_RE = re.compile(r'([(\[{])\s+')
_BRACKET_ELLIPSIS_RE = re.compile(r'\s*\[\s*\.\.\.\s*\]\s*')
_PERIOD_DASH_RE = re.compile(r'\.\s*—\s*')
_MULTI_DASH_RE = re.compile(r'\s*—\s*')
_BROKEN_WORD_WITH_PAGE_RE = re.compile(r'([A-Za-z]{2,})-\s+\d+\s+([A-Za-z]{2,})')
_BROKEN_WORD_WITH_SPACED_HYPHEN_RE = re.compile(r'([A-Za-z]{2,})\s*-\s+([A-Za-z]{2,})')
_SHORT_PARAGRAPH_MAX_WORDS = 2


def _normalise_title(title: str, counters: Counter[str]) -> str:
    """Decode entities and normalize whitespace in a title string."""
    decoded = html.unescape(title or '')
    if decoded != (title or ''):
        counters['decoded_title_entities'] += 1
    return _WHITESPACE_RE.sub(' ', decoded).strip()


def _normalise_paragraph(text: str, counters: Counter[str]) -> str:
    """Apply conservative paragraph normalization for Hansard text."""
    original = text or ''
    text = html.unescape(original)
    if text != original:
        counters['decoded_paragraph_entities'] += 1

    new_text = _LEADING_SECTION_RE.sub('', text)
    if new_text != text:
        counters['stripped_section_markers'] += 1
        text = new_text

    new_text = _BROKEN_WORD_WITH_PAGE_RE.sub(r'\1\2', text)
    if new_text != text:
        counters['fixed_page_break_words'] += 1
        text = new_text

    # After page-number repair, collapse hyphen-plus-space word breaks such as
    # ``com- mittee`` or ``mo- tion`` into a single token. This does not touch
    # lexical hyphens like ``to-morrow`` because those do not contain a space
    # after the hyphen.
    new_text = _BROKEN_WORD_WITH_SPACED_HYPHEN_RE.sub(r'\1\2', text)
    if new_text != text:
        counters['joined_hyphen_space_words'] += 1
        text = new_text

    new_text = _SPACE_BEFORE_PUNCT_RE.sub(r'\1', text)
    if new_text != text:
        counters['fixed_space_before_punct'] += 1
        text = new_text

    new_text = _OPEN_QUOTE_SPACE_RE.sub(r'\1', text)
    if new_text != text:
        counters['fixed_open_bracket_spacing'] += 1
        text = new_text

    new_text = _BRACKET_ELLIPSIS_RE.sub('', text)
    if new_text != text:
        counters['removed_bracket_ellipsis'] += 1
        text = new_text

    new_text = _PERIOD_DASH_RE.sub('. ', text)
    if new_text != text:
        counters['normalized_period_dash_boundary'] += 1
        text = new_text

    new_text = _MULTI_DASH_RE.sub(' ', text)
    if new_text != text:
        counters['removed_em_dashes'] += 1
        text = new_text

    return _WHITESPACE_RE.sub(' ', text).strip()


def _clean_paragraphs(paragraphs: list[str], counters: Counter[str]) -> list[str]:
    """Normalize one section's paragraph list and drop trivial leftovers."""
    cleaned: list[str] = []
    for para in paragraphs:
        counters['paragraphs_seen'] += 1
        text = _normalise_paragraph(str(para), counters)
        if not text:
            counters['dropped_empty_paragraph'] += 1
            continue
        if count_words(text) <= _SHORT_PARAGRAPH_MAX_WORDS and not any(ch.isalpha() for ch in text):
            counters['dropped_trivial_paragraph'] += 1
            continue
        cleaned.append(text)
    counters['kept_paragraphs'] += len(cleaned)
    return cleaned


def _build_output_row(row: dict, paragraphs: list[str], counters: Counter[str]) -> dict:
    """Build the cleaned output row for one Hansard section."""
    title = _normalise_title(str(row.get('title') or ''), counters)
    return {
        'date': row.get('date', ''),
        'year': row.get('year'),
        'month': row.get('month', ''),
        'day': row.get('day'),
        'house': row.get('house', ''),
        'title': title,
        'slug': row.get('slug', ''),
        'n_paragraphs': len(paragraphs),
        'word_count': count_words(' '.join(paragraphs)),
        'paragraphs': paragraphs,
    }


def _write_parquet(rows: list[dict], path: Path) -> None:
    """Write one cleaned parquet shard."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path, compression='zstd')


def _maybe_write_sample(
    row: dict,
    sample_fh,
    sample_prob: float,
    rng: random.Random,
    counters: Counter[str],
) -> None:
    """Write one full cleaned section to the sample file with small probability."""
    if sample_fh is None or sample_prob <= 0:
        return
    if rng.random() >= sample_prob:
        return
    sample_fh.write(json.dumps(row, ensure_ascii=False) + '\n')
    counters['sampled_sections'] += 1


def clean_hansard(
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
    """Clean Hansard processed shards into lightly normalized output."""
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
        for path in tqdm(files, desc='Hansard clean shards', unit='shard'):
            out_path = output_dir / path.name
            if out_path.exists() and not overwrite:
                counters['skipped_existing_shards'] += 1
                continue

            out_rows: list[dict] = []
            pf = pq.ParquetFile(path)
            for batch in pf.iter_batches(batch_size=batch_size):
                for row in batch.to_pylist():
                    counters['sections_seen'] += 1
                    paragraphs = _clean_paragraphs(row.get('paragraphs') or [], counters)
                    if not paragraphs:
                        counters['sections_dropped_empty'] += 1
                        continue

                    out_row = _build_output_row(row, paragraphs, counters)
                    out_rows.append(out_row)
                    counters['sections_kept'] += 1
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
        description='Clean processed Hansard shards with light normalization.'
    )
    parser.add_argument('--input-dir', default=_DEFAULT_INPUT_DIR, type=Path, help='Directory containing processed Hansard parquet shards.')
    parser.add_argument('--output-dir', default=_DEFAULT_OUTPUT_DIR, type=Path, help='Directory for cleaned output shards.')
    parser.add_argument('--sample-path', default=_DEFAULT_SAMPLE, type=Path, help='Optional JSONL path for random full-section samples.')
    parser.add_argument('--report-path', default=_DEFAULT_REPORT, type=Path, help='Optional JSON report for aggregate cleaning counts.')
    parser.add_argument('--sample-prob', default=5 / 10000, type=float, metavar='P', help='Probability of saving a cleaned section to the sample file.')
    parser.add_argument('--sample-seed', default=42, type=int, metavar='N', help='Random seed for reproducible sampling.')
    parser.add_argument('--max-shards', default=0, type=int, metavar='N', help='Process at most N input shards (0 = all).')
    parser.add_argument('--batch-size', default=128, type=int, metavar='N', help='Parquet batch size per read step.')
    parser.add_argument('--overwrite', action='store_true', help='Overwrite output shards if they already exist.')
    return parser


def main() -> None:
    """Entry point for the Hansard cleaner."""
    args = _build_parser().parse_args()
    clean_hansard(
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
