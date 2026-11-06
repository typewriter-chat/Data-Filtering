"""Clean processed Royal Society shards with light OCR-aware normalization.

Reads Parquet shards where each row is one article with a ``paragraphs`` list.
The cleaner:
- drops obvious front-matter or figure-label fragments
- removes article-title duplicates already stored in metadata
- merges continuation fragments that begin with closing brackets/parentheses
- normalizes quote/backtick and spacing artifacts conservatively
- writes cleaned Parquet shards plus a random sample of full cleaned articles

uv run python scripts/clean/clean_royal_society.py --max-shards 1
"""

from __future__ import annotations

import argparse
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
_DEFAULT_INPUT_DIR = _REPO_ROOT / 'data_test' / 'processed' / 'royal_society'
_DEFAULT_OUTPUT_DIR = _REPO_ROOT / 'data_test' / 'clean' / 'royal_society_clean'
_DEFAULT_SAMPLE = _REPO_ROOT / 'data_test' / 'clean' / 'royal_society_clean_sample.jsonl'
_DEFAULT_REPORT = _REPO_ROOT / 'data_test' / 'clean' / 'royal_society_clean_report.json'

# Paragraph-shape heuristics for short fragments and front matter.
_WHITESPACE_RE = re.compile(r'\s+')
_SPACE_BEFORE_PUNCT_RE = re.compile(r'\s+([,.;:?!])')
_ROMAN_HEADING_RE = re.compile(r'^[IVXLCDM]+\.?$', re.IGNORECASE)
_YEAR_ONLY_RE = re.compile(r'^\d{4}\.?$')
_DATE_LINE_RE = re.compile(
    r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|'
    r'January|February|March|April|June|July|August|September|October|November|December)\b',
    re.IGNORECASE,
)
_FIGURE_LABEL_RE = re.compile(r'\b(?:Fig|Tab|Plate|Philos[:=]?\s*Transact)\b', re.IGNORECASE)
_SYMBOL_HEAVY_RE = re.compile(r'[~*=]|[A-Z]{1,3}\s*[~=]\s*\d')
_CLOSING_FRAGMENT_RE = re.compile(r'^[\]\)]\s*')
_BACKTICK_START_RE = re.compile(r'^`+\s*')
_BYLINE_RE = re.compile(r'^(?:By|Communicated by|To the Honourable)\b', re.IGNORECASE)
# Bare Arabic numeral section markers: '1.' '2.' etc.
_ARABIC_NUMERAL_RE = re.compile(r'^\d{1,2}\.$')
# Physical book format annotations found in meeting-minutes records.
_BOOK_FORMAT_RE = re.compile(r'^\d*(?:8vo|4to|fol|folio|12mo|16mo)\.?$', re.IGNORECASE)
# Abstract/meeting-minutes structural fragments.
_ABSTRACT_FRAGMENT_RE = re.compile(r'^\(?Abstract\.?\)?$|\)\s*\(Abstract', re.IGNORECASE)
# Meeting-minutes administrative boilerplate.
_MEETING_BOILERPLATE_RE = re.compile(
    r'^(?:Transactions\.?|The Presents received\b|The following Papers were read\b'
    r'|THE PRESIDENT in the Chair\b|Presents\.\s*$)',
    re.IGNORECASE,
)
# Standalone section-header stubs.
_SECTION_HEADER_RE = re.compile(
    r'^(?:Introduction|Conclusion|Summary|Abstract|References|Appendix'
    r'|Note|Notes|Remarks|Preface|Postscript)\.?$',
    re.IGNORECASE,
)
# Entire paragraph is only closing punctuation/brackets.
_LONE_BRACKET_RE = re.compile(r'^[\])\}]+\.?$')
# Short bibliographic citation fragments: 'p. 15.', 'Vol. I.', 'Mus. R. S. p. 4.'
_CITATION_RE = re.compile(r'\b(?:vol|pp?|ibid|op\.?\s*cit|loc\.?\s*cit|mus|no)\b\.', re.IGNORECASE)
_CITATION_MAX_WORDS = 8
# Letter sign-off formulas.
_SIGNOFF_RE = re.compile(
    r'^I (?:am|remain),|'
    r'\b(?:humble|obedient|dutiful|affectionate|faithful)\s+(?:servant|friend)\b',
    re.IGNORECASE,
)
_SIGNOFF_MAX_WORDS = 20
# Horizontal rule / dot-leader separators.
_HORIZ_RULE_RE = re.compile(r'^[.\-_=]{4,}$')
_MIN_PARAGRAPH_WORDS = 10
_MIN_MERGED_LEN = 100
_SHORT_SYMBOL_WORDS = 20
_SHORT_FRONT_MATTER_WORDS = 12


def _normalise_text(text: str, counters: Counter[str]) -> str:
    """Apply light normalization without modernizing genuine historical prose."""
    original = text
    text = text.replace('` `', '" ')
    text = _BACKTICK_START_RE.sub('" ', text)
    if text != original:
        counters['normalized_backtick_quotes'] += 1
        original = text

    text = _SPACE_BEFORE_PUNCT_RE.sub(r'\1', text)
    text = _WHITESPACE_RE.sub(' ', text).strip()
    return text


def _normalized_cmp(text: str) -> str:
    """Return a coarse normalization for duplicate-title comparison."""
    return re.sub(r'[^a-z0-9]+', ' ', text.lower()).strip()


def _drop_reason(text: str, title: str) -> str | None:
    """Classify short Royal Society fragments that should be removed."""
    words = count_words(text)

    if not text:
        return 'empty'

    if _ROMAN_HEADING_RE.fullmatch(text):
        return 'roman_heading'

    if _ARABIC_NUMERAL_RE.fullmatch(text):
        return 'arabic_numeral'

    if _YEAR_ONLY_RE.fullmatch(text):
        return 'year_only'

    if _HORIZ_RULE_RE.fullmatch(text):
        return 'horiz_rule'

    if _LONE_BRACKET_RE.fullmatch(text):
        return 'lone_bracket'

    if _BOOK_FORMAT_RE.fullmatch(text):
        return 'book_format'

    if _ABSTRACT_FRAGMENT_RE.match(text):
        return 'abstract_fragment'

    if _MEETING_BOILERPLATE_RE.match(text):
        return 'meeting_boilerplate'

    if _SECTION_HEADER_RE.fullmatch(text):
        return 'section_header'

    if _normalized_cmp(text) and _normalized_cmp(text) == _normalized_cmp(title):
        return 'duplicate_title'

    if (
        words <= _SHORT_FRONT_MATTER_WORDS
        and _DATE_LINE_RE.search(text)
        and not re.search(r'[.!?].+[.!?]', text)
    ):
        return 'dateline'

    if words <= _SHORT_FRONT_MATTER_WORDS and _BYLINE_RE.search(text):
        return 'byline'

    if words <= _SIGNOFF_MAX_WORDS and _SIGNOFF_RE.search(text):
        return 'letter_signoff'

    if words <= _CITATION_MAX_WORDS and _CITATION_RE.search(text):
        return 'citation_fragment'

    if (
        words <= _SHORT_SYMBOL_WORDS
        and (_FIGURE_LABEL_RE.search(text) or _SYMBOL_HEAVY_RE.search(text))
    ):
        return 'figure_formula'

    if words < _MIN_PARAGRAPH_WORDS and not re.search(r'[a-z]{3,}', text, re.IGNORECASE):
        return 'tiny_fragment'

    return None


def _should_merge(prev_para: str, para: str) -> bool:
    """Return True when ``para`` is a continuation of the previous paragraph."""
    if not prev_para or not para:
        return False
    if _CLOSING_FRAGMENT_RE.match(para):
        return True
    if para[:1].islower() and count_words(para) <= 12:
        return True
    return False


def _merge_paragraphs(paragraphs: list[str], counters: Counter[str]) -> list[str]:
    """Merge bracket-led continuation fragments into the previous paragraph."""
    merged: list[str] = []
    for para in paragraphs:
        if merged and _should_merge(merged[-1], para):
            if _CLOSING_FRAGMENT_RE.match(para):
                merged[-1] = f'{merged[-1]}{para}'
            else:
                merged[-1] = f'{merged[-1]} {para}'
            counters['merged_paragraphs'] += 1
            continue
        merged.append(para)

    return [p for p in merged if len(p) > _MIN_MERGED_LEN]


def _clean_paragraphs(paragraphs: list[str], title: str, counters: Counter[str]) -> list[str]:
    """Normalize and filter one article's paragraph list."""
    kept: list[str] = []
    for para in paragraphs:
        counters['paragraphs_seen'] += 1
        text = _normalise_text(str(para or ''), counters)
        reason = _drop_reason(text, title)
        if reason is not None:
            counters[f'dropped_{reason}'] += 1
            continue
        kept.append(text)

    kept = _merge_paragraphs(kept, counters)
    counters['kept_paragraphs'] += len(kept)
    return kept


def _build_output_row(row: dict, paragraphs: list[str]) -> dict:
    """Build the cleaned output row for one article."""
    meta = {key: value for key, value in row.items() if key != 'paragraphs'}
    meta['n_sentences'] = len(paragraphs)
    meta['word_count'] = count_words(' '.join(paragraphs))
    meta['paragraphs'] = paragraphs
    return meta


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
    """Write one full cleaned article to the sample file with small probability."""
    if sample_fh is None or sample_prob <= 0:
        return
    if rng.random() >= sample_prob:
        return
    sample_fh.write(json.dumps(row, ensure_ascii=False) + '\n')
    counters['sampled_articles'] += 1


def clean_royal_society(
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
    """Clean processed Royal Society shards into lightly normalized output."""
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
        for path in tqdm(files, desc='Royal Society clean shards', unit='shard'):
            out_path = output_dir / path.name
            if out_path.exists() and not overwrite:
                counters['skipped_existing_shards'] += 1
                continue

            out_rows: list[dict] = []
            pf = pq.ParquetFile(path)
            for batch in pf.iter_batches(batch_size=batch_size):
                for row in batch.to_pylist():
                    counters['articles_seen'] += 1
                    title = str(row.get('title') or '')
                    paragraphs = _clean_paragraphs(row.get('paragraphs') or [], title, counters)
                    if not paragraphs:
                        counters['articles_dropped_empty'] += 1
                        continue
                    out_row = _build_output_row(row, paragraphs)
                    out_rows.append(out_row)
                    counters['articles_kept'] += 1
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
        description='Clean processed Royal Society shards with light normalization.'
    )
    parser.add_argument('--input-dir', default=_DEFAULT_INPUT_DIR, type=Path, help='Directory containing processed Royal Society parquet shards.')
    parser.add_argument('--output-dir', default=_DEFAULT_OUTPUT_DIR, type=Path, help='Directory for cleaned output shards.')
    parser.add_argument('--sample-path', default=_DEFAULT_SAMPLE, type=Path, help='Optional JSONL path for random full-article samples.')
    parser.add_argument('--report-path', default=_DEFAULT_REPORT, type=Path, help='Optional JSON report for aggregate cleaning counts.')
    parser.add_argument('--sample-prob', default=5 / 10000, type=float, metavar='P', help='Probability of saving a cleaned article to the sample file.')
    parser.add_argument('--sample-seed', default=42, type=int, metavar='N', help='Random seed for reproducible sampling.')
    parser.add_argument('--max-shards', default=0, type=int, metavar='N', help='Process at most N input shards (0 = all).')
    parser.add_argument('--batch-size', default=128, type=int, metavar='N', help='Parquet batch size per read step.')
    parser.add_argument('--overwrite', action='store_true', help='Overwrite output shards if they already exist.')
    return parser


def main() -> None:
    """Entry point for the Royal Society cleaner."""
    args = _build_parser().parse_args()
    clean_royal_society(
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
