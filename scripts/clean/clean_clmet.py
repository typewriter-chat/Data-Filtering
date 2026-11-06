"""Clean processed CLMET shards by removing structural layout fragments.

Reads Parquet shards where each row is one text with a ``paragraphs`` list.
The cleaner:
- strips inline ``} N {`` footnote-number artifacts from paragraph text
- drops section headers (SERMON / CHAP / PART / SECTION / BOOK + numeral)
- drops letter chapter headers (LETTER I, II, ...)
- drops short all-caps stubs (titles, FINIS, ERRATA, table headers, salutations)
- drops asterisk separator lines
- drops bylines, scripture reference headers, publisher imprints
- drops letter salutation stubs
- writes cleaned Parquet shards plus a random sample of full cleaned texts
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
_DEFAULT_INPUT_DIR = _REPO_ROOT / 'data' / 'processed' / 'clmet'
_DEFAULT_OUTPUT_DIR = _REPO_ROOT / 'data' / 'clean' / 'clmet_clean'
_DEFAULT_SAMPLE = _REPO_ROOT / 'data' / 'clean' / 'clmet_clean_sample.jsonl'
_DEFAULT_REPORT = _REPO_ROOT / 'data' / 'clean' / 'clmet_clean_report.json'

_WHITESPACE_RE = re.compile(r'\s+')

# Inline } N { footnote artifacts.
_INLINE_FOOTNOTE_RE = re.compile(r'\s*\}\s*\d+\s*\{\s*')

# Section headers: SERMON / CHAP(TER) / SECTION / PART / BOOK + numeral.
_SECTION_HEADER_RE = re.compile(
    r'^(?:SERMON|CHAP(?:TER)?\.?|SECTION|PART|BOOK)\s+[IVXLCDM0-9]',
    re.IGNORECASE,
)
_SECTION_HEADER_MAX_WORDS = 20

# Letter chapter headers: LETTER I, II, ...
_LETTER_HEADER_RE = re.compile(r'^LETTER\s+[IVXLCDM]+', re.IGNORECASE)
_LETTER_HEADER_MAX_WORDS = 6

# Short all-caps stubs: entire paragraph is uppercase + spaces/punctuation.
_SHORT_ALL_CAPS_RE = re.compile(r"^[A-Z][A-Z\s\.\-,';:&()]+$")
_SHORT_ALL_CAPS_MAX_WORDS = 8

# Asterisk separator lines.
_ASTERISK_SEP_RE = re.compile(r'^[\*\s]+$')

# Bylines: "By <CapitalName> ..."
_BYLINE_RE = re.compile(r'^By [A-Z]', re.IGNORECASE)
_BYLINE_MAX_WORDS = 10

# Scripture reference headers (book name + chapter/verse, standalone).
_SCRIPTURE_RE = re.compile(
    r'^(?:Romans?|Matthew|James|Numbers?|Rom\.|John|Luke|Mark|Acts|Hebrews?'
    r'|Psalms?|Proverbs?|Corinthians?|Cor\.|Philippians?|Galatians?'
    r'|Colossians?|Thessalonians?|Timothy|Titus|Genesis|Exodus|Deuteronomy'
    r'|Isaiah|Jeremiah|Ezekiel|Daniel|Hosea|Joel|Amos|Obadiah|Jonah|Micah'
    r'|Nahum|Habakkuk|Zephaniah|Haggai|Zechariah|Malachi|Revelation|Rev\.)\s+',
    re.IGNORECASE,
)
_SCRIPTURE_MAX_WORDS = 8

# Publisher / printer imprints.
_CITY_IMPRINT_RE = re.compile(
    r'^(?:LONDON|DUBLIN|EDINBURGH|OXFORD|CAMBRIDGE|GLASGOW|BRISTOL'
    r'|BATH|YORK|LIVERPOOL|MANCHESTER)\s*:',
    re.IGNORECASE,
)
_PRINTED_FOR_RE = re.compile(r'^Printed (?:for|by)\b', re.IGNORECASE)
_IMPRINT_MAX_WORDS = 30

# Letter salutation stubs (mixed-case variants not caught by all-caps rule).
_SALUTATION_RE = re.compile(
    r'^(?:My Lord|Dear Sir|Dear Madam|Dear Friend|Dear Brother|Dear Sister'
    r'|Gentlemen)\s*[,.]?$',
    re.IGNORECASE,
)

_MIN_PARAGRAPH_WORDS = 3


def _normalize(text: str, counters: Counter) -> str:
    """Strip inline footnote artifacts and normalize whitespace."""
    cleaned, n = _INLINE_FOOTNOTE_RE.subn(' ', text)
    if n:
        counters['stripped_inline_footnotes'] += n
    return _WHITESPACE_RE.sub(' ', cleaned).strip()


def _drop_reason(text: str) -> str | None:
    """Return a drop label for structural fragments, or None to keep."""
    words = count_words(text)

    if not text:
        return 'empty'

    if _ASTERISK_SEP_RE.fullmatch(text):
        return 'asterisk_separator'

    if words <= _SECTION_HEADER_MAX_WORDS and _SECTION_HEADER_RE.match(text):
        return 'section_header'

    if words <= _LETTER_HEADER_MAX_WORDS and _LETTER_HEADER_RE.match(text):
        return 'letter_header'

    if words <= _SHORT_ALL_CAPS_MAX_WORDS and _SHORT_ALL_CAPS_RE.fullmatch(text):
        return 'short_all_caps'

    if words <= _BYLINE_MAX_WORDS and _BYLINE_RE.match(text):
        return 'byline'

    if words <= _SCRIPTURE_MAX_WORDS and _SCRIPTURE_RE.match(text):
        return 'scripture_ref'

    if words <= _IMPRINT_MAX_WORDS and _CITY_IMPRINT_RE.match(text):
        return 'city_imprint'

    if words <= _IMPRINT_MAX_WORDS and _PRINTED_FOR_RE.match(text):
        return 'printed_for'

    if _SALUTATION_RE.fullmatch(text):
        return 'letter_salutation'

    if words < _MIN_PARAGRAPH_WORDS:
        return 'too_short'

    return None


def _clean_paragraphs(paragraphs: list[str], counters: Counter) -> list[str]:
    """Clean one text's paragraph list."""
    kept: list[str] = []
    for para in paragraphs:
        counters['paragraphs_seen'] += 1
        text = _normalize(str(para or ''), counters)
        reason = _drop_reason(text)
        if reason is not None:
            counters[f'dropped_{reason}'] += 1
            continue
        kept.append(text)
    counters['kept_paragraphs'] += len(kept)
    return kept


def _build_output_row(row: dict, paragraphs: list[str]) -> dict:
    meta = {k: v for k, v in row.items() if k != 'paragraphs'}
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
    counters['sampled_texts'] += 1


def clean_clmet(
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
    """Clean processed CLMET shards."""
    files = sorted(input_dir.glob('part-*.parquet'))
    if max_shards > 0:
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
        for path in tqdm(files, desc='CLMET clean shards', unit='shard'):
            out_path = output_dir / path.name
            if out_path.exists() and not overwrite:
                counters['skipped_existing_shards'] += 1
                continue

            out_rows: list[dict] = []
            for batch in pq.ParquetFile(path).iter_batches(batch_size=batch_size):
                for row in batch.to_pylist():
                    counters['texts_seen'] += 1
                    paragraphs = _clean_paragraphs(row.get('paragraphs') or [], counters)
                    if not paragraphs:
                        counters['texts_dropped_empty'] += 1
                        continue
                    out_row = _build_output_row(row, paragraphs)
                    out_rows.append(out_row)
                    counters['texts_kept'] += 1
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
        description='Clean processed CLMET shards by removing structural layout fragments.'
    )
    parser.add_argument('--input-dir', default=_DEFAULT_INPUT_DIR, type=Path)
    parser.add_argument('--output-dir', default=_DEFAULT_OUTPUT_DIR, type=Path)
    parser.add_argument('--sample-path', default=_DEFAULT_SAMPLE, type=Path)
    parser.add_argument('--report-path', default=_DEFAULT_REPORT, type=Path)
    parser.add_argument('--sample-prob', default=5 / 10000, type=float, metavar='P')
    parser.add_argument('--sample-seed', default=42, type=int, metavar='N')
    parser.add_argument('--max-shards', default=0, type=int, metavar='N')
    parser.add_argument('--batch-size', default=128, type=int, metavar='N')
    parser.add_argument('--overwrite', action='store_true')
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    clean_clmet(
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
