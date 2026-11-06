"""Clean processed Lampeter Corpus shards with light structural filtering.

Reads Parquet shards where each row is one pamphlet with a ``paragraphs`` list.
The cleaner:
- drops paragraphs with ≤2 words (too short for LM training)
- drops Latin parliamentary date formulas
- drops royal cipher lines (GEORGIUS R., etc.)
- drops standalone "God save the King" proclamation stubs
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
_DEFAULT_INPUT_DIR = _REPO_ROOT / 'data' / 'processed' / 'lampeter'
_DEFAULT_OUTPUT_DIR = _REPO_ROOT / 'data' / 'clean' / 'lampeter_clean'
_DEFAULT_SAMPLE = _REPO_ROOT / 'data' / 'clean' / 'lampeter_clean_sample.jsonl'
_DEFAULT_REPORT = _REPO_ROOT / 'data' / 'clean' / 'lampeter_clean_report.json'

_WHITESPACE_RE = re.compile(r'\s+')

# Latin parliamentary date: "Die Jovis 9 Februarii, 1715."
_LATIN_DATE_RE = re.compile(r'^Die [A-Z][a-z]+', re.IGNORECASE)

# Royal cipher: "GEORGIUS R.", "CAROLUS R.", etc.
_ROYAL_CIPHER_RE = re.compile(
    r'^(?:GEORGIUS|CAROLUS|GULIELMUS|ANNA|JACOBUS|ELIZABETHA|VICTORIA)\s+R\.?$',
    re.IGNORECASE,
)

# Proclamation stub.
_GOD_SAVE_RE = re.compile(r'^God save the (?:King|Queen)\.?$', re.IGNORECASE)

_MIN_WORDS = 3


def _drop_reason(text: str) -> str | None:
    words = count_words(text)
    if not text:
        return 'empty'
    if _LATIN_DATE_RE.match(text):
        return 'latin_date'
    if _ROYAL_CIPHER_RE.fullmatch(text):
        return 'royal_cipher'
    if _GOD_SAVE_RE.fullmatch(text):
        return 'god_save_stub'
    if words < _MIN_WORDS:
        return 'too_short'
    return None


def _clean_paragraphs(paragraphs: list[str], counters: Counter) -> list[str]:
    kept: list[str] = []
    for para in paragraphs:
        counters['paragraphs_seen'] += 1
        text = _WHITESPACE_RE.sub(' ', str(para or '')).strip()
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


def clean_lampeter(
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
    """Clean processed Lampeter Corpus shards."""
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
        for path in tqdm(files, desc='Lampeter clean shards', unit='shard'):
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
        description='Clean processed Lampeter Corpus shards.'
    )
    parser.add_argument('--input-dir', default=_DEFAULT_INPUT_DIR, type=Path)
    parser.add_argument('--output-dir', default=_DEFAULT_OUTPUT_DIR, type=Path)
    parser.add_argument('--sample-path', default=_DEFAULT_SAMPLE, type=Path)
    parser.add_argument('--report-path', default=_DEFAULT_REPORT, type=Path)
    parser.add_argument('--sample-prob', default=5 / 100, type=float, metavar='P')
    parser.add_argument('--sample-seed', default=42, type=int, metavar='N')
    parser.add_argument('--max-shards', default=0, type=int, metavar='N')
    parser.add_argument('--batch-size', default=128, type=int, metavar='N')
    parser.add_argument('--overwrite', action='store_true')
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    clean_lampeter(
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
