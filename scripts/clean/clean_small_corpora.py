"""Minimal cleaner for CED, CLME_PROSE, and Pamphlets corpora.

These three corpora are near-clean: they contain very few structural fragments.
The cleaner applies a minimal filter:
- drops empty paragraphs
- drops paragraphs with < 3 words
- drops standalone "FINIS ." end markers

Usage:
    uv run python scripts/clean/clean_small_corpora.py --corpus ced
    uv run python scripts/clean/clean_small_corpora.py --corpus clme_prose
    uv run python scripts/clean/clean_small_corpora.py --corpus pamphlets
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from historical_lm.stats import count_words

_REPO_ROOT = Path(__file__).resolve().parents[2]

_WHITESPACE_RE = re.compile(r'\s+')
_FINIS_RE = re.compile(r'^FINIS\.?$', re.IGNORECASE)
_MIN_WORDS = 3


def _drop_reason(text: str) -> str | None:
    if not text:
        return 'empty'
    if _FINIS_RE.fullmatch(text):
        return 'finis'
    if count_words(text) < _MIN_WORDS:
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


def clean_corpus(
    corpus: str,
    input_dir: Path,
    output_dir: Path,
    report_path: Path | None,
    overwrite: bool,
    batch_size: int,
) -> None:
    """Clean one small corpus."""
    files = sorted(input_dir.glob('part-*.parquet'))
    output_dir.mkdir(parents=True, exist_ok=True)
    if report_path is not None:
        report_path.parent.mkdir(parents=True, exist_ok=True)

    counters: Counter = Counter()

    for path in tqdm(files, desc=f'{corpus} clean', unit='shard'):
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
                out_rows.append(_build_output_row(row, paragraphs))
                counters['texts_kept'] += 1

        if out_rows:
            path.parent.mkdir(parents=True, exist_ok=True)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.Table.from_pylist(out_rows), out_path, compression='zstd')
            counters['shards_written'] += 1
        else:
            counters['empty_output_shards'] += 1

    if report_path is not None:
        report_path.write_text(
            json.dumps(dict(sorted(counters.items())), indent=2), encoding='utf-8'
        )

    print(f'\n--- {corpus} cleaning summary ---')
    for key, value in sorted(counters.items()):
        print(f'{key}: {value:,}')
    print(f'Output written -> {output_dir}')
    if report_path is not None:
        print(f'Report written -> {report_path}')


_CORPORA = {
    'ced': 'ced',
    'clme_prose': 'clme_prose',
    'pamphlets': 'pamphlets',
}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Minimal cleaner for CED, CLME_PROSE, and Pamphlets corpora.'
    )
    parser.add_argument(
        '--corpus', required=True, choices=list(_CORPORA),
        help='Which corpus to clean.',
    )
    parser.add_argument('--batch-size', default=128, type=int, metavar='N')
    parser.add_argument('--overwrite', action='store_true')
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    corpus = args.corpus
    input_dir = _REPO_ROOT / 'data_test' / 'processed' / corpus
    output_dir = _REPO_ROOT / 'data_test' / 'clean' / f'{corpus}_clean'
    report_path = _REPO_ROOT / 'data_test' / 'clean' / f'{corpus}_clean_report.json'

    clean_corpus(
        corpus=corpus,
        input_dir=input_dir,
        output_dir=output_dir,
        report_path=report_path,
        overwrite=args.overwrite,
        batch_size=args.batch_size,
    )


if __name__ == '__main__':
    main()
