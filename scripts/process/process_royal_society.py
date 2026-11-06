"""Process the Royal Society Corpus open v6.0 (plain-text version).

Dataset : Royal Society Corpus open v6.0
Coverage: 1665-1996, articles from Philosophical Transactions and Proceedings.
Homepage: https://fedora.clarin-d.uni-saarland.de/rsc_open/

Files
-----
Texts    : ``<txt-dir>/Royal_Society_Corpus_open_v6.0_text_{id}.txt``
           One sentence per line.  Spaces before punctuation (from VRT
           tokenisation), HTML entities (``&amp;`` etc.) still present.

Metadata : ``<meta-tsv>`` — tab-separated, one row per article.
           ``id`` column matches the ``{id}`` portion of each text filename.

Unit of output: one row per article.  The ``paragraphs`` field is a list of
cleaned sentence strings extracted from the text file (one per line), after filtering lines shorter than ``--min-line-words`` words.

Usage
-----
    # Stats only
    uv run python scripts/process/process_royal_society.py

    # Full extraction
    uv run python scripts/process/process_royal_society.py --output-dir data_test/processed/royal_society --preview-path data_test/samples/royal_society_preview.jsonl --year-min 1700 --year-max 1938 --min-line-words 0
"""
from __future__ import annotations

import argparse
import html
import json
import re
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from historical_lm.stats import count_words, save_stats

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_TXT_DIR = (
    _REPO_ROOT / 'data' / 'raw' / 'royal_society'
    / 'Royal_Society_Corpus_open_v6.0_texts_txt'
)
_DEFAULT_META_TSV = (
    _REPO_ROOT / 'data' / 'raw' / 'royal_society'
    / 'Royal_Society_Corpus_open_v6.0_meta.tsv'
)
_DEFAULT_OUTPUT = _REPO_ROOT / 'data_test' / 'stats' / 'royal_society_stats.parquet'
_DEFAULT_SHARD_SIZE = 1000

# Spaces before punctuation left over from VRT tokenisation
_PRE_PUNCT = re.compile(r' (?=[,.:;!?\')\]])')
_POST_OPEN = re.compile(r'(?<=\() ')
_APOS_S = re.compile(r" ('s|n't)\b")


def _clean_line(line: str) -> str:
    """Decode HTML entities and remove tokenisation-artefact spaces."""
    text = html.unescape(line.strip())
    text = _PRE_PUNCT.sub('', text)
    text = _POST_OPEN.sub('', text)
    text = _APOS_S.sub(r'\1', text)
    return ' '.join(text.split())


def _load_metadata(meta_tsv: Path) -> dict[str, dict]:
    """Load the metadata TSV into a dict keyed by ``id``."""
    df = pl.read_csv(
        meta_tsv,
        separator='\t',
        infer_schema_length=0,
        truncate_ragged_lines=True,
        null_values=[''],
    )
    result: dict[str, dict] = {}
    for row in df.iter_rows(named=True):
        doc_id = (row.get('id') or '').strip()
        if doc_id:
            result[doc_id] = row
    return result


def extract_paragraphs(txt_path: Path, min_words: int) -> list[str]:
    """Read a plain-text RSC file and return cleaned non-trivial lines.

    Each line in the file is one sentence (derived from a VRT ``<s>`` block).
    Lines shorter than ``min_words`` tokens are discarded (section numbers,
    stray punctuation, etc.).

    Args:
        txt_path: Path to the plain-text article file.
        min_words: Minimum word count for a line to be kept.

    Returns:
        List of cleaned sentence strings.
    """
    try:
        raw = txt_path.read_text(encoding='utf-8', errors='replace')
    except OSError:
        return []

    paragraphs = []
    for line in raw.splitlines():
        cleaned = _clean_line(line)
        if cleaned and count_words(cleaned) >= min_words:
            paragraphs.append(cleaned)
    return paragraphs


def _flush_shard(rows: list[dict], output_dir: Path, shard_idx: int) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f'part-{shard_idx:05d}.parquet'
    pq.write_table(pa.Table.from_pylist(rows), path, compression='zstd')
    print(f'  Wrote shard {shard_idx:05d} ({len(rows)} articles) -> {path}')


def collect_stats(
    txt_dir: Path,
    meta_tsv: Path,
    year_min: int,
    year_max: int,
    min_line_words: int,
    max_texts: int,
    output_dir: Path | None,
    preview_path: Path | None,
    shard_size: int,
    preview_sentences: int,
) -> pl.DataFrame:
    """Process all RSC plain-text files, join metadata, and optionally save text.

    Args:
        txt_dir: Directory containing the plain-text ``*.txt`` files.
        meta_tsv: Path to the metadata TSV file.
        year_min: Keep only articles with year >= this value.
        year_max: Keep only articles with year <= this value.
        text_types: RSC ``type`` values to keep (empty = keep all).
        min_line_words: Minimum words per line to keep as a paragraph.
        max_texts: Stop after this many accepted articles (0 = no limit).
        output_dir: If given, write Parquet text shards here.
        preview_path: If given, write a JSONL preview file here.
        shard_size: Articles per Parquet shard.
        preview_sentences: Sentences per article in the JSONL preview.

    Returns:
        Polars DataFrame with one row per accepted article (no text).
    """
    print(f'Loading metadata from {meta_tsv} ...')
    meta_by_id = _load_metadata(meta_tsv)
    print(f'  {len(meta_by_id):,} metadata records')

    files = sorted(txt_dir.glob('*.txt'))
    if not files:
        print(f'ERROR: no .txt files found in {txt_dir}')
        return pl.DataFrame()

    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
    if preview_path is not None:
        preview_path.parent.mkdir(parents=True, exist_ok=True)

    stat_rows: list[dict] = []
    shard_buf: list[dict] = []
    shard_idx = 0
    n_no_meta = 0
    n_year_skip = 0
    n_type_skip = 0
    n_empty_skip = 0

    preview_fh = preview_path.open('w', encoding='utf-8') if preview_path is not None else None
    try:
        for txt_path in tqdm(files, desc='RSC articles', unit='file'):
            # Extract doc_id from filename: ..._text_{id}.txt
            doc_id = txt_path.stem.split('_text_', 1)[-1]
            meta = meta_by_id.get(doc_id)
            if meta is None:
                n_no_meta += 1
                continue

            year_str = (meta.get('year') or '').strip()
            try:
                year = int(year_str)
            except ValueError:
                n_year_skip += 1
                continue
            if year < year_min or year > year_max:
                n_year_skip += 1
                print(f'  Skipping {doc_id} due to year {year} out of range')
                continue

            article_type = (meta.get('type') or '').strip()

            paragraphs = extract_paragraphs(txt_path, min_line_words)
            if not paragraphs:
                n_empty_skip += 1
                print(f'  Skipping {doc_id} because no paragraphs left after filtering')
                continue

            stat_row = {
                'doc_id': doc_id,
                'title': (meta.get('title') or '').strip(),
                'year': year,
                'author': (meta.get('author') or '').strip(),
                'type': article_type,
                'journal': (meta.get('journal') or '').strip(),
                'n_sentences': len(paragraphs),
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
                        {**stat_row, 'text_preview': paragraphs[:preview_sentences]},
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
    print(f'  No metadata match : {n_no_meta:,}')
    print(f'  Year out of range : {n_year_skip:,}')
    print(f'  Type skipped      : {n_type_skip:,}')
    print(f'  Empty after filter: {n_empty_skip:,}')
    print(f'  Articles accepted : {len(stat_rows):,}')

    if output_dir is not None:
        print(f'Text saved -> {output_dir}')
    if preview_path is not None:
        print(f'Preview written -> {preview_path}')

    return pl.DataFrame(stat_rows)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Process RSC open v6.0 plain-text files into Parquet shards.'
    )
    parser.add_argument(
        '--txt-dir',
        default=_DEFAULT_TXT_DIR,
        type=Path,
        help='Directory containing the RSC plain-text *.txt files.',
    )
    parser.add_argument(
        '--meta-tsv',
        default=_DEFAULT_META_TSV,
        type=Path,
        help='Path to the RSC metadata TSV file.',
    )
    parser.add_argument(
        '--output',
        default=_DEFAULT_OUTPUT,
        type=Path,
        help='Output Parquet file for per-article statistics.',
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
        default=1700,
        type=int,
        help='Minimum publication year (default: 1700).',
    )
    parser.add_argument(
        '--year-max',
        default=1938,
        type=int,
        help='Maximum publication year inclusive (default: 1920).',
    )
    parser.add_argument(
        '--min-line-words',
        default=3,
        type=int,
        help='Minimum words per line to keep as a sentence (default: 5).',
    )
    parser.add_argument(
        '--shard-size',
        default=_DEFAULT_SHARD_SIZE,
        type=int,
        help=f'Articles per Parquet shard (default: {_DEFAULT_SHARD_SIZE}).',
    )
    parser.add_argument(
        '--preview-sentences',
        default=20,
        type=int,
        help='Sentences per article in the JSONL preview (default: 5).',
    )
    parser.add_argument(
        '--max-texts',
        default=0,
        type=int,
        help='Stop after this many accepted articles (0 = no limit).',
    )
    return parser


def main() -> None:
    """Entry point for Royal Society Corpus processing."""
    args = _build_parser().parse_args()

    if not args.txt_dir.is_dir():
        print(f'ERROR: directory not found: {args.txt_dir}')
        return
    if not args.meta_tsv.is_file():
        print(f'ERROR: metadata file not found: {args.meta_tsv}')
        return

    print(f'Processing {args.txt_dir} ...')
    df = collect_stats(
        args.txt_dir,
        args.meta_tsv,
        args.year_min,
        args.year_max,
        args.min_line_words,
        args.max_texts,
        args.output_dir,
        args.preview_path,
        args.shard_size,
        args.preview_sentences,
    )

    save_stats(df, args.output)
    print(f'\nSaved {len(df):,} article records -> {args.output}')

    if df.is_empty():
        return

    print('\n--- Summary ---')
    print(f'Total articles  : {len(df):,}')
    print(f'Total sentences : {df["n_sentences"].sum():,}')
    print(f'Total words     : {df["word_count"].sum():,}')
    year_col = df['year'].drop_nulls()
    if year_col.len() > 0:
        print(f'Year range      : {year_col.min()} - {year_col.max()}')
    print(
        '\nBy decade:\n'
        + str(
            df.group_by((pl.col('year') // 10 * 10).alias('decade'))
            .agg(pl.len().alias('n_articles'), pl.col('word_count').sum())
            .sort('decade')
        )
    )
    print(
        '\nBy type:\n'
        + str(
            df.group_by('type')
            .agg(pl.len().alias('n_articles'))
            .sort('n_articles', descending=True)
        )
    )


if __name__ == '__main__':
    main()
