"""Process the Pamphlets of the American Revolution corpus.

Dataset : Pamphlets of the American Revolution (transcribed by A.C.D.)
Coverage: ~1764-1776, 5 pamphlet texts.
Format  : Plain text — ``$...`` header lines (author, title, place, date),
          ``<c N>`` chapter markers, then prose paragraphs separated by blank lines.  Curly brackets ``{text}`` mark italics; square brackets ``[text]`` mark footnotes.

Unit of output: one row per pamphlet file.  The ``paragraphs`` field contains cleaned paragraph strings (curly/square bracket markup removed), after filtering paragraphs shorter than ``--min-para-words`` words.

Usage
-----
    # Stats only
    uv run python scripts/process/process_pamphlets.py

    # Full extraction
    uv run python scripts/process/process_pamphlets.py --output-dir   data/processed/pamphlets --preview-path data/samples/pamphlets_preview.jsonl
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from historical_lm.stats import count_words, save_stats

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_TXT_DIR = _REPO_ROOT / 'data' / 'raw' / 'pamphlets_american_rev'
_DEFAULT_OUTPUT = _REPO_ROOT / 'data' / 'stats' / 'pamphlets_stats.parquet'
_DEFAULT_SHARD_SIZE = 10

# Header block: content between '$' markers (may span multiple lines)
_HEADER_BLOCK_RE = re.compile(r'\$([^$]+)\$', re.DOTALL)

# Chapter marker: <c N> ...
_CHAPTER_RE = re.compile(r'^<c\s+\d+>', re.IGNORECASE)

# Italics markup: {text} -> keep text
_ITALIC_RE = re.compile(r'\{([^}]*)\}')

# Footnote markup: [text] -> remove
_FOOTNOTE_RE = re.compile(r'\[[^\]]*\]')

# Year patterns in header lines
_YEAR_RE = re.compile(r'\b(1[67]\d{2})\b')


def _clean_text(text: str) -> str:
    """Strip markup from a paragraph."""
    text = _ITALIC_RE.sub(r'\1', text)
    text = _FOOTNOTE_RE.sub('', text)
    return ' '.join(text.split())


def parse_pamphlet_file(
    path: Path,
    min_para_words: int,
) -> tuple[dict, list[str]] | None:
    """Parse one pamphlet text file.

    Args:
        path: Path to the plain-text pamphlet file.
        min_para_words: Minimum word count for a paragraph to be kept.

    Returns:
        Tuple of (meta_dict, paragraphs), or None on read error.
    """
    try:
        raw = path.read_text(encoding='utf-8', errors='replace')
    except OSError:
        return None

    meta: dict[str, str | int | None] = {
        'file_id': path.stem,
        'author': '',
        'title': '',
        'place': '',
        'year': None,
    }

    # Extract header section: all content before the first <c N> tag or first
    # blank-line-then-non-$ line.  Header blocks are delimited by '$...$'
    # (potentially multi-line).
    first_chapter = re.search(r'^<c\s+\d+>', raw, re.MULTILINE | re.IGNORECASE)
    assert first_chapter is not None, f'No chapter marker found in {path}'
    header_raw = raw[:first_chapter.start()]
    header_fields = [
        ' '.join(m.group(1).split())
        for m in _HEADER_BLOCK_RE.finditer(header_raw)
    ]
    if header_fields:
        meta['author'] = header_fields[0] if len(header_fields) > 0 else ''
        meta['title'] = header_fields[1] if len(header_fields) > 1 else ''
        # Search all fields for a year
        for field in header_fields:
            ym = _YEAR_RE.search(field)
            if ym:
                meta['year'] = int(ym.group(1))
                break

    paragraphs: list[str] = []
    current_para_lines: list[str] = []
    in_header = True

    def _flush_para() -> None:
        if current_para_lines:
            text = _clean_text(' '.join(current_para_lines))
            if text and count_words(text) >= min_para_words:
                paragraphs.append(text)
        current_para_lines.clear()

    for line in raw.splitlines():
        stripped = line.strip()

        # Skip header blocks (lines containing only $...$ or being part of them)
        if in_header and (stripped.startswith('$') or not stripped):
            if _CHAPTER_RE.search(line):
                in_header = False
            continue

        # Chapter markers — paragraph boundary
        if _CHAPTER_RE.match(stripped):
            _flush_para()
            in_header = False
            # rest = _CHAPTER_RE.sub('', stripped).strip()
            # if rest:
            #     current_para_lines.append(rest)
            continue

        in_header = False

        # Blank line = paragraph boundary
        if not stripped:
            _flush_para()
            continue

        current_para_lines.append(stripped)

    _flush_para()

    return dict(meta), paragraphs


def _flush_shard(rows: list[dict], output_dir: Path, shard_idx: int) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f'part-{shard_idx:05d}.parquet'
    pq.write_table(pa.Table.from_pylist(rows), path, compression='zstd')
    print(f'  Wrote shard {shard_idx:05d} ({len(rows)} pamphlets) -> {path}')


def collect_stats(
    txt_dir: Path,
    year_min: int,
    year_max: int,
    min_para_words: int,
    max_texts: int,
    output_dir: Path | None,
    preview_path: Path | None,
    shard_size: int,
    preview_paragraphs: int,
) -> pl.DataFrame:
    """Process all pamphlet text files, filter by year, and optionally save.

    Args:
        txt_dir: Directory containing ``*.txt`` pamphlet files.
        year_min: Keep only pamphlets with year >= this value.
        year_max: Keep only pamphlets with year <= this value.
        min_para_words: Minimum word count for a paragraph to be kept.
        max_texts: Stop after this many accepted pamphlets (0 = no limit).
        output_dir: If given, write Parquet text shards here.
        preview_path: If given, write a JSONL preview file here.
        shard_size: Pamphlets per Parquet shard.
        preview_paragraphs: Paragraphs per pamphlet in the JSONL preview.

    Returns:
        Polars DataFrame with one row per accepted pamphlet (no text).
    """
    files = [
        f for f in sorted(txt_dir.glob('*.txt'))
        if not f.name.startswith('readme')
    ]
    if not files:
        print(f'ERROR: no *.txt files found in {txt_dir}')
        return pl.DataFrame()

    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
    if preview_path is not None:
        preview_path.parent.mkdir(parents=True, exist_ok=True)

    stat_rows: list[dict] = []
    shard_buf: list[dict] = []
    shard_idx = 0
    n_year_skip = 0
    n_empty_skip = 0

    preview_fh = (
        preview_path.open('w', encoding='utf-8') if preview_path is not None else None
    )
    try:
        for path in tqdm(files, desc='Pamphlets', unit='file'):
            result = parse_pamphlet_file(path, min_para_words)
            if result is None:
                continue
            meta, paragraphs = result

            year = meta.get('year')
            if year is None or year < year_min or year > year_max:
                n_year_skip += 1
                continue

            if not paragraphs:
                n_empty_skip += 1
                continue

            stat_row = {
                'file_id': meta['file_id'],
                'author': meta['author'],
                'title': meta['title'],
                'place': meta['place'],
                'year': year,
                'n_paragraphs': len(paragraphs),
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
                        {**stat_row, 'text_preview': paragraphs[:preview_paragraphs]},
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
    print(f'  Year out of range : {n_year_skip:,}')
    print(f'  Empty after filter: {n_empty_skip:,}')
    print(f'  Pamphlets accepted: {len(stat_rows):,}')

    if output_dir is not None:
        print(f'Text saved -> {output_dir}')
    if preview_path is not None:
        print(f'Preview written -> {preview_path}')

    return pl.DataFrame(stat_rows)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Process Pamphlets of the American Revolution into Parquet shards.'
    )
    parser.add_argument(
        '--txt-dir',
        default=_DEFAULT_TXT_DIR,
        type=Path,
        help='Directory containing the pamphlet *.txt files.',
    )
    parser.add_argument(
        '--output',
        default=_DEFAULT_OUTPUT,
        type=Path,
        help='Output Parquet file for per-pamphlet statistics.',
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
        default=1740,
        type=int,
        help='Minimum year (default: 1740).',
    )
    parser.add_argument(
        '--year-max',
        default=1780,
        type=int,
        help='Maximum year inclusive (default: 1780).',
    )
    parser.add_argument(
        '--min-para-words',
        default=5,
        type=int,
        help='Minimum words per paragraph to keep (default: 5).',
    )
    parser.add_argument(
        '--shard-size',
        default=_DEFAULT_SHARD_SIZE,
        type=int,
        help=f'Pamphlets per Parquet shard (default: {_DEFAULT_SHARD_SIZE}).',
    )
    parser.add_argument(
        '--preview-paragraphs',
        default=10,
        type=int,
        help='Paragraphs per pamphlet in JSONL preview (default: 3).',
    )
    parser.add_argument(
        '--max-texts',
        default=0,
        type=int,
        help='Stop after this many accepted pamphlets (0 = no limit).',
    )
    return parser


def main() -> None:
    """Entry point for Pamphlets processing."""
    args = _build_parser().parse_args()

    if not args.txt_dir.is_dir():
        print(f'ERROR: directory not found: {args.txt_dir}')
        return

    print(f'Processing {args.txt_dir} ...')
    df = collect_stats(
        args.txt_dir,
        args.year_min,
        args.year_max,
        args.min_para_words,
        args.max_texts,
        args.output_dir,
        args.preview_path,
        args.shard_size,
        args.preview_paragraphs,
    )

    save_stats(df, args.output)
    print(f'\nSaved {len(df):,} pamphlet records -> {args.output}')

    if df.is_empty():
        return

    print('\n--- Summary ---')
    print(f'Total pamphlets : {len(df):,}')
    print(f'Total paragraphs: {df["n_paragraphs"].sum():,}')
    print(f'Total words     : {df["word_count"].sum():,}')
    year_col = df['year'].drop_nulls()
    if year_col.len() > 0:
        print(f'Year range      : {year_col.min()} - {year_col.max()}')
    for row in df.iter_rows(named=True):
        print(f'  {row["year"]}  {row["author"][:40]:<40}  {row["word_count"]:,} words')


if __name__ == '__main__':
    main()
