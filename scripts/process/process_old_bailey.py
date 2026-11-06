"""Process the Old Bailey Corpus 2 (OBC2, vertical/VRT format).

Dataset : Old Bailey Corpus 2.0
Coverage: 1720–1913, 638 session files (~639 MB).
Format  : VRT — one file per court session; tokens have two tab-separated
          fields (word, POS using CLAWS tagset).  Structural units are
          XML-like tags: ``<DIV0>``, ``<DIV1>``, ``<P>`` for paragraphs,
          ``<PERSNAME>`` for named persons.

Unit of output: one row per session file.  The ``paragraphs`` field is a
list of ``<P>`` block texts reconstructed by joining their tokens.  Tiny
paragraphs (fewer than ``--min-para-words`` words) are discarded.

Usage
-----
    # Stats only
    uv run python scripts/process/process_old_bailey.py \\
        --vrt-dir data/raw/old_bailey/OBC2_vertical

    # Full extraction with text shards and preview
    uv run python scripts/process/process_old_bailey.py --vrt-dir data/raw/old_bailey/OBC2_vertical --output-dir data/processed/old_bailey --preview-path data/samples/old_bailey_preview.jsonl --year-min 1700 --year-max 1920 --min-para-words 0
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
from historical_lm.vrt import parse_attrs, tokens_to_text

_DEFAULT_VRT_DIR = (
    Path(__file__).resolve().parents[2]
    / 'data' / 'raw' / 'old_bailey' / 'OBC2_vertical'
)
_DEFAULT_OUTPUT = (
    Path(__file__).resolve().parents[2]
    / 'data' / 'stats' / 'old_bailey_stats.parquet'
)
_DEFAULT_SHARD_SIZE = 50

# OBC2 session date is encoded in the filename: OBC2-YYYYMMDD.vrt
_DATE_RE = re.compile(r'OBC2-(\d{4})(\d{2})(\d{2})')


def _flush_shard(rows: list[dict], output_dir: Path, shard_idx: int) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f'part-{shard_idx:05d}.parquet'
    pq.write_table(pa.Table.from_pylist(rows), path, compression='zstd')
    print(f'  Wrote shard {shard_idx:05d} ({len(rows)} sessions) -> {path}')


def parse_obc2_file(path: Path, min_para_words: int = 3) -> tuple[dict, list[str]] | None:
    """Parse one OBC2 VRT file and return (session_metadata, paragraphs).

    Tokens inside ``<P>`` blocks are joined into paragraph strings.
    Markup lines (second tab-field ``00``, or closing tags starting with ``</``) are skipped.  Person names inside ``<PERSNAME>`` blocks are retained as plain tokens.

    Args:
        path: Path to a single OBC2 ``.vrt`` file.
        min_para_words: Discard paragraphs with fewer than this many words.

    Returns:
        Tuple of (meta_dict, paragraphs_list), or ``None`` on read error.
        ``meta_dict`` contains ``session_id``, ``year``, ``date``.
    """
    try:
        content = path.read_text(encoding='utf-8', errors='replace')
    except OSError:
        return None

    # Extract year from filename (authoritative; INTERP tags may disagree)
    m = _DATE_RE.search(path.name)
    year = int(m.group(1)) if m else None
    date_str = f'{m.group(1)}-{m.group(2)}-{m.group(3)}' if m else ''
    session_id = path.stem  # e.g. OBC2-17200427

    paragraphs: list[str] = []
    current_tokens: list[str] = []
    in_para = False

    for line in content.splitlines():
        stripped = line.strip()
        if not stripped:
            continue

        # Paragraph boundary tags
        if stripped.startswith('<P>') or stripped == '<P':
            in_para = True
            current_tokens = []
            continue
        if stripped.startswith('</P>'):
            if in_para and current_tokens:
                text = tokens_to_text(current_tokens)
                if count_words(text) >= min_para_words:
                    paragraphs.append(text)
            in_para = False
            current_tokens = []
            continue

        # Skip all other markup lines (start with '<' or have '\t00' suffix)
        if stripped.startswith('<'):
            continue

        if not in_para:
            continue

        # Token line: word\tPOS  (POS != '00' for genuine tokens)
        parts = stripped.split('\t')
        if len(parts) >= 2 and parts[1] != '00':
            current_tokens.append(parts[0])
        # If POS == '00', it's a stray markup token — skip

    meta = {
        'session_id': session_id,
        'year': year,
        'date': date_str,
    }
    return meta, paragraphs


def collect_stats(
    vrt_dir: Path,
    year_min: int,
    year_max: int,
    min_para_words: int,
    max_sessions: int,
    output_dir: Path | None,
    preview_path: Path | None,
    shard_size: int,
    preview_paragraphs: int,
) -> pl.DataFrame:
    """Process all OBC2 VRT files, filter by year, optionally save text.

    Args:
        vrt_dir: Directory containing ``OBC2-*.vrt`` files.
        year_min: Keep only sessions with year >= this value.
        year_max: Keep only sessions with year <= this value.
        min_para_words: Minimum word count for a paragraph to be kept.
        max_sessions: Stop after this many accepted sessions (0 = no limit).
        output_dir: If given, write Parquet text shards here.
        preview_path: If given, write a JSONL preview file here.
        shard_size: Sessions per Parquet shard.
        preview_paragraphs: Paragraphs to include per session in the preview.

    Returns:
        Polars DataFrame with one row per accepted session (no text).
    """
    files = sorted(vrt_dir.glob('OBC2-*.vrt'))
    if not files:
        print(f'ERROR: no OBC2-*.vrt files found in {vrt_dir}')
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

    preview_fh = preview_path.open('w', encoding='utf-8') if preview_path is not None else None
    try:
        for path in tqdm(files, desc='OBC2 sessions', unit='file'):
            result = parse_obc2_file(path, min_para_words)
            if result is None:
                continue
            meta, paragraphs = result

            year = meta['year']
            if year is None or year < year_min or year > year_max:
                n_year_skip += 1
                continue

            if not paragraphs:
                n_empty_skip += 1
                continue

            stat_row = {
                'session_id': meta['session_id'],
                'year': year,
                'date': meta['date'],
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

            if max_sessions > 0 and len(stat_rows) >= max_sessions:
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
    print(f'  Sessions accepted : {len(stat_rows):,}')

    if output_dir is not None:
        print(f'Text saved -> {output_dir}')
    if preview_path is not None:
        print(f'Preview written -> {preview_path}')

    return pl.DataFrame(stat_rows)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Process Old Bailey Corpus 2 VRT files into Parquet shards.'
    )
    parser.add_argument(
        '--vrt-dir',
        default=_DEFAULT_VRT_DIR,
        type=Path,
        help='Directory containing the OBC2 *.vrt files.',
    )
    parser.add_argument(
        '--output',
        default=_DEFAULT_OUTPUT,
        type=Path,
        help='Output Parquet file for per-session statistics.',
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
        default=1720,
        type=int,
        help='Minimum session year (default: 1720).',
    )
    parser.add_argument(
        '--year-max',
        default=1913,
        type=int,
        help='Maximum session year inclusive (default: 1913).',
    )
    parser.add_argument(
        '--min-para-words',
        default=3,
        type=int,
        help='Discard paragraphs shorter than this many words (default: 3).',
    )
    parser.add_argument(
        '--shard-size',
        default=_DEFAULT_SHARD_SIZE,
        type=int,
        help=f'Sessions per Parquet shard (default: {_DEFAULT_SHARD_SIZE}).',
    )
    parser.add_argument(
        '--preview-paragraphs',
        default=20,
        type=int,
        help='Paragraphs to include per session in the JSONL preview (default: 5).',
    )
    parser.add_argument(
        '--max-sessions',
        default=0,
        type=int,
        help='Stop after this many accepted sessions (0 = no limit).',
    )
    return parser


def main() -> None:
    """Entry point for Old Bailey Corpus processing."""
    args = _build_parser().parse_args()

    if not args.vrt_dir.is_dir():
        print(f'ERROR: directory not found: {args.vrt_dir}')
        return

    print(f'Processing {args.vrt_dir} …')
    df = collect_stats(
        args.vrt_dir,
        args.year_min,
        args.year_max,
        args.min_para_words,
        args.max_sessions,
        args.output_dir,
        args.preview_path,
        args.shard_size,
        args.preview_paragraphs,
    )

    save_stats(df, args.output)
    print(f'\nSaved {len(df):,} session records -> {args.output}')

    if df.is_empty():
        return

    print('\n--- Summary ---')
    print(f'Total sessions    : {len(df):,}')
    print(f'Total paragraphs  : {df["n_paragraphs"].sum():,}')
    print(f'Total words       : {df["word_count"].sum():,}')
    year_col = df['year'].drop_nulls()
    if year_col.len() > 0:
        print(f'Year range        : {year_col.min()} – {year_col.max()}')
    print(
        f'\nBy decade:\n'
        f'{df.with_columns((pl.col("year") // 10 * 10).alias("decade")).group_by("decade").agg(pl.len().alias("n_sessions"), pl.col("word_count").sum()).sort("decade")}'
    )


if __name__ == '__main__':
    main()
