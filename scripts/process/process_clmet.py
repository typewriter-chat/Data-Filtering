"""Explore and collect statistics from the CLMET 3.1 plain-text corpus.

Corpus of Late Modern English Texts, version 3.1.
Homepage: https://fedora.clarin-d.uni-saarland.de/clmet/clmet.html
License : CC BY-NC-SA 4.0
Coverage: 1710–1920, ~34M words, 333 texts.

Expected directory layout (after unzipping)
-------------------------------------------
    <txt-dir>/          (default: data/raw/clmet/corpus/txt/plain)
        CLMET3_1_1_1.txt
        CLMET3_1_1_2.txt
        ...

File format
-----------
Each file has a flat XML header (one ``<tag>value</tag>`` per line) followed
by a ``<text>`` body.  Paragraphs are delimited by ``<p>…</p>`` blocks; the
only inline noise is ``<page type="…"/>`` self-closing tags::

    <year>1710</year>
    <title>A treatise…</title>
    <author>Berkeley, George</author>
    ...

    <text>
    <p>
    A TREATISE Concerning the PRINCIPLES OF Human Knowlege .
    </p>
    <p>
    <page type="Page"/> WHat I here make Publick …
    </p>
    ...
    </text>

This script:
    1. Reads all ``.txt`` files from ``--txt-dir``.
    2. Parses header metadata (year, title, author, genre, …).
    3. Extracts paragraph text from ``<p>…</p>`` blocks, stripping ``<page>`` tags.
    4. Computes word count from the extracted paragraphs.
    5. Optionally filters to texts published before ``--year-cutoff``.
    6. Writes per-text statistics to a Parquet file.

Usage
-----
    uv run python scripts/process/process_clmet.py \\
        --txt-dir data/raw/clmet/corpus/txt/plain \\
        --output data/stats/clmet_stats.parquet \\
        --year-cutoff 1900
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl
from tqdm import tqdm

from historical_lm.stats import count_words, save_stats

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Header: <tag>value</tag>  (value may be empty)
_HEADER_RE = re.compile(r'^<(\w+)>([^<]*)</\1>\s*$')

# Paragraph blocks inside <text>
_P_RE = re.compile(r'<p>(.*?)</p>', re.DOTALL | re.IGNORECASE)

# Inline <page …/> self-closing tags to strip from paragraph content
_PAGE_RE = re.compile(r'<page\b[^>]*/>', re.IGNORECASE)

# Default paths (relative to the repo root)
_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_TXT_DIR = _REPO_ROOT / 'data' / 'raw' / 'clmet' / 'corpus' / 'txt' / 'plain'
_DEFAULT_OUTPUT = _REPO_ROOT / 'data' / 'stats' / 'clmet_stats.parquet'
_DEFAULT_SHARD_SIZE = 50


def _flush_shard(rows: list[dict], output_dir: Path, shard_idx: int) -> None:
    """Write a list of book dicts to a zstd-compressed Parquet shard."""
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f'part-{shard_idx:05d}.parquet'
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path, compression='zstd')
    print(f'  Wrote shard {shard_idx:05d} ({len(rows)} texts) → {path}')


# ---------------------------------------------------------------------------
# Per-file processing
# ---------------------------------------------------------------------------

def _parse_header(content: str) -> dict[str, str]:
    """Parse the flat XML header into a dict.

    Args:
        content: Full decoded file content.

    Returns:
        Dict mapping tag names to their text values.
    """
    header: dict[str, str] = {}
    for line in content.splitlines():
        m = _HEADER_RE.match(line)
        if m:
            header[m.group(1)] = m.group(2).strip()
    return header


def extract_paragraphs(content: str) -> list[str]:
    """Extract cleaned paragraph strings from ``<p>…</p>`` blocks.

    ``<page type="…"/>`` self-closing tags are stripped; the resulting
    whitespace is normalised.  Empty paragraphs are discarded.

    Args:
        content: Full decoded file content.

    Returns:
        List of non-empty paragraph strings.
    """
    paragraphs = []
    for m in _P_RE.finditer(content):
        text = _PAGE_RE.sub(' ', m.group(1))
        text = ' '.join(text.split())  # normalise whitespace
        if text:
            paragraphs.append(text)
    return paragraphs


def process_file(path: Path) -> dict | None:
    """Parse one CLMET plain-text file and return a statistics record.

    Args:
        path: Path to a single ``.txt`` corpus file.

    Returns:
        Dict with metadata and word count, or ``None`` on read error.
    """
    try:
        content = path.read_text(encoding='utf-8', errors='replace')
    except OSError:
        return None

    header = _parse_header(content)
    paragraphs = extract_paragraphs(content)
    full_text = ' '.join(paragraphs)
    word_count = count_words(full_text)
    n_paragraphs = len(paragraphs)

    year_str = header.get('year', '')
    try:
        year = int(year_str) if year_str else None
    except ValueError:
        year = None

    return {
        'filename': path.name,
        'text_id': header.get('id', path.stem),
        'title': header.get('title', ''),
        'author': header.get('author', ''),
        'year': year,
        'period': header.get('period', ''),
        'quartcent': header.get('quartcent', ''),
        'decade': header.get('decade', ''),
        'genre': header.get('genre', ''),
        'subgenre': header.get('subgenre', ''),
        'gender': header.get('gender', ''),
        'author_birth': header.get('author_birth', ''),
        'source': header.get('source', ''),
        'language': 'en',
        'n_paragraphs': n_paragraphs,
        'word_count': word_count,
        'paragraphs': paragraphs,
    }


def collect_stats(
    txt_dir: Path,
    year_cutoff: int | None,
    output_dir: Path | None = None,
    preview_path: Path | None = None,
    shard_size: int = _DEFAULT_SHARD_SIZE,
    preview_paragraphs: int = 3,
) -> pl.DataFrame:
    """Process all ``.txt`` files in ``txt_dir``, collect statistics, and
    optionally save the extracted text as Parquet shards and a JSONL preview.

    Args:
        txt_dir: Directory containing the CLMET plain-text files.
        year_cutoff: Exclude texts published in this year or later.
        output_dir: If given, write Parquet shards (with full paragraph text)
            here.  Each shard holds ``shard_size`` texts.
        preview_path: If given, write a JSONL file with the first
            ``preview_paragraphs`` paragraphs per text for manual review.
        shard_size: Number of texts per Parquet shard.
        preview_paragraphs: Paragraphs to include per text in the preview.

    Returns:
        Polars DataFrame with one row per accepted text (no text column).
    """
    files = sorted(txt_dir.glob('*.txt'))
    if not files:
        print(f'ERROR: no .txt files found in {txt_dir}', file=sys.stderr)
        sys.exit(1)

    stat_rows: list[dict] = []
    shard_buf: list[dict] = []
    shard_idx: int = 0

    preview_fh = preview_path.open('w', encoding='utf-8') if preview_path is not None else None
    try:
        for path in tqdm(files, desc='CLMET plain', unit='file'):
            rec = process_file(path)
            if rec is None:
                continue

            year = rec['year']
            if year_cutoff is not None and year is not None and year >= year_cutoff:
                continue

            paragraphs = rec.pop('paragraphs')
            stat_rows.append(rec)

            if output_dir is not None:
                shard_buf.append({**rec, 'paragraphs': paragraphs})
                if len(shard_buf) >= shard_size:
                    _flush_shard(shard_buf, output_dir, shard_idx)
                    shard_buf = []
                    shard_idx += 1

            if preview_fh is not None:
                preview_record = {**rec, 'text_preview': paragraphs[:preview_paragraphs]}
                preview_fh.write(json.dumps(preview_record, ensure_ascii=False) + '\n')

        if output_dir is not None and shard_buf:
            _flush_shard(shard_buf, output_dir, shard_idx)

    finally:
        if preview_fh is not None:
            preview_fh.close()

    if output_dir is not None:
        print(f'Text saved → {output_dir}')
    if preview_path is not None:
        print(f'Preview written → {preview_path}')

    return pl.DataFrame(stat_rows)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Collect per-text statistics from the CLMET 3.1 plain-text corpus.'
    )
    parser.add_argument(
        '--txt-dir',
        default=_DEFAULT_TXT_DIR,
        type=Path,
        help='Directory containing the plain .txt corpus files.',
    )
    parser.add_argument(
        '--output',
        default=_DEFAULT_OUTPUT,
        type=Path,
        help='Output Parquet file for per-text statistics.',
    )
    parser.add_argument(
        '--year-cutoff',
        default=None,
        type=int,
        metavar='YEAR',
        help='Exclude texts published in YEAR or later (e.g. 1900).',
    )
    parser.add_argument(
        '--output-dir',
        default=None,
        type=Path,
        help='Directory for Parquet shards containing full paragraph text.',
    )
    parser.add_argument(
        '--preview-path',
        default=None,
        type=Path,
        help='Path for JSONL preview file (first N paragraphs per text).',
    )
    parser.add_argument(
        '--shard-size',
        default=_DEFAULT_SHARD_SIZE,
        type=int,
        help=f'Number of texts per Parquet shard (default: {_DEFAULT_SHARD_SIZE}).',
    )
    parser.add_argument(
        '--preview-paragraphs',
        default=3,
        type=int,
        help='Number of paragraphs to include per text in the JSONL preview (default: 3).',
    )
    return parser


def main() -> None:
    """Entry point for CLMET statistics collection."""
    args = _build_parser().parse_args()

    if not args.txt_dir.is_dir():
        print(f'ERROR: directory not found: {args.txt_dir}', file=sys.stderr)
        sys.exit(1)

    print(f'Processing {args.txt_dir} …')
    df = collect_stats(
        args.txt_dir,
        args.year_cutoff,
        output_dir=args.output_dir,
        preview_path=args.preview_path,
        shard_size=args.shard_size,
        preview_paragraphs=args.preview_paragraphs,
    )

    save_stats(df, args.output)
    print(f'Saved {len(df):,} text records → {args.output}')

    print('\n--- Summary ---')
    print(f'Total texts      : {len(df):,}')
    print(f'Total paragraphs : {df["n_paragraphs"].sum():,}')
    print(f'Total words      : {df["word_count"].sum():,}')
    year_col = df['year'].drop_nulls()
    if year_col.len() > 0:
        print(f'Year range       : {year_col.min()} – {year_col.max()}')
    print(f'\nBy genre:\n{df.group_by("genre").agg(pl.len().alias("n_texts"), pl.col("word_count").sum()).sort("n_texts", descending=True)}')
    print(f'\nBy period:\n{df.group_by("period").agg(pl.len().alias("n_texts"), pl.col("word_count").sum()).sort("period")}')


if __name__ == '__main__':
    main()
