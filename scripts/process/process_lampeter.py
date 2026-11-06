"""Process the Lampeter Corpus of Early Modern English Tracts (TEI XML).

Dataset : The Lampeter Corpus of Early Modern English Tracts
Coverage: 17th–18th century English tracts, 120 texts.
Homepage: https://ota.bodleian.ox.ac.uk/repository/xmlui/handle/20.500.14106/3193
Format  : TEI XML — a single ``teiCorpus`` file containing 120 ``<TEI>``
          elements.  Each ``<TEI>`` has a ``<teiHeader>`` with title, author,
          and date (``<creation><date notAfter="YYYY"/></creation>``), and a
          ``<body>`` with ``<p>`` paragraph elements.  Inline elements
          (``<hi>``, ``<emph>``, etc.) wrap styled text; all text content is
          retained via ``itertext()``.

Unit of output: one row per tract (one per ``<TEI>`` element).  The
``paragraphs`` field contains cleaned ``<p>`` block strings, after filtering
paragraphs shorter than ``--min-para-words`` words.

Usage
-----
    # Stats only
    uv run python scripts/process/process_lampeter.py

    # Full extraction
    uv run python scripts/process/process_lampeter.py --output-dir data/processed/lampeter --preview-path data/samples/lampeter_preview.jsonl --year-min 1700 --year-max 1920 --min-para-words 0
"""
from __future__ import annotations

import argparse
import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from historical_lm.stats import count_words, save_stats

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_XML = (
    _REPO_ROOT / 'data' / 'raw'
    / 'The Lampeter Corpus of Early Modern English Tracts' / '3193.xml'
)
_DEFAULT_OUTPUT = _REPO_ROOT / 'data' / 'stats' / 'lampeter_stats.parquet'
_DEFAULT_SHARD_SIZE = 30

_TEI_NS = 'http://www.tei-c.org/ns/1.0'
_NS = {'tei': _TEI_NS}

# Date attribute patterns: notAfter="1641" → 1641
_DATE_ATTR_RE = re.compile(r'\b(\d{4})\b')


def _year_from_date_elem(elem: ET.Element | None) -> int | None:
    """Extract year from a TEI ``<date>`` element's attributes."""
    if elem is None:
        return None
    for attr in ('when', 'notAfter', 'notBefore', 'from', 'to'):
        val = elem.get(attr, '')
        m = _DATE_ATTR_RE.search(val)
        if m:
            return int(m.group(1))
    return None


def _clean_para(elem: ET.Element) -> str:
    """Join all text within a <p> element into a clean string."""
    return ' '.join(''.join(elem.itertext()).split())


def _flush_shard(rows: list[dict], output_dir: Path, shard_idx: int) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f'part-{shard_idx:05d}.parquet'
    pq.write_table(pa.Table.from_pylist(rows), path, compression='zstd')
    print(f'  Wrote shard {shard_idx:05d} ({len(rows)} tracts) -> {path}')


def collect_stats(
    xml_path: Path,
    year_min: int,
    year_max: int,
    min_para_words: int,
    max_texts: int,
    output_dir: Path | None,
    preview_path: Path | None,
    shard_size: int,
    preview_paragraphs: int,
) -> pl.DataFrame:
    """Process the Lampeter Corpus TEI XML, filter by year, optionally save.

    Args:
        xml_path: Path to the ``teiCorpus`` XML file.
        year_min: Keep only tracts with year >= this value.
        year_max: Keep only tracts with year <= this value.
        min_para_words: Minimum word count for a paragraph to be kept.
        max_texts: Stop after this many accepted tracts (0 = no limit).
        output_dir: If given, write Parquet text shards here.
        preview_path: If given, write a JSONL preview file here.
        shard_size: Tracts per Parquet shard.
        preview_paragraphs: Paragraphs per tract in the JSONL preview.

    Returns:
        Polars DataFrame with one row per accepted tract (no text).
    """
    print(f'Parsing {xml_path} ...')
    try:
        tree = ET.parse(xml_path)
    except ET.ParseError as e:
        print(f'ERROR: failed to parse {xml_path}: {e}')
        return pl.DataFrame()

    root = tree.getroot()
    # Handle namespace prefix
    tei_texts = root.findall(f'.//{{{_TEI_NS}}}TEI')
    print(f'  {len(tei_texts):,} TEI documents found')

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
        for tei in tqdm(tei_texts, desc='Lampeter tracts', unit='tract'):
            # Extract metadata
            title_elem = tei.find(f'.//{{{_TEI_NS}}}titleStmt/{{{_TEI_NS}}}title')
            title_raw = ''.join(
                (title_elem.itertext() if title_elem is not None else [])
            ).strip()
            # Title contains "ID : real title" — strip the ID prefix
            title = re.sub(r'^[A-Za-z0-9]+\s*:\s*', '', title_raw).strip()

            author_elem = tei.find(
                f'.//{{{_TEI_NS}}}titleStmt/{{{_TEI_NS}}}author'
            )
            author = (
                ''.join(author_elem.itertext()).strip()
                if author_elem is not None else ''
            )

            date_elem = tei.find(
                f'.//{{{_TEI_NS}}}creation/{{{_TEI_NS}}}date'
            )
            year = _year_from_date_elem(date_elem)

            if year is None or year < year_min or year > year_max:
                n_year_skip += 1
                continue

            # Extract paragraphs from body
            body = tei.find(f'.//{{{_TEI_NS}}}body')
            if body is None:
                n_empty_skip += 1
                continue

            paragraphs = []
            for p_elem in body.findall(f'.//{{{_TEI_NS}}}p'):
                text = _clean_para(p_elem)
                if text and count_words(text) >= min_para_words:
                    paragraphs.append(text)

            if not paragraphs:
                n_empty_skip += 1
                continue

            # Extract text_id from title prefix
            m = re.match(r'^([A-Za-z0-9]+)\s*:', title_raw)
            text_id = m.group(1) if m else title_raw[:20]

            stat_row = {
                'text_id': text_id,
                'title': title,
                'author': author,
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
    print(f'  TEI docs found    : {len(tei_texts):,}')
    print(f'  Year out of range : {n_year_skip:,}')
    print(f'  Empty after filter: {n_empty_skip:,}')
    print(f'  Tracts accepted   : {len(stat_rows):,}')

    if output_dir is not None:
        print(f'Text saved -> {output_dir}')
    if preview_path is not None:
        print(f'Preview written -> {preview_path}')

    return pl.DataFrame(stat_rows)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Process Lampeter Corpus TEI XML into Parquet shards.'
    )
    parser.add_argument(
        '--xml',
        default=_DEFAULT_XML,
        type=Path,
        help='Path to the Lampeter Corpus XML file.',
    )
    parser.add_argument(
        '--output',
        default=_DEFAULT_OUTPUT,
        type=Path,
        help='Output Parquet file for per-tract statistics.',
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
        default=1600,
        type=int,
        help='Minimum year (default: 1600).',
    )
    parser.add_argument(
        '--year-max',
        default=1800,
        type=int,
        help='Maximum year inclusive (default: 1800).',
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
        help=f'Tracts per Parquet shard (default: {_DEFAULT_SHARD_SIZE}).',
    )
    parser.add_argument(
        '--preview-paragraphs',
        default=3,
        type=int,
        help='Paragraphs per tract in JSONL preview (default: 3).',
    )
    parser.add_argument(
        '--max-texts',
        default=0,
        type=int,
        help='Stop after this many accepted tracts (0 = no limit).',
    )
    return parser


def main() -> None:
    """Entry point for Lampeter Corpus processing."""
    args = _build_parser().parse_args()

    if not args.xml.is_file():
        print(f'ERROR: file not found: {args.xml}')
        return

    df = collect_stats(
        args.xml,
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
    print(f'\nSaved {len(df):,} tract records -> {args.output}')

    if df.is_empty():
        return

    print('\n--- Summary ---')
    print(f'Total tracts    : {len(df):,}')
    print(f'Total paragraphs: {df["n_paragraphs"].sum():,}')
    print(f'Total words     : {df["word_count"].sum():,}')
    year_col = df['year'].drop_nulls()
    if year_col.len() > 0:
        print(f'Year range      : {year_col.min()} - {year_col.max()}')
    print(
        '\nBy decade:\n'
        + str(
            df.with_columns((pl.col('year') // 10 * 10).alias('decade'))
            .group_by('decade')
            .agg(pl.len().alias('n_tracts'), pl.col('word_count').sum())
            .sort('decade')
        )
    )


if __name__ == '__main__':
    main()
