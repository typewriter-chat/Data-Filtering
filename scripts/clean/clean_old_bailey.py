"""Clean processed Old Bailey shards by removing session-level boilerplate.

Reads Parquet shards where each row is one Old Bailey session with a
``paragraphs`` list. The cleaner:
- drops repeated session preamble material
- drops jury headers and juror-name lists
- removes short all-caps layout fragments in the preamble
- keeps the main proceedings once substantive session text begins
- writes cleaned Parquet shards plus a random sample of full cleaned sessions

uv run python scripts/clean/clean_old_bailey.py --max-shards 2
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
_DEFAULT_INPUT_DIR = _REPO_ROOT / 'data' / 'processed' / 'old_bailey'
_DEFAULT_OUTPUT_DIR = _REPO_ROOT / 'data' / 'clean' / 'old_bailey_clean'
_DEFAULT_SAMPLE = _REPO_ROOT / 'data' / 'clean' / 'old_bailey_clean_sample.jsonl'
_DEFAULT_REPORT = _REPO_ROOT / 'data' / 'clean' / 'old_bailey_clean_report.json'

# Preamble and jury-list detection.
_WHITESPACE_RE = re.compile(r'\s+')
# Catches both early form ("PROCEEDINGS ON THE KING/QUEEN") and later CCC form
# ("WHOLE PROCEEDINGS ON THE").
_PROCEEDINGS_RE = re.compile(
    r'THE (?:WHOLE )?PROCEEDINGS ON THE|ON THE (?:KING\'S|QUEEN\'S) COMMISSION OF',
    re.IGNORECASE,
)
_COURT_HEADER_RE = re.compile(
    r'(Oyer and Termin|Goal-?Delivery|Gaol.?Delivery|Justice-Hall|OLD-?BAILEY'
    r'|MAJESTY|LORD-?MAYOR|MAYORALTY|CENTRAL CRIMINAL COURT'
    r'|MINUTES OF EVIDENCE|Sessions Paper)',
    re.IGNORECASE,
)
# Catches plain "London/Middlesex Jury" and numbered variants
# "First/Second London/Middlesex Jury".
_JURY_HEADER_RE = re.compile(
    r'(?:(?:First|Second|Third)\s+)?(?:London|Middlesex)\s+Jury'
    r'|Jurors Names were as followeth',
    re.IGNORECASE,
)
_DATE_ONLY_RE = re.compile(
    r'^(?:\d{1,2}\.?\s+)?(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Octob|Nov|Dec|'
    r'January|February|March|April|June|July|August|September|October|November|December)\.?\s+\d{4}\.?$',
    re.IGNORECASE,
)
_YEAR_ONLY_RE = re.compile(r'^\d{4}\.?$')
_SHORT_ALL_CAPS_RE = re.compile(r"^[A-Z][A-Z\s\-,'.;:()]+$")
_NAME_LINE_RE = re.compile(
    r'^(?:[A-Z][a-z]+|[A-Z][a-z]+\.)'
    r'(?:\s+(?:[A-Z][a-z]+|[A-Z]\.|de|van|von|la|le))*[,]?$'
)
_JURY_END_HINT_RE = re.compile(r'(sworn|indicted|arraigned|tried|was indicted|was charg)', re.IGNORECASE)
_PREAMBLE_LINE_RE = re.compile(
    r'(being the .*Year of His MAJESTY|His MAJESTY\'s Reign|'
    r'BEFORE the Right Honourable|BEFORE the Right Honorable|'
    r'During the MAYORALTY|Rt\. Honble|CITY of LONDON|COUNTY of MIDDLE)',
    re.IGNORECASE,
)
_SESSION_DAY_LINE_RE = re.compile(
    r'^(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday).*\bbeing the\b',
    re.IGNORECASE,
)
# Publisher / printer imprint block (preamble §7).
_PUBLISHER_IMPRINT_RE = re.compile(
    r'^(?:Printed (?:for|by)\b|PUBLISHED BY\b|LONDON:\s*$)',
    re.IGNORECASE,
)
_PRICE_LINE_RE = re.compile(r'^\[?\s*\(?\s*Price\b', re.IGNORECASE)
_SHORTHAND_CREDIT_RE = re.compile(
    r'\b(?:SHORT.?HAND|Barrister.at.Law|MIDDLE TEMPLE|INNER TEMPLE|LINCOLN.S INN'
    r'|REVISED AND EDITED|Short.hand Writers? to the Court)\b',
    re.IGNORECASE,
)
_PUBLISHER_ADDRESS_RE = re.compile(
    r'\b(?:CHANCERY LANE|FLEET STREET|CHEAPSIDE|THE STRAND|LINCOLN.S INN'
    r'|ROLLS CHAMBERS|WARWICK LANE|CORNHILL)\b',
    re.IGNORECASE,
)

# ── Body-level structural fragments (§8) ─────────────────────────────────────
# [ X. See summary.] internal cross-references.
_SUMMARY_XREF_RE = re.compile(r'^\[.*\bSee summary\b', re.IGNORECASE)
# Short verdict / sentence lines (≤ _VERDICT_MAX_WORDS words).
_VERDICT_RE = re.compile(
    r'^(?:NOT GUILTY|GUILTY|Guilty|Acquitted|ACQUITTED|Not Guilty'
    r'|Transported|Confined|Judgment respited|Death'
    r'|Imprisoned|Whipp(?:ed|ing)|Brand(?:ed|ing)|Fined?)',
    re.IGNORECASE,
)
_VERDICT_MAX_WORDS = 8
# Section header stubs.
_SECTION_HEADER_RE = re.compile(
    r"^(?:Prisoner'?s?\s+Defenc[e]?|Cross.?Examination"
    r'|For the (?:Prisoner|Prosecution|Crown|Defence|Defendant)'
    r'|Re.?Examined|DEFENCE\.|PROSECUTION\.)\s*\.?$',
    re.IGNORECASE,
)
# Jury and judge assignment lines (≤ _JUDGE_LINE_MAX_WORDS words).
_JUDGE_LINE_RE = re.compile(
    r'\bBefore\b.*\b(?:Recorder|Common\s+Serj?eant|Baron|Justice|Judge)\b'
    r'|(?:(?:First|Second|Third|London|Middlesex)\s+)+(?:jury|Jury)\b.*\bbefore\b'
    r'|^Tried by the\b',
    re.IGNORECASE,
)
_JUDGE_LINE_MAX_WORDS = 12
# Short procedural parenthetical notes.
_PROC_NOTE_RE = re.compile(
    r'^\((?:Property produced|Witnesses? (?:for|examined)|prisoner received'
    r'|Indictment read|Deposition read|Witness paused)',
    re.IGNORECASE,
)
_PROC_NOTE_MAX_WORDS = 12

_MIN_PARAGRAPH_WORDS = 2
_MAX_SHORT_CAPS_WORDS = 8


def _normalize_text(text: str) -> str:
    """Apply light whitespace normalization."""
    decoded = html.unescape(str(text or ''))
    return _WHITESPACE_RE.sub(' ', decoded).strip()


def _is_preamble_fragment(text: str) -> bool:
    """Return True for standard session-heading or publisher-block material."""
    words = count_words(text)
    return (
        bool(_PROCEEDINGS_RE.search(text))
        or bool(_COURT_HEADER_RE.search(text))
        or bool(_PREAMBLE_LINE_RE.search(text))
        or bool(_SESSION_DAY_LINE_RE.search(text))
        or bool(_DATE_ONLY_RE.fullmatch(text))
        or (words <= 5 and bool(re.search(r'\b\d{4}\.?$', text)))
        or bool(_YEAR_ONLY_RE.fullmatch(text))
        or (words <= _MAX_SHORT_CAPS_WORDS and bool(_SHORT_ALL_CAPS_RE.fullmatch(text)))
        # Publisher / printer block (§7)
        or bool(_PUBLISHER_IMPRINT_RE.match(text))
        or bool(_PRICE_LINE_RE.match(text))
        or bool(_SHORTHAND_CREDIT_RE.search(text))
        or (words <= 8 and bool(_PUBLISHER_ADDRESS_RE.search(text)))
    )


def _is_body_fragment(text: str) -> bool:
    """Return True for short structural fragments in the trial body (§8)."""
    words = count_words(text)
    return (
        bool(_SUMMARY_XREF_RE.match(text))
        or (words <= _VERDICT_MAX_WORDS and bool(_VERDICT_RE.match(text)))
        or bool(_SECTION_HEADER_RE.fullmatch(text))
        or (words <= _JUDGE_LINE_MAX_WORDS and bool(_JUDGE_LINE_RE.search(text)))
        or (words <= _PROC_NOTE_MAX_WORDS and bool(_PROC_NOTE_RE.match(text)))
    )


def _clean_paragraphs(paragraphs: list[str], counters: Counter[str]) -> list[str]:
    """Remove preamble and jury-list paragraphs from one session."""
    cleaned: list[str] = []
    in_preamble = True
    in_jury_list = False

    for para in paragraphs:
        counters['paragraphs_seen'] += 1
        text = _normalize_text(para)
        if not text:
            counters['dropped_empty_paragraph'] += 1
            continue

        if _JURY_HEADER_RE.search(text):
            in_jury_list = True
            counters['dropped_jury_header'] += 1
            continue

        if in_preamble:
            if _is_preamble_fragment(text):
                counters['dropped_preamble'] += 1
                continue

            if count_words(text) < _MIN_PARAGRAPH_WORDS and not any(ch.isalpha() for ch in text):
                counters['dropped_tiny_fragment'] += 1
                continue

            in_preamble = False

        if in_jury_list:
            if _NAME_LINE_RE.fullmatch(text):
                counters['dropped_juror_name'] += 1
                continue
            if _JURY_END_HINT_RE.search(text):
                in_jury_list = False
            else:
                # Continue dropping short list-like remnants until the
                # proceedings clearly resume.
                if count_words(text) <= 6:
                    counters['dropped_jury_fragment'] += 1
                    continue
                in_jury_list = False

        if _is_body_fragment(text):
            counters['dropped_body_fragment'] += 1
            continue

        cleaned.append(text)

    counters['kept_paragraphs'] += len(cleaned)
    return cleaned


def _build_output_row(row: dict, paragraphs: list[str]) -> dict:
    """Build the cleaned output row for one session."""
    meta = {key: value for key, value in row.items() if key != 'paragraphs'}
    meta['n_paragraphs'] = len(paragraphs)
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
    """Write one full cleaned session to the sample file with small probability."""
    if sample_fh is None or sample_prob <= 0:
        return
    if rng.random() >= sample_prob:
        return
    sample_fh.write(json.dumps(row, ensure_ascii=False) + '\n')
    counters['sampled_sessions'] += 1


def clean_old_bailey(
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
    """Clean processed Old Bailey shards into proceedings-focused output."""
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
        for path in tqdm(files, desc='Old Bailey clean shards', unit='shard'):
            out_path = output_dir / path.name
            if out_path.exists() and not overwrite:
                counters['skipped_existing_shards'] += 1
                continue

            out_rows: list[dict] = []
            pf = pq.ParquetFile(path)
            for batch in pf.iter_batches(batch_size=batch_size):
                for row in batch.to_pylist():
                    counters['sessions_seen'] += 1
                    paragraphs = _clean_paragraphs(row.get('paragraphs') or [], counters)
                    if not paragraphs:
                        counters['sessions_dropped_empty'] += 1
                        continue

                    out_row = _build_output_row(row, paragraphs)
                    out_rows.append(out_row)
                    counters['sessions_kept'] += 1
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
        description='Clean processed Old Bailey shards by removing session boilerplate.'
    )
    parser.add_argument('--input-dir', default=_DEFAULT_INPUT_DIR, type=Path, help='Directory containing processed Old Bailey parquet shards.')
    parser.add_argument('--output-dir', default=_DEFAULT_OUTPUT_DIR, type=Path, help='Directory for cleaned output shards.')
    parser.add_argument('--sample-path', default=_DEFAULT_SAMPLE, type=Path, help='Optional JSONL path for random full-session samples.')
    parser.add_argument('--report-path', default=_DEFAULT_REPORT, type=Path, help='Optional JSON report for aggregate cleaning counts.')
    parser.add_argument('--sample-prob', default=5 / 10000, type=float, metavar='P', help='Probability of saving a cleaned session to the sample file.')
    parser.add_argument('--sample-seed', default=42, type=int, metavar='N', help='Random seed for reproducible sampling.')
    parser.add_argument('--max-shards', default=0, type=int, metavar='N', help='Process at most N input shards (0 = all).')
    parser.add_argument('--batch-size', default=128, type=int, metavar='N', help='Parquet batch size per read step.')
    parser.add_argument('--overwrite', action='store_true', help='Overwrite output shards if they already exist.')
    return parser


def main() -> None:
    """Entry point for the Old Bailey cleaner."""
    args = _build_parser().parse_args()
    clean_old_bailey(
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
