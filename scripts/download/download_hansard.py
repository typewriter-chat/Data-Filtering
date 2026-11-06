"""Download historic Hansard debates (1803-2005) from api.parliament.uk.

Four-phase pipeline
-------------------
1. **Discover sitting dates** – scrape monthly calendar pages to find days
   that have parliamentary sittings.
2. **Discover sections** – fetch the per-day ``.js`` JSON metadata to
   enumerate debate sections with their house and URL slug.
3. **Expand container sections** – some section types (oral questions, written
   answers groups) are index pages whose actual content lives in child sections.
   This phase fetches each container page and replaces it with its children.
4. **Download section text** – fetch each leaf section's HTML page and extract
   paragraph text.

Intermediate results are cached to ``<out-dir>/.cache/`` so the script is
fully resumable: re-running it will skip completed phases and already-saved
sections.

Output
------
JSONL  ``<out-dir>/hansard_<year>.jsonl``  – one file per calendar year.
Each line is a JSON object::

    {
        "date":  "1802-07-02",
        "year":  1802,
        "month": "jul",
        "day":   2,
        "house": "commons",
        "title": "MINUTES.",
        "slug":  "minutes",
        "url":   "https://api.parliament.uk/historic-hansard/commons/1802/jul/02/minutes",
        "paragraphs": ["On the motion of ...", "..."]
    }

Usage
-----
    # Full run (1802-1920)
    uv run python scripts/download/download_hansard.py --out-dir data/raw/hansard

    # Quick test with a narrow year range
    uv run python scripts/download/download_hansard.py \\
        --out-dir data/raw/hansard \\
        --start-year 1802 --end-year 1810 \\
        --workers 2
"""

from __future__ import annotations

import argparse
import json
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import TextIO

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_BASE_URL = 'https://api.parliament.uk/historic-hansard'
_MONTHS = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
           'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_OUT_DIR = _REPO_ROOT / 'data' / 'raw' / 'hansard'
_DEFAULT_START_YEAR = 1802
_DEFAULT_END_YEAR = 1938
_DEFAULT_WORKERS = 4
# Polite delay (seconds) between requests *per worker thread*.
# 4 workers × 0.3 s ≈ ~13 req/s sustained, well within typical limits.
_DEFAULT_DELAY = 0.5

# ---------------------------------------------------------------------------
# Thread-local HTTP sessions
# ---------------------------------------------------------------------------
_tls = threading.local()


def _get_session(delay: float) -> requests.Session:
    """Return (or create) the per-thread requests session.

    Args:
        delay: Seconds to sleep before each request.

    Returns:
        A configured :class:`requests.Session`.
    """
    if not hasattr(_tls, 'session'):
        s = requests.Session()
        s.headers['User-Agent'] = (
            'hansard-research-downloader/1.0 '
            '(historical NLP research; contact data@parliament.uk)'
        )
        _tls.session = s
        _tls.delay = delay
    return _tls.session


def _get(url: str, delay: float, **kwargs) -> requests.Response:
    """Rate-limited GET with retries on transient errors.

    Args:
        url: URL to fetch.
        delay: Per-request sleep in seconds.
        **kwargs: Forwarded to :meth:`requests.Session.get`.

    Returns:
        :class:`requests.Response`.
    """
    session = _get_session(delay)
    time.sleep(delay)
    for attempt in range(3):
        try:
            return session.get(url, timeout=30, **kwargs)
        except requests.RequestException:
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)
    raise RuntimeError(f'unreachable: {url}')  # pragma: no cover


# ---------------------------------------------------------------------------
# Phase 1 – discover sitting dates
# ---------------------------------------------------------------------------

def _discover_dates_for_month(year: int, mon: str, delay: float) -> list[tuple[int, str, int]]:
    """Scrape one monthly calendar page and return sitting days found.

    Args:
        year: Four-digit year.
        mon: Three-letter lowercase month abbreviation (e.g. ``'jul'``).
        delay: Per-request sleep.

    Returns:
        List of ``(year, month, day)`` tuples.
    """
    url = f'{_BASE_URL}/sittings/{year}/{mon}'
    try:
        r = _get(url, delay)
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, 'html.parser')
        pattern = re.compile(
            rf'^(?:https?://[^/]+)?/historic-hansard/sittings/{year}/{mon}/(\d+)$'
        )
        days = []
        seen: set[int] = set()
        for a in soup.find_all('a', href=True):
            m = pattern.match(str(a['href']))
            if m:
                day = int(m.group(1))
                if day not in seen:
                    seen.add(day)
                    days.append((year, mon, day))
        return days
    except Exception as exc:
        tqdm.write(f'  [warn] {url}: {exc}')
        return []


def discover_sitting_dates(
    start_year: int,
    end_year: int,
    delay: float,
    workers: int,
) -> list[tuple[int, str, int]]:
    """Return all sitting dates in the given year range.

    Scrapes monthly calendar pages in parallel.

    Args:
        start_year: First year to include (inclusive).
        end_year: Last year to include (inclusive).
        delay: Per-request sleep.
        workers: Number of parallel workers.

    Returns:
        Sorted list of ``(year, month, day)`` tuples.
    """
    tasks = [
        (year, mon)
        for year in range(start_year, end_year + 1)
        for mon in _MONTHS
    ]
    dates: list[tuple[int, str, int]] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_discover_dates_for_month, y, m, delay): (y, m) for y, m in tasks}
        for fut in tqdm(as_completed(futures), total=len(tasks), desc='Phase 1: discover dates'):
            dates.extend(fut.result())
    dates.sort()
    return dates


# ---------------------------------------------------------------------------
# Phase 2 – discover sections
# ---------------------------------------------------------------------------

# Section types whose top-level page is a navigational index rather than content.
# These are expanded in Phase 2.5 to their individual child sections.
_CONTAINER_SECTION_TYPES: frozenset[str] = frozenset({
    'oral_questions',
    'written_answers_group',
})

# Maps .js wrapper key → URL path segment. Unlisted keys fall back to _key_to_house_path().
_KEY_TO_HOUSE_PATH: dict[str, str] = {
    'house_of_commons_sitting': 'commons',
    'house_of_lords_sitting': 'lords',
    'commons_written_answers_sitting': 'written-answers',
    'lords_written_answers_sitting': 'lords-written-answers',
    'house_of_lords_report': 'lords-reports',
    'written_statements_sitting': 'written-statements',
    'westminster_hall_sitting': 'westminster-hall',
    'grand_committee_report': 'grand-committee-reports',
}


def _key_to_house_path(key: str) -> str:
    """Derive the URL path segment from a sitting wrapper key.

    Args:
        key: Top-level key from the ``.js`` JSON (e.g. ``'house_of_lords_report'``).

    Returns:
        URL path segment (e.g. ``'lords-reports'``).
    """
    if key in _KEY_TO_HOUSE_PATH:
        return _KEY_TO_HOUSE_PATH[key]
    path = key
    if path.startswith('house_of_'):
        path = path[len('house_of_'):]
    if path.endswith('_sitting'):
        path = path[:-len('_sitting')]
    return path.replace('_', '-')


def _parse_sitting_json(data: object, year: int, mon: str, day: int) -> list[dict]:
    """Extract section records from raw sitting JSON.

    The ``.js`` endpoint can return either a single sitting dict (possibly
    wrapped in an outer dict keyed by ``house_of_*_sitting``) or a list of
    such objects.  This function normalises both forms.

    Args:
        data: Parsed JSON value.
        year: Year of the sitting.
        mon: Month abbreviation.
        day: Day of the month.

    Returns:
        List of section dicts with keys ``year``, ``month``, ``day``,
        ``house``, ``slug``, ``title``.
    """
    results = []

    def _process_sitting(sitting: dict, house_path: str) -> None:
        for entry in sitting.get('top_level_sections', []):
            if not entry:
                continue
            # Entry is {"<type_key>": {"slug": ..., "title": ...}}
            type_key = next(iter(entry.keys()))
            sec = entry.get(type_key)
            if not isinstance(sec, dict):
                continue
            slug = sec.get('slug', '').strip()
            if not slug or re.fullmatch(r'-\d+', slug):
                continue
            results.append({
                'year': year,
                'month': mon,
                'day': day,
                'house': house_path,
                'slug': slug,
                'title': sec.get('title', ''),
                'section_type': type_key,
            })

    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                _process_wrapped_or_direct(item, _process_sitting)
    elif isinstance(data, dict):
        _process_wrapped_or_direct(data, _process_sitting)

    return results


def _process_wrapped_or_direct(d: dict, callback) -> None:
    """Handle both ``{'house_of_commons_sitting': {...}}`` and direct sitting dicts.

    Args:
        d: Dict to inspect.
        callback: Called with ``(sitting_dict, house_path)``.
    """
    matched = False
    for key, val in d.items():
        if ('sitting' in key or 'report' in key) and isinstance(val, dict):
            callback(val, _key_to_house_path(key))
            matched = True
    if not matched and ('top_level_sections' in d or 'title' in d):
        title = d.get('title', '')
        house_path = 'lords' if 'lord' in title.lower() else 'commons'
        callback(d, house_path)


def _discover_sections_for_date(
    year: int,
    mon: str,
    day: int,
    delay: float,
) -> list[dict]:
    """Fetch the per-day JSON and return section metadata.

    Args:
        year: Four-digit year.
        mon: Three-letter month abbreviation.
        day: Day of month.
        delay: Per-request sleep.

    Returns:
        List of section dicts.
    """
    url = f'{_BASE_URL}/sittings/{year}/{mon}/{day:02d}.js'
    try:
        r = _get(url, delay)
        if r.status_code != 200:
            return []
        return _parse_sitting_json(r.json(), year, mon, day)
    except Exception as exc:
        tqdm.write(f'  [warn] sections {year}/{mon}/{day}: {exc}')
        return []


def discover_sections(
    dates: list[tuple[int, str, int]],
    delay: float,
    workers: int,
) -> list[dict]:
    """Return all section records for the given sitting dates.

    Args:
        dates: List of ``(year, month, day)`` tuples.
        delay: Per-request sleep.
        workers: Number of parallel workers.

    Returns:
        List of section dicts.
    """
    sections: list[dict] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_discover_sections_for_date, y, m, d, delay): (y, m, d)
            for y, m, d in dates
        }
        for fut in tqdm(as_completed(futures), total=len(dates), desc='Phase 2: discover sections'):
            sections.extend(fut.result())

    return sections


# ---------------------------------------------------------------------------
# Phase 2.5 – expand container sections to leaf sections
# ---------------------------------------------------------------------------

def _extract_child_section_links(
    soup: BeautifulSoup,
    house: str,
    year: int,
    mon: str,
    day: int,
    parent_slug: str,
) -> list[tuple[str, str]]:
    """Extract child section (slug, title) pairs from a container section page.

    Container pages (oral questions, written answers groups) list child sections
    as links at the same URL depth sharing the same house/date prefix.
    Normalises ``-`` vs ``_`` in the house path segment when matching.

    Args:
        soup: Parsed HTML of the container page.
        house: URL path segment for the house (e.g. ``'written-answers'``).
        year: Four-digit year.
        mon: Three-letter month abbreviation.
        day: Day of month.
        parent_slug: Slug of the container itself (excluded from results).

    Returns:
        List of ``(slug, title)`` tuples for child sections.
    """
    def _norm(s: str) -> str:
        return s.replace('-', '_')

    pattern = re.compile(
        r'^(?:https?://[^/]+)?/historic-hansard/([^/]+)/'
        rf'{year}/{mon}/0?{day}/([^/\s]+)$'
    )
    seen: set[str] = set()
    children = []
    for a in soup.find_all('a', href=True):
        m = pattern.match(str(a['href']))
        if not m:
            continue
        if _norm(m.group(1)) != _norm(house):
            continue
        slug = m.group(2)
        if slug and slug != parent_slug and slug not in seen:
            seen.add(slug)
            children.append((slug, a.get_text(' ', strip=True)))
    return children


def _expand_container_section(sec: dict, delay: float) -> list[dict]:
    """Fetch a container page and return its child section dicts.

    Falls back to the original section dict if no children are found or the
    page cannot be fetched.

    Args:
        sec: Container section metadata dict (from Phase 2).
        delay: Per-request sleep.

    Returns:
        List of leaf section dicts replacing the container.
    """
    year, mon, day = sec['year'], sec['month'], sec['day']
    house, slug = sec['house'], sec['slug']
    url = f'{_BASE_URL}/{house}/{year}/{mon}/{day:02d}/{slug}'
    try:
        r = _get(url, delay)
        if r.status_code != 200:
            tqdm.write(f'  [warn] HTTP {r.status_code} expanding container: {url}')
            return [sec]
        soup = BeautifulSoup(r.text, 'html.parser')
        children = _extract_child_section_links(soup, house, year, mon, day, slug)
        if not children:
            return [sec]
        return [
            {**sec, 'slug': child_slug, 'title': child_title, 'section_type': 'section'}
            for child_slug, child_title in children
        ]
    except Exception as exc:
        tqdm.write(f'  [warn] expand container {url}: {exc}')
        return [sec]


def expand_container_sections(
    sections: list[dict],
    delay: float,
    workers: int,
) -> list[dict]:
    """Expand container sections to their individual child sections.

    Sections whose ``section_type`` is in :data:`_CONTAINER_SECTION_TYPES` are
    fetched in parallel; each is replaced by its child section dicts.  Leaf
    sections pass through unchanged.

    Args:
        sections: Section dicts from Phase 2.
        delay: Per-request sleep.
        workers: Number of parallel workers.

    Returns:
        Expanded list of leaf section dicts.
    """
    containers = [s for s in sections if s.get('section_type') in _CONTAINER_SECTION_TYPES]
    leaves = [s for s in sections if s.get('section_type') not in _CONTAINER_SECTION_TYPES]
    if not containers:
        return sections

    expanded: list[dict] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_expand_container_section, s, delay): s for s in containers}
        for fut in tqdm(
            as_completed(futures),
            total=len(containers),
            desc='Phase 2.5: expand containers',
        ):
            expanded.extend(fut.result())
    return leaves + expanded


# ---------------------------------------------------------------------------
# Phase 3 – download section text
# ---------------------------------------------------------------------------

def _extract_paragraphs(soup: BeautifulSoup) -> list[str]:
    """Extract debate paragraph text from a section page.

    Tries to find a main content wrapper first; falls back to all ``<p>``
    tags in the document.

    Args:
        soup: Parsed HTML.

    Returns:
        Non-empty paragraph strings with whitespace normalised.
    """
    # Try common wrapper selectors in priority order
    content = (
        soup.find('div', class_='section-inner')
        or soup.find('div', id='content')
        or soup.find('article')
        or soup
    )
    paragraphs = []
    for p in content.find_all('p'):
        text = p.get_text(' ', strip=True)
        # Strip column-number markers like "[718]" that appear inline
        text = re.sub(r'\s*\[\d+\]\s*', ' ', text).strip()
        if text:
            paragraphs.append(text)
    return paragraphs


def download_section(sec: dict, delay: float) -> dict | None:
    """Download and parse one section page.

    Args:
        sec: Section metadata dict (from Phase 2).
        delay: Per-request sleep.

    Returns:
        Record dict ready for JSONL serialisation, or ``None`` on failure.
    """
    year, mon, day = sec['year'], sec['month'], sec['day']
    house, slug = sec['house'], sec['slug']
    url = f'{_BASE_URL}/{house}/{year}/{mon}/{day:02d}/{slug}'
    try:
        r = _get(url, delay)
        if r.status_code != 200:
            tqdm.write(f'  [warn] HTTP {r.status_code}: {url}')
            return None
        soup = BeautifulSoup(r.text, 'html.parser')
        paragraphs = _extract_paragraphs(soup)
        return {
            'date': f'{year}-{mon}-{day:02d}',
            'year': year,
            'month': mon,
            'day': day,
            'house': house,
            'title': sec['title'],
            'slug': slug,
            'url': url,
            'paragraphs': paragraphs,
        }
    except Exception as exc:
        tqdm.write(f'  [warn] download {url}: {exc}')
        return None


# ---------------------------------------------------------------------------
# Resumability helpers
# ---------------------------------------------------------------------------

def _load_done_keys(out_dir: Path) -> set[str]:
    """Scan existing JSONL output files and return the set of done section keys.

    The key format is ``"{date}/{house}/{slug}"``.

    Args:
        out_dir: Directory containing ``hansard_*.jsonl`` files.

    Returns:
        Set of already-completed section keys.
    """
    done: set[str] = set()
    for path in out_dir.glob('hansard_*.jsonl'):
        with path.open(encoding='utf-8') as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    done.add(f"{rec['date']}/{rec['house']}/{rec['slug']}")
                except (json.JSONDecodeError, KeyError):
                    pass
    return done


def _section_key(sec: dict) -> str:
    """Compute the resumability key for a section dict.

    Args:
        sec: Section metadata dict.

    Returns:
        Key string matching the format used by :func:`_load_done_keys`.
    """
    y, m, d = sec['year'], sec['month'], sec['day']
    return f'{y}-{m}-{d:02d}/{sec["house"]}/{sec["slug"]}'


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run(
    out_dir: Path,
    start_year: int,
    end_year: int,
    workers: int,
    delay: float,
) -> None:
    """Execute the full three-phase download pipeline.

    Args:
        out_dir: Root output directory.
        start_year: First year to include.
        end_year: Last year to include (inclusive).
        workers: Parallel HTTP workers.
        delay: Per-worker per-request sleep in seconds.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = out_dir / '.cache'
    cache_dir.mkdir(exist_ok=True)

    dates_cache = cache_dir / f'dates_{start_year}_{end_year}.json'
    sections_cache = cache_dir / f'sections_{start_year}_{end_year}.json'
    expanded_cache = cache_dir / f'sections_expanded_{start_year}_{end_year}.json'
    # Phase 1
    if dates_cache.exists():
        print(f'Loading cached dates from {dates_cache}')
        raw = json.loads(dates_cache.read_text())
        dates: list[tuple[int, str, int]] = [tuple(d) for d in raw]  # type: ignore[misc]
    else:
        print(f'Phase 1: discovering sitting dates {start_year}–{end_year} …')
        dates = discover_sitting_dates(start_year, end_year, delay, workers)
        dates_cache.write_text(json.dumps(dates))
    print(f'  {len(dates):,} sitting dates found')

    # Phase 2
    if sections_cache.exists():
        print(f'Loading cached sections from {sections_cache}')
        sections: list[dict] = json.loads(sections_cache.read_text())
    else:
        print('Phase 2: discovering debate sections …')
        sections = discover_sections(dates, delay, workers)
        sections_cache.write_text(json.dumps(sections))
    print(f'  {len(sections):,} sections found')

    # Phase 2.5
    if expanded_cache.exists():
        print(f'Loading cached expanded sections from {expanded_cache}')
        sections = json.loads(expanded_cache.read_text())
    else:
        print('Phase 2.5: expanding container sections …')
        sections = expand_container_sections(sections, delay, workers)
        expanded_cache.write_text(json.dumps(sections))
    print(f'  {len(sections):,} sections after expansion')

    # Phase 3
    done = _load_done_keys(out_dir)
    pending = [s for s in sections if _section_key(s) not in done]
    print(
        f'Phase 3: downloading sections … '
        f'({len(done):,} already done, {len(pending):,} remaining)'
    )

    # Open one output file per year, keyed by year int
    out_files: dict[int, 'TextIO'] = {}
    try:
        for year in range(start_year, end_year + 1):
            path = out_dir / f'hansard_{year}.jsonl'
            out_files[year] = path.open('a', encoding='utf-8')

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(download_section, sec, delay): sec
                for sec in pending
            }
            for fut in tqdm(
                as_completed(futures),
                total=len(pending),
                desc='Phase 3: download sections',
            ):
                result = fut.result()
                if result is None:
                    continue
                fh = out_files.get(result['year'])
                if fh is not None:
                    fh.write(json.dumps(result, ensure_ascii=False) + '\n')
    finally:
        for fh in out_files.values():
            fh.close()  # type: ignore[union-attr]

    print(f'\nDone. Output in {out_dir}')


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Download historic Hansard debates (default 1802-1920) to JSONL.'
    )
    parser.add_argument(
        '--out-dir',
        default=_DEFAULT_OUT_DIR,
        type=Path,
        help=f'Output directory (default: {_DEFAULT_OUT_DIR}).',
    )
    parser.add_argument(
        '--start-year',
        default=_DEFAULT_START_YEAR,
        type=int,
        metavar='YEAR',
        help=f'First year to download, inclusive (default: {_DEFAULT_START_YEAR}).',
    )
    parser.add_argument(
        '--end-year',
        default=_DEFAULT_END_YEAR,
        type=int,
        metavar='YEAR',
        help=f'Last year to download, inclusive (default: {_DEFAULT_END_YEAR}).',
    )
    parser.add_argument(
        '--workers',
        default=_DEFAULT_WORKERS,
        type=int,
        metavar='N',
        help=f'Number of parallel HTTP workers (default: {_DEFAULT_WORKERS}).',
    )
    parser.add_argument(
        '--delay',
        default=_DEFAULT_DELAY,
        type=float,
        metavar='SEC',
        help=f'Sleep between requests per worker in seconds (default: {_DEFAULT_DELAY}).',
    )
    return parser


def main() -> None:
    """Entry point for the Hansard downloader."""
    args = _build_parser().parse_args()
    run(
        out_dir=args.out_dir,
        start_year=args.start_year,
        end_year=args.end_year,
        workers=args.workers,
        delay=args.delay,
    )


if __name__ == '__main__':
    main()
