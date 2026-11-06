"""Shared parser for corpus files using the ICAME/Helsinki-style header format.

Several English historical corpora (CED, CLME Prose, Helsinki Corpus) share
the same file format:

    <B text_id>            name of text file (8 chars: subperiod + type + ID)
    <Q subperiod type id>  text identifier: "1 TRIAL D1TNORFO" etc.
    <N title>              short title
    <A author>             surname first
    <C subperiod>          subperiod number + date range, e.g. "1: 1560-1599"
    <O date>               date of speech event / first printing / publication;
                           may be "speech_year/publication_year", e.g. "1571/1730"
                           — we extract the first (earlier) year
    <K contemporaneity>    SAME | CONTEMP | NON-CONTEMP
    <T text_type>          TRIAL | WITNESS DEPOSITION | COMEDY | DRAMA COMEDY |
                           FICTION | MISCELLANEOUS | HANDBOOK FRE/ENG/GER | …
    <P page_num>           paragraph boundary (page number in source)
    <R line_num>           line reference number (sub-paragraph; skipped)
    <S SAMPLE label>       sample start marker
    (^ ... ^)              font other than the basic font (kept, markers stripped)
    (\\ ... \\)            foreign language (kept, markers stripped)
    [} ... }]              heading (kept, markers stripped)
    [{ ... {]              editorial emendation (kept, markers stripped)
    [\\ ... \\]            editorial comment (stripped entirely)
    [^ ... ^]              corpus compilers' comment (stripped entirely)
    [$ ... $]              running text other than direct speech, e.g. stage
                           directions, speaker IDs (stripped entirely)
    [^..^]                 text on the line omitted (stripped)
    [^---^]                text in the same sentence omitted (stripped)
    #                      line-continuation marker when source line > 70 chars
                           (removed; mid-word splits rejoined without space)

    Others:
    (1) accents: rendered with a grave accent ('`') after the accented letter, e.g. ‘Libertá’ becomes ‘Liberta`’. 
    (2) Abbreviations: a macron above the letters are shown by placing a tilde ('~') after the letter so marked, e.g. frõ -> fro~
    (3) Superscripts: indicated by equals sign ('='), e.g. 'yt' becomes 'y=t=', 'you' becomes 'yo=u='

Paragraphs are delimited by ``<P N>`` tags.  Text between ``<P>`` tags that is not a header line constitutes paragraph content.

Usage:
    from historical_lm.corpus_header import parse_corpus_file
    meta, paragraphs = parse_corpus_file(path, min_para_words=5)
"""
from __future__ import annotations

import re
from pathlib import Path

# Header lines: single uppercase letter tag optionally followed by content
_HEADER_TAG_RE = re.compile(r'^<([A-Z])\s*(.*?)>\s*$')

# Paragraph tag: <P ...> marks the start of a new paragraph
_PARA_TAG_RE = re.compile(r'^<P\b', re.IGNORECASE)

# Sample marker: <S ...>
_SAMPLE_TAG_RE = re.compile(r'^<S\b', re.IGNORECASE)

# Line/running number: <R ...>
_LINE_TAG_RE = re.compile(r'^<R\b', re.IGNORECASE)

# [^ ... ^]  corpus compilers' comment, omitted text markers -> strip entirely
_EDITORIAL_RE = re.compile(r'\[\^.*?\^\]', re.DOTALL)

# [$ ... $]  running text other than direct speech (stage directions, speaker IDs, action descriptions) -> strip entirely
_RUNNING_TEXT_RE = re.compile(r'\[\$.*?\$\]', re.DOTALL)

# [\ ... \]  editorial comment -> strip entirely
_EDIT_COMMENT_RE = re.compile(r'\[\\.*?\\\]', re.DOTALL)

# [{ ... {]  editorial emendation -> keep text, strip markers
_EMEND_RE = re.compile(r'\[\{(.*?)\{]', re.DOTALL)

# [} ... }]  heading -> keep text, strip markers
_HEADING_RE = re.compile(r'\[}(.*?)}]', re.DOTALL)

# (^ ... ^)  font other than the basic font -> keep text, strip markers
_EMPH_RE = re.compile(r'\(\^(.*?)\^\)', re.DOTALL)

# (\ ... \)  foreign language -> keep text, strip markers
_FOREIGN_RE = re.compile(r'\(\\(.*?)\\\)', re.DOTALL)

# #  line-continuation marker (line cut at 70 chars in source):
#    "word# nextpart" -> "wordnextpart"  (mid-word split, no space)
#    "word # next"   -> "word next"     (whole-word break)
_HASH_MIDWORD_RE = re.compile(r'(\w)# (\w)')
_HASH_RE = re.compile(r'# ?')

# Date tag value patterns:
#   "1: 1599"              -> 1599
#   "1560-1599"            -> 1560 (start of range)
#   "1860-1889"            -> 1860
#   "-850"                 -> -850 (negative for BC / Old English)
_DATE_VALUE_RE = re.compile(r'(-?\d{3,4})')


def _parse_date(value: str) -> int | None:
    """Extract the first year-like integer from a header date field."""
    m = _DATE_VALUE_RE.search(value)
    if m:
        return int(m.group(1))
    return None


def _clean_paragraph(text: str) -> str:
    """Strip CED/Helsinki markup from a raw paragraph string.

    Application order matters for nested markup (e.g. [} (^ (\\FINIS.\\) ^) }]):
    inner markers are resolved before outer wrappers are stripped.
    """
    # Strip-entirely blocks first (so their content doesn't leak through)
    text = _EDITORIAL_RE.sub('', text)        # [^ ... ^]  compilers' comments
    text = _RUNNING_TEXT_RE.sub('', text)     # [$ ... $]  non-speech running text
    text = _EDIT_COMMENT_RE.sub('', text)     # [\ ... \]  editorial comments
    # Keep-text blocks: resolve inner markup, then strip outer markers
    text = _FOREIGN_RE.sub(r'\1', text)       # (\ ... \)  foreign language
    text = _EMPH_RE.sub(r'\1', text)          # (^ ... ^)  non-basic font
    text = _EMEND_RE.sub(r'\1', text)         # [{ ... {]  editorial emendation
    text = _HEADING_RE.sub(r'\1', text)       # [} ... }]  headings
    # # line-continuation: mid-word split first, then whole-word breaks
    text = _HASH_MIDWORD_RE.sub(r'\1\2', text)
    text = _HASH_RE.sub('', text)
    # subscripts, accent, abbreviations
    text = text.replace('=', '').replace('`', '').replace('~', '')
    return ' '.join(text.split())


def parse_corpus_file(
    path: Path,
    min_para_words: int = 5,
) -> tuple[dict, list[str]] | None:
    """Parse one corpus file in Helsinki/ICAME header format.

    Args:
        path: Path to the text file.
        min_para_words: Minimum word count for a paragraph to be kept.

    Returns:
        Tuple of (meta_dict, paragraphs), or ``None`` on read error.
        ``meta_dict`` keys: ``text_id``, ``title``, ``author``, ``year``,
        ``period_code``, ``date_orig``, ``text_type``, ``contemporaneity``.
    """
    try:
        raw = path.read_text(encoding='utf-8', errors='replace')
    except OSError:
        return None

    meta: dict[str, str | int | None] = {
        'text_id': path.stem,
        'title': '',
        'author': '',
        'year': None,
        'period_code': '',
        'date_orig': '',
        'text_type': '',
        'contemporaneity': '',
    }

    paragraphs: list[str] = []
    current_lines: list[str] = []
    in_text = False   # True once we've seen at least one <P> or <S> tag

    def _flush_para() -> None:
        if current_lines:
            text = _clean_paragraph(' '.join(current_lines))
            words = len(text.split())
            if text and words >= min_para_words:
                paragraphs.append(text)
        current_lines.clear()

    for line in raw.splitlines():
        stripped = line.strip()

        # Skip empty lines
        if not stripped:
            continue

        # Paragraph boundary
        if _PARA_TAG_RE.match(stripped):
            _flush_para()
            in_text = True
            continue

        # Other structural tags (R, S)
        if _LINE_TAG_RE.match(stripped) or _SAMPLE_TAG_RE.match(stripped):
            # <R> is a line number within a paragraph — not a boundary
            # <S> marks a sample start — treat like a paragraph boundary
            if _SAMPLE_TAG_RE.match(stripped):
                _flush_para()
                in_text = True
            continue

        # Single-letter header tag
        m = _HEADER_TAG_RE.match(stripped)
        if m:
            tag, value = m.group(1), m.group(2).strip()
            if tag == 'B':
                meta['text_id'] = value or path.stem
            elif tag == 'N':
                meta['title'] = value
            elif tag == 'A':
                meta['author'] = value
            elif tag == 'C':
                meta['period_code'] = value
                if meta['year'] is None:
                    meta['year'] = _parse_date(value)
            elif tag == 'O':
                meta['date_orig'] = value
                # <O> gives speech/event date or first printing date,
                # sometimes as "speech_year/publication_year" (e.g. "1571/1730").
                # We extract the first (earlier) year as the language date.
                year = _parse_date(value)
                if year is not None:
                    meta['year'] = year
            elif tag == 'T':
                meta['text_type'] = value
            elif tag == 'K':
                meta['contemporaneity'] = value
            continue
        # Anything else is paragraph content (once we're in the text body)
        if in_text:
            current_lines.append(stripped)

    _flush_para()

    return dict(meta), paragraphs

    
