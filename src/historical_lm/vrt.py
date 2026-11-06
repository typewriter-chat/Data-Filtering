"""Shared utilities for reading VRT (vertical text format) corpus files.

VRT is a line-oriented format used by the IMS Open Corpus Workbench (CWB)
and related tools.  Each token occupies one line with tab-separated fields;
XML-style tags delimit structural units (sentences, paragraphs, texts).

This module provides helpers shared by both the Royal Society Corpus and the
Old Bailey Corpus 2 processing scripts.
"""
from __future__ import annotations

import re

# Match key="value" pairs inside XML-like tag lines
_ATTR_RE = re.compile(r'(\w+)="([^"]*)"')

# Punctuation that should not be preceded by a space when joining tokens
_NO_SPACE_BEFORE = re.compile(r' (?=[,.:;!?\')}\]])')
_NO_SPACE_AFTER = re.compile(r'(?<=\() ')
_APOS_S = re.compile(r" ('s|n't)\b")


def parse_attrs(line: str) -> dict[str, str]:
    """Return a dict of key=value pairs extracted from an XML-like tag line.

    Args:
        line: A line such as ``<text id="123" year="1750" author="Newton">``.

    Returns:
        Dict mapping attribute names to their string values.
    """
    return dict(_ATTR_RE.findall(line))


def tokens_to_text(tokens: list[str]) -> str:
    """Join a list of word tokens into a readable string.

    Removes spaces before common punctuation characters and after opening
    parentheses so the result looks like natural prose rather than a
    space-separated token sequence.

    Args:
        tokens: List of word-form strings, including punctuation tokens.

    Returns:
        Reconstructed text string.
    """
    if not tokens:
        return ''
    text = ' '.join(tokens)
    text = _NO_SPACE_BEFORE.sub('', text)
    text = _NO_SPACE_AFTER.sub('', text)
    text = _APOS_S.sub(r'\1', text)
    return ' '.join(text.split())
