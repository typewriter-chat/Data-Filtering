# Institutional Cleaning Notes

Source inspected:
- `data/samples/institutional_preview.jsonl`

Context:
- The downstream unit is paragraph-level pretraining text.
- Some paragraph boundaries are likely wrong, so cleaning should cover both removing bad paragraphs and flagging places where adjacent paragraphs may need to be merged or re-split.

## Summary

The `institutional` corpus contains a large amount of non-content OCR text that appears to come from:
- library ownership and provenance stamps
- title pages and catalog-card style front matter
- shelf marks and call numbers
- gift, transfer, and bequest notes
- OCR-corrupted institutional logos and mottos

These are often formatted as short line-broken blocks and can easily survive naive paragraph extraction even when they are not useful pretraining text.

## Observed Problematic Patterns

### 1. Library stamp / ownership paragraphs

Highly repeated lines include:
- `HARVARD COLLEGE LIBRARY`
- `HARVARD LAW LIBRARY`
- `WIDENER LIBRARY`
- `COUNTWAY LIBRARY`
- `LITTAUER LIBRARY`
- `SCIENCE CENTER LIBRARY`
- `SCIENCE LIBRARY`

Typical examples:
- `HARVARD COLLEGE / VERO TAS / SCIENCE CENTER LIBRARY`
- `GODFREY LOWELL CABOT / SCIENCE LIBRARY / HARVARD COLLEGE LIBRARY`
- `HARVARD LAW LIBRARY GIFT OF ...`

Cleaning note:
- These should usually be removed entirely when they form standalone paragraphs or short blocks with little to no prose content.

### 2. Motto / seal fragments with OCR variation

Common repeated fragments include:
- `VERI TAS`
- `VE RI TAS`
- `VERO TAS`
- `CHRISTO`
- `HARVARDIAN`
- `ACADEMIAE`

These often appear as broken logo text rather than meaningful content, and OCR introduces many variants.

Cleaning note:
- Treat these as high-confidence non-content when they appear in short all-caps institutional blocks.
- Avoid removing them blindly if they occur inside real prose, but that seems unlikely in this corpus.

### 3. Call numbers, shelf marks, barcodes, accession strings

Typical examples:
- `QA 27 J3 S5 1914`
- `BM 535 .G3`
- `3 2044 031 718 562`
- `GE 728 PS`
- `QA 529 L43 1858`

These often occur alone or mixed with library stamp text.

Cleaning note:
- Standalone paragraphs dominated by alphanumeric identifiers, shelf marks, or accession numbers should usually be dropped.

### 4. Gift / bequest / transfer / provenance notes

Typical patterns:
- `GIFT OF ...`
- `FROM THE BEQUEST OF ...`
- `FROM THE LIBRARY OF ...`
- `TRANSFERRED TO ...`
- `NEDL TRANSFER`
- `FROM by Exchange`

These are common and are not usually useful for language-model pretraining.

Cleaning note:
- Drop when they form short standalone paragraphs or title-page metadata blocks.
- If they are embedded inside a larger OCR block, the full block should likely be removed.

### 5. Title-page and front-matter layout treated as paragraphs

Typical structure:
- title in all caps or title case
- author line
- publisher/year fragments
- institution stamp on the next lines
- large amounts of line breaks rather than prose sentences

Representative examples:
- `A HISTORY / OF / JAPANESE MATHEMATICS / SMITH AND MIKAMI`
- thesis/title-page style paragraphs such as `A THESIS ... Presented to the Faculty ...`

Cleaning note:
- Not all title-page material should necessarily be deleted, but many of these blocks are not paragraph-like prose.
- For pretraining, they may be lower priority or removable depending on the desired corpus style.

### 6. Very short non-prose fragments

Examples:
- isolated numbers
- one-line shelf marks
- single institutional phrases
- tiny OCR scraps such as `ON`
- near-empty previews

Observed in the preview:
- around 10k sampled rows had empty previews
- many more were extremely short and dominated by metadata-like strings

Cleaning note:
- Very short paragraphs with low alphabetic content or no sentence structure should be filtered aggressively.

### 7. OCR-corrupted mixed-script or symbol-heavy noise

Examples include:
- corrupted Latin words
- mixed scripts in the same short block
- stray symbols and malformed characters

Representative sample:
- a preview containing Tibetan-like or corrupted Unicode mixed with library metadata

Cleaning note:
- Paragraphs with extreme symbol noise or implausible script mixing should be flagged as OCR artifacts.
- This likely needs heuristic scoring rather than exact string matching.

### 8. Broken institutional words caused by OCR

Examples from the preview:
- `VERI TASK`
- `ARVARDIANS`
- `ECCLESIE HARVARD`
- `CADEMIAE`
- `HARV toi`-style partial fragments

Cleaning note:
- These are usually not worth repairing.
- If they appear inside clearly non-content institutional blocks, drop the whole block instead of trying to normalize the words.

## Boundary Problems Relevant To Paragraph Cleaning

Because the processing unit is paragraph, structural errors matter in addition to OCR noise.

Potential paragraph-boundary issues to watch for:
- a valid paragraph split into several short line-broken fragments
- title-page or stamp blocks split into multiple adjacent "paragraphs"
- a paragraph ending in a hyphen or without sentence closure, followed by a lowercase-start paragraph
- very short middle paragraphs sandwiched between two prose-like paragraphs

Suggested treatment:
- separate `drop` rules from `merge/re-split` rules
- do not rely on OCR score alone
- inspect adjacent paragraphs when a paragraph is short but not obviously removable

### 9. Table-of-contents entries with dot leaders

Government bulletins and technical monographs often have a detailed TOC that OCR preserves verbatim. Each entry is short and ends with dot-leader fill before the page number.

Typical examples:
- `Physiography....`
- `Pre-Cambrian rocks..`
- `Cambrian system. . . .`
- `Archean system....`
- `Metamorphism of the limestone and paragenesis of its minerals.....`

The distinguishing signal is two or more trailing dots (consecutive or spaced).

Cleaning note:
- Entries of ≤ 15 words ending with `(?:\.\s*){2,}` can be dropped confidently as `toc_entry`.

### 10. Short single-period headings without dot leaders

A subset of TOC entries and section headings end with a single `.` rather than dot leaders. These are hard to distinguish from real short sentences on a per-block basis.

Typical examples (all from TOC):
- `General character.`
- `Historical sketch.`
- `Boston Basin.`
- `Cheshire quartzite.`
- `SOURCES OF MATERIAL.`

Cleaning note:
- A minimum character-length filter applied post-merge catches nearly all of these (they are typically < 60 characters). See §15 below.
- Pure punctuation blocks (`"`, `.`, `---`, `—`) are dropped as `punct_only` before any word-count check.

### 13. OCR page numbers bleeding into prose blocks

Institutional books are scanned page by page. The page number printed at the bottom or top of one page sometimes appears at the start of the next OCR block:

- `15 in a zone where the ancient unfolded rocks...`
- `4 Geology of the Green Mountains in Massachusetts:`

The leading number is not prose and can prevent the block from merging with its preceding sentence.

Cleaning note:
- Strip a leading 1–4 digit number followed by whitespace with `^\d{1,4}\s+` before classification. This converts a leading page number into a lowercase-start block that the merge pass can then join with the preceding block.

### 14. Long page-number tables from OCR of TOC pages

Detailed TOC pages list section names next to page numbers. In some government bulletins the page column runs to many entries and the OCR concatenates them as a long digit string, sometimes with non-ASCII digit characters from other Unicode blocks (Khmer, Arabic-Indic, etc.):

- `39 39 ន ន ន ន ន ន 39 39 40 40 40 40 41 41...` (59 words)
- `161 161 164 164 165 165 165 165 167 168...` (53 words)

These exceed the naive `words ≤ 50` threshold of the digit-ratio check.

Cleaning note:
- Raise the strong-digit-ratio word-count guard to 100: `digit_ratio > 0.3 and words ≤ 100`.

### 15. Minimum paragraph length for training quality

After all structural filters and the merge pass, many surviving blocks are still too short to be useful pretraining text. Short survivors include:

- Broken OCR sentence halves that could not be merged (uppercase continuation)
- Short section headings with single trailing period
- Footnote markers, bylines, location tags

Cleaning note:
- Apply a hard minimum of **100 characters** after the merge pass (`_MIN_PARA_CHARS = 100`).
- This filter is placed post-merge so that short continuation fragments have the chance to be joined with their preceding prose before being evaluated.
- Drawback: legitimate short sentences (~10-15 words) are also dropped. This is an acceptable loss for academic/institutional prose where real paragraphs are almost always longer.

### 11. Invisible Unicode characters from OCR

Zero-width spaces and other invisible Unicode characters occasionally appear in OCR output, either from the scanning software or from garbled metadata:

Typical examples:
- `\u200bBEEWEESON FOTOZI POITTO BRATKINI TEAM LEVON Ters`
- `文\u200b起`
- `\u200bF. O. BOWER, SD FRS`

The characters are visually invisible but cause standard whitespace normalization to miss them.

Relevant code points: `U+200B` (zero-width space), `U+200C` (zero-width non-joiner), `U+200D` (zero-width joiner), `U+FEFF` (BOM), `U+00AD` (soft hyphen), `U+2028` (line separator), `U+2029` (paragraph separator).

Cleaning note:
- Any short block (≤ 30 words) containing at least one of these code points should be dropped as `symbol_noise`.

### 12. "IN MEMORY OF" and similar provenance notes

Books donated to Harvard libraries often carry memorial inscriptions:

- `IN MEMORY OF LAURENCE AND ELLA FINE`
- `IN MEMORY OF`
- `The bequest of Mrs. Eliza Farrar in memory of her husband...`

These are provenance annotations, not book content.

Cleaning note:
- Extend `_PROVENANCE_RE` to include `IN\s+MEMORY\s+OF` alongside existing `GIFT OF`, `BEQUEST`, etc.

## Practical Cleaning Categories

The current evidence supports these categories:

**Pipeline order matters**: short continuation fragments must survive the pre-merge pass so they can be rejoined before the final length filter is applied.

1. Pre-merge: drop high-confidence non-prose (these are safe to drop before merging because they cannot be prose continuations)
   - library stamps (LIBRARY, HARVARD, COLLEGE, UNIVERSITY, …)
   - shelfmark / barcode paragraphs
   - provenance and bequest notes (GIFT OF, BEQUEST, FROM THE LIBRARY OF, IN MEMORY OF, …)
   - empty blocks and pure-punctuation blocks (`punct_only`)
   - motto/seal fragments (VERITAS, CHRISTO, ACADEMIAE, …)
   - symbol-heavy OCR garbage including invisible Unicode
   - table-of-contents entries with dot leaders (two or more trailing dots)

2. Merge continuation fragments (lowercase-start or hyphen-broken blocks joined to the preceding block)

3. Post-merge: minimum length filter
   - Drop paragraphs with fewer than 100 characters (`_MIN_PARA_CHARS = 100`)
   - This replaces the earlier per-block rules for single tokens, short fragments, and short caps fragments

4. Review or score before dropping (not yet implemented, left for a later pass)
   - title-page material
   - dedication pages
   - advertisements or catalog-like matter
   - multi-word section headers without institutional keywords

## Useful Detection Heuristics

Promising paragraph-level features:
- high uppercase ratio
- many short lines
- presence of repeated institutional keywords such as `LIBRARY`, `HARVARD`, `GIFT`, `BEQUEST`, `TRANSFER`
- high digit/alphanumeric-code density (with word-count guard raised to 100 for long number tables)
- low sentence-like punctuation density
- trailing dot leaders `(?:\.\s*){2,}` → TOC entry
- high symbol or mixed-script ratio
- presence of invisible Unicode characters
- final character count < 100 → drop post-merge
- strong overlap with a curated stoplist of repeated institutional lines

## Examples To Preserve Caution

Not everything non-prose should automatically be removed:
- real titles
- author names
- publication statements
- some scholarly front matter may still be linguistically useful

So the first cleaning pass should prioritize:
- obviously non-content institutional blocks
- repeated ownership metadata
- code-like identifier paragraphs

And leave more ambiguous front matter for a later decision.

---

## Deep Cleaning Pass

Findings from a quality inspection of the first-pass output (`data/clean/institutional_clean/`). These issues are addressed by a separate second-pass script (`clean_processed/institutional_deep.py`) that reads the already-cleaned parquet files.

### D1. OCR open-quote rendered as "66 66"

Old typography OCR renders open double-quotes (`"`) as `66` and close double-quotes as `99`. These leak into otherwise valid paragraphs as bare repeated tokens:

- `C. VII. 1" Or know ye not... 66 66 66 66 over a man as long as he liveth?`
- `8" himself; ... 66 66 66 66`

The pattern is always 2 or more consecutive `66` or `99` tokens (not adjacent to other digits). They appear at column-margin positions in books with inline scripture or legal quotations.

Cleaning note:
- Strip sequences of two or more consecutive bare `66` or `99` tokens using `(?<!\d)(?:66|99)(?:\s+(?:66|99))+(?!\d)`.
- Do not strip isolated `66` or `99` (could be a year, page number, or section heading).
- Apply as a normalization step before re-evaluating paragraph length, since stripping may push a paragraph below `_MIN_PARA_CHARS`.

### D2. Number-table prefix merged with prose

The digit-ratio guard (`digit_ratio > 0.3 and words ≤ 100`) does not catch cases where a short number table is followed by a long prose sentence. The merge pass joins them into one paragraph that then passes the length filter:

- `100 100 100 100 100 100 It thus appears that the yield from succession duties...`
- `No Return www www www 232 www 899,425 224,244 511,014 www www 2. www www www ...`

The leading number run is visually distinct: three or more identical (or near-identical) short tokens before the first alphabetic word.

Cleaning note:
- Strip a leading sequence of 3+ digits-or-`www` tokens with `^\s*(?:\d[\d,]*|www)(?:\s+(?:\d[\d,]*|www)){2,}\s+`.
- After stripping, re-apply the minimum length filter.
- If the remaining text is too short, drop the paragraph rather than keeping an orphaned fragment.

### D3. Uncaught all-caps structural blocks

Chapter headings, edition notices, and TOC-style summary strings survive the first pass because they exceed 100 characters and contain no institutional keywords. Examples:

- `BOOK III PUBLIC REVENUE (continued) THE PRINCIPLES OF TAXATION CHAPTER I DEFINITION AND CLASSIFICATION OF TAXATION § 1. ...`
- `THIRD EDITION, REVISED AND ENLARGED London MACMILLAN AND CO., LIMITED NEW YORK: THE MACMILLAN COMPANY`
- `SHIPS AND NAVIGATION OF THE ANCIENTS. ROMAN COMMERCE IN THE MEDITERRANEAN. CORN TRADE BETWEEN ALEXANDRIA AND PUTEOLI.`

Distinguishing signals:
- Uppercase ratio > 0.55 of alphabetic characters.
- Contains structural keywords: `CHAPTER`, `BOOK`, `VOLUME`, `PART`, `EDITION`, `SECTION`, `APPENDIX`.
- OR is a dot-separated list of topic phrases (no verb, no pronoun, no article sequence).

Cleaning note:
- Drop paragraphs where `upper_ratio > 0.55` AND a structural keyword is present.
- As a fallback, drop paragraphs where `upper_ratio > 0.7` regardless of keywords (nearly pure caps blocks that are too long for the first-pass caps-fragment filter).
- Preserve legitimate all-caps legal case names, titles used in prose, etc. by requiring the block to end without sentence-closing punctuation (`[.!?]`) OR to contain no lowercase run of 4+ consecutive words.

### D4. Duplicate books

The same book can appear in multiple shards. A random sample from 5 shards found identical paragraphs appearing 3 times from the same source. Deduplication should happen at the book level, not the paragraph level, to avoid partial-book duplicates.

Investigation findings:
- Duplicates originate in the raw data (confirmed present before any cleaning step). The same physical book was held by multiple Harvard libraries and scanned independently.
- Deduplication by `(title_src, author_src)` alone is incorrect: it collapses multi-volume works. Examples found across 5 shards: "The Works of Alexander Hamilton" x22, "The Writings of Henry David Thoreau" x20, "The Works of William Shakespeare" x14. These are distinct volumes sharing a series title, not duplicate scans.
- True scan duplicates can be distinguished by also matching `year` and checking that `page_count_src` values are within ~15% of each other.

Status: **deferred**. D4 is not implemented in `institutional_deep.py`. A correct dedup pass requires the stricter `(title_src, author_src, year, ~page_count_src)` criterion and needs to operate across all shards simultaneously.
