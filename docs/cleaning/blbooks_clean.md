# BL Books Cleaning Notes

Source inspected:
- `data/samples/blbooks_preview.jsonl`
- `data/processed/blbooks/part-00000.parquet`

Context:
- The BL Books corpus is page-based OCR.
- The dominant issue is not library-stamp noise like `institutional`, but OCR distortion inside otherwise useful prose.
- Cleaning should therefore focus on page-layout leakage, fragmentary pages, and OCR corruption patterns without over-deleting real historical spelling.

## Summary

The `blbooks` corpus contains many pages with real prose, but the OCR is noisy in consistent ways:
- broken intra-word spacing
- page numbers and running heads mixed into text
- isolated fragment pages
- OCR confusions that resemble long-s / historical typography errors
- title, contents, appendix, and catalogue pages that are structurally unlike normal prose

This corpus should be cleaned more conservatively than `institutional`. Much of the text is salvageable, but many page openings and short fragments need filtering or normalization.

## Observed Problematic Patterns

### 1. Broken words caused by OCR spacing

This is the most common visible issue.

Typical examples:
- `redun dancy`
- `de stroy`
- `com posed`
- `ap pearance`
- `Theo bald`
- `Mi ramont`
- `un doubtedly`
- `in debted`
- `de ceived`
- `pre sume`
- `them selves`
- `exist ed`

Representative examples from sample pages:
- `the redun dancy of a syllable does not necessarily de stroy the metre`
- `it has more the ap pearance of originality`
- `Mr. Steevens, whose observations on it I shall transcribe`

Cleaning note:
- This is a strong candidate for normalization.
- However, merges should be conservative, because not every short-token boundary is an OCR error.

### 2. Page-number leakage at the beginning of pages

Many pages begin with page numbers that are kept as part of the text.

Typical examples:
- `15 Page 60. ...`
- `23 Late means lately. ...`
- `14 Blest is the Man ...`
- `38 NOTES. ...`
- `NOTES 58 ...`
- `NOTES 62 ...`

Observed in the sample:
- many page previews begin with a number or number-plus-header
- there are also pages consisting only of a number such as `17`

Cleaning note:
- standalone numeric pages should be dropped
- leading page numbers and page-number-plus-header prefixes should usually be stripped before paragraph extraction

### 3. Running heads and section headers mixed into prose

The OCR frequently preserves running heads from page tops.

Typical examples:
- `MISCELLANEOUS PIECES. II.`
- `LORD CHESTERFIELD'S`
- `NOTES`
- `PART I`
- `BOOK I`
- `APPENDIX`
- `POEMS`
- `POEMS.`

Representative sample:
- `LORD CHESTERFIELD'S 14 Having thus demonstrated ...`
- `MISCELLANEOUS PIECES. II. ...`

Cleaning note:
- many of these should be stripped as page-top headers rather than dropping the whole page
- some, such as `APPENDIX` or `BOOK I`, may be meaningful section boundaries and can be preserved separately if desired

### 4. Isolated tiny fragments and non-pages

Some pages are effectively unusable as prose.

Typical examples:
- `17`
- `.`
- `I`
- `a`
- `1`
- `-`
- `'`

Frequency signal from the preview:
- many first lines are single characters or punctuation
- short fragments are common enough that a simple minimum-length rule would help

Cleaning note:
- pages or paragraphs that are only a page number, punctuation mark, or one-token fragment should usually be removed

### 5. OCR confusions from historical print / long-s style

The text often contains OCR substitutions that likely come from old typefaces and long-s confusion.

Typical examples:
- `meafur'd` for `measur'd`
- `fame` where `same` is intended
- `fick` for `sick`
- `foould` / `sore` / similar letter-substitution noise
- `Skil` for `Skill`

Representative sample:
- `Whose meafur'd Passions reach no wild Excess`
- `the effects are the fame`
- `fick man's salve`

Cleaning note:
- these are OCR corrections rather than filtering targets
- corrections should be selective, because some archaic spellings are genuine and should remain

### 6. Hyphenation and word-breaking at line/page boundaries

Although the sample shows intra-word spaces more often than explicit line-end hyphens, page OCR still contains boundary artifacts such as:
- broken words split across visual lines
- interrupted compounds
- occasional hyphenation preserved inconsistently

Representative examples:
- `im- possible`
- `impor tant`
- `ad mitted`
- `ex- pose`

Cleaning note:
- line-level normalization should happen before paragraph segmentation if possible
- careful dehyphenation is likely beneficial

### 7. Title pages, notes pages, contents pages, and catalogue matter

Not every page is running prose. Some belong to paratextual material such as:
- title pages
- notes pages
- appendices
- publisher catalogues
- poem headers / section headers

Representative examples:
- `Catalogue of Messrs Blackwood & Sons' Publications`
- `NOTES`
- `APPENDIX`
- `TO MY WIFE`
- `TO MY MOTHER`

Cleaning note:
- these are not necessarily noise, but they are structurally different from prose
- depending on the downstream corpus design, they may be filtered, downweighted, or retained with lighter cleaning

### 8. OCR garbage inside otherwise real content

Some pages contain text-like material that is still too corrupted to trust.

Representative examples from sample pages:
- `Sjcdye`
- `soould`
- unusual punctuation runs such as `glows ,•`
- malformed or implausible word forms inside otherwise fluent text

Cleaning note:
- this is harder than header stripping
- useful signals include abnormal character sequences, symbol-heavy tokens, and very high rates of split words

### 9. Mixed header-number-prose openings

A very common BL Books pattern is:
- page header
- page number
- immediate continuation of prose

Typical examples:
- `LORD CHESTERFIELD'S 14 Having thus demonstrated ...`
- `NOTES 58 wish it to be so ...`
- `20 HISTORY OF WISCONSIN. ...`

Cleaning note:
- these should usually be normalized by removing the header and page number prefix, not by dropping the page

### 10. Format A running heads not stripped after leading number removal

The most common BL Books page layout is:

    NUMBER RUNNING_HEAD. prose continues...

For example:
- `38 NOTES. free-thinkers, all who profess...`
- `18 PRELIMINARY ESSAY. admit a bare possibility...`
- `3 PREFACE. n^HE writing of Characters was...`
- `10 HISTORY OF ENGLAND. chap, formidable princes...`
- `209 THE SCARBOROUGH TOUR. of contention, no land...`

The current `_LEADING_NUMBER_RE` strips the leading number, but leaves the all-caps running head prefix. After stripping `38 `, we get `NOTES. free-thinkers...` where `NOTES.` is unwanted noise.

The secondary strip fires on approximately 4,400 pages in a 56k-page sample (about 8%).

Cleaning note:
- After stripping a leading number, apply a secondary regex `^([A-Z][A-Z\'&,\- ]{2,60})\.\s+(?=[a-z])` to strip the remaining all-caps running head.
- The period (`.`) is excluded from the character class to prevent greedy matching across multiple heading segments (e.g. `MISCELLANEOUS PIECES. II.`).
- Only fire this secondary strip when a leading number was actually removed, to avoid stripping legitimate section-title page openings like `PREFACE. of considerable...` that appear without a leading number.

### 11. Spaced-letter OCR titles

Some title pages and chapter headings have letters typeset with wide tracking or placed as individual characters across the line. OCR preserves this spacing as spaces between each letter:

Typical examples:
- `THE FIRST EPISTLE O F T H E FIRST BOOK O F HORACE.`
- `POSTSCRIPT, CONTAINING OBSERVATIONS O N T R A G E D Y.`
- `P O E M S.`
- `O B SERVATI ONS O N T H E Nineteenth Book`
- `LISTS O F The BARONS or BURGESSES of PARLIAMENT`

The distinguishing signal is isolated uppercase letters separated by single spaces (`O F`, `T H E`, `O N`).

Cleaning note:
- These are almost always title-page matter or chapter headings, not prose.
- They appear at fewer than 100 chars in most cases and are removed by the minimum-length filter below.
- No targeted regex is needed at this stage; the length filter is sufficient.

### 12. Minimum paragraph length for training quality

After prefix stripping and normalisation, many surviving blocks are too short to be useful pretraining text:

- Chapter / section titles: `'POETICAL ESSAYS.'`, `'THE FLEECE BOOK I.'`
- Verse section headings: `'MARGARET NICHOLSON.'`, `'DRAMATIC POEMS.'`
- Copyright notices: `"ENTERED AT STATIONERS' HALL."`
- OCR-spaced titles: `'P O E M S.'`

In a 56k-page sample, about 10% of kept paragraphs are under 100 characters.

Cleaning note:
- Apply a hard minimum of **100 characters** after normalisation (`_MIN_PARAGRAPH_CHARS = 100`).
- This is applied at the paragraph level inside `_extract_paragraphs`, after the prefix-strip and word-join passes, matching the approach used in the `institutional` cleaner.
- Drawback: genuinely short prose sentences (~10–15 words) are dropped. Acceptable for this corpus where real paragraphs are nearly always longer.

## Practical Cleaning Categories

### 1. Drop directly

- pages that are only a number
- punctuation-only fragments
- one-token or near-empty OCR scraps
- pages dominated by non-linguistic junk
- paragraphs under 100 characters after all normalisation

### 2. Strip prefix / normalize, then keep

- page numbers at the start of a page (`_LEADING_NUMBER_RE`)
- running heads such as `NOTES`, `APPENDIX`, `LORD CHESTERFIELD'S` preceding page numbers (`_LEADING_HEADER_WITH_NUMBER_RE`)
- Format A pages: strip leading number, then strip remaining all-caps running head before lowercase prose
- intra-word OCR spaces (`_join_split_words`)
- line-break hyphenation artifacts

### 3. Review before deciding

- title pages
- poems / section headings
- catalogues and publisher matter
- notes pages

## Useful Detection Heuristics

Promising signals for BL Books:
- page begins with `^\d+\s+`
- page begins with `^[A-Z .'-]+ \d+\s+`
- high rate of short lowercase tokens followed by another lowercase token inside a word-like context
- page length below a small threshold
- first line is all caps and short
- page contains many split-word patterns
- punctuation-heavy OCR tokens such as `,•`
- isolated single uppercase letters separated by spaces (spaced-letter OCR)
- paragraph under 100 characters after normalisation

## Caution

This corpus contains genuine historical spelling, verse formatting, and typography.
Cleaning should not aggressively modernize the text.

The safest first pass is:
1. remove obvious non-pages and page-number-only fragments
2. strip running heads and page-number prefixes, including Format A secondary strip
3. normalize obvious OCR word splits
4. apply 100-character minimum paragraph length filter
5. leave deeper lexical correction for a later pass
