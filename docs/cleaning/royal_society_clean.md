# Royal Society Cleaning Notes

Source inspected:
- `data/samples/royal_society_preview.jsonl`
- `data/processed/royal_society/part-00000.parquet`

Context:
- The Royal Society corpus is paragraph-based, but many records contain short front-matter fragments, figure/table labels, and OCR-style layout debris mixed into otherwise strong prose.
- Cleaning should be conservative for real article text and more aggressive for tiny structural fragments.

## Summary

The dominant suspicious patterns in `royal_society` are:
- Roman numeral article headings such as `III.` and `VI.`
- title-page and front-matter fragments split into separate short paragraphs
- year-only and dateline-only fragments like `1706.`
- figure/table/formula labels and symbol-heavy scraps
- bracket-split continuations where a paragraph begins with `]` or `)`
- backtick/quote artifacts from OCR or text conversion

This corpus mostly needs structural cleanup rather than deep lexical correction.

## Observed Problematic Patterns

### 1. Roman numeral article headings

Very common short fragments:
- `III.`
- `IV.`
- `VI.`
- `VIII.`

These are usually article-number markers rather than meaningful paragraph text.

Cleaning note:
- Safe to drop when they occur as standalone short paragraphs.

### 2. Front-matter split across multiple tiny paragraphs

Many articles begin with a sequence like:
- article number
- title
- date
- salutation or byline

Examples:
- `III.`
- `Microscopical Observations on the Seeds ...`
- `1706.`
- `To the Honourable the Members of the Royal Society in London.`

Cleaning note:
- Keep the main title in metadata, but many of these tiny front-matter lines do not need to remain as body paragraphs.

### 3. Year-only and dateline-only fragments

Typical examples:
- `1706.`
- `Upminster, March 28, 1706.`
- `Leeds, August 20, 1705.`

Cleaning note:
- Standalone date lines are usually metadata-like and can be dropped from the cleaned paragraph list.

### 4. Figure, table, inscription, and formula-like scraps

These are very frequent and often clearly non-prose.

Examples:
- `f = 5.`
- `Tab = 1.`
- `R *~f= 7.`
- `Philos=Transact No=306 ...`
- `VEIN SIG~.`
- `LEG.`
- `VIIIIAN.`

Cleaning note:
- Drop short symbol-heavy fragments and figure/table labels.
- Be careful not to remove genuine prose paragraphs that merely mention measurements.

### 5. Bracket-split continuation fragments

Some paragraphs begin with a closing bracket or parenthesis, indicating that the previous paragraph was split incorrectly.

Examples:
- `] does remark, that it must be somewhere in Britain ...`
- `) It held more than three Gallons ...`
- `) The Bottle now being again weighed ...`

Cleaning note:
- These should usually be merged into the previous paragraph rather than dropped.

### 6. Backtick and quote artifacts

There are OCR/text-conversion artifacts where quotations begin with backticks.

Examples:
- `` ` ` I am mightily pleased ...``
- `` ` 2d of Sept. 1672.``
- `` ` That the Child was uneasy ...``

Cleaning note:
- Normalize these to ordinary quotation marks or strip the backtick artifacts.

### 7. Duplicate title or byline content in paragraph text

Some paragraphs repeat information already present in metadata:
- article title repeated as the first paragraph
- `By Mr Fra.`
- `Communicated by Dr Douglas, F. R. S.`

Cleaning note:
- If the exact title already exists in metadata, it can often be dropped from the body.
- Very short byline-only paragraphs are usually safe to remove.

### 8. Short lexical/OCR oddities inside otherwise good prose

Examples visible in the sample:
- `Perpeudicular`
- `Lcather`
- `stiled`
- `abovemention'd`

Cleaning note:
- These are real OCR/spelling irregularities, but they are not the main cleanup target.
- A first pass should focus on structural fragments, not aggressive spelling modernization.

## Practical Cleaning Categories

### 1. Safe to drop

- Roman numeral headings
- year-only fragments
- short date lines
- short byline-only paragraphs
- figure/table/formula labels
- symbol-heavy inscription scraps

### 2. Better to merge than drop

- paragraphs starting with `]` or `)`
- short continuation fragments attached to the previous paragraph

### 3. Usually keep

- main prose paragraphs
- historically spelled but readable scientific text
- measurement-heavy prose that is still sentence-like

## Useful Detection Heuristics

Promising signals:
- paragraph matches `^[IVXLCDM]+\\.?$`
- paragraph is only a year
- paragraph contains `Fig`, `Tab`, `Plate`, or `Philos...Transact`
- high symbol density with `~`, `=`, `*`
- paragraph starts with `]` or `)`
- paragraph starts with backticks
- paragraph duplicates the title metadata after normalization

## Caution

Royal Society articles often contain:
- scientific measurements
- named figures and tables inside prose
- older spellings and typography

So cleaning should prefer:
1. removing obvious structural fragments
2. merging obvious split continuations
3. preserving substantive prose even when the spelling is old or slightly noisy
