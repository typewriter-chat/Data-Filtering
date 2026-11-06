# Hansard Cleaning Notes

Source inspected:
- `data/samples/hansard_preview.jsonl`
- `data/processed/hansard/part-00000.parquet`

Context:
- The Hansard corpus is already paragraph-based and much cleaner than OCR-heavy corpora like `institutional` or `blbooks`.
- Most of the text is usable as-is.
- Cleaning should therefore focus on light normalization and structural consistency, not aggressive deletion.

## Summary

The `hansard` dataset mostly contains well-formed parliamentary paragraphs, but several recurring suspicious patterns show up:
- editorial section markers such as `§`
- paragraph starts that look truncated or depend on previous context
- titles with HTML/entity artifacts
- punctuation spacing inconsistencies
- em-dash or dash-chaining used as inline discourse separators
- occasional OCR-like or tokenization glitches

This looks more like a corpus needing normalization and segmentation repair than one needing heavy noise removal.

## Observed Problematic Patterns

### 1. Editorial section marker `§`

This is the most common visible pattern in the sample.

Typical examples:
- `§ Mr. Whitbread brought in a bill ...`
- `§ On the order of the day for the third reading ...`
- `§ Motion made, and Question proposed ...`
- `§ Order for Second Reading read.`
- `§ PUBLIC BILLS— First Reading ...`

Observed frequency signal:
- `section_mark`: very frequent in the preview sample

Cleaning note:
- `§` is likely structural/editorial markup rather than natural text.
- It may be worth stripping at the paragraph start while preserving the paragraph content.

### 2. Paragraphs that begin mid-sentence or mid-context

Many paragraphs begin with lowercase verbs or parenthetical fragments, suggesting that:
- the speaker name/header has been dropped
- paragraph segmentation may be imperfect
- some paragraphs depend heavily on prior context

Typical examples:
- `informed the house, that his Majesty had been waited upon ...`
- `rose to move for leave to bring in a bill ...`
- `moved for leave to bring up a petition ...`
- `said, that though some of the gentlemen near him ...`
- `(who was not in the house when Sir William Scott made the above motion) said ...`

Observed frequency signal:
- `leading_lower_or_paren`: common in the preview sample

Cleaning note:
- These are not necessarily bad paragraphs, but they are suspicious as standalone units.
- They may be acceptable for pretraining, but they are worth flagging if paragraph independence matters.

### 3. Em-dash chaining used as inline separator

The corpus frequently uses em-dashes or dash-like sequences to join procedural statements.

Typical examples:
- `alone.—The bill was then ordered ...`
- `accordingly.—Sir P. Stephens presented ...`
- `expense.—The Chancellor of the Exchequer said ...`

Cleaning note:
- This is not pure noise, but it may benefit from normalization.
- Depending on downstream use, you may want either:
  - to keep the dash as historical punctuation, or
  - split these into separate paragraphs/sentences when they clearly mark discourse transitions.

### 4. Punctuation spacing inconsistencies

There are many spacing irregularities around punctuation.

Typical examples:
- `Mr. Creevey , made his protest ...`
- `Habeas Corpus Act , and the other ...`
- `their, own expense`
- `Prince "William of Gloucester`

Observed frequency signal:
- `spacing_punct`: common

Cleaning note:
- This is a good target for light normalization:
  - remove space before commas/periods/semicolons/colons
  - repair some quote spacing
  - preserve historical spelling while fixing obvious tokenization artifacts

### 5. HTML/entity artifacts in titles and metadata-like strings

Some titles still contain HTML entity fragments instead of decoded punctuation.

Typical examples:
- `CURATES RELIEF BILL.&#x2014;`
- `STEPENDIARY CURATES BILL.&#x2014;`
- `FORTIFICATION OF LIVERPOOL.&#x2014;`
- `IRISH HABEAS CORPUS AND MARTIAL LAW BILL.&#x2014;`
- `EXCHESUER BILLS.&#x2014;`

Cleaning note:
- Titles should probably be HTML-decoded before downstream use.
- This is especially easy to fix and high-value because it affects metadata consistency.

### 6. Occasional OCR-like or token corruption

Hansard is much cleaner than OCR corpora, but a few paragraphs still show suspicious token-level errors.

Representative examples:
- `I hem` instead of `them`
- `to morrow` / similar split forms in context where normalization may be useful
- `P Stephens` where punctuation may be inconsistent
- stray numerals inside prose such as `60 such fortifications`
- `Pie` where `He` is likely intended
- `rind` where `find` is likely intended
- `die right hon. gent.` where `the` is likely intended

Cleaning note:
- These are relatively rare.
- They should be handled conservatively, because some split forms such as `to-morrow` or older parliamentary typography may be historically valid.

### 7. Broken line-wrap or pagination artifacts inside words

In the broader sample there are occasional signs that line-break or page-layout noise has been flattened into the paragraph text.

Typical examples:
- `com- 320 mittee`
- `re- 499 solved`
- `mo- 643 tion`
- `the; Army of Reserve`
- `to; him`

Cleaning note:
- These look like OCR/page-layout joins rather than genuine spelling.
- They are good candidates for light normalization, especially when a digit is embedded inside a broken word.

### 8. Formulaic procedural boilerplate

The corpus has a large amount of repeated parliamentary formulae.

Common openings include:
- `§ Motion made, and Question proposed ...`
- `§ Order for Second Reading read.`
- `§ House in Committee ...`
- `§ Brought from the Commons; read ...`
- `§ Report to lie upon the Table ...`

Cleaning note:
- This is not noise, but it is highly repetitive boilerplate.
- Depending on corpus design, you may want to:
  - keep it as part of parliamentary language, or
  - downweight/filter some procedural material if you want more substantive debate content.

### 9. Single-paragraph records that may actually contain multiple discourse units

Some records have only one paragraph, but that paragraph internally contains several procedural events joined together.

Representative example:
- `... the Speaker attended in the house of lords, and on his return informed the commons ...`

Cleaning note:
- If you want cleaner paragraph segmentation, some long procedural paragraphs may need splitting on strong separators like `.—` or repeated event transitions.

## Practical Cleaning Categories

### 1. Safe normalization

- remove leading `§`
- decode HTML entities in titles and text
- normalize spaces before punctuation
- normalize repeated dash patterns if needed

### 2. Review / optional normalization

- lowercase-start paragraphs
- parenthetical-start paragraphs
- long procedural paragraphs containing multiple event units
- repetitive procedural boilerplate
- flattened line-wrap artifacts with embedded page numbers

### 3. Rare anomaly cleanup

- token glitches like `I hem`
- suspicious in-paragraph numerals
- malformed speaker abbreviations or punctuation around names
- titles containing HTML entities or spelling distortions

## Useful Detection Heuristics

Promising signals for Hansard:
- paragraph begins with `§`
- paragraph begins with lowercase letter
- paragraph begins with `(`
- title contains `&#...;`
- space before punctuation
- dash sequence such as `.—`
- extremely short procedural boilerplate paragraphs

## Caution

Hansard is not primarily an OCR-noise corpus.
Cleaning should be light-touch.

The safest first pass is:
1. strip editorial markers like `§`
2. decode HTML entities
3. normalize punctuation spacing
4. leave speaker/context structure mostly intact
5. only then consider whether some procedural boilerplate or multi-event paragraphs should be re-segmented
