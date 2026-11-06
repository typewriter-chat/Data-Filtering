# CLMET Cleaning Notes

Source inspected:
- `data/processed/clmet/part-00000.parquet` (sample docs)
- Full survey across all 6 shards (281 docs, 306,417 paragraphs)

Context:
- CLMET (Corpus of Late Modern English Texts) contains 281 literary and non-fiction texts from
  c. 1710–1920 covering sermons, novels, letters, philosophical treatises, plays, and essays.
- Each row is one text with a `paragraphs` list. Paragraphs were extracted preserving original
  layout, which means structural front-matter items appear as separate paragraphs.
- The main cleaning targets are structural layout fragments in the front matter and body, plus
  inline OCR footnote markers embedded in prose paragraphs.

## Summary

Main problematic patterns (counts across all 6 shards):
- Short all-caps stubs ≤8 words: 7,823
- Section headers (SERMON/CHAP/PART etc.): 4,560
- Asterisk separator lines: 2,246
- Letter headers (LETTER I, II, ...): 708
- Letter salutation stubs: 112
- Scripture reference lines: 38
- City/publisher imprint lines: 28+6 = 34
- Bylines: 24
- Errata blocks: 6
- Inline `} N {` footnote artifacts: 601 paragraphs (normalize, don't drop)

Total estimated drop: ~15,500 paragraphs (~5% of corpus).

## Observed Problematic Patterns

### 1. Section headers

Short structural markers for sermons, chapters, parts, sections, books.

Examples:
- `SERMON I. UPON HUMAN NATURE .`
- `SERMON III .`
- `CHAP . I .`
- `CHAP . II .`
- `PART I .`
- `SECTION III .`
- `BOOK IV .`

Cleaning note:
- Drop when paragraph starts with `SERMON|CHAP(TER)?|SECTION|PART|BOOK` + roman/arabic numeral
  and is ≤20 words.

### 2. Letter chapter headers

Epistolary texts label sections with roman-numeral letter markers.

Examples:
- `LETTER I`
- `LETTER II`
- `LETTER III`

Cleaning note:
- Drop when starts with `LETTER` + roman numeral and ≤6 words.

### 3. Short all-caps stubs (≤8 words)

A wide range of layout markers appear as all-uppercase fragments.

Examples:
- `THE PREFACE .`
- `FINIS .`
- `ERRATA .`
- `MEN .` / `WOMEN .` (table column headers from population tables)
- `THE FORTUNATE FOUNDLINGS .` (repeated title)
- `CHAP . IV .` (also caught by §1)
- `MY LORD ,` / `SIR ,` / `MADAM ,` (letter salutations)

Cleaning note:
- Drop any paragraph that is ≤8 words and consists entirely of uppercase letters,
  spaces, punctuation, and hyphens.
- This covers titles, section ends, table headers, and salutation stubs.

### 4. Asterisk separator lines

Horizontal dividers made of asterisks or mixed whitespace/asterisks.

Examples:
- `* * * * *`
- `* * * *`

Cleaning note:
- Drop any paragraph that matches `^[\*\s]+$`.

### 5. Bylines

Author attribution lines appearing on the title page or before dedication.

Examples:
- `By Mr. LILLO .`
- `By Mr. BROOKE .`
- `By HENRY FIELDING , Esq`
- `By Laurence Sterne`
- `By George Berkeley , M. A. Fellow of Trinity-College , Dublin .`

Cleaning note:
- Drop when starts with `By [A-Z]` and is ≤10 words.

### 6. Scripture reference headers

Short Bible citation lines appearing before sermon body text.

Examples:
- `ROMANS xii . 4 , 5 .`
- `ROMANS ii . 14 .`
- `JAMES i. 26 .`
- `NUMBERS xxiii . 10 .`
- `MATTHEW xxii . 37 .`

Cleaning note:
- Drop when paragraph starts with a Bible book name and is ≤8 words.

### 7. Publisher / printer imprints

Title-page imprint lines giving publisher name, city, and date.

Examples:
- `LONDON : Printed for R. and J. DODSLEY in Pall-Mall . MDCCLXIII .`
- `DUBLIN : Printed for the AUTHOR . By DILLON CHAMBERLAINE ...`
- `Printed for R. GRIFFITHS , in Paul 's Church-Yard .`

Cleaning note:
- Drop when starts with a city name + colon (`LONDON:`, `DUBLIN:`, `OXFORD:`, etc.)
  and is ≤30 words.
- Drop when starts with `Printed for` or `Printed by` and is ≤30 words.

### 8. Letter salutation stubs

Short salutation lines at the head of individual letters.

Examples:
- `MY LORD ,`
- `My Lord ,`
- `SIR ,`
- `MADAM ,`
- `DEAR SIR ,`
- `GENTLEMEN ,`

Cleaning note:
- These are ≤4 words and consist only of the addressee form + punctuation.
- Mostly covered by the short all-caps rule; explicit match for mixed-case variants.

### 9. Inline `} N {` footnote artifacts

601 paragraphs contain inline `} N {` patterns where N is a footnote number. These are
OCR artifacts from footnote callouts typeset as `} 2 {` in the original.

Examples (excerpt):
- `...principle of benevolence } 2 { in man , which is in some degree...`
- `...the several passions and affections , which are distinct } 3 {`

Cleaning note:
- Strip `} N {` patterns (with surrounding whitespace) in-place; do not drop the paragraph.
- Regex: `\s*\}\s*\d+\s*\{\s*`

## Practical Cleaning Categories

### 1. Safe to drop (whole paragraph)

- Section headers (SERMON/CHAP/PART/SECTION/BOOK)
- Letter chapter headers (LETTER I, II, ...)
- Short all-caps stubs ≤8 words
- Asterisk separator lines
- Bylines (By Author ≤10 words)
- Scripture reference headers ≤8 words
- Publisher/printer imprints (city: or Printed for/by, ≤30 words)
- Letter salutation stubs

### 2. Normalize in place

- `} N {` inline footnote artifacts → strip

### 3. Keep

- All substantive prose paragraphs (sermons, novel text, essays, letters, plays)
- Scripture quotes (≥9 words, actual Bible text)
- Dedications (≥9 words)

## Useful Detection Heuristics

- `^(?:SERMON|CHAP(?:TER)?\.?|SECTION|PART|BOOK)\s+[IVXLCDM0-9]+` + ≤20 words
- `^LETTER\s+[IVXLCDM]+` + ≤6 words
- `^[\*\s]+$` (asterisk separator)
- `^By [A-Z]` + ≤10 words
- `^(?:ROMANS?|MATTHEW|JAMES|NUMBERS?|...) ` + ≤8 words (scripture ref)
- `^(?:LONDON|DUBLIN|OXFORD|CAMBRIDGE|EDINBURGH)\s*:` + ≤30 words
- `^Printed (?:for|by)\b` + ≤30 words
- Whole paragraph is all-caps ≤8 words
- `\}\s*\d+\s*\{` (inline footnote artifact, strip not drop)
