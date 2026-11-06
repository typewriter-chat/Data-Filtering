# Old Bailey Cleaning Notes

Source inspected:
- `data/samples/old_bailey_preview.jsonl`
- `data/processed/old_bailey/part-00000.parquet`

Context:
- The Old Bailey corpus is already paragraph-based.
- The dominant problem is not OCR-heavy corruption in the trial narratives, but repeated session-level front matter before the actual proceedings.
- Cleaning should therefore remove boilerplate preamble and jury-list material while preserving the trial text itself.

## Summary

The main suspicious patterns in `old_bailey` are:
- repeated session front matter describing the court session
- all-caps or header-like layout fragments split into separate paragraphs
- jury-list headers and long runs of juror names
- short date-only lines and mayoralty/court administration lines
- a smaller amount of OCR or spelling variation inside the front matter

This corpus mostly needs structural trimming at the start of each session, not aggressive lexical cleanup across all paragraphs.

## Observed Problematic Patterns

### 1. Session preamble boilerplate

This is the dominant repeated pattern.

Typical examples:
- `THE PROCEEDINGS ON THE KING's Commission of the Peace, AND`
- `Oyer and Terminer, and Goal-Delivery of Newgate, held for the CITY of London, and COUNTY of Middlesex, at Justice-Hall in the Old Bayly,`
- `Wednesday, Thursday and Friday, being the 27th, 28th and 29th of April, 1720. In the Sixth Year of His MAJESTY's Reign.`
- `BEFORE the Right Honourable Sir GEORGE THOROLD ...`

Cleaning note:
- These are session metadata, not the trial proceedings themselves.
- They should usually be removed from the cleaned paragraph list.

### 2. All-caps layout fragments split into separate paragraphs

Examples:
- `ON`
- `FOR THE`
- `HELD AT`
- `LORD-MAYOR of the CITY of LONDON.`

These appear because page/layout text has been segmented too literally.

Cleaning note:
- Short all-caps fragments are safe to drop when they occur in the session preamble.

### 3. Jury-list headers

Examples:
- `The Jurors Names were as followeth:`
- `London Jury.`
- `London Jury,`
- `Middlesex Jury.`

Cleaning note:
- These are structural metadata, not trial content.
- They should be removed together with the juror name list that follows.

### 4. Runs of juror names as standalone paragraphs

Examples:
- `John Keeble,`
- `Henry Brookhouse,`
- `Edward Gregg,`
- `Richard Ward,`

Observed pattern:
- many consecutive single-name paragraphs after `London Jury`

Cleaning note:
- These should be dropped as part of the session preamble/jury block.

### 5. Date-only and session-administration fragments

Examples:
- `6 May 1720.`
- `19. Sept. 1720.`
- `1722.`
- `During the MAYORALTY of the Rt. Honble Sir William Stewart ...`

Cleaning note:
- These are metadata or calendrical markers, not trial narrative.
- They are safe to remove at the start of a session.

### 6. Front-matter OCR/spelling variation

Examples:
- `Old Bayly`
- `Old Bailey`
- `Oyer and Terniner`
- `Gaol Dallvery`
- `MIDDLSEX`
- `Middleses`
- `Pouys`

Cleaning note:
- This variation exists, but it is concentrated in the preamble.
- Since the preamble is being dropped, most of this does not require explicit normalization.

### 7. Additional preamble patterns (found from processed data)

The early-18th-century preamble form is covered above. Later sessions (mid-18th century onward) use variant headers and include a publisher/printer block:

- `CENTRAL CRIMINAL COURT` — 19th-century sessions use this instead of `Justice-Hall in the Old Bailey`
- `THE WHOLE PROCEEDINGS ON THE ...` — variant of the standard `THE PROCEEDINGS ON THE KING`
- `MINUTES OF EVIDENCE,` / `Sessions Paper.` / `SESSIONS PAPER.`
- `Taken in SHORT-HAND by JOSEPH GURNEY.` / `TAKEN IN SHORT-HAND, BY ...` — shorthand writer credits
- `REVISED AND EDITED BY` / `Short-hand Writers to the Court,` — editor credit lines
- `OF THE MIDDLE TEMPLE, BARRISTER-AT-LAW.` — counsel credit
- `LONDON:` — standalone city imprint line
- `Printed for BENJAMIN MOTTE ...` / `PUBLISHED BY GEORGE HEBERT, CHEAPSIDE.` — publisher imprints
- `(Price Six Pence.)` / `[ Price Four-pence.]` / `Price Four Pence` — price lines
- `ROLLS CHAMBERS, No. 89, CHANCERY LANE.` / address lines — publisher addresses

Cleaning note:
- All of the above are part of the publisher/title-page block and should be dropped as preamble.
- `_PROCEEDINGS_RE` should be extended to match `WHOLE PROCEEDINGS` and `QUEEN'S Commission` variants.
- `_JURY_HEADER_RE` should be extended to match `First/Second London/Middlesex Jury` numbered variants.

### 8. Body-level structural fragments (found from processed data)

Inspection of the full processed shards reveals that many short structural fragments exist throughout the **body** of the proceedings, not just the preamble. These are not prose and are too short to be useful for LM pretraining:

**`[ X. See summary.]` cross-references** (3,207 occurrences):
- `[ Death. See summary.]`
- `[ Transportation. See summary.]`
- `[ Whipping. See summary.]`

These are internal pointers to a separate summary document. Not prose.

**Short verdict/sentence lines** (14,077 occurrences):
- `NOT GUILTY.` / `GUILTY.` / `Acquitted.`
- `Transported for Seven Years.` / `Transported for Life.`
- `Confined Six Months.` / `Judgment respited.`
- `Guilty. T.` / `Guilty 10 d.`

One to four words; not useful as standalone training text.

**Section header lines** (2,263 occurrences):
- `Prisoner's Defence.` / `PRISONER'S DEFENCE.`
- `Cross Examination.` / `Cross-examination.`
- `For the Prisoner.` / `For the Prosecution.`

Pure structural markers with no content.

**Jury and judge assignment lines** (6,582 occurrences):
- `First Middlesex jury, before Mr. Recorder.`
- `Second Middlesex Jury, before Mr. Recorder.`
- `Before Mr. Common Serjeant.`
- `Tried by the London Jury before Mr. RECORDER.`

Administrative metadata about case assignment, not trial content.

**Procedural parenthetical notes** (650 occurrences):
- `(Property produced and sworn to.)`
- `(Witnesses examined separate.)`
- `(The prisoner received a good character.)`

Short parenthetical court actions.

Cleaning note:
- All of the above should be dropped throughout the body of the proceedings, not just in the preamble.
- Implement as a separate `_is_body_fragment()` check applied after preamble detection ends.

## Practical Cleaning Categories

### 1. Safe to drop

- session preamble paragraphs
- jury headers and juror name lists
- date-only lines at the start of a session
- short all-caps layout fragments in the preamble
- publisher/printer imprint blocks (§7)
- shorthand writer and editor credit lines (§7)
- `[ X. See summary.]` cross-references (§8)
- short verdict/sentence lines ≤ 8 words (§8)
- section header stubs: `Prisoner's Defence`, `Cross Examination`, etc. (§8)
- jury and judge assignment lines (§8)
- short procedural parenthetical notes (§8)

### 2. Usually keep

- actual proceedings narrative
- witness examination and testimony
- longer verdict and sentencing passages with context

### 3. Optional later cleanup

- residual OCR variants inside retained trial paragraphs
- inconsistent capitalization in speaker/reporting text

## Useful Detection Heuristics

Promising signals:
- paragraph contains `THE PROCEEDINGS ON THE KING/QUEEN` or `WHOLE PROCEEDINGS`
- paragraph contains `Oyer and Terminer` / `Goal-Delivery` / `Justice-Hall` / `CENTRAL CRIMINAL COURT`
- paragraph contains `The Jurors Names were as followeth`
- paragraph matches a short juror-name line like `John Keeble,`
- paragraph is short and all-caps
- paragraph is a date-only line at the session start
- paragraph matches `^\[.*See summary\b`
- paragraph matches a short verdict: `NOT GUILTY`, `Transported`, `Acquitted` ≤ 8 words
- paragraph matches section header: `Prisoner's Defence`, `Cross Examination`, `For the Prisoner`
- paragraph matches jury assignment: `(First|Second) (London|Middlesex) jury, before`

## Caution

The main risk is over-dropping short trial paragraphs after the preamble.

A safe first pass is:
1. remove session front matter at the start (preamble + publisher block)
2. remove jury headers and juror lists
3. remove body-level structural fragments throughout the proceedings
4. keep substantive narrative paragraphs
