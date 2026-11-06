# Lampeter Cleaning Notes

Source inspected:
- Full survey across all 3 shards (60 docs, 4,188 paragraphs)

Context:
- The Lampeter Corpus contains 60 early 18th-century English pamphlets and tracts
  (c. 1640–1740), largely economic, political, and religious texts.
- The corpus is predominantly clean prose; structural noise is rare.
- Main issues are a small number of parliamentary/legal procedural stubs, Latin date
  formulas, and very short fragments.

## Summary

Pattern counts across all 3 shards:
- Very short (≤2 words): 16
- Latin/parliamentary date formulas: 3
- Procedural resolution lines: 3
- Royal cipher: 1
- "God save the King": 1
- Single letter running headers: 1
- Short date line: 1

Total estimated drop: ~26 paragraphs out of 4,188 (<1%).

## Observed Problematic Patterns

### 1. Very short fragments (≤2 words)

Most are question/dialogue stubs from rhetorical pamphlet forms.

Examples:
- `What else?`
- `What more?`
- `Consider.`
- `Sheweth,`
- `Ay, Ay.`

Cleaning note:
- Drop paragraphs with ≤2 words. These are too short for LM pretraining.

### 2. Latin parliamentary date formulas

Examples:
- `Die Jovis 9 Februarii, 1715.`
- `Die Jovis 19 Januarii, 1715.`
- `Die Mercurii 24[deg] Aprilis 1723.`

Cleaning note:
- Drop lines starting with `Die [A-Z][a-z]+` (Latin day + date formula).

### 3. Parliamentary procedural resolution lines

Examples:
- `Resolved,`
- `Ordered,`

Cleaning note:
- Drop standalone `Resolved` or `Ordered` lines (≤2 words).
- Covered by the ≤2-word rule.

### 4. Royal cipher and proclamation stubs

Examples:
- `GEORGIUS R.`
- `God save the King.`

Cleaning note:
- Drop `GEORGIUS R.`, `CAROLUS R.`, etc. (royal cipher with Latin initial).
- Drop `God save the King.` as a standalone stub.

### 5. Single-letter running headers

Examples:
- `E.`

Cleaning note:
- Drop single-letter paragraphs (covered by ≤2-word rule + letter check).

## Practical Cleaning Categories

### 1. Safe to drop

- Paragraphs ≤2 words
- Latin date formulas (`Die [Day] [Date]`)
- Royal ciphers (`GEORGIUS R.`, etc.)
- `God save the King.` stubs

### 2. Keep

- All substantive prose (pamphlet arguments, political and economic essays)

## Caution

The corpus is very clean. The ≤2-word filter removes legitimate short rhetorical questions
(`What else?`, `Consider.`) but these are too short to be useful training text. Risk of
over-dropping is negligible at this scale.
