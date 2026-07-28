# Plan: CGN AWD TextGrid Database

## Feature 1: Dutch Syllabifier Inventory

### Requirements

- Keep `phone_mapper.cgn.cgn_to_ipa` unchanged and authoritative.
- Extend the Dutch IPA vowel inventory with the six mapped vowels that are
  currently unknown: `ɒː`, `ʉ`, `œ̃`, `æ̃`, `ɑ̃ː`, and `ɒ̃ː`.
- Preserve the current syllabifier API and default maximal-onset behavior.
- Treat sequences without a vowel nucleus as data edge cases, not unknown
  symbol errors.

### Tests

- Every added vowel is accepted as a nucleus.
- Existing syllabification behavior remains unchanged.
- Every known phone continues to have a sonority class.

## Feature 2: Phraser IPA Coverage

### Requirements

- Add `phone_mapper` as a runtime dependency.
- Ensure every value emitted by `cgn.cgn_to_ipa` has Phraser phone-feature and
  phone-type coverage.
- Generate feature data through `scripts/build_ipa_features.py`; do not edit
  only the generated JSON.

### Tests

- The CGN-to-IPA value set is covered by `ipa_features.json`.
- Added vowels are classified as vowels and mapped consonants as consonants.
- Feature vectors remain complete and positionally aligned.

## Feature 3: Independent CGN AWD Database Builder

### Requirements

- Add `scripts/build_cgn_awd_textgrid_db.py`.
- Do not import `scripts.process_cgn`,
  `scripts.load_cgn_to_db`, or the WebMAUS-specific TextGrid converter.
- Read paired `.ort` and `.awd` files by CGN recording stem.
- Use non-empty speaker tiers from `.ort` as phrase anchors, excluding
  `BACKGROUND` and `COMMENT`.
- Validate each AWD speaker triple:
  `<speaker>`, `<speaker>_FON`, `<speaker>_SEG`.
- Assign AWD word intervals to ORT phrases by interval midpoint because aligned
  word boundaries can cross an ORT boundary slightly.
- Derive final Phrase spans from their assigned AWD word intervals so
  same-speaker phrases do not overlap.
- Create Words from AWD speaker tiers and Phones from `_SEG` tiers.
- Map CGN phone labels to IPA with `phone_mapper` before constructing Phones.
- Map word-level `_FON` transcription symbols to IPA while retaining AWD
  boundary markers.
- Derive Syllables per Word with `dutch_syllabifier`.
- For a mapped sequence without a vowel nucleus, retain its Phones under one
  fallback Syllable with unknown phone positions.
- Preserve unreliable `!` alignment units as Words but do not interpret their
  unsegmented `_SEG` label as a Phone.
- Preserve `_` shared-plosive alignment units as pseudo-Words so their aligned
  Phone is not discarded.
- Assign a cross-boundary Phone to the AWD word interval containing its start;
  store each Phone exactly once.
- Skip non-phone markers such as `[]` and `#`, recording them in the report.
- Require an explicit target LMDB path and refuse to use the configured legacy
  CGN LMDB path.
- Refuse a non-empty target unless resume mode is explicitly selected.
- Save one recording at a time so a complete CGN import does not accumulate
  the corpus in memory.
- Report missing pairs, missing audio or speakers, tier mismatches, unknown
  symbols, unreliable units, fallbacks, saved recordings, and skipped
  recordings.

### Tests

- Parse synthetic one- and multi-speaker ORT/AWD pairs.
- Reject malformed or mismatched tier triples.
- Assign boundary-crossing words to phrases by midpoint.
- Map all normal CGN Phone labels to IPA.
- Build Word/Syllable/Phone relationships and fallback syllables.
- Handle `!`, `_`, `[]`, `#`, pauses, and shared cross-boundary Phones.
- Refuse the legacy or a non-empty target database by default.
- Resume a previously completed recording without duplicating phrase trees.
- Perform a read-after-write hierarchy check using a temporary Store.

## Feature 4: Legacy WebMAUS Script Naming

### Requirements

- Rename `scripts/process_cgn.py` to
  `scripts/prepare_cgn_webmaus_import.py`.
- Rename `scripts/load_cgn_to_db.py` to
  `scripts/load_cgn_webmaus_textgrids_to_db.py`.
- Update their internal relative import and documentation references.
- Do not change legacy behavior.

### Tests

- Both renamed modules compile and import.
- The renamed loader resolves the renamed preparation module.

## Feature 5: Documentation And Validation

### Requirements

- Document the new builder's inputs, fresh database behavior, resume mode, and
  distinction from the WebMAUS path.
- Run targeted tests for the importer, phone features, and Dutch syllabifier.
- Run repository style checks on every added or modified Python file.

### Tests

- The command-line help runs from the project virtual environment.
- A small real ORT/AWD pair completes the staging path without writing to the
  configured legacy database.
