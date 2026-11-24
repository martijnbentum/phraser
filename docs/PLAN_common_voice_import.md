# Plan: Common Voice import

Load Common Voice (CV) clips with their WebMAUS TextGrids into one phraser
store per language, following the CGN WebMAUS import
(`phraser/corpora/cgn/load_cgn_webmaus_textgrids_to_db.py`) as a guide.

## Source data

Location: `/vol/tensusers/mbentum/INDEEP/LD/COMMON_VOICE_<LANGUAGE>/` on
fancypants.

Per language: `clips/*.mp3`, split TSVs (`validated.tsv`, `other.tsv`,
`invalidated.tsv`, ...), `textgrids/<stem>.TextGrid`, `transcriptions/`,
and BAS `maus_<language>_phoneme_map.txt` files.

| language  | iso | validated clips | TextGrids | hours | speakers |
|-----------|-----|-----------------|-----------|-------|----------|
| Dutch     | nld | 90,095          | 90,095    | 109   | 1,599    |
| English   | eng | 490,483         | 490,379   | 586   | 26,744   |
| French    | fra | 152,362         | 152,362   | 178   | 2,903    |
| German    | deu | 277,602         | 277,568   | 324   | 4,823    |
| Hungarian | hun | 60,358          | 60,358    | 87    | 1,475    |
| Italian   | ita | 84,358          | 84,358    | 130   | 4,998    |
| Polish    | pol | 132,661         | 132,663   | 162   | 3,208    |
| Spanish   | spa | 112,127         | 112,119   | 165   | 7,265    |

- TextGrids cover the `validated.tsv` clips only; clips in `other` and
  `invalidated` have none. These are not alignment failures.
- Every TextGrid has the tiers `ORT-MAU`, `KAN-MAU`, `KAS-MAU`, `MAU`, `MAS`,
  the same tiers `textgrid_filename_to_database_objects` reads for CGN.
- Labels are MAUS SAMPA (`D`, `V`, `dZ`, `d_j`, `<p:>`), not IPA.
- One clip = one speaker = one TextGrid = one phrase, offset 0.
- Turkish has no TextGrids (only MFA dictionaries) and is out of scope.
- Polish has stray TextGrids at the language root (`web.TextGrid`,
  `1common_voice_pl_*.TextGrid`); only read `textgrids/`.

## Decisions

### 1. SAMPA to IPA: `phone_mapper.maus`

- Add `phone_mapper/maus.py` (like `cgn.py`, `coolest.py`) with per-language
  tables `maus_sampa_to_ipa[iso]` and inverse `ipa_to_maus_sampa[iso]`, data in
  `phone_mapper/data/maus/<iso>.json`, built from the BAS phoneme maps.
- Keys are MAUS symbols, which are not always strict SAMPA (`d_j` vs `d'`,
  `ts_j` vs `ts'` in Hungarian/Polish); document this in the module docstring.
- The inverse is not one-to-one: `<p:>`, `<p>`, `<` all map to `(...)`.
- English = union of the five variant maps (au, gb, nz, sc, us); verified to
  have no conflicting symbol-to-IPA entries.
- The generic `Mapper.sampa_to_ipa` stays untouched. It covers only 30-60% of
  MAUS symbols and carries Dutch choices (`w`->`ʋ`, `aI`->`ɑɪ`, `EI`->`ɛi`).
- The importer relabels the `MAU`, `MAS` and `KAN-MAU` tiers to IPA before
  saving, so CV stores match CGN, which WebMAUS delivered as IPA.
- Status: done in phone_mapper (commit 84ec132, version 0.1.18). Every MAU
  label in a 10k-TextGrid sample per language is covered by its table.
- `MAS` and `KAN-MAU` hold space-separated sequences (`'D V'`, `'n_j e'`):
  split, map each symbol, rejoin with spaces (WebMAUS IPA output uses the
  same spacing, e.g. `'d ə'`).
- Per-language corrections in phone_mapper
  (`data_sources/maus/corrections_<iso>.tsv`, each checked against the
  BAS value it replaces and backed by example words from the CV
  alignments): BAS IPA is the generic meaning of a symbol, not always the
  sound in that language. English `R`/`r` -> `ɹ`; Spanish `r` -> `ɾ`,
  `rr` -> `r`, `jj` -> `ʝ`; Hungarian `d_j`/`t_j` -> `ɟ`/`c`; Polish
  `s_j z_j n_j ts_j dz_j` -> `ɕ ʑ ɲ t͡ɕ d͡ʑ`, `I` -> `ɨ`, `e~ o~` ->
  `ɛ̃ ɔ̃`; French `a~ e~ 9~ o~` -> `ɑ̃ ɛ̃ œ̃ ɔ̃`, `R` -> `ʁ`; German
  `r` -> `ʁ`. Dutch has none. The raw BAS table stays available as
  `bas_maus_sampa_to_ipa`.
- French `9~`: MAUS G2P writes it for -in/-aim words (chemin, matin, vin,
  faim) that are /ɛ̃/; 31% of `9~` tokens in a sample. Fixed in the
  TextGrid copies (`/vol/mlusers/mbentum/common_voice/fra/textgrids/`, not
  the originals): 9~ -> e~ in KAN-MAU, KAS-MAU, MAU, MAS for words without
  un/um spelling, labels only, logged per change
  (`fra/textgrid_fix_9nasal_log.tsv`, `fra/README_textgrid_fix.md`). The
  CV import must read TextGrids from the copy. The copy step uses
  `rsync --ignore-existing` so reruns keep the fix.
- English `V` / `3``: the eng-US model (accent '', us, canada, other; ~75%
  of clips) has no schwa, so `V` covers ʌ and ə and `3`` covers ɝ and ɚ;
  other accents use variants with `@`. Fixed in the TextGrid copies
  (`eng/textgrids/`) with CMUdict stress (`resources/cmudict.dict`, source
  and sha256 in `cmudict.SOURCE.txt`): AH0 -> `@`, ER0 -> `@`` (ɚ, added
  to the phone_mapper English table), first pronunciation with matching
  count ('the' -> unstressed). Resolves ~97% of `V`, ~98% of `3``; the rest
  is left and listed in `eng/textgrid_fix_stress_log_skipped.tsv`. MAU/MAS
  only relabelled when their count inside the word matches KAN-MAU.
  Applied 2026-11-24: 992,009 words in 404,432 US-variant files.
- German `6` (ɐ): MAUS uses it for vocalised r after a vowel (der, wir;
  67%) and the -er ending (aber, oder; 33%); its syllable tiers already
  join vocalised r to the vowel (`e:6`). Fixed in the TextGrid copies
  (`deu/textgrids/`): post-vocalic `6` -> `6_^` (ɐ̯, added to the
  phone_mapper German table) in KAN-MAU, KAS-MAU, MAU; joined syllable
  tokens split (`e:6` -> `e: 6_^`). -er stays `6`. Only German has joined
  tokens; all other languages map fully in MAS and KAN-MAU.
- Notation normalisation, also in the phone_mapper build, so the phraser
  relabel step is a plain lookup:
  - affricates get a tie bar: `tʃ` -> `t͡ʃ`, `dʒ` -> `d͡ʒ`, `ts` -> `t͡s`,
    `dz` -> `d͡z`, including palatalised and geminate forms (`t͡sʲ`,
    `t͡ʃː`); Dutch MAUS has no affricates, so no stored data changes.
  - Unicode NFC (NFD would split the Dutch key `ç`), and the BAS typo
    U+0342 (Greek perispomeni, English nasal schwa) -> U+0303 tilde.
  - palatalisation before length (`tːʲ` -> `tʲː`).
  - phraser keeps `phonology/ipa_labels.py` as the stated convention; a
    test checks every `phone_mapper.maus` label already satisfies it.

### 2. Phone features per language

- `phraser/phonology/data/ipa_features.json` stays the Dutch file, unchanged.
- Add `ipa_features_<iso>.json` per language. Phone types, syllable positions
  and the label audit look up the table via `Audio.language`, defaulting to
  `nld`, so CGN behaviour does not change.
- Only declare symbols that occur in the TextGrids (coverage count first);
  MAUS maps list many marginal symbols (75 missing for Italian, 53 for
  Hungarian, 16 even for Dutch) that may never appear.
- Coverage (MAU phones, 10k TextGrids per language, vs ipa_features.json):

  | iso | phone types | missing types | missing tokens |
  |-----|-------------|---------------|----------------|
  | nld | 43          | 0             | 0%             |
  | eng | 71          | 26            | 9.1%           |
  | fra | 38          | 5             | 6.9%           |
  | deu | 52          | 7             | 4.6%           |
  | hun | 67          | 33            | 3.8%           |
  | ita | 54          | 25            | 6.1%           |
  | pol | 40          | 15            | 10.3%          |
  | spa | 33          | 6             | 3.4%           |

  Missing symbols are mostly notation: affricates without tie bar,
  diphthongs (`aɪ`, `eɪ`, `aʊ`, `əʊ`, `ɔɪ`, `ɔʏ`), geminates (`tː`,
  `tːʃ`), palatalisation (`nʲ`, `tsʲ`), nasal vowels (`ã`, `õ`, `ẽ`),
  syllabic consonants (`l̩`). New base sounds after corrections: `β`, `ɥ`,
  `ʎ`, `ɝ`, `ɦ`, `ɱ`, `c`, `ɟ`, `ʝ`, `ɕ`, `ʑ`, `t͡ɕ`, `d͡ʑ`, `ʁ`, `t͡s`,
  `d͡z`, `ɜ`. Only `(...)` and rare `(..)` non-speech labels occur; `(.)`
  never does.
- Status: drafts in `scripts/ipa_declarations/<iso>.tsv` (one row per
  symbol with sample count and how it was derived), built into
  `phraser/phonology/data/ipa_features_<iso>.json`; Dutch file verified
  byte-identical. `build_ipa_features.py` gained the places
  `labial-palatal` (ɥ) and `alveolo-palatal` (ɕ ʑ t͡ɕ d͡ʑ).
- Review decisions (2026-11-24), all in the per-language files only:
  - diphthongs keep the Dutch convention: features of the first
    component, plus `diphthong` and `components` descriptors.
  - English long vowels (`iː uː ɑː ɔː ɜː ɐː ʉː`) tense.
  - Spanish `β ð ɣ` approximants (lenited b d g).
  - Polish `ɨ` lax.
  - `ɝ` and `ɚ` get the descriptor `rhotic`; the binary matrix has no
    rhotic feature (adding one would change every vector, Dutch too).
  - German `ɐ̯`: vowel with `syllabic -`, so it joins the nucleus.
  - TSV `sample_count` is from the original MAUS output; symbols added by
    the TextGrid fixes (`ɚ`, `ɐ̯`) have none.
- Building the files: rules (base sound + length, palatalisation, nasal,
  syllabic, affricate, diphthong) generate a per-language declaration list
  once; it is reviewed and committed as the source of truth and afterwards
  edited per line, so errors are traceable with git blame. The feature
  matrix stays derived from descriptors, as for Dutch.

### 3. English MAUS variant

- Not recorded anywhere; phone labels suggest the variant was chosen per clip
  from the accent column. It does not affect IPA mapping (see 1) and is not
  stored.

### 4. Turkish

- Out of scope.

### 5. Speaker metadata

- `Speaker.name = client_id` (CV's anonymous contributor hash, the only speaker
  identifier), `dataset = 'common_voice'`, `language = iso`.
- CV stores demographics per clip; some contributors change them over time
  (Dutch 96, Hungarian 223, Polish 83 speakers). Take the most common
  non-empty value per client_id.
- Gender: `male`, `male_masculine` -> male; `female`, `female_feminine` ->
  female; `''` -> unknown; add a new `other` code to `utils.gender_dict`.
  `non_binary` / `do_not_wish_to_say` do not occur in the data, not added.
- Age: band to middle value (`teens` -> 15, `twenties` -> 25, ...,
  `nineties` -> 95); `''` -> 0 (unknown). Put a docstring reminder on the
  mapping helper and on `Speaker.age`.
- Accent: raw string in `Speaker.region`; `dialect` left empty for a
  normalised code later. Values are a fixed list in EN/FR/DE/ES/IT,
  semi-fixed in NL, free text in HU/PL; longest is 253 chars, well within
  the u16 string limit. Cost is per speaker (~8 MB for all languages).

### 6. Audio

- Keep the mp3 files for now; sox with mp3 support is available.
- Store header duration (`clip_durations.tsv` where present, else
  `sox --i`). It is consistently ~35 ms (one mp3 frame) longer than the
  decoded audio; fixed when the 16 kHz copy is made.
- Keep absolute paths in `Audio.filename` and `Phrase.filename` (relative
  paths considered and dropped: savings ~60 B/path, portability not needed).
- Postponed: consolidated copy at 16 kHz mono FLAC (~110 GB vs ~201 GB WAV
  for 1,741 h, plus ~17 GB TextGrids). Then update `filename`, `sample_rate`
  and `duration` on each Audio in place; identifiers and links are kept.

### 7. Scale and selection

- One store per language: `locations.common_voice_lmdb(iso)` ->
  `data / f'cv_{iso}_lmdb'`, with a `PHRASER_CV_LMDB_DIR` override.
- Save in chunks (a few thousand clips); English is ~490k phrases plus tens
  of millions of segments, too many for one `save_many`.
- Store size estimator: load a pilot sample (e.g. 500 clips) into a temporary
  store, measure LMDB bytes per clip, extrapolate to the selection.
- `max_clips` limit with ranking: metadata completeness (non-empty gender,
  age, accent), then `up_votes - down_votes`, with a per-speaker clip cap
  (single speakers contribute up to 15,754 clips in Dutch).

## Module layout

`phraser/corpora/common_voice/`

- `prepare_common_voice_import.py` (no store access, JSON cache under
  `locations.data / 'common_voice'`, not a cwd-relative `../data`):
  - language config: folder, iso, MAUS map, column names
  - read `validated.tsv` with `csv.DictReader` by column name (headers vary
    by release: `accent`/`accents`, optional `sentence_id`, `variant`,
    `locale`, `segment`)
  - keep rows whose `textgrids/<stem>.TextGrid` exists; report the rest
  - output speaker infos, audio infos, clip infos
  - ranking and `max_clips` / per-speaker cap selection
- `load_common_voice_to_db.py`:
  1. `save_speakers_to_db`
  2. `save_audio_to_db`
  3. `clips_to_db`: `textgrid_filename_to_database_objects(tg, offset=0,
     audio=..., speaker=..., multiple_speakers=False, store=...)`, relabel to
     IPA, chunked save. Skip a clip when its audio already has a phrase
     (exact for one phrase per clip; avoids the CGN filename shortcut). No
     `update_audio_overlap` call needed.
- `estimate_store_size(...)` pilot estimator.

## Build order

1. `phone_mapper.maus` module + tests.
2. Phone coverage report per language (relabelled MAU labels vs
   `ipa_features.json`).
3. Language-aware phone tables in phraser (`Audio.language`, default `nld`);
   per-language feature files for covered symbols.
4. `gender_dict` `other` code; age band helper.
5. CV prepare module + reports (missing TextGrids, metadata conflicts).
6. CV loader with chunked saves; pilot on Dutch, then size estimator.
7. Remaining languages.
8. (Postponed) 16 kHz FLAC consolidation and in-place Audio update.

## Open points

- Mismatches between validated rows and TextGrids (104 English, 34 German,
  8 Spanish missing; 2 extra Polish): report only, do not fail.
- CV release per language is unknown; TSV headers suggest NL/HU/PL/IT are
  newer than EN/FR/DE/ES. Record a `cv_version` in the config if known.

## Side finding (CGN)

`save_audio_to_db` applies `seconds_to_miliseconds` to durations that
`audio_info` already returns in milliseconds. It only works with the cached
`cgn_audio_info_dict.json`; a fresh run with `overwrite=True` would store
durations 1000x too large and can overflow the u32 field. The CGN cache
paths are also cwd-relative (`Path('../data/...')`). Do not copy either
pattern.
