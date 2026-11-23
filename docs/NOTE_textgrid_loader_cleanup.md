# TextGrid Loader Cleanup

## TextGrid Staging Writes

TextGrid conversion stages objects by constructing `Phrase`, `Word`,
`Syllable`, and `Phone` instances with `save=False`, then writing through
`save_textgrid_items()` when TextGrid-aware persistence is requested.

The low-level generators `textgrid_to_words()`, `textgrid_to_syllables()`,
`textgrid_to_phones()`, and `textgrid_filename_to_database_objects()` are
staging-only and do not accept a `save_to_db` argument. Callers should use
`save_textgrid_items()` or a high-level loader to persist staged objects.

This avoids mutating store-wide write state during conversion. Partial generator
consumption and exceptions therefore cannot leave the store in a different write
state.

## TextGrid Persistence Policies

`overwrite=True` is intentionally not supported for TextGrid imports. LMDB
overwrite only replaces an exact key, while TextGrid conversion creates fresh
object identifiers for each imported `Phrase`, `Word`, `Syllable`, and `Phone`.

Use `save_textgrid_items(items, existing=...)` for TextGrid-aware persistence:

- `append`: no existence check; save staged items directly
- `add_missing`: run an existence check; save only when no matching phrase exists

Existence checks are audio-scoped and match `Phrase` objects by
`(audio_id, speaker_id, start)`. The high-level loaders require an existing
`Audio` object for `add_missing`; creating a fresh
`Audio` from a filename would produce a new `audio_id` and cannot match previous
imports.

The former `replace` and `upsert` policies were removed. Because start time is
part of the match, they only worked when a new alignment kept every phrase
start unchanged, and replacement gave the tree new identifiers, cutting links
from markers or embeddings. Their delete-then-save was also not atomic. To
replace stored phrase trees, use `Store.save_phrase_trees(..., overwrite=True)`
or the repair change-plan API.

## CGN Import Note

`phraser/corpora/cgn/load_cgn_webmaus_textgrids_to_db.py` still uses phrase
`filename` values to skip existing CGN TextGrid imports. That is an importer
shortcut for now; it is not the TextGrid phrase identity. Move CGN import
to `save_textgrid_items(..., existing='add_missing')` or another explicit
policy if phrase-level skipping is needed there.
