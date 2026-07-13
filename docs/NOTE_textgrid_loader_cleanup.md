# TextGrid Loader Cleanup

## TextGrid Staging Writes

TextGrid conversion stages objects by constructing `Phrase`, `Word`,
`Syllable`, and `Phone` instances with `save=False`, then writing through
`save_items_to_db()` when persistence is requested.

The low-level generators `textgrid_to_words()`, `textgrid_to_syllables()`, and
`textgrid_to_phones()` are staging-only and do not accept a `save_to_db`
argument. Callers should use `save_items_to_db()` or
`textgrid_filename_to_database_objects(..., save_to_db=True)` to persist staged
objects.

This avoids mutating store-wide write state during conversion. Partial generator
consumption and exceptions therefore cannot leave the store in a different write
state.

## Future Replace Existing Imports

`overwrite=True` is intentionally not supported for TextGrid imports. LMDB
overwrite only replaces an exact key, while TextGrid conversion creates fresh
object identifiers for each imported `Phrase`, `Word`, `Syllable`, and `Phone`.

If repeated imports should replace older annotations, add an explicit
`replace_existing=True` workflow instead of reusing `overwrite`.

Start with the narrow behavior:

- require an existing `audio` argument
- match existing phrases by `audio.identifier` and `phrase.filename`
- delete the matched phrase trees (`Phrase`, `Word`, `Syllable`, `Phone`)
- delete their stale label-index links
- save the newly staged TextGrid objects
- refresh query roots after the replacement

Do not delete or replace `Audio` or `Speaker` in this first version. Extending
replacement to the higher-level audio-loading helpers needs a separate policy:
either reuse existing audio by filename or define how audio replacement should
work.
