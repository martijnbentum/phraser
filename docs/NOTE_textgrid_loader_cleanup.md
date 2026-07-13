# TextGrid Loader Cleanup

## Generator Write State

`textgrid_to_words()`, `textgrid_to_syllables()`, and `textgrid_to_phones()`
temporarily change `Store.db_saving_allowed` through the module-global
`db_save_state`.

The main loader path currently exhausts these generators with `list(...)`, so
the write state is restored in normal TextGrid ingestion. Direct callers can
still partially consume a generator or hit an exception before the trailing
restore call runs. In that case, store writes can remain disabled unexpectedly.

When this is fixed, prefer a local state guard instead of the module global.
For generator safety, restore the previous write state before yielding each
object, or use a `try/finally` shape that reliably restores state when the
generator is closed.

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
