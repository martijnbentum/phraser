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
