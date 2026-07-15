# Segment linking and batch persistence — continuation notes

Status notes for continuing the phrase-tree work. Written 2026-07-15,
after commit `6dc7dd1`.

## Goal

Build and persist whole phrase trees with the plain Segment API — no
wrapper class. Top-down construction:

```python
phrase = Phrase(label='hello', start=0, end=1000, audio_id=..., store=store)
phrase.add_children(words)
word.add_children(syllables)
syllable.add_children(phones)
store.save_phrase_trees([phrase_one, phrase_two])   # not built yet
```

The `PhraseHierarchy` wrapper experiment (external registry class) was
rejected and deleted: it duplicated Segment behavior (`CHILD_TYPES` vs
`allowed_child_type`, `_items` vs `iter_descendants`) and kept a second
graph that had to be kept in sync with the objects' own caches.

## Decisions made

- Linking (`add_parent`/`add_child`/`add_children`) is **staging-only**:
  it never writes to LMDB. `update_database` was removed from linking.
  Persistence is always an explicit `save`/`save_many`.
- Only the upward link is persisted (`parent_id`, `parent_start`,
  `phrase_id`, `phrase_start`); the DB derives children by key scan.
  The downward link lives only in the in-memory `_children` cache,
  which linking keeps complete so staged trees are navigable from the
  root. See the comment block in `segment.py` (hierarchy helpers).
- Segments stage by default (`save=False` in the constructor); saving
  requires audio; `audio_id` is immutable after persistence; saves
  record `_key`.
- Iteration rule (standard Python, enforced by convention): code that
  iterates a parent's children while re-linking children to that same
  parent must iterate a copy — see the snapshot in `syllabify_phrase`.
- Speaker policy for batch persistence (decided 2026-07-15): require an
  explicit speaker; do NOT auto-create per-phrase placeholder speakers —
  Phrase identity is `(audio_id, speaker_id, start)`, so fresh
  placeholders break the `add_missing`/`upsert` existence policies in
  the TextGrid loader. Enforced in `store.save_phrase_trees`.

## Done (committed to main)

- `2dadbc9` — Stage segments by default and validate saves
  (`_validate_for_save`, `_validate_audio_assignment`, `_key`
  recording, `get_or_create` removed).
- `6dc7dd1` — Make linking staging-only and bidirectional
  (`_validate_parent_link` with pre-mutation checks, `_cache_child` /
  `_uncache_child`, phrase inheritance for Syllable/Phone at link time,
  atomic `add_children`, all call sites cleaned).

## Next steps, in order

1. ~~**Tests for the new linking**~~ — done (`tests/test_segment_linking.py`,
   plus regression tests and fixes for the linking review findings 1/2/4;
   finding 3 — bottom-up phrase inheritance — is pinned as an expected
   failure until step 5).
2. ~~**`Phrase.items`**~~ — done.
3. ~~**`store.save_phrase_trees(phrases)`**~~ — done (requires explicit
   speaker, rejects duplicate `(audio_id, speaker_id, start)` in the
   batch, flattens via `items`, writes through `save_many`).
4. **Loader migration** — TextGrid loader and `syllabify_phones` no
   longer need their explicit `_add_phrase` calls (linking inherits the
   phrase now); remove them and verify with the loader tests.
5. **model_helper cleanup** (after step 4):
   - `fix_references` — zero callers already, delete;
   - `write_changes_to_db` + `_save_status` — the `'update'` branch is
     dead (`_old_key` is never set); replace the flag with `_apply_*`
     returning changed-flags, then delete both;
   - `ensure_consistent_link` — replaceable by direct identity
     inheritance at link time.
6. Optional hardening: refactor `syllabify_phrase` to build into a
   fresh Phrase (the `syllabify_phones` pattern), removing its
   dependence on the snapshot rule structurally.

## Gotchas

- `_children` and `_related` must always be created together — the
  `related` property assumes it. `_cache_child` goes through the
  `children` property for bound parents (to merge with persisted
  children) and starts empty caches only for unbound parents.
- The `children` property speaker-filters DB-loaded children into
  `_children` vs `_related`.
- Persisted speaker *reassignment* is a supported interactive feature
  (commit `9918097`); `add_audio`/`add_speaker` keep `update_database`
  for now — only linking lost it.
- Run tests with the project venv:
  `.venv/bin/python -m pytest tests/ -q` (163 tests + 15 subtests
  green at `6dc7dd1`). A pre-commit hook bumps the version on every
  commit.
- `scripts/check_style.py` is untracked; apply it to touched files.
