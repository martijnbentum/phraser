# Segment linking and batch persistence — continuation notes

Status notes for continuing the phrase-tree work. Rewritten 2026-07-15,
after commit `88890d9`. The previous version of this file tracked steps
1–4, all now done; see `git log 31f3f21..88890d9` for the full trail.

## Where the work stands

The staging workflow from the original goal works end to end:

```python
phrase = Phrase(label='hello', start=0, end=1000, audio_id=..., 
    speaker_id=..., store=store)
phrase.add_children(words)          # staged, never writes
word.add_children(syllables)
syllable.add_children(phones)
phrase.validate_tree()              # optional early check
store.save_phrase_trees([phrase_one, phrase_two])
```

Done since the last rewrite (each with tests, suite green throughout):

- `2dadbc9`/`6dc7dd1` — staging by default, staging-only bidirectional
  linking (pre-dates this session).
- `4d39030` — linking review fixes: `add_children` cross-child conflict
  validation, old-parent uncaching via the store cache
  (`Store.get_cached`), unbound segments return empty child caches
  instead of raising mid-propagation.
- `8815e46` — `tests/test_segment_linking.py`: 24 tests pinning all
  linking + batch-persistence behavior. One test is
  `@unittest.expectedFailure` (see step 5 below).
- `b355d32` + `6ce50f6` — Phrase identity cleanup, then a partial
  revert. Net kept: `Phrase.IDENTITY_FIELDS = {audio_id, speaker_id,
  start}`, branch-free `__eq__`/`__hash__`/`phrase_key`, and
  `exists_in_db` aligned with phrase equality.
- `4d72304` — `Phrase.items` (flattened tree) and
  `store.save_phrase_trees` (batch persist at the Store boundary).
- `27a23b4` — `Phrase.validate_tree()`: explicit speaker, per-item
  `_validate_for_save`, speaker-coherence across the tree. The Store
  keeps only batch-level checks (isinstance, duplicate identity).
- `7fc4dcd` — loader migration: TextGrid loader links top-down so
  phrase refs inherit at `add_parent` time; all explicit `_add_phrase`
  calls removed from the loader and `syllabify_phones`.
- `88890d9` — loader segments get `audio_id`/`speaker_id` at
  construction (threaded through the interval builders' `kwargs`);
  the post-hoc `add_audio`/`add_speaker` pass is gone.

## Decisions made (do not relitigate)

- Linking (`add_parent`/`add_child`/`add_children`) is staging-only;
  persistence is an explicit `save`/`save_many`/`save_phrase_trees`.
  Only the upward link is persisted; the DB derives children by
  time-range key scan + speaker filter.
- Speaker policy: `save_phrase_trees` requires an explicit speaker per
  phrase; never auto-create per-phrase placeholder speakers (they break
  the loader's `add_missing`/`upsert` existence policies). A deliberate
  shared "unknown" speaker per corpus is acceptable if ever needed.
- Orphan policy (loader): phones/syllables outside every parent
  interval (e.g. pause phones) still get phrase refs, via the fallback
  loop at the end of `textgrid_filename_to_database_objects`. This is
  currently the only production caller of `_add_phrase`.
- Style: the owner prefers explicit `object_type == '...'` branches in
  the Segment base class over flags/overrides/mixins — the hierarchy is
  closed (4 classes, one file). Data-driven overrides (per-class
  `IDENTITY_FIELDS`) are fine. Don't reintroduce dispatch indirection.
- Identity at construction: agreed direction is to make identity
  mandatory constructor params, AFTER the step 5 cleanup. Target
  signature:

  ```python
  def __init__(self, label, start, end, audio_id, speaker_id,
      parent_id=EMPTY_ID, parent_start=0, save=False,
      overwrite=False, store=None, **kwargs):
  ```

  (label/start/end/audio_id/speaker_id required; the rest keeps its
  defaults; `**kwargs` stays for metadata fields.)

## Next steps, in order

1. **model_helper cleanup (old step 5)**
   - `fix_references` — zero callers, delete.
   - `write_changes_to_db` + `_save_status` — the `'update'` branch is
     dead (`_old_key` is never set). `add_audio`/`add_speaker` still
     use the flag for reassignment; replace with `_apply_*` returning
     changed-flags, then delete the flag machinery.
   - `ensure_consistent_link` — replace with direct identity
     inheritance at link time inside `add_parent`.
   - **Fix bottom-up phrase inheritance while in there**: when a
     segment gains phrase refs at link time, push them down through its
     staged `_children`. This flips the one xfail test
     (`test_bottom_up_construction_inherits_phrase_refs` in
     `tests/test_segment_linking.py`) — remove its
     `@unittest.expectedFailure` with the fix; an unexpected pass fails
     the suite, so it can't be forgotten.
   - Settle `_add_phrase` / `apply_phrase_id_and_start`: the loader
     orphan fallback is `_add_phrase`'s only production caller;
     `apply_phrase_id_and_start` has none. Inline or delete.

2. **Mandatory identity constructor params** (the signature above)
   - Migrate remaining bare construction sites first, then flip the
     signature: `syllabify_phones._build_syllable`/`_build_word`
     (identity available from the source phones/word),
     `textgrid_loader` is already done, check
     `scripts/dummy_data_generator.py`, `scripts/load_cgn_to_db.py`,
     and tests (many `test_segment_linking` tests construct bare
     segments to exercise propagation — those tests change meaning
     once propagation is gone; rewrite them as validation tests).
   - Then the enforcement makes more machinery dead: the
     missing-audio branch of `_validate_for_save` (keep the audio
     immutability check), the missing-speaker branch of
     `validate_tree` (keep coherence), and
     `_validate_children_consistency` (parent always has identity, so
     the per-child mismatch check in `_validate_parent_link` covers
     everything).

3. Optional hardening (unchanged from before): refactor
   `syllabify_phrase` to build into a fresh Phrase, removing its
   dependence on the iterate-a-copy snapshot rule.

## Gotchas

- `_children` and `_related` must always be created together; the
  `children` property and `_cache_child` are the only creators now.
- The `children` property speaker-filters DB-loaded children into
  `_children` vs `_related`; a descendant with a diverging speaker_id
  silently lands in `related` on reload — `validate_tree` catches this
  before save.
- Re-parenting relies on `Store.get_cached` to find the old parent of
  DB-loaded children; a parent held only in an external variable after
  `store._cache.clear()` cannot be uncached.
- Run tests with the project venv: `.venv/bin/python -m pytest tests/
  -q` (189 tests + 25 subtests + 1 xfail green at `88890d9`).
- `scripts/check_style.py` (untracked) must be run on touched files;
  compare ERROR counts against `git show HEAD:<file>` — the codebase
  has pre-existing findings, the bar is "no new errors".
- A pre-commit hook bumps the version on every commit.
