# Segment linking and batch persistence — continuation notes

Status notes for continuing the phrase-tree work. Rewritten 2026-07-15,
after commit `62a1669`; amended the same day after `6b71c18`. The
previous version tracked the model_helper cleanup and the
mandatory-identity plan; both are complete — see
`git log 4c8bd21..6b71c18` for the trail.

## Where the work stands

Segment identity is mandatory and immutable: every Segment is
constructed with `label, start, end, audio_id, speaker_id` (the
constructor rejects `None`/`EMPTY_ID` identity), and there is no
reassignment API. The staging workflow is unchanged:

```python
phrase = Phrase(label='hello', start=0, end=1000, audio_id=...,
    speaker_id=..., store=store)
phrase.add_children(words)          # staged, never writes
word.add_children(syllables)
syllable.add_children(phones)
phrase.validate_tree()              # optional early check
store.save_phrase_trees([phrase_one, phrase_two])
```

Done this session (suite green after every commit):

- `f1b4a82` — deleted unused `model_helper.fix_references`.
- `2d65b94` — `_save_status` flag machinery replaced with
  changed-flags; `write_changes_to_db` deleted (its `'update'` branch
  was dead).
- `02b24ab` — `add_parent` inherited identity directly
  (`ensure_consistent_link` deleted); new `_set_phrase_refs` pushes
  phrase refs down through staged children — flipped the bottom-up
  xfail and closed the orphan-syllable-phones loader gap.
- `9e76304` — retired `_add_phrase` and `apply_phrase_id_and_start`;
  callers use `_set_phrase_refs`.
- `5b2cbd9` — last bare production construction sites got identity
  (syllabify builders, dummy-data generator).
- `0cc7694` — mandatory identity constructor params, validated
  non-empty; the TextGrid loader requires audio and speaker; the
  dead validation branches went with it. The DB load path is
  unaffected (it bypasses `__init__` via `cls.__new__`).
- `62a1669` — `add_audio`/`add_speaker`/`iter_family` deleted:
  identity cannot be reassigned; rebuild-and-replace is the only
  identity operation.
- `a747b23` — syllabify hardening: `_rebuild_word` returns an
  unlinked word; callers seed empty child caches and link via
  `phrase.add_children`. The snapshot rule is gone.
- `6b71c18` — Audio and Speaker stage by default (`save=False`),
  matching Segments; persistence is always an explicit ask.

`model_helper.py` now contains only `EMPTY_ID`. Segment tests:
184 + 33 subtests green at `62a1669`.

## Decisions made (do not relitigate)

- Linking (`add_parent`/`add_child`/`add_children`) is staging-only;
  persistence is an explicit `save`/`save_many`/`save_phrase_trees`.
  Only the upward link is persisted; the DB derives children by
  time-range key scan + speaker filter.
- Identity is fixed at construction. Audio immutability is verifiable
  at save time (`_validate_audio_assignment` compares the persisted
  key); speaker immutability is enforced by having no API to change
  it, with the link-time equality check and `validate_tree` coherence
  as tamper guards.
- Wrong identity means rebuild-and-replace, never in-place mutation.
  A repair helper (clone a tree under a new speaker via `Phrase.items`
  + `save_phrase_trees` + `delete`) is deliberately NOT built until a
  real caller needs it.
- Speaker policy: never auto-create placeholder speakers; a deliberate
  shared "unknown" speaker per corpus is acceptable if ever needed.
- Orphan policy (loader): phones/syllables outside every parent
  interval still get phrase refs via the loader fallback, which uses
  `_set_phrase_refs` (push-down included).
- Style: explicit `object_type == '...'` branches in the Segment base
  over flags/overrides/mixins; the hierarchy is closed (4 classes, one
  file). Don't reintroduce dispatch indirection.
- `Speaker.add_audio` / `Audio.add_speaker` (corpus-level
  speaker–audio links on the non-Segment classes) are a separate
  mechanism; the deletion of Segment reassignment does not touch them.

## Next steps

1. **Benched: cross-tree descendant collisions** in
   `save_phrase_trees` — the batch dedup is phrase-level only. Before
   adding a check, verify whether `DB.write_many` detects duplicate
   keys within one batch or silently last-writes-wins.
2. **Benched: `scripts/check_style.py` is untracked** but referenced
   in the gotchas below; commit it or drop the references.

## Gotchas

- `_children` and `_related` must always be created together; the
  `children` property and `_cache_child` are the only creators.
- The `children` property speaker-filters DB-loaded children into
  `_children` vs `_related`; a descendant with a diverging speaker_id
  silently lands in `related` on reload — `validate_tree` catches this
  before save.
- Re-parenting relies on `Store.get_cached` to find the old parent of
  DB-loaded children; a parent held only in an external variable after
  `store._cache.clear()` cannot be uncached.
- Run tests with the project venv: `.venv/bin/python -m pytest tests/
  -q` (184 tests + 33 subtests green at `62a1669`).
- `scripts/check_style.py` (untracked) must be run on touched files;
  compare ERROR counts against `git show HEAD:<file>` — the codebase
  has pre-existing findings, the bar is "no new errors".
- A pre-commit hook bumps the version on every commit.
