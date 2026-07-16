# Segment linking and batch persistence — continuation notes

Status notes for continuing the phrase-tree work. Rewritten
2026-07-16 (second rewrite that day), after steps 1–6 of the previous
version landed in the working tree on top of `75ff328` (uncommitted —
commit before continuing; the pre-commit hook bumps the version).
Suite green: 194 tests + 33 subtests
(`.venv/bin/python -m pytest tests/ -q`). Every new guard was
negative-verified: with the guard temporarily disabled, its tests
fail.

## The governing rule (state it in code, step 1 below)

The persistence signal lives in the RECEIVER, not the verb: on a
Segment, only `save` and `delete` touch the database; every other
method and property is in-memory. Tree persistence goes through the
Store. Verbs (`add_`, `replace_`) describe the in-memory effect only.
This rule is decided and load-bearing but not yet written into the
`Segment` class docstring or README — that is step 1.

## Where the work stands (all in working tree, tested)

- `Segment.child_keys` → `_candidate_child_keys`: private, docstring
  says time-range candidate scan, not ownership. The `children`
  property now scans once (was twice) and a dead `_child_keys`
  assignment is gone.
- Ownership filter: `children` classifies candidates by
  `candidate.parent_id == self.identifier`. Everything else in range
  — other speakers, same-speaker foreign-parent, unlinked — lands in
  `overlapping` (renamed from `related`; `_related` → `_overlapping`
  everywhere). Sole consumer `overlap_items` reads
  `parent.overlapping`; `check_overlap.py` filters speakers itself,
  so overlap codes are unaffected.
- `replace_children(new_children)`: staging-only wholesale relink.
  Validates ALL children BEFORE displacing, so a bad batch leaves the
  staged view intact (same promise as `add_children`). The two
  seed-idiom call sites in `syllabify_phones.py` now use it; the
  remaining direct `_children, _overlapping = ...` assignments there
  and in `resyllabifier.py` are fill-before-wiring, a different
  operation — leave them.
- Intra-batch duplicate-key check in `save_many`
  (`_check_intra_batch_keys`): raises before packing/writing;
  `DB.write_many` still silently last-write-wins within a txn, the
  guard sits above it. Runs regardless of overwrite/fail_gracefully.
- Same-speaker overlap enforcement in `save_phrase_trees`
  (`_check_same_speaker_overlap` + `_persisted_phrases_by_group`):
  one start-sorted running-max-end sweep per (audio_id, speaker_id)
  over batch phrases MERGED with the audio's persisted phrases.
  Batch-vs-batch and batch-vs-persisted overlaps raise; a phrase's
  own persisted row is key-exempt (overwrite re-saves pass);
  persisted-vs-persisted overlap is legacy dirt and does not block.
  Persisted fetch: `audio_id_to_child_keys` per audio, early break at
  `key_to_start(key) >= batch max end` (new helper in key_helper.py;
  segment keys are start-ordered).

## Decisions made (do not relitigate)

- Receiver rule above; no staging-flavored verb renames
  (`stage_`/`cache_children` rejected). `add_children` is honest
  (merges with the loaded view); `replace_children` displaces the
  view, not the disk. `add_parent` IS replace-parent: singular slot,
  displaces the old link; re-parenting cannot strand stale rows
  because only the upward link is persisted.
- NO `store.replace_phrase_trees` — overclaims when the staged edit
  was one layer. Instead: strengthen `save_phrase_trees(
  overwrite=True)` to mean "persisted tree becomes exactly this
  staged tree" (step 2 below).
- NO stored child lists on the parent record; the upward link is the
  single source of truth. NO `delete_children`. NO load/staging split
  of `children` (lazy touch-to-load is the navigation contract;
  `Phrase.delete` depends on it).
- `_cache_child`/`_uncache_child` keep their names: private,
  mechanics-accurate (they maintain the `_children` cache); the
  receiver rule protects users, private names serve maintainers.
- Identity fixed at construction; wrong identity means
  rebuild-and-replace. Never auto-create placeholder speakers.
- The overlap check tolerates persisted-persisted legacy overlap so
  dirty old data cannot block unrelated saves.
- Style: explicit `object_type == '...'` branches, no mixins, helper
  modules with plain functions over class indirection.

## Next steps

1. **Receiver-rule docs.** One sentence in the `Segment` class
   docstring and above the README staging example: only `save`/
   `delete` on a Segment touch the database; everything else is
   in-memory; tree persistence goes through the Store.
2. **Strengthen `save_phrase_trees(overwrite=True)`**: before
   writing, delete persisted descendants of each saved phrase that
   are not in its staged tree (persisted tree becomes exactly the
   staged tree). Closes the remaining stale-layer footgun: a rebuild
   whose phrase boundaries CHANGED is already caught by the overlap
   check, but same-boundary rebuilds leave old descendant rows that
   re-merge on load (they share the phrase's parent_id). Include
   label-index cleanup for deleted rows (see
   `scripts/fix_syllable_labels.py`). Test: rebuild with changed word
   boundaries, save with overwrite=True, reload → no doubled layer.
3. **Extract `save_validation.py`** together with step 2 (so the
   module is born complete): move `_check_intra_batch_keys`,
   `_validate_phrase_trees`, `_check_same_speaker_overlap`,
   `_persisted_phrases_by_group` (+ step 2's stale-descendant check)
   into plain functions taking `store`/`phrases` explicitly, repo
   helper-module style. `store.py` is 653 lines; the family is ~120
   and growing.
4. **Housekeeping** (old step 7): guard speaker in `store.update`
   like audio, or comment that the asymmetry is deliberate; tag the
   post-refactor state (pyproject 0.2.6x, last tag `v0.1.33`).
5. **Naming backlog** (each mechanical, do when touching the file):
   - `DB.instance_to_child_keys` → a candidate-scan name; do NOT
     rename `audio_id_to_child_keys` (audio→phrase ownership is
     unambiguous).
   - `Segment.overlapping` hides a level shift: `phrase.overlapping`
     yields WORDS (child class), not other phrases. Rename when a
     better name surfaces; `overlapping_children` is also ambiguous.

## Gotchas

- `_children` and `_overlapping` must be created together; creators:
  `children` property, `_cache_child`, `replace_children`.
- The parent_id filter does NOT protect same-boundary rebuild flows —
  replacement children share the old layer's parent_id. Staging is
  safe via `replace_children`; the DISK stays dirty until delete
  (step 2 closes this).
- Phrase-level overlap scanning must not reuse the candidate-scan
  window: that scan finds segments STARTING in range and misses
  earlier-starting sprawlers. `_persisted_phrases_by_group` scans all
  keys with `start < batch max end` instead;
  `test_..._rejects_overlap_inside_persisted` pins it.
- `test_foreign_parent_same_speaker_lands_in_overlapping` persists
  via `save_many` DELIBERATELY — `save_phrase_trees` now rejects that
  fixture as same-speaker overlap; the test pins the read-side filter
  at the layer below. Don't "fix" it to use save_phrase_trees.
- Persisting a re-parented word requires saving the moved SUBTREE:
  descendants carry phrase refs, and `_set_phrase_refs` push-down
  only reaches staged children. The cross-phrase guard in
  `_validate_parent_link` blocks syllable/phone moves outright.
- Re-parenting relies on `Store.get_cached` to uncache from a
  DB-loaded old parent; a parent held only in a variable after
  `store._cache.clear()` cannot be uncached — this is also the one
  path that can put one object in two staged trees (the intra-batch
  key check catches it at save).
- Deletion paths must clean label-index entries
  (`scripts/fix_syllable_labels.py` pattern); saves never do.
- Run tests with the project venv:
  `.venv/bin/python -m pytest tests/ -q` (194 + 33 green).
- `scripts/check_style.py` on touched files; compare ERROR counts
  against `git show HEAD:<file>` — bar is "no new errors"
  (pre-existing findings shift line numbers when code is inserted;
  diff the outputs, not just counts).
- A pre-commit hook bumps the version on every commit.
