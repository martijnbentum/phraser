# Segment linking and batch persistence — continuation notes

Status notes for continuing the phrase-tree work. Rewritten
2026-07-17. The phrase-hierarchy arc is COMPLETE and tagged:
`pre-phrase-hierarchy` (0.2.38) → `post-phrase-hierarchy` (`086a9b5`,
v0.2.65); the diff between the tags is the whole arc. This commit
adds the speaker identity guard on top and rewrites these notes.
Suite green: 201 tests + 33 subtests
(`.venv/bin/python -m pytest tests/ -q`). Every guard added in this
arc was negative-verified: with the guard temporarily disabled, its
tests fail (for the replace semantics, the deletion and its
persisted-end scan window were disabled separately).

## The governing rule (stated in code)

The persistence signal lives in the RECEIVER, not the verb: on a
Segment, only `save` and `delete` touch the database; every other
method and property is in-memory. Tree persistence goes through the
Store. Verbs (`add_`, `replace_`) describe the in-memory effect only.
Written into the `Segment` class docstring and the README staging
section.

## Where the work stands (all committed)

- Receiver-rule docs: sentence in the `Segment` docstring (now a real
  docstring; the old literal sat after the class attributes and never
  became `__doc__`) and a rewritten README staging section. The
  README's construction/linking examples were repaired against the
  current API and executed verbatim against a temp store.
- `children` classifies time-range candidates by
  `candidate.parent_id == self.identifier`; everything else in range
  lands in `overlapping`. `replace_children` is the staging-only
  wholesale relink (validates ALL children before displacing).
- Save guards: intra-batch duplicate keys (`save_many` and the
  replace path), same-speaker phrase overlap (start-sorted
  running-max-end sweep per (audio_id, speaker_id), batch merged with
  persisted; own row key-exempt; persisted-persisted overlap is
  legacy dirt and does not block).
- `save_phrase_trees(overwrite=True)` REPLACES: each phrase's
  persisted tree becomes exactly its staged tree.
  `save_validation.persisted_tree_rows` collects every persisted row
  from RAW rows (phrase row + Word/Syllable/Phone rows in range,
  attributed by parent_id/phrase_id; scan window = the PERSISTED
  row's end); `DB.replace_many` deletes old rows + label-index
  entries and writes staged rows + new entries in ONE LMDB
  transaction. `overwrite=False` is unchanged: `write_many`
  fail-early raises with nothing written.
- `save_validation.py` holds the save-time check family as plain
  functions; `save_many` is factored into
  `_prepare_batch`/`_finalize_batch`, shared by
  `_replace_phrase_trees` (which cannot reuse `save_many`: deletes
  and writes must share a transaction).
- Speaker identity guard (this commit): the loader and every save
  path stamp `_persisted_speaker_id` via
  `store.stamp_persisted_identity` (speaker_id is value-only; `_key`
  already snapshots the audio id);
  `Segment._validate_speaker_assignment` compares on later saves.
  Force-mutated speaker_id now raises on `save`, `save_many` and
  `update`, mirroring the audio guard. Cost: one attribute copy at
  load/save, one equality check at save.

## Decisions made (do not relitigate)

- Receiver rule above; no staging-flavored verb renames. `add_parent`
  IS replace-parent (singular slot). NO `store.replace_phrase_trees`,
  NO stored child lists, NO `delete_children`, NO load/staging split
  of `children` (lazy touch-to-load is the navigation contract).
- `Segment.save` stays row-level. Remapping it to
  `save_phrase_trees([self.phrase])` was considered and REJECTED: the
  verb would overclaim (a leaf save commits every staged edit in the
  tree and, with replace semantics, gains delete authority), unlinked
  segments would need a second conditional path, and `save_many`
  remains a batch backdoor regardless. Candidate narrow fix if forced
  validation is ever wanted: `store.save` running the overlap check
  when handed a Phrase.
- Persisted-tree collection for deletion reads RAW rows, never
  `store.load`/object navigation: the staged tree occupies the cache
  under the same keys, and cache-first navigation reflects session
  state (a re-parented child's mutated parent_id would hide its row
  from the walk). Attribution by identifier needs no recursion —
  words by parent_id, syllables/phones by phrase_id.
- Overwrite replace is delete-first inside one transaction; crash
  safety comes from atomicity, not operation ordering.
- Identity fixed at construction; wrong identity means
  rebuild-and-replace. Never auto-create placeholder speakers. Both
  identity fields are now save-guarded via load/save snapshots
  (`_key` for audio, `_persisted_speaker_id` for speaker).
- Style: explicit `object_type == '...'` branches, no mixins, helper
  modules with plain functions over class indirection.

## Next steps

1. **Modernize `store.update`**: it is the last delete-then-write
   path — two transactions (crash window) and no label-index cleanup
   for the deleted old row. Rebuild it on `DB.replace_many` (one read
   of the old row recovers the label for index cleanup).
2. **Naming backlog** (each mechanical, do when touching the file):
   - `DB.instance_to_child_keys` → a candidate-scan name; do NOT
     rename `audio_id_to_child_keys` (audio→phrase ownership is
     unambiguous).
   - `Segment.overlapping` hides a level shift: `phrase.overlapping`
     yields WORDS (child class), not other phrases. Rename when a
     better name surfaces; `overlapping_children` is also ambiguous.
3. **Optional**: a README-example smoke test (the staging snippet run
   against a temp store) to stop future README rot; a friendlier
   pre-check message for the non-overwrite existing-key case (the DB
   error does not name the colliding object).
4. **Candidate simplification**: `resyllabifier` and
   `fix_syllable_labels` predate replace semantics; their
   save-new/delete-old dances could collapse into
   `save_phrase_trees(overwrite=True)` when next touched.

## Gotchas

- `_children` and `_overlapping` must be created together; creators:
  `children` property, `_cache_child`, `replace_children`.
- The parent_id filter does NOT protect same-boundary rebuild flows —
  replacement children share the old layer's parent_id. Staging is
  safe via `replace_children`; the disk is cleaned by
  `save_phrase_trees(overwrite=True)` (replace semantics). A plain
  `save_many` of a rebuilt layer still leaves the doubled layer.
- Phrase-level overlap scanning must not reuse the candidate-scan
  window: that scan finds segments STARTING in range and misses
  earlier-starting sprawlers. `persisted_phrases_by_group` scans all
  keys with `start < batch max end` instead;
  `test_..._rejects_overlap_inside_persisted` pins it.
- The deletion scan window must be the PERSISTED phrase row's end: a
  re-save with a shrunk end has stale descendants starting beyond the
  staged end. `test_overwrite_reaches_stale_rows_beyond_shrunk_end`
  pins it.
- `test_foreign_parent_same_speaker_lands_in_overlapping` persists
  via `save_many` DELIBERATELY — `save_phrase_trees` rejects that
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
  (`scripts/fix_syllable_labels.py` pattern); saves never do. The
  replace path cleans them inside `DB.replace_many`;
  `Store.delete_many` and `store.update` still do NOT (next step 1).
- Run tests with the project venv:
  `.venv/bin/python -m pytest tests/ -q` (201 + 33 green).
- `scripts/check_style.py` on touched files; compare ERROR counts
  against `git show HEAD:<file>` — bar is "no new errors"
  (pre-existing findings shift line numbers when code is inserted;
  diff the outputs, not just counts).
- A pre-commit hook bumps the version on every commit.
