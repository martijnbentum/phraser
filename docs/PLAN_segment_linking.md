# Segment linking and batch persistence — continuation notes

Status notes for continuing the phrase-tree work. Rewritten
2026-07-16 (third rewrite that day), after steps 1–3 of the previous
version landed in the working tree on top of `c2da1a4` (uncommitted —
commit before continuing; the pre-commit hook bumps the version).
Suite green: 199 tests + 33 subtests
(`.venv/bin/python -m pytest tests/ -q`). Every new guard was
negative-verified: with the guard temporarily disabled, its tests
fail (the overwrite deletion and its persisted-end scan window were
each disabled separately).

## The governing rule (now stated in code)

The persistence signal lives in the RECEIVER, not the verb: on a
Segment, only `save` and `delete` touch the database; every other
method and property is in-memory. Tree persistence goes through the
Store. Verbs (`add_`, `replace_`) describe the in-memory effect only.
Written into the `Segment` class docstring and the README staging
section (step 1, done).

## Where the work stands (all in working tree, tested)

- Receiver-rule docs (old step 1): sentence in the `Segment`
  docstring — which is now a real docstring; the old string literal
  sat after the class attributes and never became `__doc__` — and a
  rewritten README example section. The README's construction and
  linking examples were broken against the current API (missing
  mandatory identity, deleted `add_audio`, removed `update_database`)
  and were repaired in the same pass; both sections were executed
  verbatim against a temp store to verify.
- `save_phrase_trees(overwrite=True)` now REPLACES (old step 2): each
  phrase's persisted tree becomes exactly its staged tree. Design is
  delete-first, atomic: `save_validation.persisted_tree_rows` collects
  every persisted row of the phrase from RAW rows (the phrase row plus
  Word/Syllable/Phone rows in range attributed by
  parent_id/phrase_id), then `DB.replace_many` deletes old rows + old
  label-index entries and writes staged rows + new label entries in
  ONE LMDB transaction (`env.begin` spans named dbs). A crash leaves
  the old trees or the new ones, never neither. `overwrite=False` is
  unchanged: `write_many`'s fail-early existence check raises with
  nothing written.
- `save_validation.py` extracted (old step 3): plain functions
  `check_intra_batch_keys`, `validate_phrase_trees`,
  `check_same_speaker_overlap`, `persisted_phrases_by_group`,
  `persisted_tree_rows`; Store delegates. `save_many` was factored
  into `_prepare_batch`/`_finalize_batch`, shared by the
  `_replace_phrase_trees` path (which cannot reuse `save_many`: the
  deletes and writes must share a transaction).
- `DB.time_range_keys(audio_id, child_class, start, end)`: explicit
  range scan; `instance_to_child_keys` now delegates to it.
  `key_helper.label_to_label_index_key` builds label-index keys from
  raw row parts; the instance variant delegates.
- Earlier (committed in `c2da1a4`): ownership-filtered `children` +
  `overlapping` rename, `replace_children`, intra-batch key guard,
  same-speaker overlap sweep, `key_to_start`.

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
- Style: explicit `object_type == '...'` branches, no mixins, helper
  modules with plain functions over class indirection.

## Next steps

1. **Housekeeping**: guard speaker in `store.update` like audio, or
   comment that the asymmetry is deliberate; tag the post-refactor
   state (pyproject 0.2.6x, last tag `v0.1.33`).
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
  (`scripts/fix_syllable_labels.py` pattern); saves never do. The
  replace path cleans them inside `DB.replace_many`;
  `Store.delete_many` still does NOT.
- The deletion scan window must be the PERSISTED phrase row's end: a
  re-save with a shrunk end has stale descendants starting beyond the
  staged end. `test_overwrite_reaches_stale_rows_beyond_shrunk_end`
  pins it.
- Run tests with the project venv:
  `.venv/bin/python -m pytest tests/ -q` (199 + 33 green).
- `scripts/check_style.py` on touched files; compare ERROR counts
  against `git show HEAD:<file>` — bar is "no new errors"
  (pre-existing findings shift line numbers when code is inserted;
  diff the outputs, not just counts).
- A pre-commit hook bumps the version on every commit.
