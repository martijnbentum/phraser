# Repair records beside an audit

Repairs record the reason, original/proposed bytes and readable row context
before applying edits. They support inspection and possible reconstruction;
a saved record is not a guarantee that later restoration is appropriate.
The Store is assumed to remain otherwise unchanged while a repair runs.

## Propose, inspect, save and apply

```python
from phraser import audit_store
from phraser.audit import compare_reports
from phraser.repair import apply_change_plan, create_change_plan, plan_delete
from phraser.repair import load_change_plan, plan_audio_ids, save_change_plan

before = audit_store(store, directory='reports')
audit_directory = f'reports/audit_{before.info.stamp}'

# Select roots only after reviewing the findings. Deleting a root includes
# its descendants; an audit finding alone is not a reason to delete a tree.
roots = [reviewed_syllable.key, another_reviewed_syllable.key]
with store.DB.env.begin() as txn:
    plan = create_change_plan(store, txn, 'Remove reviewed empty artifacts',
        metadata={'audit': before.info.stamp})
    plan_delete(plan, store, txn, roots)

plan.summary()                       # counts and up to five examples
plan.changes                         # every affected row, including indexes
path = save_change_plan(plan, audit_directory, 'remove_empty_pairs')
preview = load_change_plan(path)      # does not read the live Store
apply_change_plan(store, path)

after = audit_store(store, audio_ids=plan_audio_ids(plan), save=False)
compare_reports(before, after).summary()
```

Records are named `change_001_remove_empty_pairs.json`,
`change_002_correct_labels.json`, etc. Numbering is for readability, not an
undo chain. Audit loading ignores these extra files. You can instead use
`save_change_plan(plan, path='reports/my_repair.json')`. Existing files are
never overwritten by save; applying updates the existing execution receipt.
Reports must be outside the LMDB directory.

`plan.changes` contains database, key, before/after bytes as hex, operation
and captured context. `None` means an absent row; `''` means empty bytes.
Context includes decoded fields where available, so previewing a saved
record never substitutes newer database values. For arbitrary byte edits,
context may contain only the key. `summary(limit=10)` adjusts the example
count. `count_changes()` groups by database; `by='operation'` or
`by='object_type'` provides other counts. Index changes are separate entries.

Application checks database file identity and every original byte value
inside one write transaction. A mismatch aborts the whole transaction. An
unrelated write since planning does not block application; a completed
record cannot be applied again. Regenerating a proposal in a rerun script
can produce a new operation, so the script still owns its logical checks.
Bulk receipts and malformed ordinary records are refused.

Verification checks resulting bytes and refreshes the Store cache and query
roots. It does not perform a domain audit or refresh references to objects
already held by your code. Reload those objects. Whether to proceed after
re-auditing is your decision; there is no package-enforced verification gate.

## Segment helpers

`plan_delete(plan, store, txn, keys)` accepts a single segment key or multiple
roots. It includes descendants using stored parent links, not time ranges,
and handles overlapping roots once. Submit related roots together: each
affected recording is scanned once and the label index is scanned once for
the combined selection. The scan assumes normal recording ownership and
checks references within those recordings, not arbitrary cross-recording
corruption. It blocks retained parent/phrase references and markers rather
than silently deleting unrelated material.

Deletion removes every label-index entry pointing to a removed row, even
stale entries, and removes a speaker/audio link if its last phrase is deleted.
It never deletes Audio, Speaker or Marker rows through this helper. It can
leave retained parents childless; evaluate that consequence in your review.

`plan_update(plan, store, txn, key, **fields)` updates stored non-relationship
fields, including labels, flags and codes. An explicit `end` must exceed
`start`; an existing bad duration does not prevent a label or flag edit.
Start, version, identity and relationship fields are refused. The helper
moves the index for a changed label; unrelated stale index entries require
separate cleanup. Children are not checked against a new end.

Helpers stage their additions: if a helper fails, the proposal is unchanged.
For unusual corrections, use `add_change(plan, database, key, before, after)`
with exact bytes read from your transaction. That low-level API performs no
domain validation or automatic index maintenance. Metadata must be a plain
JSON dictionary; audit classes and finding serializers are not required.

## Immediate undo and interrupted operations

```python
from phraser.repair import check_change_state, rollback_changes

state = check_change_state(store, path)
print(state)
rollback_changes(store, path)         # only if still immediately undoable
```

Immediate undo requires a completed ordinary record, matching resulting
bytes and the exact database transaction ID recorded after application.
Any later write, even to unrelated rows, makes it stale. After A, B, undo B,
A stays stale. Older restoration is a new reviewed repair, using the saved
bytes as evidence. There is no archive, session lifecycle or undo chain.

`check_change_state` reads one snapshot and changes neither the Store nor the
receipt. It returns `state`, per-row `counts`, `transaction_id` and
`recorded_status`. States are `original`, `proposed`, `mixed`, `conflicting`,
or `both` for empty/no-op ambiguity. These describe current bytes, not proof
of which operation produced them.

An interrupted operation can leave an `applying` or `rolling_back` receipt
although its transaction committed. Application and undo refuse these
records by default. After inspecting the state, an `applying` record can be
retried explicitly with `apply_change_plan(store, path, retry_original=True)`.
Every affected row must match its original bytes inside the write
transaction; mixed, proposed or conflicting changes are refused atomically.
Original bytes describe current state, not proof that nothing ever committed.
The caller still assumes no intervening changes made the repair inappropriate.
Interrupted undo requires reviewed reconstruction. This package does not
automatically reconcile receipts.

## Scoped verification

`plan_audio_ids(plan)` derives recording IDs from main, label-index and
speaker/audio-link keys. Speaker-only changes cannot be scoped from these
keys; use a full audit or broader inspection for those changes.

Scoped audits skip population-wide outlier checks and speaker tempo. Report
comparison tracks finding identities, not changes in their magnitude. The
original audit remains a historical snapshot.

Comparisons between full audits include outlier checks by default. Their
population fences can move when durations or group membership change, so an
unchanged segment can become or cease to be an outlier. Use
`compare_reports(before, after, include_outliers=False)` to omit these checks.
Across different scopes, parent-reference kinds `other_audio` and
`missing_row` compare as the same problem because a scoped snapshot cannot
see the other recording. Their original kinds remain available for display.
New reports retain each stale label-index entry's `index_key` so partial
index cleanup is visible. Older reports lack that identity and should be
regenerated for precise stale-entry comparisons.

## Exceptional bulk operations

A bulk script can maintain one logical repair and one progress receipt:

```python
from phraser.repair import save_record, write_receipt

receipt = {'version': 1, 'kind': 'bulk',
    'database': {'path': str(store.path)},
    'reason': 'Assess duration flags', 'status': 'prepared',
    'full_row_backups': False, 'completed_batches': 0}
path = save_record(receipt, audit_directory, 'duration_flags')
# The script performs and verifies batches, then updates the same receipt.
receipt['completed_batches'] = completed_batches
receipt['status'] = 'in_progress'
write_receipt(path, receipt)
```

This is a durable log writer, not a batch executor. The script owns byte
backups or other reconstruction evidence, progress, verification, partial
completion and resume behavior. Neither progress nor completion implies
undo or automatic recovery. `kind='bulk'` records cannot be passed to the
ordinary application, state-check or undo functions.
