# Repair records beside an audit

This describes the current runtime API. For the latest design goals and
proposed simplifications, see
[the repair design review](NOTE_repair_design_review_2026-11-16.md).

Repairs save a small JSON receipt and immutable compressed binary evidence
before applying edits. Evidence contains database names, keys and exact
original/proposed bytes. Full decoded rows are not copied into JSON.
They support inspection and possible reconstruction; a saved record is not
a guarantee that later restoration is appropriate.
The Store is assumed to remain otherwise unchanged while a repair runs.

## Propose, inspect, save and apply

```python
import json
from pathlib import Path

from phraser.repair import apply_change_plan, create_change_plan, plan_delete
from phraser.repair import audit_change_plan, verify_change_plan
from phraser.repair import save_change_plan
from phraser.repair import verification_checks

audit_directory = Path('reports/audit_under_review')

# Select roots only after reviewing the findings. Deleting a root includes
# its descendants; an audit finding alone is not a reason to delete a tree.
roots = [reviewed_syllable.key, another_reviewed_syllable.key]
with store.DB.env.begin() as txn:
    plan = create_change_plan(store, txn, 'Remove reviewed empty artifacts',
        metadata={'source_report': str(audit_directory)})
    plan_delete(plan, store, txn, roots)

plan.summary()                       # counts and up to five examples
checks = verification_checks(plan)['checks']
baseline = audit_change_plan(store, plan) if checks else None
path = save_change_plan(plan, audit_directory, 'remove_empty_pairs')
applied = apply_change_plan(store, path)

verification = applied.domain_verification
if checks:
    after = verify_change_plan(store, path, baseline)
    verification = json.loads(path.read_text())['domain_verification']
print(f'{len(applied):,} rows; bytes verified: {applied.verified}')
print(verification)
```

Records are named `change_001_remove_empty_pairs.json`,
`change_002_correct_labels.json`, etc. Numbering is for readability, not an
undo chain. Audit loading ignores these extra files. You can instead use
`save_change_plan(plan, path='reports/my_repair.json')`. Existing files are
never overwritten by save; applying updates only the JSON execution receipt.
Each receipt references a sibling `change_001_remove_empty_pairs.rows.gz`
payload by filename, SHA-256 and row count. The payload is written once and
does not change during application, verification or optional undo.
Both files must stay together and outside the LMDB directory. Current byte
records use version 2; old embedded-JSON record loading is not supported.

`plan.changes` returns an ordered inspection snapshot; extend plans through
the helpers rather than mutating that list. List mutation raises a clear
error; use `merge_change_plan(plan, other)` to combine plans. Each inspection
entry contains database, key, before/after bytes as hex, operation and
decoded context. `None` means an absent row; `''` means empty bytes.
Context is decoded on demand from the saved evidence, so previewing a saved
record never substitutes newer database values. Internal traversal and
bounded summaries do not build full inspection snapshots. For arbitrary edits,
context may contain only the key. `summary(limit=10)` adjusts the example
count. `count_changes()` groups by database; `by='operation'` or
`by='object_type'` provides other counts. Index changes are separate entries.
`plan.to_receipt()` returns small metadata for receipt updates;
`plan.to_json()` explicitly materializes a full inspection snapshot and is
not the persisted record format. Avoid it and `plan.changes` in large loops.

Use the returned `applied` plan for counts and byte-verification status.
Do not reload a completed payload just to report these. Domain verification
updates the receipt, not a previously returned plan object; read that small
JSON file when the latest domain result is needed. Reserve load_change_plan
for inspecting evidence, reconstruction or interrupted-state diagnosis.

Application checks database file identity and every original byte value
inside one write transaction. A mismatch aborts the whole transaction. An
unrelated write since planning normally does not block application; a completed
record cannot be applied again. Regenerating a proposal in a rerun script
can produce a new operation, so the script still owns its logical checks.
Bulk receipts and malformed ordinary records are refused.
The payload checksum and framing are validated before any execution receipt
or store mutation. Corrupted, truncated or duplicate evidence is refused.

Verification checks resulting bytes and refreshes the Store cache and query
roots. It does not perform a domain audit or refresh references to objects
already held by your code. Reload those objects. `audit_change_plan` and
`verify_change_plan` provide optional field-based
domain verification, recorded separately as `domain_verification`. Application
itself still performs only byte verification. There is no automatic rollback:
if domain verification finds new errors, stop further writes and ask the user.
Waiting for approval does not invalidate rollback; an intervening write does.

Byte verification establishes faithful execution of the proposal, not the
correctness of the repair decision. The controlled CGN overlap test passed
byte verification while deliberately creating a structural error. Agent
review and the relevant domain checks remain separate responsibilities.

## Combining plans

```python
from phraser.repair import merge_change_plan

merge_change_plan(plan, local_plan)
```

This replaces the former `plan.changes.extend(local_plan.changes)` pattern,
which now raises an error instead of silently discarding edits. Both plans
must be planned against the same store and transaction. Duplicate keys or
invalid source records refuse the entire addition, leaving the destination
unchanged. The source remains unchanged; the destination reason and metadata
remain in effect. An explicit freshness precondition on either plan is
preserved by merging. Metadata has no special execution meaning.

Use `create_change_plan(..., expected_transaction_id=txn.id())` when the
judgment depends on an unchanged whole store, not just the affected bytes.
Application checks that precondition inside the write transaction. Assessment
planning sets it automatically because other rows affect population fences
and normalization. Ordinary plans leave it unset.

## Store loading and repair refresh

`Store(path, eager_query_roots=False)` opts out of the initial main-key scan.
The default remains `True`. Direct reads and repair planning can work without
initializing query-root keys; querying the whole class loads them when needed.
Sampling or requesting database counts also needs key loading.

Application and rollback always clear cached objects and preload markers,
and recreate the Store's query objects. For in-place updates they retain key
collections when those collections match the pre-repair transaction. Label
index changes do not invalidate main-key membership. If keys have not yet
been loaded in a deferred Store, an in-place repair leaves them unloaded.

Insertion/deletion of main rows, or a cached collection predating other
writes, triggers the existing full rebuild. This conservative fallback also
covers general writes made before the repair. Subsequent in-place repairs
can reuse the refreshed collections.

This optimization belongs to repair application/rollback. General saves and
loads keep their behavior, and an explicit `store.refresh_query_roots()` still
rebuilds from the database. Objects and derived queries already held by callers
remain snapshots and must be reloaded/recreated after a repair.

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

Helpers stage only their new additions: if a helper fails, the proposal is
unchanged. The plan centrally tracks changed keys and rejects duplicates at
addition, including low-level `add_change` calls. Repeated segment updates do
not copy or rescan the accumulated plan. Save/load still validate the complete
record, without persisting a decoded `changes` list. Update planning decodes
the original row once and reuses it for validation and minimal context.
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

Rollback attempts retain the domain-verification result for the original
applied transaction, including when rollback is refused. Its transaction IDs
identify the state it describes; it is not a fresh audit of the rolled-back
store.

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

A later restoration may return previously committed rows to the original
state. Inspect the surrounding repair history before deciding on a retry;
record restoration as a separate repair and preserve the earlier receipt.

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

Subprocess tests terminate a real writer after its first uncommitted row
and after commit but before the completion receipt. Reopening sees the
original or proposed rows/indexes respectively, while the receipt remains
applying. Inspection and a normal application attempt do not change the
receipt, payload or database. No implicit recovery is performed.

## Selected verification

`verification_checks(plan)` returns check names, affected recording IDs and
reasons for omitted checks. `audit_change_plan(store, plan)` takes a fresh
baseline with that selection. After application,
`verify_change_plan(store, path, before)` runs the same selection and updates
the receipt's domain-verification result, returning the after report.
For byte-only changes, `audit_change_plan` returns None without loading any
audit data or creating an empty report. The agent can skip both audit calls;
application still checks all bytes and reports domain verification as
`not_run`. Calling `verify_change_plan(store, path)` is optional in that case:
it records the selected-check omission reasons and returns None.

- Duration-assessment and timing-uncertainty flags: byte verification only.
- Shared/missing-shared flags: containment interpretation as well.
- Ends: relevant timing, containment, sibling, coverage and overlap checks.
- Labels: relevant structural/quality/overlap and label-index checks.
- Deletions: structural/quality/overlap and index/link checks.
- Unknown fields or unsupported row types: a full audit.

Known changes leave population reassessment to an explicit milestone. Omitted
checks and reasons are recorded; byte verification and `not_run` domain status
are distinct from a passed domain audit. Losing a sharing explanation counts
as a new domain error even when the raw finding identity is unchanged.
Increasing containment/audio excess or overlap, or making an invalid duration
more negative, also fails verification. Each regressed finding counts once.
`new_errors` includes these regressions; `worsened_errors` and
`new_attention_errors` provide the breakdown. Reduced or unchanged error
measurements do not count as regressions. No rollback is automatic.

When domain checks apply, verification requires a baseline immediately before
application and no writes between application and verification. A finding-only
recheck does not establish absence of new errors elsewhere. Keep the no-other-
writer assumption throughout the repair cycle.

`plan_audio_ids(plan)` remains available for manual workflows. Speaker-only
changes cannot be scoped from these keys. Scoped audits skip population
outliers and speaker tempo. Check selection is available through
`audit_store(store, checks=['within_parent', 'within_audio'])`.

Report comparison remains optional. It tracks finding identities, not changes
in magnitude. `compare_reports(before, after, include_outliers=False)` excludes
moving population fences. Parent-reference `other_audio` and `missing_row`
findings normalize across scopes, and stale label-index entries retain their
`index_key` identity. Current store flags and relationships are sufficient for
assessment/interpretation reporting without an earlier audit.

## Persisting duration assessments

```python
from phraser.repair import plan_duration_assessments

plan = plan_duration_assessments(store, 'Assess current durations',
    levels=['phone', 'syllable', 'word', 'phrase'], k=3, min_count=30)
plan.summary()
print(plan.metadata['duration_assessment'])
# Review and approve, then use the ordinary baseline/save/apply/verify flow.
```

This computes fresh whole-store log-IQR judgments using the audit's grouping
and pooling rules. It prepares an ordinary byte-change plan and never writes
the store. It accepts no historical report, so stale measurements cannot be
used as input. Application requires the same store transaction as assessment planning, so
changes to population or child normalization inputs require a fresh plan.
Original-byte checks remain in place.

Only judged, timing-certain rows are eligible. By default, existing assessments
are preserved and only unset values receive `short`, `long` or `within_range`.
`reassess=True` explicitly replaces existing eligible assessments. Unjudged
and timing-uncertain rows remain unchanged, including any old flags; the audit
reports those separately. All unrelated bits are preserved. Counts and settings
are included in the plan metadata. This is distinct from fiemrok's raw-duration
per-type assessments.

## Exceptional bulk operations

Choose bounded groups before constructing a large proposal. Each ordinary
plan now uses compressed evidence and a small receipt; that storage change
does not make an already huge in-memory plan bounded. Each application is
atomic, and the caller chooses whether to continue to the next group.
If a later group stops, earlier committed groups remain applied. Inspect
and report the stopped group rather than replaying the completed groups.

Reuse the same API for large flag jobs, relabeling and subtree repairs. Keep
each group's related index/link edits together. For deletions, choose groups
such as recordings and submit their roots together rather than cutting an
already prepared tree into arbitrary byte batches. The agent owns grouping,
progress and the decision to continue; no batch/session manager is required.

For example, after reviewing and approving a bounded update rule:

The caller chooses group size. Start with 100,000 for independent flag
edits, then measure the first representative group's preparation, saving,
application and selected verification before choosing the remaining sizes.
That size is an observation from the CGN trial, not a package default or an
appropriate split for every repair type.

```python
for group in approved_groups:
    with store.DB.env.begin() as txn:
        plan = create_change_plan(store, txn, reason)
        for key, fields in group:
            plan_update(plan, store, txn, key, **fields)
    plan.summary(limit=5)
    checks = verification_checks(plan)['checks']
    baseline = audit_change_plan(store, plan) if checks else None
    path = save_change_plan(plan, audit_directory, 'reviewed_group')
    applied = apply_change_plan(store, path)
    if checks:
        after = verify_change_plan(store, path, baseline)
        verification = json.loads(path.read_text())['domain_verification']
        if verification['new_errors']: break  # Stop and report to the user.
    print(f'{len(applied):,} rows; bytes verified: {applied.verified}')
```

This is a caller loop over the general helpers, not an automatic executor.
No group may silently expand an approved selection. Measure preparation,
saving, application and verification on a representative bounded group
before choosing sizes for a large operation. save_record/write_receipt alone
are persistence helpers, not a bulk executor.

For byte-only groups, application already verifies the original and
resulting bytes; skip the optional audit helpers. Repeated payload loading
for reporting added about 40 seconds to the million-row CGN trial, and the
optional verification-receipt calls added another 40 seconds. Those calls
were used to exercise the API, not requirements for routine flag repairs.

Keep complete-store checksums for explicit milestones such as validating a
new copy, a changed storage implementation or an agreed large repair job.
Do not repeat them after every group. This does not relax per-group byte
checks or the relevant selected domain audits.

### Reusing an unchanged audit for duration flags

A separately reviewed plan can use saved duration findings when the audit's
resolved store identity (path, device and inode) and transaction ID exactly
match the planning snapshot. Record checksums of the source report files,
its settings and the selected whole-store check. Transaction IDs are local
to an LMDB environment; matching numbers alone do not identify a store.
Device numbers are host-local. Across NFS clients, establish the shared
filesystem/file identity before accepting a device-number difference;
preserve the historical report rather than rewriting its identity.
An explicit user-approved device-number exception must be recorded with both
identities. Continue checking path, inode and transaction, and use the current
device for execution checks.

Saved reports omit transient per-row judgments. Assign within_range to
non-outliers only when the saved assessment counts prove all persisted rows
of that level were judged. Otherwise establish the eligible keys separately
or recompute the selected check. Preserve timing-uncertain rows completely.
Build updates with plan_update, preserving all bits except the lowest two.
The fresh-measurement helper above does not accept saved reports.

Any intervening write invalidates a new saved-audit plan. An approved batch
operation must track its own consecutive transaction chain and stop on any
unexpected write. Retain original-byte checks and durable repair records.

### Bulk receipts

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
