# Minimal repair package

Status: implemented; ordinary repairs, records, segment helpers and audit
integration have targeted regression coverage. Bulk execution remains an
operation-specific script responsibility.

Simplify the existing package in place. Its main purpose is to support
reviewed store edits and keep a useful record for inspection and possible
reconstruction. Automatic undo and recovery are conveniences, not promises.
Keep audit read-only and independent of repair.

## Workflow and scope

Audit, propose a repair, inspect its preview, save its record, apply, verify,
then continue working. The caller decides whether verification is sufficient
before proceeding. There is no package-enforced verification gate.

Assume the Store is not modified elsewhere during a repair. A general
concurrency protocol and guards against unrelated writes are outside scope.
Still check the edited rows against their saved original bytes: this keeps
the record truthful and refuses stale proposals or repeated application.
Transaction IDs may be recorded as provenance; do not build a session chain
around them.

Three responsibilities remain distinct:

- The change writer saves the intended edits and original bytes, and applies
  an ordinary repair in one atomic transaction.
- Segment helpers handle normal related edits, such as descendant deletion,
  label-index maintenance and retained-reference checks.
- The repair script supplies domain judgment and operation-specific checks.

## Feature 1: change records and execution

Requirements:

- Retain a small ChangePlan container if useful. Record reason, optional
  plain JSON metadata, database identity, original and proposed bytes, and
  execution results. Do not import audit's private serializers.
- Capture original bytes and readable row context during planning.
- Save the record before any mutation, even for immediate application.
- Apply ordinary repairs in one locked transaction, including index changes.
- Retain database-identity and record-status checks before application.
- Before writing, check inside the write transaction that every affected
  row holds its recorded original bytes. On any mismatch, abort without
  writing. Keep the existing byte-checking implementation and coverage.
  This validates the proposal; it is not a general concurrency protocol.
- Refuse applying a completed record again. Regenerating a proposal can
  legitimately capture newer original bytes; preventing repeated logical
  transformations remains the repair script's responsibility.
- Verify the resulting bytes and refresh Store caches and query roots.
- Preserve durable atomic receipt replacement. Record completion only after
  execution and verification; an incomplete receipt is not proof that no
  changes were committed.
- Remove fixed store-side folders, session chains, previous-plan links,
  archiving, reader checks and mandatory snapshot-ID guards.
- Preserve useful existing implementation and tests rather than rewriting
  sound transaction code merely to reduce its line count.

Undo and reconstruction:

- Exact before/after bytes support a possible restoration operation. Keeping
  this information is required; a general automatic recovery engine is not.
- Retain simple immediate undo only if it remains small and can establish
  that restoration is appropriate. Otherwise explain why automatic undo is
  unavailable and leave the record usable for a reviewed reconstruction.
- If automatic undo is offered, retain straightforward identity, byte and
  transaction checks needed to refuse stale restoration. These are guards
  for the optional undo convenience, not a general concurrency workflow.
- Do not offer an undo chain or out-of-order automatic undo. After A, B,
  undo B, restoring A is a new reviewed repair.
- Do not automatically retry an ambiguous interrupted operation. Report the
  uncertainty and retain its evidence for inspection or reconstruction.
  An explicit retry of an applying record is allowed when every original
  byte value still matches inside the write transaction.
- Offer a read-only state check comparing current rows with recorded bytes
  in one read snapshot: all original, all proposed, or mixed/conflicting.
  Report when both match, including empty/no-op plans. This describes the
  current state, not proof that this particular operation committed.
  It must not change the Store, rewrite the receipt or retry the operation.
- Ordinary use of the Store after repair can make restoration inappropriate;
  do not describe saved original bytes as a guarantee of safe undo.

Tests:

- Insert, update and delete, including absent versus empty byte values.
- A write failure aborts all rows and index edits in an ordinary transaction.
- Exact saved original/proposed bytes and resulting-byte verification.
- Original-byte conflicts abort the complete operation; stale proposals,
  wrong databases and repeated application are refused without mutation.
- Read-only state checks cover original, proposed, mixed, conflicting and
  ambiguous no-op states, leaving the Store and receipt untouched.
- Interrupted receipt updates do not falsely report completion or silently
  repeat an operation.
- If immediate undo is retained, verify exact restoration and refusal when
  its simple eligibility checks fail.

## Feature 2: storage and preview

Requirements:

- Save inside the audit report directory, alongside its summary and findings:
  change_001_remove_empty_pairs.json, change_002_correct_durations.json.
- Take a caller-supplied audit directory and short name, choosing the next
  available number without overwriting an existing record. Numbers are for
  readability, not dependency tracking.
- Allow an explicit destination path for external scripts.
- Keep all repair records outside the LMDB directory.
- Show counts by operation and segment type, plus a few readable examples
  with recording, label, timing and changed fields where applicable.
- Make all affected rows inspectable without printing everything.
- Previews use values captured during planning, not newer live Store values.
- Keep exact byte data in the same ordinary repair file. Optional metadata
  can refer to the original audit without requiring audit objects.

Tests:

- Number allocation, collision refusal, explicit paths and save/load round
  trips, including metadata and readable row context.
- Bounded previews and access to the full affected-row list.
- Clear errors for unusable records before attempting application.

## Feature 3: practical segment helpers

Requirements:

- Accept multiple deletion roots in one proposal. Collect their descendants
  using stored parent links, and scan references and label indexes once for
  the combined selection rather than once per tree.
- Work with the normal model: a phrase and its descendants belong to one
  recording, and an item belongs to one phrase. Do not introduce a general
  malformed-hierarchy framework or new persistent reverse indexes.
- Include descendants automatically in the preview. Refuse known retained
  references, including markers and explicit phrase references outside the
  selected tree, and show the blockers. Do not silently expand deletion to
  unrelated material. Obvious ambiguity should be reported, not guessed at.
- Delete all label-index entries targeting removed rows, including stale
  entries; remove speaker/audio links when the last phrase is removed.
- Support routine label, end, flag and code edits without enumerating every
  possible repair policy. Validate fields against stored data and keep the
  established boundary: start and identity/relationship changes need a
  separately reviewed operation rather than the routine update helper.
- When explicitly changing end, require end > start. Do not reject label,
  flag or code edits merely because the existing duration is non-positive.
  Unusual timing edits remain possible through the lower-level byte API.
- Maintain the label index when a label changes. Other domain consequences
  remain the responsibility of the repair script and subsequent audit.
- Reuse existing model/storage hierarchy definitions where practical.
- Build helper additions before extending the proposal so a refused helper
  does not leave partial edits behind.

Tests:

- Multiple-root tree deletion with overlapping roots handled consistently.
- Retained markers and phrase references block deletion.
- Stale-index removal, last-phrase speaker links and unchanged retained rows.
- Routine updates and label-index maintenance; refused structural edits.
- Explicit end edits reject end <= start; unrelated field edits remain
  possible on existing zero- or negative-duration segments.
- Failed helper calls leave the proposal unchanged.

## Feature 4: audit integration

Requirements:

- Retain scoped auditing and report comparison as general audit features.
- Keep audit independent of repair.
- Retain plan_audio_ids(plan), deriving affected recording IDs from changed
  keys, including label-index entries and speaker/audio links. Use it for
  scoped re-auditing. Speaker-only changes cannot be fully scoped from their
  keys; document that they require broader inspection or a full audit.
- Distinguish successful byte verification from a successful domain audit.
- Document the normal workflow and the record's reconstruction limits.

Tests:

- Retain scoped snapshot and before/after comparison coverage.
- Exercise audit, preview, save, apply and scoped re-audit together.
- Recording-ID extraction covers main rows, label indexes and speaker/audio
  links, deduplicates IDs and documents the speaker-only limitation.

## Feature 5: exceptional bulk operations

Ordinary repairs use one atomic transaction and exact byte records. Do not
add a generic batch runner.

An operation-specific script can maintain one numbered logical repair and
one evolving receipt across multiple transactions. Reuse small durable
receipt-writing helpers where helpful. A simple format version and record
kind distinguish ordinary byte plans from bulk receipts; ordinary apply or
undo must not accept a bulk receipt as an exact byte plan.

The script owns batching, progress, verification and reconstruction policy.
Record whether original bytes were retained, how far execution progressed,
and what restoration information is available. No automatic resume or undo
is implied by a progress counter or a completed receipt.

The remote duration-flag operation is the example: one script, explanation
and JSON receipt; a temporary key/state plan; 398 batches after an initial
outlier transaction; operation-specific transaction checks; final state and
count checks plus a checksum masking only changed bits. It retained no
per-row backups and implemented no automatic resume. Its verified initial
zero flags enabled a limited restoration strategy. Those specialized checks
need not become requirements of the general package.

Tests:

- Repeated receipt checkpoints remain readable and durably saved.
- Bulk receipts cannot be mistaken for executable ordinary byte plans.
- Operation-specific execution and reconstruction need their own checks.

## Implementation order

1. Simplify the existing record/writer and remove session machinery.
2. Add numbered audit-folder saving and readable previews.
3. Adapt segment helpers for combined tree deletion and routine updates.
4. Update documentation and run targeted repair/audit tests.
5. Review exports and remove obsolete session and recovery-policy tests.

Keep unrelated working-tree changes intact. No changes to the remote
Store or its historical repair records are part of this work.
