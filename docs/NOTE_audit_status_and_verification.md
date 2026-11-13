# Current-store audit status and repair verification

The current store is the ground truth. Auditing is read-only; comparisons with
historical audits remain optional. No source files are reparsed during audit.

## Selecting checks

```python
from phraser.audit import audit_store

report = audit_store(store, checks=['within_parent', 'within_audio'])
report.summary()
```

`checks=None` keeps the default full set. Names are available in
`phraser.audit.selection.DEPENDENCIES`. Select `speaker_tempo` explicitly when
wanted alongside other selected checks. Unknown names raise an error. Scoped
runs omit population checks and record the reason in `info.skipped_checks`.

Only required row classes are decoded. Label indexes and speaker/audio links
are loaded only for checks using them. Label-index audits still scan the full
index to detect stale aliases. Audit key scans are unchanged. Store opening
can separately opt out of eager query-root loading; RAM reduction is not an
optimization goal.

`progress=True` prints loading/scanning/checking/writing phases with elapsed
time. Pass `False` for silence or a callable such as `messages.append`.

Reports include store file identity, transaction ID at loading, selected
checks, skipped checks, settings and package version. The workflow assumes no
other writer; this does not introduce a concurrency or baseline-reuse system.
Existing reports remain loadable with their original, more limited metadata.

## Status and duration assessments

Raw findings and `n_errors` / `n_warnings` remain unchanged. Findings have a
`status` of `needs_attention`, `acknowledged` or `explained`.
`report.n_attention_errors` counts errors still needing attention.

For duration checks, each outlier includes `stored_assessment`,
`current_assessment`, `assessment_status` and `timing_uncertain`. A matching
assessment acknowledges an outlier; it does not claim corrected timing.

Each check has:

- `assessment_counts`: matching, unset, conflicting and unjudged totals across
  all persisted segments of that level, including excluded speech labels.
- `assessments`: stored assessments conflicting with a current non-outlier
  judgment, or on currently unjudged rows. Conflicts already represented by a
  raw outlier appear on that finding instead of being duplicated here.
- `judgments`: transient current judgments used by assessment planning; these
  are not saved for later application. Small groups, missing normalization
  inputs, nonpositive durations and excluded labels are not positive judgments.

Flags remain unchanged by the audit. Use `plan_duration_assessments` from
`phraser.repair` to prepare reviewed updates from fresh measurements. See
[repair operations](NOTE_repairs.md).

## Shared-phone containment

A small rule explains an owning Syllable's end crossing its Word when:

- Both owning units carry `shared_phone` and have consistent persisted links.
- Their crossing child Phones are marked shared and their ends explain the
  Syllable end without an additional tail.
- The following same-speaker Word in the same phrase has
  `missing_shared_phone` and receives the crossing within its end.
- Relevant durations, speaker/phrase relationships and audio bounds agree.

This interprets stored review flags, rather than independently proving source
provenance. It does not waive start overruns, malformed links, audio overruns
or arbitrary flagged rows. `inserted_phone` alone is not a sharing exception.
Cases outside this narrow rule remain `needs_attention`. Raw containment
findings remain available, including the original excess in milliseconds.

## Deliberately deferred

- Generic reviewed exceptions and dependency fingerprints.
- Rich evidence summaries and additional historical-comparison views.
- Detailed byte-check timing, RAM optimization and concurrent-writer handling.

Store loading and repair refresh are now implemented separately: the default
remains `eager_query_roots=True`, with an explicit `False` opt-out. In-place
repairs reuse valid main-key collections while clearing model caches. See
[Store loading and repair refresh](NOTE_repairs.md#store-loading-and-repair-refresh).
