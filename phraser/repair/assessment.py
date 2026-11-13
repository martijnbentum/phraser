'''Prepare current duration assessments as an ordinary reviewed repair.'''

from ..audit import audit_store, load_audit_data
from ..audit.selection import POPULATION_CHECKS
from ..models.segment_flags import DURATION_OUTLIER_STATES
from .changes import create_change_plan
from .segment_changes import plan_update


def plan_duration_assessments(store, reason, levels=None, k=3, min_count=30,
    reassess=False, progress=True):
    '''Compute current judgments and prepare flag updates without writing.
    store:       open Store with no other writer during this operation
    reason:      reason saved in the repair record
    levels:      phone, syllable, word, phrase; None selects all four
    k:           multiplier for log-IQR fences
    min_count:   minimum population size to judge a group
    reassess:    replace existing assessments; default updates only unset
    progress:    print audit phases, or a callback; False disables output

    Timing-uncertain and unjudged rows are never assigned an assessment.
    Save and apply the returned plan through the ordinary review workflow.
    No saved audit is accepted: population measurements are computed afresh.
    Any store write before application requires preparing a fresh plan.
    '''
    if levels is None: levels = ('phone', 'syllable', 'word', 'phrase')
    if isinstance(levels, str): levels = [levels]
    checks = [f'outliers_{level}' for level in levels]
    allowed = set(POPULATION_CHECKS) - {'speaker_tempo'}
    if not checks or not set(checks) <= allowed:
        raise ValueError('Select phone, syllable, word or phrase assessments')
    data = load_audit_data(store, checks=checks, progress=progress)
    report = audit_store(store, data=data, checks=checks, k=k,
        min_count=min_count, save=False, progress=progress)
    counts = {'changed': 0, 'unjudged': 0, 'timing_uncertain': 0,
        'already_assessed': 0, 'already_matching': 0}
    metadata = {'duration_assessment': {'checks': checks, 'k': k,
        'min_count': min_count, 'reassess': reassess,
        'phraser_version': report.info.phraser_version, 'counts': counts}}
    with store.DB.env.begin() as txn:
        if txn.id() != data.transaction_id:
            raise ValueError('Store changed during duration assessment')
        plan = create_change_plan(store, txn, reason, metadata)
        for result in report.results:
            level = result.name.removeprefix('outliers_')
            segments = getattr(data, f'{level}s')
            _plan_assessments(plan, store, txn, segments, result.judgments,
                reassess, counts)
    plan.metadata['duration_assessment']['counts'] = counts
    return plan


def _plan_assessments(plan, store, txn, segments, judgments, reassess, counts):
    for segment in segments:
        expected = judgments.get(segment.key)
        skip = _skip_reason(segment, expected, reassess)
        if skip:
            counts[skip] += 1
            continue
        code = DURATION_OUTLIER_STATES.index(expected)
        flags = (segment.flags & ~3) | code
        plan_update(plan, store, txn, segment.key, flags=flags)
        counts['changed'] += 1


def _skip_reason(segment, expected, reassess):
    if expected is None: return 'unjudged'
    if segment.timing_uncertain: return 'timing_uncertain'
    if segment.duration_outlier == expected: return 'already_matching'
    if segment.duration_outlier != 'unset' and not reassess:
        return 'already_assessed'
    return None
