'''Choose domain verification from changed fields, separate from byte checks.'''

from ..audit import audit_store, compare_reports
from ..audit.report import _finding_identity
from ..audit.selection import DEPENDENCIES, POPULATION_CHECKS
from ..audit.store_snapshot import store_identity
from ..models import segment_flags
from .changes import _check_database, load_change_plan, plan_audio_ids
from .records import write_receipt

STRUCTURAL = set(DEPENDENCIES) - set(POPULATION_CHECKS)
TIMING = {'start_before_end', 'within_audio', 'within_parent',
    'siblings_do_not_overlap', 'same_speaker_phrases', 'children_span_parent',
    'overlap_code_matches'}
FIELD_CHECKS = {'end': TIMING,
    'label': STRUCTURAL - {'speaker_exists', 'speaker_audio_links'},
    'position_code': {'phone_positions', 'syllable_nucleus'},
    'overlap_code': {'overlap_computed', 'overlap_code_matches'}}


ERROR_MEASURES = {
    'start_before_end': (('duration_ms', -1),),
    'within_audio': (('excess_ms', 1),),
    'within_parent': (('start_excess_ms', 1), ('end_excess_ms', 1)),
    'siblings_do_not_overlap': (('overlap_ms', 1),),
    'same_speaker_phrases': (('overlap_ms', 1),)}


def verification_checks(plan):
    '''Return check names, recording scope and explicit omissions for a plan.
    plan:    ordinary ChangePlan; unknown fields use a full audit
    '''
    selected = set()
    broad = False
    for change in plan.changes:
        checks = _change_checks(change)
        if checks is None:
            broad = True
            break
        selected.update(checks)
    if broad: selected = set(DEPENDENCIES)
    audio_ids = None if broad else plan_audio_ids(plan) or None
    skipped = {}
    for name in DEPENDENCIES:
        if name in selected: continue
        reason = 'changed fields do not require this check'
        if name in POPULATION_CHECKS:
            reason = 'population reassessment is a separate milestone'
        skipped[name] = reason
    names = [name for name in DEPENDENCIES if name in selected]
    return {'checks': names, 'audio_ids': audio_ids, 'skipped_checks': skipped}


def _change_checks(change):
    database = change['database']
    if database == 'label_segment': return {'label_index'}
    if database == 'speaker_audio': return {'speaker_audio_links'}
    context = change['context']
    segments = ('Phrase', 'Word', 'Syllable', 'Phone')
    if context.get('object_type') not in segments:
        return None
    if change['operation'] != 'update': return STRUCTURAL
    fields = context.get('changed_fields')
    if not fields: return None
    checks = set()
    for field in fields:
        if field == 'flags':
            affected = _flag_checks(context)
        else: affected = FIELD_CHECKS.get(field)
        if affected is None: return None
        checks.update(affected)
    return checks


def _flag_checks(context):
    changed = context['before']['flags'] ^ context['after']['flags']
    assessment = segment_flags.DURATION_OUTLIER_MASK
    assessment |= segment_flags.TIMING_UNCERTAIN_MASK
    sharing = segment_flags.SHARED_PHONE_MASK
    sharing |= segment_flags.MISSING_SHARED_PHONE_MASK
    known = assessment | sharing | segment_flags.INSERTED_PHONE_MASK
    if changed & ~known: return None
    return {'within_parent'} if changed & sharing else set()


def audit_change_plan(store, plan, progress=True):
    '''Take a fresh domain baseline for an approved plan before applying it.
    store:       open Store, with no concurrent writer
    plan:        ChangePlan whose fields determine checks and recording scope
    progress:    True, False, or a phase-message callback
    '''
    _check_database(store, plan)
    selection = verification_checks(plan)
    report = audit_store(store, checks=selection['checks'],
        audio_ids=selection['audio_ids'], save=False, progress=progress)
    report.info.skipped_checks.update(selection['skipped_checks'])
    return report


def verify_change_plan(store, path, before, progress=True):
    '''Run selected domain checks and save their result beside byte status.
    store:       store just changed; no further writes before verification
    path:        applied ordinary repair record
    before:      fresh report from audit_change_plan immediately before apply
    progress:    True, False, or a phase-message callback

    Returns the after report. New errors never trigger automatic rollback;
    stop further writes and ask the user. Byte verification remains separate.
    '''
    plan = load_change_plan(path)
    _check_baseline(store, plan, before)
    after = audit_change_plan(store, plan, progress)
    comparison = compare_reports(before, after, include_outliers=False)
    new_errors = 0
    for name, findings in comparison.new.items():
        if comparison.severities[name] != 'error': continue
        new_errors += sum(f.status == 'needs_attention' for f in findings)
    attention, worsened = _existing_error_regressions(before, after)
    new_errors += attention + worsened
    status = 'passed' if new_errors == 0 else 'new_errors'
    if not after.results and not after.speakers: status = 'not_run'
    plan.domain_verification = {'status': status,
        'before_transaction_id': before.info.transaction_id,
        'after_transaction_id': after.info.transaction_id,
        'checks': after.info.selected_checks,
        'skipped_checks': after.info.skipped_checks,
        'new_errors': new_errors, 'worsened_errors': worsened,
        'new_attention_errors': attention, 'new_findings': comparison.n_new,
        'resolved_findings': comparison.n_resolved}
    write_receipt(path, plan.to_json())
    return after


def _check_baseline(store, plan, before):
    _check_database(store, plan)
    if plan.status != 'applied' or not plan.verified:
        raise ValueError('Apply and byte-verify the record first')
    if store.DB.env.info()['last_txnid'] != plan.applied_txn_id:
        raise ValueError('Store changed after application')
    expected = plan.applied_txn_id - bool(plan.changes)
    if before.info.transaction_id != expected:
        raise ValueError('Use a fresh baseline immediately before application')
    if before.info.store_identity != store_identity(store.path):
        raise ValueError('Baseline belongs to a different store')
    selection = verification_checks(plan)
    ids = selection['audio_ids']
    scope = None if ids is None else sorted(key.hex() for key in ids)
    if before.info.audio_ids != scope:
        raise ValueError('Baseline scope does not match the repair')
    if before.info.selected_checks != selection['checks']:
        raise ValueError('Baseline checks do not match the repair')


def _existing_error_regressions(before, after):
    '''Count lost explanations and increased error measurements once each.'''
    previous = {}
    for result in before.errors:
        for finding in result.findings:
            identity = (result.name, _finding_identity(finding, False))
            previous[identity] = finding
    attention, worsened = 0, 0
    for result in after.errors:
        for finding in result.findings:
            identity = (result.name, _finding_identity(finding, False))
            old = previous.get(identity)
            if old is None or finding.status != 'needs_attention': continue
            if old.status in ('explained', 'acknowledged'): attention += 1
            elif _worsened_error(result.name, old, finding): worsened += 1
    return attention, worsened


def _worsened_error(check, before, after):
    '''Compare only error measurements with a defined direction of severity.'''
    for field, direction in ERROR_MEASURES.get(check, ()):
        old, new = getattr(before, field, None), getattr(after, field, None)
        if old is None or new is None: continue
        if direction * new > direction * old: return True
    return False
