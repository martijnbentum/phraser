'''Run a full audit of a store and assemble its AuditReport.

The check tuples fix the order in which checks run and are reported. When
saving, the output folder is validated before any rows are loaded.
'''

from datetime import datetime
from importlib import metadata
from types import SimpleNamespace

from .hierarchy import check_childless, check_children_span_parent
from .hierarchy import check_identity_matches_parent, check_orphans
from .hierarchy import check_parent_exists, check_siblings_do_not_overlap
from .hierarchy import check_within_parent
from .labels import check_phone_positions, check_syllable_label_matches_phones
from .labels import check_syllable_nucleus, check_unknown_phone_labels
from .outliers import DEFAULT_K, DEFAULT_MIN_COUNT, check_outliers_phone
from .outliers import check_outliers_phrase, check_outliers_word
from .overlap_checks import check_overlap_code_matches, check_overlap_computed
from .references import check_label_index, check_speaker_audio_links
from .references import check_speaker_exists
from .report import AuditReport, _make_audit_directory
from .speaker_tempo import audit_speakers
from .store_snapshot import load_audit_data
from .timing import check_same_speaker_phrases, check_start_before_end
from .timing import check_within_audio

ERROR_CHECKS = (check_start_before_end, check_within_audio,
    check_within_parent, check_parent_exists, check_identity_matches_parent,
    check_siblings_do_not_overlap, check_same_speaker_phrases,
    check_speaker_exists, check_label_index, check_speaker_audio_links)

QUALITY_CHECKS = (check_orphans, check_childless, check_children_span_parent,
    check_unknown_phone_labels, check_phone_positions, check_syllable_nucleus,
    check_syllable_label_matches_phones)

OVERLAP_CHECKS = (check_overlap_computed, check_overlap_code_matches)

OUTLIER_CHECKS = (check_outliers_phone, check_outliers_word,
    check_outliers_phrase)


def audit_structure(data):
    '''Run the error checks: rules that rows must not break.'''
    return [check(data) for check in ERROR_CHECKS]


def audit_quality(data):
    '''Run the warning checks: suspicious rows that can be legitimate.'''
    return [check(data) for check in QUALITY_CHECKS]


def audit_overlap(data):
    '''Run the overlap checks against the rule in overlap.py.'''
    return [check(data) for check in OVERLAP_CHECKS]


def audit_outliers(data, k=DEFAULT_K, min_count=DEFAULT_MIN_COUNT):
    '''Run the duration outlier checks; these are always warnings.'''
    return [check(data, k, min_count) for check in OUTLIER_CHECKS]


def audit_store(store, data=None, k=DEFAULT_K, min_count=DEFAULT_MIN_COUNT,
    save=True, directory='.'):
    '''Survey a store without changing it and return an AuditReport.
    store:        open Store to audit
    data:         AuditData from load_audit_data, to skip loading the rows
    k:            IQR multiplier for the duration outlier fences
    min_count:    smallest group that is judged for outliers
    save:         write the report to audit_{stamp}; False for an experiment
    directory:    parent folder of the audit_{stamp} folder
    '''
    stamp = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    if save: _make_audit_directory(directory, stamp, store.path)
    if data is None: data = load_audit_data(store)
    if not data.segments: print(f'warning: no segments in {data.path}')
    results = audit_structure(data) + audit_quality(data)
    results += audit_overlap(data) + audit_outliers(data, k, min_count)
    speakers = audit_speakers(data, k, min_count)
    groups = [group for result in results for group in result.groups]
    info = SimpleNamespace(store_path=str(data.path), stamp=stamp,
        phraser_version=_phraser_version(), k=k, min_count=min_count,
        row_counts=data.row_counts)
    report = AuditReport(results, speakers, groups, info)
    if save: print(f'saved audit to {report.save(directory)}')
    return report


def _phraser_version():
    try: return metadata.version('phraser')
    except metadata.PackageNotFoundError: return None
