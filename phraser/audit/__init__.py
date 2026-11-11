'''Read-only survey of the rows in a Phraser store.

audit_store(store) runs every named check and returns an AuditReport. Rows are
decoded straight from disk into fresh objects, never walked through
.children: that is a time-range scan, so a child starting outside its parent
would be invisible to the check meant to find it. The store and its cache
are left untouched.

A check is a function that examines the rows of an AuditData snapshot for
one specific problem and returns a CheckResult: its severity (error or
warning), how many items it examined and a Finding per problem found.

Modules:
    findings          Finding, CheckResult and the other result records
    store_snapshot    AuditData and load_audit_data, all or some recordings
    hierarchy         checks on the parent and child segment tree
    timing            checks on the time range of each segment
    references        checks on the label index and speaker-audio links
    labels            checks on phone and syllable labels and positions
    overlap_checks    checks on the stored overlap codes
    outliers          duration outlier checks
    speaker_tempo     per-speaker tempo summary
    report            AuditReport, saving, load_report and compare_reports
    run               audit_store() and the ordered check groups

Typical use:
    data = load_audit_data(store)
    report = audit_store(store, data=data)
    report.summary()
    report.checks.within_parent.findings[0].load(store).play()
    check_within_parent(data)
'''

from .findings import ERROR, WARNING, CheckResult, Finding, OutlierGroup
from .findings import SpeakerTempo
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
from .report import AuditReport, ReportComparison, compare_reports
from .report import load_report
from .run import ERROR_CHECKS, OUTLIER_CHECKS, OVERLAP_CHECKS, QUALITY_CHECKS
from .run import audit_outliers, audit_overlap, audit_quality, audit_store
from .run import audit_structure
from .speaker_tempo import audit_speakers
from .store_snapshot import AuditData, load_audit_data
from .timing import check_same_speaker_phrases, check_start_before_end
from .timing import check_within_audio

__all__ = ['audit_store', 'load_audit_data', 'load_report', 'AuditData',
    'AuditReport', 'ReportComparison', 'compare_reports', 'CheckResult',
    'Finding', 'OutlierGroup', 'SpeakerTempo',
    'ERROR', 'WARNING', 'DEFAULT_K', 'DEFAULT_MIN_COUNT', 'ERROR_CHECKS',
    'QUALITY_CHECKS', 'OVERLAP_CHECKS', 'OUTLIER_CHECKS', 'audit_structure',
    'audit_quality', 'audit_overlap', 'audit_outliers', 'audit_speakers',
    'check_start_before_end', 'check_within_audio', 'check_within_parent',
    'check_parent_exists', 'check_identity_matches_parent',
    'check_siblings_do_not_overlap', 'check_same_speaker_phrases',
    'check_speaker_exists', 'check_label_index', 'check_speaker_audio_links',
    'check_orphans', 'check_childless', 'check_children_span_parent',
    'check_unknown_phone_labels', 'check_phone_positions',
    'check_syllable_nucleus', 'check_syllable_label_matches_phones',
    'check_overlap_computed', 'check_overlap_code_matches',
    'check_outliers_phone', 'check_outliers_word', 'check_outliers_phrase']
