'''Checks on the stored overlap codes of segments.

Overlap codes must be computed for every audio and must match a
recomputation by the rule in phraser/overlap.py, done on copies so the store
is not changed.
'''

import copy

from ..overlap import set_overlap_codes
from ..storage import key_helper
from .findings import ERROR, WARNING, CheckResult, Finding, _finding
from .store_snapshot import SEGMENT_CLASSES

OVERLAP_NOT_COMPUTED = 9


def _segments_by_audio(data):
    groups = {}
    for segment in data.segments:
        groups.setdefault(segment.audio_id, []).append(segment)
    return groups


def _uncomputed_finding(audio_id, object_type, n_segments, n_uncomputed):
    return Finding(key=key_helper.audio_id_to_key(audio_id),
        object_type=object_type, audio_id=audio_id, n_segments=n_segments,
        n_uncomputed=n_uncomputed, share=n_uncomputed / n_segments)


def check_overlap_computed(data):
    '''Find audios holding segments whose overlap code was never computed.
    Reports per audio and class because a whole store can be uncomputed, so
    n_checked counts those (audio, class) groups and not single segments.'''
    findings = []
    n_checked = 0
    for audio_id, segments in _segments_by_audio(data).items():
        for object_type in SEGMENT_CLASSES:
            group = [s for s in segments if s.object_type == object_type]
            if not group: continue
            n_checked += 1
            codes = [s.overlap_code for s in group]
            n_uncomputed = codes.count(OVERLAP_NOT_COMPUTED)
            if n_uncomputed == 0: continue
            findings.append(_uncomputed_finding(audio_id, object_type,
                len(group), n_uncomputed))
    return CheckResult('overlap_computed', WARNING, n_checked, findings)


def check_overlap_code_matches(data):
    '''Find stored overlap codes that differ from a recomputation by the
    rule in overlap.py. The writer runs on copies, so stored codes stay
    available and the store is not written; uncomputed codes are skipped.'''
    findings = []
    n_checked = 0
    for segments in _segments_by_audio(data).values():
        copies = [copy.copy(segment) for segment in segments]
        set_overlap_codes(copies)
        for segment, recomputed in zip(segments, copies, strict=True):
            if segment.overlap_code == OVERLAP_NOT_COMPUTED: continue
            n_checked += 1
            if segment.overlap_code == recomputed.overlap_code: continue
            findings.append(_finding(segment, stored=segment.overlap_code,
                expected=recomputed.overlap_code))
    return CheckResult('overlap_code_matches', ERROR, n_checked, findings)
