'''Checks that the store's lookup tables agree with its segments.

Besides the segment rows, a store keeps a label index and speaker-audio
links; these checks verify both, and that every speaker a segment names
exists.
'''

from ..storage import key_helper
from .findings import ERROR, CheckResult, Finding, _finding
from .store_snapshot import SEGMENT_CLASSES


def check_speaker_exists(data):
    '''Find segments whose speaker_id points at no Speaker row.'''
    findings = []
    for segment in data.segments:
        if segment.speaker_id in data.speaker_by_id: continue
        findings.append(_finding(segment))
    n_checked = len(data.segments)
    return CheckResult('speaker_exists', ERROR, n_checked, findings)


def check_label_index(data):
    '''Find rows without an index entry and entries without a matching row.
    Marker entries are skipped because markers are not audited.'''
    findings = []
    expected = set()
    for segment in data.segments:
        entry = key_helper.label_to_label_index_key(segment.label,
            segment.object_type, segment.key)
        expected.add(entry)
        if entry in data.label_index_keys: continue
        findings.append(_finding(segment, kind='missing_entry'))
    ranks = {key_helper.CLASS_RANK_MAP[name] for name in SEGMENT_CLASSES}
    n_entries = 0
    for entry in data.label_index_keys:
        if key_helper.label_index_key_to_rank(entry) not in ranks: continue
        n_entries += 1
        if entry in expected: continue
        key = key_helper.label_index_key_to_instance_key(entry)
        findings.append(Finding(key=key, index_key=entry, kind='stale_entry',
            row_exists=key in data.by_key))
    n_checked = len(data.segments) + n_entries
    return CheckResult('label_index', ERROR, n_checked, findings)


def check_speaker_audio_links(data):
    '''Find speakers with phrases but no link, and links without phrases.'''
    used = {(p.speaker_id, p.audio_id) for p in data.phrases}
    links = data.speaker_audio_links
    findings = []
    for speaker_id, audio_id in sorted(used - links):
        findings.append(Finding(audio_id=audio_id, speaker_id=speaker_id,
            kind='missing_link'))
    for speaker_id, audio_id in sorted(links - used):
        findings.append(Finding(audio_id=audio_id, speaker_id=speaker_id,
            kind='unused_link'))
    n_checked = len(used | links)
    return CheckResult('speaker_audio_links', ERROR, n_checked, findings)
