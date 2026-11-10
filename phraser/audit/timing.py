'''Checks on the time range of each segment on its own.

Start must come before end, segments must end within their audio, and the
phrases of one speaker in one audio must not overlap.
'''

from .findings import ERROR, CheckResult, Finding, _finding, _size_group
from .hierarchy import _overlap_finding, _overlapping_pairs


def check_start_before_end(data):
    '''Find segments with a zero or negative duration.'''
    findings = []
    for segment in data.segments:
        if segment.start < segment.end: continue
        findings.append(_finding(segment, duration_ms=segment.duration))
    n_checked = len(data.segments)
    return CheckResult('start_before_end', ERROR, n_checked, findings)


def check_within_audio(data):
    '''Find segments past their audio's end, or without an audio duration.'''
    findings = []
    for audio in data.audios:
        if audio.duration: continue
        findings.append(Finding(key=audio.key, object_type='Audio',
            label=audio.filename, audio_id=audio.identifier,
            kind='missing_duration'))
    for segment in data.segments:
        audio = data.audio_by_id.get(segment.audio_id)
        if audio is None:
            findings.append(_finding(segment, kind='missing_audio'))
            continue
        if not audio.duration or segment.end <= audio.duration: continue
        excess = segment.end - audio.duration
        findings.append(_finding(segment, kind='past_audio_end',
            excess_ms=excess, size=_size_group(excess)))
    n_checked = len(data.audios) + len(data.segments)
    return CheckResult('within_audio', ERROR, n_checked, findings)


def check_same_speaker_phrases(data):
    '''Find persisted phrases overlapping a phrase of the same speaker.'''
    groups = {}
    for phrase in data.phrases:
        group_key = (phrase.audio_id, phrase.speaker_id)
        groups.setdefault(group_key, []).append(phrase)
    findings = []
    for group in groups.values():
        group.sort(key=lambda phrase: (phrase.start, phrase.end))
        for pair in _overlapping_pairs(group):
            findings.append(_overlap_finding(*pair))
    n_checked = len(data.phrases)
    return CheckResult('same_speaker_phrases', ERROR, n_checked, findings)
