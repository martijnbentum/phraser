'''What a check reports: findings and their per-check results.

A Finding is one problem found by a check, usually a segment, but sometimes
an audio, an index entry or a speaker-audio link. A CheckResult holds one
check's findings, severity and the number of items it examined.
OutlierGroup describes one comparison group of a duration check and
SpeakerTempo is one row of the speaker tempo summary.
'''

from types import SimpleNamespace

ERROR = 'error'
WARNING = 'warning'


class Finding(SimpleNamespace):
    '''One audit result; shared fields plus measures specific to its check.'''

    def load(self, store):
        '''Return the segment this finding is about, or None if it has none.'''
        key = getattr(self, 'key', None)
        if key is None or not store.DB.key_exists(key): return None
        return store.load(key)


class OutlierGroup(SimpleNamespace):
    '''Log-duration distribution and flagged counts of one comparison group.'''


class SpeakerTempo(SimpleNamespace):
    '''One speaker's tempo per level, with flagged shares around tempo.'''


class CheckResult:
    '''The findings of one check and how many items it examined.'''

    def __init__(self, name, severity, n_checked, findings, n_unjudged=0,
        groups=None):
        self.name = name
        self.severity = severity
        self.n_checked = n_checked
        self.findings = []
        self.n_unjudged = n_unjudged
        self.groups = groups or []
        self.assessments = []
        self.assessment_counts = {}
        self.judgments = {}
        for finding in findings: self.add(finding)

    def __repr__(self):
        m = f'<CheckResult {self.name} {self.severity} '
        m += f'{self.n_findings}/{self.n_checked}>'
        return m

    def add(self, finding):
        '''Append a finding, stamped with this check's name and severity.'''
        if not hasattr(finding, 'status'): finding.status = 'needs_attention'
        finding.check = self.name
        finding.severity = self.severity
        self.findings.append(finding)

    @property
    def n_findings(self):
        return len(self.findings)

    def count_by(self, attribute):
        '''Return {value: count} of a finding attribute, where present.'''
        counts = {}
        for finding in self.findings:
            value = getattr(finding, attribute, None)
            if value is None: continue
            counts[value] = counts.get(value, 0) + 1
        return counts


def _finding(segment, **measures):
    return Finding(key=segment.key, object_type=segment.object_type,
        label=segment.label, start=segment.start, end=segment.end,
        audio_id=segment.audio_id, speaker_id=segment.speaker_id, **measures)


def _size_group(ms):
    '''Separate likely rounding noise from real boundary violations.'''
    if ms <= 1: return '1ms'
    if ms <= 10: return '<=10ms'
    return '>10ms'
