'''Read-only survey of the rows in a Phraser store.

audit(store) runs every named check and returns an AuditReport. Rows are
decoded straight from disk into fresh objects, never walked through
.children: that is a time-range scan, so a child starting outside its parent
would be invisible to the check meant to find it. The store and its cache
are left untouched.

Typical use:
    data = load_audit_data(store)
    report = audit(store, data=data)
    report.summary()
    report.checks.within_parent.findings[0].load(store).play()
    check_within_parent(data)
'''

import copy
from datetime import datetime
from importlib import metadata
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np

from . import key_helper
from .key_helper import EMPTY_ID
from .overlap import SILENCE_LABELS, set_overlap_codes
from .phone_types import PHONE_TYPES
from .store import value_key_to_instance

ERROR = 'error'
WARNING = 'warning'
DEFAULT_K = 3
DEFAULT_MIN_COUNT = 30
REPORT_KS = {'n_k_1_5': 1.5, 'n_k_3': 3, 'n_k_5': 5}
# smallest log-duration IQR, so clustered durations do not flag normal values
MIN_LOG_IQR = 0.1
SEGMENT_CLASSES = ('Phrase', 'Word', 'Syllable', 'Phone')
BYTES_FIELDS = ('key', 'other_key', 'audio_id', 'speaker_id')
FLAG_MEASURES = ('vowel', 'consonant', 'word')
UNASSIGNED_POSITION = 9
NUCLEUS_POSITION = 2
OVERLAP_NOT_COMPUTED = 9


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
        self.findings = findings
        self.n_unjudged = n_unjudged
        self.groups = groups or []
        for finding in findings:
            finding.check = name
            finding.severity = severity

    def __repr__(self):
        m = f'<CheckResult {self.name} {self.severity} '
        m += f'{self.n_findings}/{self.n_checked}>'
        return m

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


class AuditData:
    '''Every persisted row of one store, decoded once from disk.'''

    def __init__(self, path, rows, label_index_keys, speaker_audio_links):
        self.path = path
        self.audios = rows['Audio']
        self.speakers = rows['Speaker']
        self.phrases = rows['Phrase']
        self.words = rows['Word']
        self.syllables = rows['Syllable']
        self.phones = rows['Phone']
        self.segments = self.phrases + self.words + self.syllables + self.phones
        self.label_index_keys = label_index_keys
        self.speaker_audio_links = speaker_audio_links
        self.audio_by_id = {a.identifier: a for a in self.audios}
        self.speaker_by_id = {s.identifier: s for s in self.speakers}
        self.by_key = {s.key: s for s in self.segments}
        self.by_identifier = {s.identifier: s for s in self.segments}
        self.children = _children_by_parent(self.segments)

    def __repr__(self):
        counts = ' '.join(f'{k}={v}' for k, v in self.row_counts.items())
        return f'<AuditData {self.path} {counts}>'

    @property
    def row_counts(self):
        return {'audios': len(self.audios), 'speakers': len(self.speakers),
            'phrases': len(self.phrases), 'words': len(self.words),
            'syllables': len(self.syllables), 'phones': len(self.phones)}

    def parent_of(self, segment):
        '''Return the persisted parent row, or None when absent or unset.'''
        return self.by_key.get(segment.parent_key)


class AuditReport:
    '''The results of one audit run, saved to or loaded from a folder.'''

    def __init__(self, results, speakers, outlier_groups, info):
        self.results = results
        self.checks = SimpleNamespace(**{r.name: r for r in results})
        self.speakers = speakers
        self.outlier_groups = outlier_groups
        self.info = info

    def __repr__(self):
        m = f'<AuditReport {self.info.stamp} errors={self.n_errors} '
        m += f'warnings={self.n_warnings}>'
        return m

    @property
    def errors(self):
        return [r for r in self.results if r.severity == ERROR]

    @property
    def warnings(self):
        return [r for r in self.results if r.severity == WARNING]

    @property
    def n_errors(self):
        return sum(r.n_findings for r in self.errors)

    @property
    def n_warnings(self):
        return sum(r.n_findings for r in self.warnings)

    def summary(self):
        '''Print row counts and the findings per check.'''
        print(f'audit of {self.info.store_path} at {self.info.stamp}')
        counts = ' '.join(f'{k}={v:,}' for k, v in self.info.row_counts.items())
        print(f'rows: {counts}')
        for title, results in (('errors', self.errors),
            ('warnings', self.warnings)):
            print(f'\n{title}')
            for result in results:
                print(_summary_line(result))
        print(f'\noutlier groups: {len(self.outlier_groups):,}', end=' | ')
        print(f'speakers: {len(self.speakers):,}')

    def save(self, directory='.'):
        '''Write the report to audit_{stamp} in directory; return its path.
        directory:    parent folder; never inside the store's LMDB folder
        '''
        path = _make_audit_directory(directory, self.info.stamp,
            self.info.store_path)
        _write_json(path / 'summary.json', _summary_to_json(self))
        for result in self.results:
            _write_json(path / f'{result.name}.json', _result_to_json(result))
        _write_json(path / 'speakers.json', _records_to_json(self.speakers))
        _write_json(path / 'outlier_groups.json',
            _records_to_json(self.outlier_groups))
        return path


# ------------------ loading ------------------

def load_audit_data(store):
    '''Decode every row of a store from disk, bypassing its cache.
    store:    open Store; it is read and left unchanged, markers are skipped
    '''
    store._ensure_open()
    keys_by_rank = store.DB.rank_to_keys_dict()
    rows = {}
    for rank, keys in keys_by_rank.items():
        class_name = key_helper.RANK_CLASS_MAP[rank]
        if class_name == 'Marker': continue
        rows[class_name] = _decode_rows(store, keys)
    label_index_keys = set(store.DB.all_label_index_keys())
    links = {(link[:8], link[8:]) for link in store.DB.all_links()}
    return AuditData(store.path, rows, label_index_keys, links)


def _decode_rows(store, keys):
    '''Decode rows into fresh objects that are never added to the cache.'''
    values = store.DB.load_many(keys)
    pairs = zip(values, keys, strict=True)
    return [value_key_to_instance(store, v, k) for v, k in pairs]


def _children_by_parent(segments):
    '''Group rows by parent_id, using the upward link and not time ranges.'''
    children = {}
    for segment in segments:
        if segment.object_type == 'Phrase': continue
        if segment.parent_id == EMPTY_ID: continue
        children.setdefault(segment.parent_id, []).append(segment)
    for group in children.values():
        group.sort(key=lambda segment: (segment.start, segment.end))
    return children


# ------------------ small helpers ------------------

def _finding(segment, **measures):
    return Finding(key=segment.key, object_type=segment.object_type,
        label=segment.label, start=segment.start, end=segment.end,
        audio_id=segment.audio_id, speaker_id=segment.speaker_id, **measures)


def _size_group(ms):
    '''Separate likely rounding noise from real boundary violations.'''
    if ms <= 1: return '1ms'
    if ms <= 10: return '<=10ms'
    return '>10ms'


def _is_speech(segment):
    '''Exclude pause labels and the non-speech placeholder labels.'''
    if segment.label.strip() in SILENCE_LABELS: return False
    return PHONE_TYPES.get(segment.label) != 'other'


def _speech_phones(data):
    return [phone for phone in data.phones if _is_speech(phone)]


def _speech_words(data):
    return [word for word in data.words if _is_speech(word)]


def _overlapping_pairs(segments):
    '''Yield (segment, other, overlap_ms) over start-sorted segments.'''
    if not segments: return
    widest = segments[0]
    for segment in segments[1:]:
        overlap = min(widest.end, segment.end) - segment.start
        if overlap > 0: yield segment, widest, overlap
        if segment.end > widest.end: widest = segment


def _overlap_finding(segment, other, overlap):
    return _finding(segment, other_key=other.key, overlap_ms=overlap,
        size=_size_group(overlap))


# ------------------ errors ------------------

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


def check_within_parent(data):
    '''Find children extending past the boundaries of their parent.'''
    findings = []
    n_checked = 0
    for segment in data.segments:
        parent = data.parent_of(segment)
        if parent is None: continue
        n_checked += 1
        start_excess = max(parent.start - segment.start, 0)
        end_excess = max(segment.end - parent.end, 0)
        excess = max(start_excess, end_excess)
        if excess == 0: continue
        findings.append(_finding(segment, other_key=parent.key,
            start_excess_ms=start_excess, end_excess_ms=end_excess,
            excess_ms=excess, size=_size_group(excess)))
    return CheckResult('within_parent', ERROR, n_checked, findings)


def _parent_references(segment):
    '''Return (reference, identifier, key) for each upward link in use.'''
    if segment.object_type == 'Phrase': return []
    references = []
    if segment.parent_id != EMPTY_ID:
        references.append(('parent', segment.parent_id, segment.parent_key))
    if segment.object_type == 'Word': return references
    if segment.phrase_id != EMPTY_ID:
        references.append(('phrase', segment.phrase_id, segment.phrase_key))
    return references


def _broken_reference_kind(data, segment, identifier):
    '''Explain a reference whose key holds no row. A key also holds the audio
    and start time, so the row can still exist under its identifier.'''
    row = data.by_identifier.get(identifier)
    if row is None: return 'missing_row'
    if row.audio_id != segment.audio_id: return 'other_audio'
    return 'stale_start'


def check_parent_exists(data):
    '''Find parent and phrase references pointing at no row.'''
    findings = []
    n_checked = 0
    for segment in data.segments:
        for reference, identifier, key in _parent_references(segment):
            n_checked += 1
            if key in data.by_key: continue
            kind = _broken_reference_kind(data, segment, identifier)
            findings.append(_finding(segment, reference=reference, kind=kind))
    return CheckResult('parent_exists', ERROR, n_checked, findings)


def _identity_mismatches(segment, parent):
    '''Return the identity fields on which a child disagrees with its parent.
    A parent on another audio has another key; check_parent_exists finds it.'''
    fields = ['speaker_id']
    if segment.object_type in ('Syllable', 'Phone'):
        fields += ['phrase_id', 'phrase_start']
    return [f for f in fields if getattr(segment, f) != getattr(parent, f)]


def check_identity_matches_parent(data):
    '''Find children whose speaker or phrase refs disagree with the parent.'''
    findings = []
    n_checked = 0
    for segment in data.segments:
        parent = data.parent_of(segment)
        if parent is None: continue
        n_checked += 1
        for field in _identity_mismatches(segment, parent):
            findings.append(_finding(segment, other_key=parent.key,
                field=field))
    return CheckResult('identity_matches_parent', ERROR, n_checked, findings)


def check_siblings_do_not_overlap(data):
    '''Find children overlapping another child of the same parent.'''
    findings = []
    n_checked = 0
    for children in data.children.values():
        n_checked += len(children)
        for pair in _overlapping_pairs(children):
            findings.append(_overlap_finding(*pair))
    return CheckResult('siblings_do_not_overlap', ERROR, n_checked, findings)


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
        findings.append(Finding(key=key, kind='stale_entry',
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


# ------------------ warnings ------------------

def check_orphans(data):
    '''Find rows without a parent; pauses may legitimately have none.'''
    findings = []
    candidates = _speech_words(data) + data.syllables + _speech_phones(data)
    for segment in candidates:
        if segment.parent_id == EMPTY_ID: findings.append(_finding(segment))
    return CheckResult('orphans', WARNING, len(candidates), findings)


def check_childless(data):
    '''Find phrases without words, speech words without syllables and
    syllables without phones.'''
    findings = []
    parents = data.phrases + _speech_words(data) + data.syllables
    for parent in parents:
        if parent.identifier in data.children: continue
        findings.append(_finding(parent))
    return CheckResult('childless', WARNING, len(parents), findings)


def _span_gaps(parent, children):
    '''Yield (kind, gap_ms) where children leave part of the parent empty.
    Silence between words is normal, so a phrase has no internal gaps.'''
    if children[0].start > parent.start:
        yield 'start_gap', children[0].start - parent.start
    widest_end = max(child.end for child in children)
    if widest_end < parent.end: yield 'end_gap', parent.end - widest_end
    if parent.object_type == 'Phrase': return
    reached = children[0].end
    for child in children[1:]:
        if child.start > reached: yield 'internal_gap', child.start - reached
        reached = max(reached, child.end)


def check_children_span_parent(data):
    '''Find parents whose children do not cover them from start to end.'''
    findings = []
    n_checked = 0
    for parent_id, children in data.children.items():
        parent = data.by_identifier.get(parent_id)
        if parent is None: continue
        n_checked += 1
        for kind, gap in _span_gaps(parent, children):
            findings.append(_finding(parent, kind=kind, gap_ms=gap,
                size=_size_group(gap)))
    return CheckResult('children_span_parent', WARNING, n_checked, findings)


def check_unknown_phone_labels(data):
    '''Find speech phones whose label is not a known phone.'''
    phones = _speech_phones(data)
    findings = []
    for phone in phones:
        if phone.label not in PHONE_TYPES: findings.append(_finding(phone))
    return CheckResult('unknown_phone_labels', WARNING, len(phones), findings)


def check_phone_positions(data):
    '''Find speech phones without an assigned syllable position.'''
    phones = _speech_phones(data)
    findings = []
    for phone in phones:
        if phone.position_code != UNASSIGNED_POSITION: continue
        findings.append(_finding(phone))
    return CheckResult('phone_positions', WARNING, len(phones), findings)


def check_syllable_nucleus(data):
    '''Find syllables with phones but no nucleus phone.'''
    findings = []
    n_checked = 0
    for syllable in data.syllables:
        phones = data.children.get(syllable.identifier)
        if phones is None: continue
        n_checked += 1
        codes = {phone.position_code for phone in phones}
        if NUCLEUS_POSITION in codes: continue
        unassigned = codes == {UNASSIGNED_POSITION}
        kind = 'positions_unassigned' if unassigned else 'no_nucleus'
        findings.append(_finding(syllable, kind=kind))
    return CheckResult('syllable_nucleus', WARNING, n_checked, findings)


def check_syllable_label_matches_phones(data):
    '''Find syllables whose label differs from their joined phone labels.
    Loaders write 'ru' or 'r u', so spaces in the label are ignored.'''
    findings = []
    n_checked = 0
    for syllable in data.syllables:
        phones = data.children.get(syllable.identifier)
        if phones is None: continue
        n_checked += 1
        joined = ''.join(phone.label for phone in phones)
        if joined == syllable.label.replace(' ', ''): continue
        findings.append(_finding(syllable, phone_labels=joined))
    name = 'syllable_label_matches_phones'
    return CheckResult(name, WARNING, n_checked, findings)


# ------------------ overlap ------------------

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


# ------------------ outliers ------------------

def _fences(log_values, k):
    '''Return the distribution of log values with its IQR fences at k.'''
    p5, q1, median, q3, p95 = np.percentile(log_values, [5, 25, 50, 75, 95])
    spread = max(q3 - q1, MIN_LOG_IQR)
    return SimpleNamespace(p5=p5, q1=q1, median=median, q3=q3, p95=p95,
        spread=spread, low=q1 - k * spread, high=q3 + k * spread)


def _n_outside(log_values, fences, k):
    low = fences.q1 - k * fences.spread
    high = fences.q3 + k * fences.spread
    return int(np.sum((log_values < low) | (log_values > high)))


def _outlier_group(level, name, unit, log_values, fences, k):
    counts = {}
    for name_k, value_k in REPORT_KS.items():
        counts[name_k] = _n_outside(log_values, fences, value_k)
    return OutlierGroup(level=level, name=name, unit=unit, n=len(log_values),
        judged=True, p5=_exp(fences.p5), q1=_exp(fences.q1),
        median=_exp(fences.median), q3=_exp(fences.q3), p95=_exp(fences.p95),
        low_fence=_exp(fences.low), high_fence=_exp(fences.high), k=k,
        n_flagged=_n_outside(log_values, fences, k), **counts)


def _exp(log_value):
    return float(np.exp(log_value))


def _flagged_findings(pairs, log_values, fences, group):
    '''Build findings for pairs of (segment, value) outside the fences.'''
    outside = (log_values < fences.low) | (log_values > fences.high)
    findings = []
    for index in np.flatnonzero(outside):
        segment, value = pairs[index]
        direction = 'short' if log_values[index] < fences.low else 'long'
        findings.append(_finding(segment, value=float(value), unit=group.unit,
            group=group.name, direction=direction, median=group.median,
            low_fence=group.low_fence, high_fence=group.high_fence,
            ratio_to_median=float(value) / group.median))
    return findings


def _measure_groups(level, unit, pairs_by_name, k, min_count):
    '''Judge each group of (segment, value) pairs against its own fences.
    A group smaller than min_count is recorded but not judged.'''
    groups, findings = [], []
    n_checked, n_unjudged = 0, 0
    for name, pairs in pairs_by_name.items():
        if not pairs or len(pairs) < min_count:
            n_unjudged += len(pairs)
            groups.append(OutlierGroup(level=level, name=name, unit=unit,
                n=len(pairs), judged=False))
            continue
        n_checked += len(pairs)
        log_values = np.log([value for _, value in pairs])
        fences = _fences(log_values, k)
        group = _outlier_group(level, name, unit, log_values, fences, k)
        groups.append(group)
        findings += _flagged_findings(pairs, log_values, fences, group)
    return groups, findings, n_checked, n_unjudged


def _duration_pairs_by_label(segments):
    '''Group (segment, duration) by label; bad durations are errors already.'''
    pairs_by_label = {}
    for segment in segments:
        if segment.duration <= 0: continue
        pairs = pairs_by_label.setdefault(segment.label, [])
        pairs.append((segment, segment.duration))
    return pairs_by_label


def check_outliers_phone(data, k=DEFAULT_K, min_count=DEFAULT_MIN_COUNT):
    '''Find speech phones with an extreme duration for their label.
    k:            IQR multiplier on log duration
    min_count:    smallest label count that gets judged
    '''
    pairs = _duration_pairs_by_label(_speech_phones(data))
    groups, findings, n_checked, n_unjudged = _measure_groups('phone', 'ms',
        pairs, k, min_count)
    return CheckResult('outliers_phone', WARNING, n_checked, findings,
        n_unjudged, groups)


def _phone_count(data, word):
    syllables = data.children.get(word.identifier, [])
    return sum(len(data.children.get(s.identifier, [])) for s in syllables)


def _rare_word_pairs(data, rare_pairs_by_label):
    '''Express rare words as ms per phone so they can be pooled.'''
    pairs = []
    for label_pairs in rare_pairs_by_label.values():
        for word, duration in label_pairs:
            n_phones = _phone_count(data, word)
            if n_phones > 0: pairs.append((word, duration / n_phones))
    return pairs


def check_outliers_word(data, k=DEFAULT_K, min_count=DEFAULT_MIN_COUNT):
    '''Find words with an extreme duration for their label; words too rare to
    compare that way are pooled and compared on ms per phone.
    k:            IQR multiplier on log value
    min_count:    smallest label count that gets its own group
    '''
    frequent, rare = {}, {}
    words = _speech_words(data)
    for label, pairs in _duration_pairs_by_label(words).items():
        group = frequent if len(pairs) >= min_count else rare
        group[label] = pairs
    groups, findings, n_checked, _ = _measure_groups('word', 'ms', frequent,
        k, min_count)
    pooled = {'rare_words': _rare_word_pairs(data, rare)}
    rare_result = _measure_groups('word', 'ms_per_phone', pooled, k, min_count)
    groups += rare_result[0]
    findings += rare_result[1]
    n_checked += rare_result[2]
    n_all = sum(len(pairs) for pairs in rare.values())
    n_unjudged = n_all - rare_result[2]
    return CheckResult('outliers_word', WARNING, n_checked, findings,
        n_unjudged, groups)


def check_outliers_phrase(data, k=DEFAULT_K, min_count=DEFAULT_MIN_COUNT):
    '''Find phrases with an extreme ms per word; labels are nearly unique.
    k:            IQR multiplier on log value
    min_count:    smallest phrase count that gets judged
    '''
    pairs = []
    n_without_rate = 0
    for phrase in data.phrases:
        words = data.children.get(phrase.identifier, [])
        n_words = sum(1 for word in words if _is_speech(word))
        if n_words == 0 or phrase.duration <= 0:
            n_without_rate += 1
            continue
        pairs.append((phrase, phrase.duration / n_words))
    result = _measure_groups('phrase', 'ms_per_word', {'all_phrases': pairs},
        k, min_count)
    groups, findings, n_checked, n_unjudged = result
    return CheckResult('outliers_phrase', WARNING, n_checked, findings,
        n_unjudged + n_without_rate, groups)


# ------------------ speakers ------------------

def _label_baselines(segments, k, min_count):
    '''Return {label: fences} for labels with at least min_count segments.'''
    baselines = {}
    for label, pairs in _duration_pairs_by_label(segments).items():
        if len(pairs) < min_count: continue
        log_values = np.log([value for _, value in pairs])
        baselines[label] = _fences(log_values, k)
    return baselines


def _speaker_measures(data, k, min_count):
    '''Return (measure, segments, baselines) for each tempo measure.'''
    phones = _speech_phones(data)
    phone_baselines = _label_baselines(phones, k, min_count)
    vowels = [p for p in phones if PHONE_TYPES.get(p.label) == 'vowel']
    consonants = [p for p in phones if PHONE_TYPES.get(p.label) == 'consonant']
    syllable_baselines = _label_baselines(data.syllables, k, min_count)
    words = _speech_words(data)
    word_baselines = _label_baselines(words, k, min_count)
    return [('vowel', vowels, phone_baselines),
        ('consonant', consonants, phone_baselines),
        ('syllable', data.syllables, syllable_baselines),
        ('word', words, word_baselines)]


def _tempo_stats(segments, baselines):
    '''Return (n, tempo, n_flagged, n_flagged_without_tempo) for one speaker.
    Tempo is the median duration relative to the store-wide label median.'''
    log_values, medians, lows, highs = [], [], [], []
    for segment in segments:
        fences = baselines.get(segment.label)
        if fences is None or segment.duration <= 0: continue
        log_values.append(np.log(segment.duration))
        medians.append(fences.median)
        lows.append(fences.low)
        highs.append(fences.high)
    if not log_values: return 0, None, 0, 0
    log_values, medians = np.array(log_values), np.array(medians)
    lows, highs = np.array(lows), np.array(highs)
    shift = np.median(log_values - medians)
    flagged = (log_values < lows) | (log_values > highs)
    adjusted = log_values - shift
    flagged_adjusted = (adjusted < lows) | (adjusted > highs)
    return (len(log_values), _exp(shift), int(flagged.sum()),
        int(flagged_adjusted.sum()))


def _new_speaker_row(data, speaker_id):
    speaker = data.speaker_by_id.get(speaker_id)
    row = SpeakerTempo(speaker_id=speaker_id,
        name=speaker.name if speaker else None, n_judged=0, n_flagged=0,
        n_flagged_without_tempo=0)
    for measure in ('vowel', 'consonant', 'syllable', 'word'):
        setattr(row, f'n_{measure}', 0)
        setattr(row, f'tempo_{measure}', None)
    return row


def _add_measure(row, measure, stats):
    n, tempo, n_flagged, n_flagged_without_tempo = stats
    setattr(row, f'n_{measure}', n)
    setattr(row, f'tempo_{measure}', tempo)
    if measure not in FLAG_MEASURES: return
    row.n_judged += n
    row.n_flagged += n_flagged
    row.n_flagged_without_tempo += n_flagged_without_tempo


def _finish_speaker_row(row):
    n = row.n_judged
    row.flagged_share = row.n_flagged / n if n else None
    without_tempo = row.n_flagged_without_tempo / n if n else None
    row.flagged_share_without_tempo = without_tempo


def audit_speakers(data, k=DEFAULT_K, min_count=DEFAULT_MIN_COUNT):
    '''Return one SpeakerTempo per speaker, highest flagged share first.
    A tempo of 1.15 means 15% slower than the store norm. A slow speaker has
    raised tempos and a flagged share that drops once tempo is removed; poor
    alignment leaves the flagged share high.
    k:            IQR multiplier on log duration
    min_count:    smallest label count used as a baseline
    '''
    rows = {}
    for measure, segments, baselines in _speaker_measures(data, k, min_count):
        by_speaker = {}
        for segment in segments:
            by_speaker.setdefault(segment.speaker_id, []).append(segment)
        for speaker_id, group in by_speaker.items():
            if speaker_id not in rows:
                rows[speaker_id] = _new_speaker_row(data, speaker_id)
            stats = _tempo_stats(group, baselines)
            _add_measure(rows[speaker_id], measure, stats)
    for row in rows.values():
        _finish_speaker_row(row)
    return sorted(rows.values(), key=_flagged_share_sort_key, reverse=True)


def _flagged_share_sort_key(row):
    return row.flagged_share if row.flagged_share is not None else -1


# ------------------ audit ------------------

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


def audit(store, data=None, k=DEFAULT_K, min_count=DEFAULT_MIN_COUNT,
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


# ------------------ saving and loading reports ------------------

def _make_audit_directory(directory, stamp, store_path):
    '''Create the audit folder; never inside the store's LMDB folder.'''
    path = Path(directory) / f'audit_{stamp}'
    store_root = Path(store_path).resolve()
    if path.resolve().is_relative_to(store_root):
        raise ValueError(f'audit folder is inside the store: {path}')
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_json(path, content):
    with open(path, 'w', encoding='utf-8') as fout:
        json.dump(content, fout)


def _read_json(path):
    with open(path, encoding='utf-8') as fin:
        return json.load(fin)


def _record_to_json(record):
    content = dict(vars(record))
    for field in BYTES_FIELDS:
        if content.get(field) is None: continue
        content[field] = content[field].hex()
    return content


def _records_to_json(records):
    return [_record_to_json(record) for record in records]


def _record_from_json(cls, content):
    for field in BYTES_FIELDS:
        if content.get(field) is not None:
            content[field] = bytes.fromhex(content[field])
    return cls(**content)


def _result_to_json(result):
    return {'name': result.name, 'severity': result.severity,
        'n_checked': result.n_checked, 'n_unjudged': result.n_unjudged,
        'findings': _records_to_json(result.findings)}


def _result_from_json(content):
    findings = [_record_from_json(Finding, f) for f in content['findings']]
    return CheckResult(content['name'], content['severity'],
        content['n_checked'], findings, content['n_unjudged'])


def _summary_to_json(report):
    counts = {r.name: r.n_findings for r in report.results}
    return {'info': vars(report.info), 'n_errors': report.n_errors,
        'n_warnings': report.n_warnings, 'finding_counts': counts}


def _summary_line(result):
    m = f'  {result.name:<32} {result.n_findings:>10,} '
    m += f'of {result.n_checked:>12,}'
    if result.n_unjudged: m += f'  ({result.n_unjudged:,} unjudged)'
    sizes = result.count_by('size')
    if sizes: m += f'  {sizes}'
    return m


def _check_name(group):
    '''Return the name of the outlier check that produced a group.'''
    return f'outliers_{group.level}'


def load_report(path):
    '''Load an AuditReport from a folder written by report.save.
    path:    an audit_{stamp} folder
    '''
    path = Path(path)
    summary = _read_json(path / 'summary.json')
    results = []
    for name in summary['finding_counts']:
        results.append(_result_from_json(_read_json(path / f'{name}.json')))
    speakers = _read_json(path / 'speakers.json')
    speakers = [_record_from_json(SpeakerTempo, s) for s in speakers]
    groups = _read_json(path / 'outlier_groups.json')
    groups = [OutlierGroup(**group) for group in groups]
    for result in results:
        result.groups = [g for g in groups if result.name == _check_name(g)]
    info = SimpleNamespace(**summary['info'])
    return AuditReport(results, speakers, groups, info)
