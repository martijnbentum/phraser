'''Checks on the segment tree (phrase > word > syllable > phone).

Each segment stores a link to its parent; these checks verify that parent
and child agree on time range, audio and speaker, that siblings do not
overlap, and that children fill their parent without gaps.
'''

from ..storage.key_helper import EMPTY_ID
from .findings import ERROR, WARNING, CheckResult, _finding, _size_group
from .sharing import explain_sharing, following_words
from .store_snapshot import _speech_phones, _speech_words


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


def check_within_parent(data):
    '''Retain raw containment findings and explain simple shared crossings.'''
    following = following_words(data)
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
        explained = explain_sharing(data, segment, parent, following)
        status = 'explained' if explained else 'needs_attention'
        explanation = 'shared_phone' if explained else None
        findings.append(_finding(segment, other_key=parent.key,
            status=status, explanation=explanation,
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
    '''Find parent and phrase references pointing at no row.
    On a scoped snapshot a parent in another recording is missing_row.'''
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
