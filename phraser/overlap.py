'''Speech overlap: segment spans intersect other-speaker word intervals.

Phrase pauses count as part of the phrase span. A phrase can overlap even
when none of its words does. Touching boundaries never count as overlap.
Codes are 0 (computed false), 1 (computed true), and 9 (not computed).
'''

from bisect import bisect_left, bisect_right

from . import utils


# Explicit pause labels used by the CGN and WebMAUS import paths.
SILENCE_LABELS = frozenset({'', '<p:>', '(...)', '[]', '#'})
SEGMENT_TYPES = ('Phrase', 'Word', 'Syllable', 'Phone')


def _is_speech_word(word):
    '''Exclude explicit pauses and empty or invalid intervals.'''
    return word.start < word.end and word.label.strip() not in SILENCE_LABELS


class _WordTimeline:
    '''Index words by start time and cumulative maximum end time.'''

    def __init__(self, words):
        speech = filter(_is_speech_word, words)
        self.words = sorted(speech, key=lambda word: (word.start, word.end))
        self.starts, self.ends = [], []
        end = float('-inf')
        for word in self.words:
            self.starts.append(word.start)
            end = max(end, word.end)
            self.ends.append(end)

    def matching_words(self, segment):
        '''Return intersecting words from other speakers in the same audio.'''
        if segment.start >= segment.end: return []
        start = bisect_right(self.ends, segment.start)
        stop = bisect_left(self.starts, segment.end)
        matches = []
        for index in range(start, stop):
            word = self.words[index]
            if word.audio_id != segment.audio_id: continue
            if word.speaker_id == segment.speaker_id: continue
            if utils.overlap(segment, word): matches.append(word)
        return matches


def _load_audio_segments(audio, object_type):
    '''Scan persisted rows directly, including unlinked segments.'''
    store = audio.store
    keys = list(store.DB.audio_id_to_child_keys(audio.identifier,
        child_class_name=object_type))
    return store.load_many(keys)


def overlap_items(segment, words=None):
    '''Return other-speaker speech words intersecting the segment span.

    segment:  phrase, word, syllable, or phone to check
    words:    explicit reference words; defaults to persisted audio words
    '''
    if words is None:
        audio = segment.audio
        if audio is None: return []
        words = _load_audio_segments(audio, 'Word')
    return _WordTimeline(words).matching_words(segment)


def overlap_at_level(segment, object_type):
    '''Return intersecting other-speaker segments at the requested level.

    segment:      segment whose time span is queried
    object_type:  Phrase, Word, Syllable, or Phone

    Words exclude silence and provide the evidence for overlap status.
    Other levels report time intersections independently of that status.
    All results come from persisted segments in the same audio.
    '''
    if object_type not in SEGMENT_TYPES:
        raise ValueError(f'Unsupported overlap level: {object_type!r}')
    if object_type == 'Word': return overlap_items(segment)
    if segment.start >= segment.end: return []
    audio = segment.audio
    if audio is None: return []
    matches = []
    for item in _load_audio_segments(audio, object_type):
        if item.audio_id != segment.audio_id: continue
        if item.speaker_id == segment.speaker_id: continue
        if item.start >= item.end: continue
        if utils.overlap(segment, item): matches.append(item)
    matches.sort(key=lambda item: (item.start, item.end))
    return matches


def set_overlap_codes(items):
    '''Compute codes in memory for a complete set of audio segments.

    items:  all phrases, words, syllables, and phones, including all speakers
    '''
    items = list(items)
    words = [item for item in items if item.object_type == 'Word']
    timeline = _WordTimeline(words)
    for item in items:
        matches = timeline.matching_words(item)
        item.overlap_code = utils.overlap_dict[bool(matches)]
        item.__dict__.pop('_overlap_items', None)


def update_audio_overlap(audio):
    '''Recompute and persist all overlap codes for a fully imported audio.

    audio:  stored audio whose complete annotations should be recomputed

    Call again after adding, deleting, or retiming annotations. Existing
    codes, including 0 and 1, are replaced. Reads ignore cached audio trees.
    '''
    items = []
    for object_type in SEGMENT_TYPES:
        items.extend(_load_audio_segments(audio, object_type))
    if not items: return
    set_overlap_codes(items)
    audio.store.save_many(items, overwrite=True)
