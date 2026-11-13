'''Select words, syllables or phones by their neighbours from the same speaker.

Neighbours are segments of the same level. They stop at a boundary: the
phrase edge when within_phrase is True, otherwise a gap between phrases above
max_gap_ms or another speaker's word in that gap.
'''

from bisect import bisect_left

from . import _selection as selection


def get_neighbours(segment, n_neighbours=1, within_phrase=False,
    max_gap_ms=3000):
    '''Return (preceding, next): up to n segments on each side of segment.
    segment:        Word, Syllable or Phone to look around
    n_neighbours:   neighbours wanted before and after the segment
    within_phrase:  the phrase edge is a boundary; otherwise continue into
                    the speaker's adjacent phrases
    max_gap_ms:     gap between phrases above which the context breaks;
                    None means no limit; only used when within_phrase is
                    False

    Neighbours are segments of the same level as segment. Meant for a single
    segment: each call rebuilds the speaker's sequence. Use the
    select_<level>_with_neighbours_in_audio functions for many segments.
    '''
    _validate_options(n_neighbours, max_gap_ms)
    attribute = _level_attribute(segment)
    message = 'segment is not part of a saved phrase'
    phrase = segment.phrase
    if phrase is None: raise ValueError(message)
    if within_phrase: sequence, breaks = _phrase_sequence(phrase, attribute)
    else:
        audio = segment.audio
        sequence, breaks = _speaker_sequence(getattr(audio, attribute),
            audio.words, segment.speaker_key, max_gap_ms)
    keys = [item.key for item in sequence]
    if segment.key not in keys: raise ValueError(message)
    return _context_window(sequence, breaks, keys.index(segment.key),
        n_neighbours)


def has_neighbours(segment, n_neighbours=1, within_phrase=False,
    max_gap_ms=3000):
    '''Whether segment has n neighbours on each side; parameters follow
    get_neighbours. Meant for a single segment; see get_neighbours.
    '''
    preceding, next_segments = get_neighbours(segment, n_neighbours,
        within_phrase, max_gap_ms)
    return _enough_neighbours(preceding, next_segments, n_neighbours)


def has_overlap(segments):
    '''Whether any of the segments overlaps another speaker's speech.'''
    return any(segment.overlap for segment in segments)


def select_words_with_neighbours_in_phrase(phrase, n_neighbours=1,
    exclude_overlap=True, return_context=False):
    '''Return words in one phrase with n neighbours on each side.
    phrase:           Phrase to select words from
    n_neighbours:     neighbours wanted before and after each word
    exclude_overlap:  word and its neighbours must not be overlapped
    return_context:   return dicts with preceding, target and next
    '''
    return _select_in_phrase(phrase, 'words', n_neighbours, exclude_overlap,
        return_context)


def select_syllables_with_neighbours_in_phrase(phrase, n_neighbours=1,
    exclude_overlap=True, return_context=False):
    '''Return syllables in one phrase with n neighbouring syllables on each
    side; parameters follow select_words_with_neighbours_in_phrase.
    '''
    return _select_in_phrase(phrase, 'syllables', n_neighbours,
        exclude_overlap, return_context)


def select_phones_with_neighbours_in_phrase(phrase, n_neighbours=1,
    exclude_overlap=True, return_context=False):
    '''Return phones in one phrase with n neighbouring phones on each side;
    parameters follow select_words_with_neighbours_in_phrase.
    '''
    return _select_in_phrase(phrase, 'phones', n_neighbours, exclude_overlap,
        return_context)


def select_words_with_neighbours_in_audio(audio, n_neighbours=1,
    within_phrase=False, exclude_overlap=True, max_gap_ms=3000,
    return_context=False):
    '''Return words in one audio with n neighbours on each side, ordered by
    speaker key and then start time; parameters follow
    select_words_with_neighbours_in_phrase and get_neighbours.
    '''
    return _select_in_audio(audio, 'words', n_neighbours, within_phrase,
        exclude_overlap, max_gap_ms, return_context)


def select_syllables_with_neighbours_in_audio(audio, n_neighbours=1,
    within_phrase=False, exclude_overlap=True, max_gap_ms=3000,
    return_context=False):
    '''Return syllables in one audio with n neighbouring syllables on each
    side; parameters and order follow select_words_with_neighbours_in_audio.
    '''
    return _select_in_audio(audio, 'syllables', n_neighbours, within_phrase,
        exclude_overlap, max_gap_ms, return_context)


def select_phones_with_neighbours_in_audio(audio, n_neighbours=1,
    within_phrase=False, exclude_overlap=True, max_gap_ms=3000,
    return_context=False):
    '''Return phones in one audio with n neighbouring phones on each side;
    parameters and order follow select_words_with_neighbours_in_audio.
    '''
    return _select_in_audio(audio, 'phones', n_neighbours, within_phrase,
        exclude_overlap, max_gap_ms, return_context)


def _select_in_phrase(phrase, attribute, n_neighbours, exclude_overlap,
    return_context):
    '''Select one level's segments in a phrase.'''
    selection.validate_count(n_neighbours, 'n_neighbours')
    sequence, breaks = _phrase_sequence(phrase, attribute)
    return _select_segments(sequence, breaks, n_neighbours, exclude_overlap,
        return_context)


def _select_in_audio(audio, attribute, n_neighbours, within_phrase,
    exclude_overlap, max_gap_ms, return_context):
    '''Select one level's segments in an audio, sequence by sequence.'''
    _validate_options(n_neighbours, max_gap_ms)
    selected = []
    for sequence, breaks in _audio_sequences(audio, attribute, within_phrase,
        max_gap_ms):
        selected.extend(_select_segments(sequence, breaks, n_neighbours,
            exclude_overlap, return_context))
    return selected


def _validate_options(n_neighbours, max_gap_ms):
    '''Require a neighbour count and a nonnegative gap limit or None.'''
    selection.validate_count(n_neighbours, 'n_neighbours')
    selection.validate_bounds(max_gap_ms, max_gap_ms, 'max_gap_ms')


def _level_attribute(segment):
    '''The audio and phrase attribute that lists segment's level.'''
    if segment.object_type == 'Word': return 'words'
    if segment.object_type == 'Syllable': return 'syllables'
    if segment.object_type == 'Phone': return 'phones'
    raise ValueError('segment must be a Word, Syllable or Phone')


def _select_segments(sequence, breaks, n_neighbours, exclude_overlap,
    return_context):
    '''Keep segments whose context window passes the neighbour checks.'''
    selected = []
    for index, segment in enumerate(sequence):
        preceding, next_segments = _context_window(sequence, breaks, index,
            n_neighbours)
        if not _enough_neighbours(preceding, next_segments, n_neighbours):
            continue
        window = preceding + [segment] + next_segments
        if exclude_overlap and has_overlap(window): continue
        item = segment
        if return_context:
            item = {'preceding': preceding, 'target': segment,
                'next': next_segments}
        selected.append(item)
    return selected


def _enough_neighbours(preceding, next_segments, n_neighbours):
    '''Both sides have n neighbours.'''
    return len(preceding) == len(next_segments) == n_neighbours


def _context_window(sequence, breaks, index, n_neighbours):
    '''Up to n segments on each side of index, stopping at a break.
    breaks[i] is True when the context breaks between segments i - 1 and i.
    '''
    start = index
    while start > 0 and index - start < n_neighbours and not breaks[start]:
        start -= 1
    stop = index
    while (stop + 1 < len(sequence) and stop - index < n_neighbours
        and not breaks[stop + 1]):
        stop += 1
    return sequence[start:index], sequence[index + 1:stop + 1]


def _audio_sequences(audio, attribute, within_phrase, max_gap_ms):
    '''Segment sequences with breaks, ordered by speaker key then time.'''
    if not within_phrase:
        segments, words = getattr(audio, attribute), audio.words
        speaker_keys = sorted({segment.speaker_key for segment in segments})
        sequences = []
        for key in speaker_keys:
            sequences.append(_speaker_sequence(segments, words, key,
                max_gap_ms))
        return sequences
    phrases = sorted(audio.phrases, key=lambda p: (p.speaker_key, p.start))
    return [_phrase_sequence(phrase, attribute) for phrase in phrases]


def _phrase_sequence(phrase, attribute):
    '''A phrase's segments in time order; the phrase edge is the only break.'''
    segments = sorted(getattr(phrase, attribute), key=_time_order)
    return segments, [False] * len(segments)


def _speaker_sequence(segments, words, speaker_key, max_gap_ms):
    '''One speaker's segments in time order, with breaks; other speakers
    are found in the audio's words.
    '''
    own = [item for item in segments if item.speaker_key == speaker_key]
    others = [word for word in words if word.speaker_key != speaker_key]
    own.sort(key=_time_order)
    starts, ends = _timeline(others)
    breaks = [False]
    for previous, segment in zip(own, own[1:]):
        breaks.append(_is_break(previous, segment, max_gap_ms, starts, ends))
    return own, breaks


def _is_break(previous, segment, max_gap_ms, starts, ends):
    '''A phrase change with a large gap or another speaker in the gap.'''
    if previous.phrase_id == segment.phrase_id: return False
    gap = segment.start - previous.end
    if max_gap_ms is not None and gap > max_gap_ms: return True
    index = bisect_left(starts, segment.start)
    return index > 0 and ends[index - 1] > previous.end


def _timeline(words):
    '''Sorted start times and cumulative maximum end times of words.'''
    starts, ends = [], []
    end = float('-inf')
    for word in sorted(words, key=_time_order):
        starts.append(word.start)
        end = max(end, word.end)
        ends.append(end)
    return starts, ends


def _time_order(segment):
    return (segment.start, segment.end)
