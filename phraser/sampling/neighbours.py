'''Select words by their neighbouring words from the same speaker.

Neighbours stop at a boundary: the phrase edge when within_phrase is True,
otherwise a gap between phrases above max_gap_ms or another speaker's word
in that gap.
'''

from bisect import bisect_left

from . import _selection as selection


def get_neighbours(word, n_neighbours=1, within_phrase=False,
    max_gap_ms=3000):
    '''Return (preceding, next): up to n words on each side of word.
    word:           Word to look around
    n_neighbours:   neighbours wanted before and after the word
    within_phrase:  the phrase edge is a boundary; otherwise continue into
                    the speaker's adjacent phrases
    max_gap_ms:     gap between phrases above which the context breaks;
                    None means no limit; only used when within_phrase is
                    False

    Meant for a single word: each call rebuilds the speaker's word sequence.
    Use select_words_with_neighbours_in_audio for many words.
    '''
    _validate_options(n_neighbours, max_gap_ms)
    message = 'word is not part of a saved phrase'
    if word.parent is None: raise ValueError(message)
    if within_phrase: sequence, breaks = _phrase_sequence(word.parent)
    else:
        sequence, breaks = _speaker_sequence(word.audio.words,
            word.speaker_key, max_gap_ms)
    keys = [item.key for item in sequence]
    if word.key not in keys: raise ValueError(message)
    return _context_window(sequence, breaks, keys.index(word.key),
        n_neighbours)


def has_neighbours(word, n_neighbours=1, within_phrase=False,
    max_gap_ms=3000):
    '''Whether word has n neighbours on each side; parameters follow
    get_neighbours. Meant for a single word; see get_neighbours.
    '''
    preceding, next_words = get_neighbours(word, n_neighbours,
        within_phrase, max_gap_ms)
    return _enough_neighbours(preceding, next_words, n_neighbours)


def has_overlap(words):
    '''Whether any of the words overlaps another speaker's speech.'''
    return any(word.overlap for word in words)


def select_words_with_neighbours_in_phrase(phrase, n_neighbours=1,
    exclude_overlap=True, return_context=False):
    '''Return words in one phrase with n neighbours on each side.
    phrase:           Phrase to select words from
    n_neighbours:     neighbours wanted before and after each word
    exclude_overlap:  word and its neighbours must not be overlapped
    return_context:   return dicts with preceding, target and next
    '''
    selection.validate_count(n_neighbours, 'n_neighbours')
    sequence, breaks = _phrase_sequence(phrase)
    return _select_words(sequence, breaks, n_neighbours, exclude_overlap,
        return_context)


def select_words_with_neighbours_in_audio(audio, n_neighbours=1,
    within_phrase=False, exclude_overlap=True, max_gap_ms=3000,
    return_context=False):
    '''Return words in one audio with n neighbours on each side, ordered by
    speaker key and then start time; parameters follow
    select_words_with_neighbours_in_phrase and get_neighbours.
    '''
    _validate_options(n_neighbours, max_gap_ms)
    selected = []
    for sequence, breaks in _audio_sequences(audio, within_phrase,
        max_gap_ms):
        selected.extend(_select_words(sequence, breaks, n_neighbours,
            exclude_overlap, return_context))
    return selected


def _validate_options(n_neighbours, max_gap_ms):
    '''Require a neighbour count and a nonnegative gap limit or None.'''
    selection.validate_count(n_neighbours, 'n_neighbours')
    selection.validate_bounds(max_gap_ms, max_gap_ms, 'max_gap_ms')


def _select_words(sequence, breaks, n_neighbours, exclude_overlap,
    return_context):
    '''Keep words whose context window passes the neighbour checks.'''
    selected = []
    for index, word in enumerate(sequence):
        preceding, next_words = _context_window(sequence, breaks, index,
            n_neighbours)
        if not _enough_neighbours(preceding, next_words, n_neighbours):
            continue
        if exclude_overlap and has_overlap(preceding + [word] + next_words):
            continue
        item = word
        if return_context:
            item = {'preceding': preceding, 'target': word, 'next': next_words}
        selected.append(item)
    return selected


def _enough_neighbours(preceding, next_words, n_neighbours):
    '''Both sides have n neighbours.'''
    return len(preceding) == len(next_words) == n_neighbours


def _context_window(sequence, breaks, index, n_neighbours):
    '''Up to n words on each side of index, stopping at a break.
    breaks[i] is True when the context breaks between words i - 1 and i.
    '''
    start = index
    while start > 0 and index - start < n_neighbours and not breaks[start]:
        start -= 1
    stop = index
    while (stop + 1 < len(sequence) and stop - index < n_neighbours
        and not breaks[stop + 1]):
        stop += 1
    return sequence[start:index], sequence[index + 1:stop + 1]


def _audio_sequences(audio, within_phrase, max_gap_ms):
    '''Word sequences with breaks, ordered by speaker key then time.'''
    if not within_phrase:
        words = audio.words
        speaker_keys = sorted({word.speaker_key for word in words})
        sequences = []
        for key in speaker_keys:
            sequences.append(_speaker_sequence(words, key, max_gap_ms))
        return sequences
    phrases = sorted(audio.phrases, key=lambda p: (p.speaker_key, p.start))
    return [_phrase_sequence(phrase) for phrase in phrases]


def _phrase_sequence(phrase):
    '''A phrase's words in time order; the phrase edge is the only break.'''
    words = sorted(phrase.words, key=_time_order)
    return words, [False] * len(words)


def _speaker_sequence(audio_words, speaker_key, max_gap_ms):
    '''One speaker's words from an audio's words in time order, with breaks.'''
    words, others = [], []
    for word in audio_words:
        if word.speaker_key == speaker_key: words.append(word)
        else: others.append(word)
    words.sort(key=_time_order)
    starts, ends = _timeline(others)
    breaks = [False]
    for previous, word in zip(words, words[1:]):
        breaks.append(_is_break(previous, word, max_gap_ms, starts, ends))
    return words, breaks


def _is_break(previous, word, max_gap_ms, starts, ends):
    '''A phrase change with a large gap or another speaker in the gap.'''
    if previous.phrase_id == word.phrase_id: return False
    gap = word.start - previous.end
    if max_gap_ms is not None and gap > max_gap_ms: return True
    index = bisect_left(starts, word.start)
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


def _time_order(word):
    return (word.start, word.end)
