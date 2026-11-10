'''Sample labelled segment tokens under explicit eligibility restrictions.'''

from . import _selection as selection


def sample_word_tokens(type_label, audios=None, store=None, n_tokens=100,
    min_duration_ms=100, max_duration_ms=1500, exclude_duration_outliers=True,
    speakers=None, gender=None, insufficient='raise', random_seed=0):
    '''Return eligible word tokens, sampled without replacement.
    type_label:                 exact label; None samples across all labels
    audios:                     recordings; None loads all audios from store
    store:                      open Store, needed when audios is None
    n_tokens:                   total tokens to return for this call
    min_duration_ms:            inclusive duration minimum; None disables it
    max_duration_ms:            inclusive duration maximum; None disables it
    exclude_duration_outliers:  exclude recorded short/long assessments
    speakers:                   eligible Speaker objects or keys, or None
    gender:                     female, male, unknown, or None
    insufficient:               raise on shortage, or drop (return [])
    random_seed:                local seed; None gives an unseeded draw

    Use each token's recorded duration assessment; unset and within_range
    remain eligible. No outlier statistics are calculated or flags changed.
    Results use stable key order before drawing and never save changes.
    '''
    return _sample_segment_tokens('words', type_label, audios, store,
        n_tokens, min_duration_ms, max_duration_ms, exclude_duration_outliers,
        speakers, gender, insufficient, random_seed)


def sample_phrase_tokens(type_label, audios=None, store=None, n_tokens=100,
    min_duration_ms=None, max_duration_ms=None, exclude_duration_outliers=True,
    speakers=None, gender=None, insufficient='raise', random_seed=0):
    '''Sample phrase tokens; parameters follow sample_word_tokens.
    Duration bounds are disabled by default; recorded outliers are excluded.
    '''
    return _sample_segment_tokens('phrases', type_label, audios, store,
        n_tokens, min_duration_ms, max_duration_ms, exclude_duration_outliers,
        speakers, gender, insufficient, random_seed)


def sample_syllable_tokens(type_label, audios=None, store=None, n_tokens=100,
    min_duration_ms=None, max_duration_ms=None, exclude_duration_outliers=True,
    speakers=None, gender=None, insufficient='raise', random_seed=0):
    '''Sample syllable tokens; parameters follow sample_word_tokens.
    Duration bounds are disabled by default; recorded outliers are excluded.
    '''
    return _sample_segment_tokens('syllables', type_label, audios, store,
        n_tokens, min_duration_ms, max_duration_ms, exclude_duration_outliers,
        speakers, gender, insufficient, random_seed)


def sample_phone_tokens(type_label, audios=None, store=None, n_tokens=100,
    min_duration_ms=None, max_duration_ms=None, exclude_duration_outliers=True,
    speakers=None, gender=None, insufficient='raise', random_seed=0):
    '''Sample phone tokens; parameters follow sample_word_tokens.
    Duration bounds are disabled by default; recorded outliers are excluded.
    '''
    return _sample_segment_tokens('phones', type_label, audios, store,
        n_tokens, min_duration_ms, max_duration_ms, exclude_duration_outliers,
        speakers, gender, insufficient, random_seed)


def _sample_segment_tokens(attribute, type_label, audios, store, n_tokens,
    min_duration_ms, max_duration_ms, exclude_duration_outliers, speakers,
    gender, insufficient, random_seed):
    '''Collect tokens matching restrictions and recorded flags, then draw.'''
    if type_label is not None and not isinstance(type_label, str):
        raise ValueError('type_label must be a string or None')
    selection.validate_bounds(min_duration_ms, max_duration_ms, 'duration')
    if type(exclude_duration_outliers) is not bool:
        raise ValueError('exclude_duration_outliers must be a bool')
    selection.validate_speaker_filters(gender)
    keys = selection.speaker_keys(speakers)
    audios = selection.get_audios(audios, store)
    tokens = []
    for audio in audios:
        tokens.extend(_eligible_tokens(getattr(audio, attribute), type_label,
            min_duration_ms, max_duration_ms, keys, gender,
            exclude_duration_outliers))
    return selection.draw(tokens, n_tokens, random_seed, insufficient)


def _eligible_tokens(tokens, type_label, minimum, maximum, keys, gender,
    exclude_duration_outliers):
    '''Filter tokens by label, duration, recorded flag, and own speaker.'''
    eligible = []
    for token in tokens:
        if type_label is not None and token.label != type_label: continue
        if exclude_duration_outliers and token.is_duration_outlier: continue
        if not selection.within_bounds(token.duration, minimum, maximum):
            continue
        if token.duration <= 0: continue
        if keys is not None or gender is not None:
            if not selection.matches_speaker(token.speaker, keys, gender):
                continue
        eligible.append(token)
    return eligible
