'''Sample labelled segment tokens under explicit eligibility restrictions.'''

from . import _selection as selection
from .selection_report import SelectionReport


def sample_word_tokens(type_label, audios=None, store=None, n_tokens=100,
    min_duration_ms=100, max_duration_ms=1500, exclude_duration_outliers=True,
    speakers=None, gender=None, insufficient='raise', random_seed=0,
    return_report=False):
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
    return_report:              return (tokens, SelectionReport) when True

    Use each token's recorded duration assessment; unset and within_range
    remain eligible. No outlier statistics are calculated or flags changed.
    Results use stable key order before drawing and never save changes.
    Report losses in label, speaker, gender, duration, then flag order.
    Shortages raise InsufficientSampleError with a report attribute.
    '''
    return _sample_segment_tokens('words', type_label, audios, store,
        n_tokens, min_duration_ms, max_duration_ms, exclude_duration_outliers,
        speakers, gender, insufficient, random_seed, return_report)


def sample_phrase_tokens(type_label, audios=None, store=None, n_tokens=100,
    min_duration_ms=None, max_duration_ms=None, exclude_duration_outliers=True,
    speakers=None, gender=None, insufficient='raise', random_seed=0,
    return_report=False):
    '''Sample phrase tokens; parameters follow sample_word_tokens.
    Duration bounds are disabled by default; recorded outliers are excluded.
    '''
    return _sample_segment_tokens('phrases', type_label, audios, store,
        n_tokens, min_duration_ms, max_duration_ms, exclude_duration_outliers,
        speakers, gender, insufficient, random_seed, return_report)


def sample_syllable_tokens(type_label, audios=None, store=None, n_tokens=100,
    min_duration_ms=None, max_duration_ms=None, exclude_duration_outliers=True,
    speakers=None, gender=None, insufficient='raise', random_seed=0,
    return_report=False):
    '''Sample syllable tokens; parameters follow sample_word_tokens.
    Duration bounds are disabled by default; recorded outliers are excluded.
    '''
    return _sample_segment_tokens('syllables', type_label, audios, store,
        n_tokens, min_duration_ms, max_duration_ms, exclude_duration_outliers,
        speakers, gender, insufficient, random_seed, return_report)


def sample_phone_tokens(type_label, audios=None, store=None, n_tokens=100,
    min_duration_ms=None, max_duration_ms=None, exclude_duration_outliers=True,
    speakers=None, gender=None, insufficient='raise', random_seed=0,
    return_report=False):
    '''Sample phone tokens; parameters follow sample_word_tokens.
    Duration bounds are disabled by default; recorded outliers are excluded.
    '''
    return _sample_segment_tokens('phones', type_label, audios, store,
        n_tokens, min_duration_ms, max_duration_ms, exclude_duration_outliers,
        speakers, gender, insufficient, random_seed, return_report)


def _sample_segment_tokens(attribute, type_label, audios, store, n_tokens,
    min_duration_ms, max_duration_ms, exclude_duration_outliers, speakers,
    gender, insufficient, random_seed, return_report):
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
        tokens.extend(getattr(audio, attribute))
    tokens = selection.unique_items(tokens)
    report = SelectionReport(attribute[:-1] + ' tokens', len(tokens), n_tokens,
        type_label)
    tokens = _filter_tokens(tokens, report, type_label, keys, gender,
        min_duration_ms, max_duration_ms, exclude_duration_outliers)
    return selection.draw(tokens, n_tokens, random_seed, insufficient,
        report, return_report)


def _filter_tokens(tokens, report, type_label, keys, gender, minimum, maximum,
    exclude_duration_outliers):
    '''Record each active condition in a fixed order before drawing.'''
    apply = selection.filter_candidates
    if type_label is not None:
        tokens = apply(tokens, report, f'Label {type_label!r}',
            lambda token: token.label == type_label)
    if keys is not None:
        tokens = apply(tokens, report, 'Speaker selection',
            lambda token: selection.matches_speaker(token.speaker, keys))
    if gender is not None:
        tokens = apply(tokens, report, f'Gender {gender}',
            lambda token: selection.matches_speaker(token.speaker,
            gender=gender))
    tokens = apply(tokens, report, 'Positive duration',
        lambda token: token.duration > 0)
    if minimum is not None or maximum is not None:
        condition = selection.describe_bounds('Duration', minimum, maximum,
            'ms')
        tokens = apply(tokens, report, condition,
            lambda token: selection.within_bounds(token.duration,
            minimum, maximum))
    if exclude_duration_outliers:
        tokens = apply(tokens, report, 'Recorded duration outlier',
            lambda token: not token.is_duration_outlier)
    return tokens
