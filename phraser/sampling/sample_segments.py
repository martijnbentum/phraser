'''Sample labelled segment tokens under explicit eligibility restrictions.'''

from . import _selection as selection
from .selection_report import SelectionReport


def sample_word_tokens(type_label, audios=None, store=None, n_tokens=100,
    min_duration_ms=100, max_duration_ms=1500, include_only_within_range=True,
    speakers=None, gender=None, min_phones=None,
    exclude_timing_uncertain=False, insufficient='raise', random_seed=0,
    return_report=False):
    '''Return eligible word tokens, sampled without replacement.
    type_label:                 exact label; None samples across all labels
    audios:                     recordings; None loads all audios from store
    store:                      open Store, needed when audios is None
    n_tokens:                   total tokens to return for this call
    min_duration_ms:            inclusive duration minimum; None disables it
    max_duration_ms:            inclusive duration maximum; None disables it
    include_only_within_range:  keep only tokens flagged within_range
    speakers:                   eligible Speaker objects or keys, or None
    gender:                     female, male, unknown, or None
    min_phones:                 minimum linked phones; None disables it
    exclude_timing_uncertain:   exclude tokens with a recorded timing concern
    insufficient:               raise on shortage, or drop (return [])
    random_seed:                local seed; None gives an unseeded draw
    return_report:              return (tokens, SelectionReport) when True

    The duration flag is assessed per label over the whole store; unset,
    short and long tokens are excluded. No statistics are calculated here.
    Results use stable key order before drawing and never save changes.
    Report losses in label, speaker, gender, phone count, duration, then
    flag order. Shortages raise InsufficientSampleError with a report.
    '''
    keys = selection.speaker_keys(speakers)
    filters = selection.TokenFilters(type_label, keys, gender, min_phones,
        min_duration_ms, max_duration_ms, include_only_within_range,
        exclude_timing_uncertain)
    return _sample_segment_tokens('words', filters, audios, store, n_tokens,
        insufficient, random_seed, return_report)


def sample_phrase_tokens(type_label, audios=None, store=None, n_tokens=100,
    min_duration_ms=None, max_duration_ms=None, include_only_within_range=True,
    speakers=None, gender=None, min_phones=None,
    exclude_timing_uncertain=False, insufficient='raise', random_seed=0,
    return_report=False):
    '''Sample phrase tokens; parameters follow sample_word_tokens.
    Duration bounds are disabled by default; only within_range is kept.
    '''
    keys = selection.speaker_keys(speakers)
    filters = selection.TokenFilters(type_label, keys, gender, min_phones,
        min_duration_ms, max_duration_ms, include_only_within_range,
        exclude_timing_uncertain)
    return _sample_segment_tokens('phrases', filters, audios, store, n_tokens,
        insufficient, random_seed, return_report)


def sample_syllable_tokens(type_label, audios=None, store=None, n_tokens=100,
    min_duration_ms=None, max_duration_ms=None, include_only_within_range=True,
    speakers=None, gender=None, min_phones=None,
    exclude_timing_uncertain=False, insufficient='raise', random_seed=0,
    return_report=False):
    '''Sample syllable tokens; parameters follow sample_word_tokens.
    Duration bounds are disabled by default; only within_range is kept.
    '''
    keys = selection.speaker_keys(speakers)
    filters = selection.TokenFilters(type_label, keys, gender, min_phones,
        min_duration_ms, max_duration_ms, include_only_within_range,
        exclude_timing_uncertain)
    return _sample_segment_tokens('syllables', filters, audios, store,
        n_tokens, insufficient, random_seed, return_report)


def sample_phone_tokens(type_label, audios=None, store=None, n_tokens=100,
    min_duration_ms=None, max_duration_ms=None, include_only_within_range=True,
    speakers=None, gender=None, exclude_timing_uncertain=False,
    insufficient='raise', random_seed=0, return_report=False):
    '''Sample phone tokens; parameters follow sample_word_tokens.
    Duration bounds are disabled by default; only within_range is kept.
    '''
    keys = selection.speaker_keys(speakers)
    filters = selection.TokenFilters(type_label, keys, gender, None,
        min_duration_ms, max_duration_ms, include_only_within_range,
        exclude_timing_uncertain)
    return _sample_segment_tokens('phones', filters, audios, store, n_tokens,
        insufficient, random_seed, return_report)


def _sample_segment_tokens(attribute, filters, audios, store, n_tokens,
    insufficient, random_seed, return_report):
    '''Collect tokens matching restrictions and recorded flags, then draw.'''
    audios = selection.get_audios(audios, store)
    tokens = []
    for audio in audios:
        tokens.extend(getattr(audio, attribute))
    tokens = selection.unique_items(tokens)
    report = SelectionReport(attribute[:-1] + ' tokens', len(tokens), n_tokens,
        filters.type_label)
    tokens = filters.apply(tokens, report)
    filters.check_assessed(report)
    return selection.draw(tokens, n_tokens, random_seed, insufficient,
        report, return_report)
