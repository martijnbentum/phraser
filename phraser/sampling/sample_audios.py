'''Sample recordings from an explicit store or recording collection.'''

from . import _selection as selection
from .selection_report import SelectionReport


def sample_audios(audios=None, store=None, n_audios=100,
    min_duration_ms=None, max_duration_ms=None, dataset=None, language=None,
    dialect=None, speakers=None, gender=None, insufficient='raise',
    random_seed=0, return_report=False):
    '''Return eligible recordings, sampled without replacement.
    audios:           source recordings; None loads all audios from store
    store:            open Store, required when audios is None
    n_audios:         number of unique recordings to return
    min_duration_ms:  inclusive duration minimum; None disables it
    max_duration_ms:  inclusive duration maximum; None disables it
    dataset:          exact dataset name; None allows all
    language:         exact language metadata; None allows all
    dialect:          exact dialect metadata; None allows all
    speakers:         eligible Speaker objects or keys; None allows all
    gender:           female, male, unknown, or None
    insufficient:     raise on shortage, or drop (return an empty list)
    random_seed:      local random seed; None gives an unseeded draw
    return_report:    return (audios, SelectionReport) when True

    Speaker restrictions require at least one linked speaker matching all
    restrictions. Other speakers in that recording remain present.
    Recordings without duration metadata fail only when bounds are supplied.
    Report losses in dataset, language, dialect, duration, speaker, then
    gender order. Shortages raise InsufficientSampleError with a report.
    '''
    selection.validate_bounds(min_duration_ms, max_duration_ms, 'duration')
    selection.validate_speaker_filters(gender)
    keys = selection.speaker_keys(speakers)
    audios = selection.get_audios(audios, store)
    report = SelectionReport('audios', len(audios), n_audios)
    eligible = _filter_audios(audios, report, dataset, language, dialect,
        min_duration_ms, max_duration_ms, keys, gender)
    return selection.draw(eligible, n_audios, random_seed, insufficient,
        report, return_report)


def _filter_audios(audios, report, dataset, language, dialect, minimum,
    maximum, keys, gender):
    '''Apply recording restrictions in documented order, recording losses.'''
    apply = selection.filter_candidates
    for attribute, value in (('dataset', dataset), ('language', language),
        ('dialect', dialect)):
        if value is None: continue
        audios = apply(audios, report, f'{attribute.capitalize()} {value!r}',
            lambda audio: getattr(audio, attribute) == value)
    if minimum is not None or maximum is not None:
        condition = selection.describe_bounds('Duration', minimum, maximum,
            'ms')
        audios = apply(audios, report, condition,
            lambda audio: _duration_matches(audio, minimum, maximum))
    if keys is not None:
        audios = apply(audios, report, 'Speaker selection',
            lambda audio: _has_matching_speaker(audio, keys, None))
    if gender is not None:
        audios = apply(audios, report, f'Gender {gender}',
            lambda audio: _has_matching_speaker(audio, keys, gender))
    return audios


def _duration_matches(audio, minimum, maximum):
    '''Only require recording duration metadata when bounds are requested.'''
    if minimum is None and maximum is None: return True
    return selection.within_bounds(getattr(audio, 'duration', None),
        minimum, maximum)


def _has_matching_speaker(audio, keys, gender):
    '''Require one speaker satisfying both identity and gender restrictions.'''
    for speaker in audio.speakers:
        if selection.matches_speaker(speaker, keys, gender): return True
    return False
