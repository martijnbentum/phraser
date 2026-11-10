'''Sample unique speakers with demographic restrictions.'''

from . import _selection as selection
from .selection_report import SelectionReport


def sample_speakers(audios=None, store=None, n_speakers=100, gender=None,
    age=None, min_age=None, max_age=None, dataset=None, speakers=None,
    insufficient='raise', random_seed=0, return_report=False):
    '''Return unique eligible speakers, sampled without replacement.
    audios:        restrict to linked speakers; None uses all store speakers
    store:         open Store, required when audios is None
    n_speakers:    number of unique speakers to return
    gender:        female, male, unknown, or None
    age:           exact age in years; None allows all ages
    min_age:       inclusive age minimum; None disables it
    max_age:       inclusive age maximum; None disables it
    dataset:       exact dataset name; None allows all
    speakers:      eligible Speaker objects or keys; None allows all
    insufficient:  raise on shortage, or drop (return an empty list)
    random_seed:   local random seed; None gives an unseeded draw
    return_report: return (speakers, SelectionReport) when True

    Apply all restrictions together. Age 0 represents unknown age and is
    excluded from age-range selection; age=0 explicitly selects it.
    Report losses in dataset, speaker, gender, exact age, then age-range
    order. Shortages raise InsufficientSampleError with a report attribute.
    '''
    selection.validate_speaker_filters(gender, age, min_age, max_age)
    keys = selection.speaker_keys(speakers)
    candidates = _collect_speakers(audios, store)
    report = SelectionReport('speakers', len(candidates), n_speakers)
    eligible = _filter_speakers(candidates, report, dataset, keys, gender,
        age, min_age, max_age)
    return selection.draw(eligible, n_speakers, random_seed, insufficient,
        report, return_report)


def _filter_speakers(candidates, report, dataset, keys, gender, age,
    min_age, max_age):
    '''Record speaker restrictions as sequential eligibility stages.'''
    apply = selection.filter_candidates
    if dataset is not None:
        candidates = apply(candidates, report, f'Dataset {dataset!r}',
            lambda speaker: speaker.dataset == dataset)
    if keys is not None:
        candidates = apply(candidates, report, 'Speaker selection',
            lambda speaker: selection.matches_speaker(speaker, keys))
    if gender is not None:
        candidates = apply(candidates, report, f'Gender {gender}',
            lambda speaker: selection.matches_speaker(speaker, gender=gender))
    if age is not None:
        candidates = apply(candidates, report, f'Age {age}',
            lambda speaker: selection.matches_speaker(speaker, age=age))
    if min_age is not None or max_age is not None:
        condition = selection.describe_bounds('Age', min_age, max_age, 'years')
        candidates = apply(candidates, report, condition,
            lambda speaker: selection.matches_speaker(speaker,
            min_age=min_age, max_age=max_age))
    return candidates


def _collect_speakers(audios, store):
    '''Collect store speakers or unique speakers linked to the given audios.'''
    if audios is None:
        if store is None:
            raise ValueError('provide store when audios is None')
        return selection.unique_items(store.speakers.all())
    speakers = []
    for audio in selection.unique_items(audios):
        speakers.extend(audio.speakers)
    return selection.unique_items(speakers)
