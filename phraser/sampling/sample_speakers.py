'''Sample unique speakers with demographic restrictions.'''

from . import _selection as selection


def sample_speakers(audios=None, store=None, n_speakers=100, gender=None,
    age=None, min_age=None, max_age=None, dataset=None, speakers=None,
    insufficient='raise', random_seed=0):
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

    Apply all restrictions together. Age 0 represents unknown age and is
    excluded from age-range selection; age=0 explicitly selects it.
    '''
    selection.validate_speaker_filters(gender, age, min_age, max_age)
    keys = selection.speaker_keys(speakers)
    candidates = _collect_speakers(audios, store)
    eligible = []
    for speaker in candidates:
        if dataset is not None and speaker.dataset != dataset: continue
        if selection.matches_speaker(speaker, keys, gender, age,
            min_age, max_age):
            eligible.append(speaker)
    return selection.draw(eligible, n_speakers, random_seed, insufficient)


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
