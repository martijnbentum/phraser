'''Sample recordings from an explicit store or recording collection.'''

from . import _selection as selection


def sample_audios(audios=None, store=None, n_audios=100,
    min_duration_ms=None, max_duration_ms=None, dataset=None, language=None,
    dialect=None, speakers=None, gender=None, insufficient='raise',
    random_seed=0):
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

    Speaker restrictions require at least one linked speaker matching all
    restrictions. Other speakers in that recording remain present.
    Recordings without duration metadata fail only when bounds are supplied.
    '''
    selection.validate_bounds(min_duration_ms, max_duration_ms, 'duration')
    selection.validate_speaker_filters(gender)
    keys = selection.speaker_keys(speakers)
    audios = selection.get_audios(audios, store)
    eligible = []
    for audio in audios:
        if dataset is not None and audio.dataset != dataset: continue
        if language is not None and audio.language != language: continue
        if dialect is not None and audio.dialect != dialect: continue
        if not _duration_matches(audio, min_duration_ms, max_duration_ms):
            continue
        if keys is not None or gender is not None:
            if not _has_matching_speaker(audio, keys, gender): continue
        eligible.append(audio)
    return selection.draw(eligible, n_audios, random_seed, insufficient)


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
