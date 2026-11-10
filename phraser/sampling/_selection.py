'''Private population selection, validation, and reproducible draws.'''

from math import isfinite
from numbers import Integral, Real
import random

from ..utils import gender_dict
from .selection_report import InsufficientSampleError


def validate_count(value, name):
    '''Require a nonnegative integer count.'''
    if isinstance(value, bool) or not isinstance(value, Integral) or value < 0:
        raise ValueError(f'{name} must be a nonnegative integer')


def validate_bounds(minimum, maximum, name):
    '''Validate optional inclusive nonnegative bounds.'''
    for value in (minimum, maximum):
        if value is None: continue
        if (isinstance(value, bool) or not isinstance(value, Real)
            or not isfinite(value) or value < 0):
            raise ValueError(f'{name} bounds must be finite and nonnegative')
    if minimum is not None and maximum is not None and minimum > maximum:
        raise ValueError(f'{name} minimum must not exceed maximum')


def within_bounds(value, minimum, maximum):
    '''Check optional inclusive bounds; missing/nonfinite values fail.'''
    if value is None or not isfinite(value): return False
    if minimum is not None and value < minimum: return False
    if maximum is not None and value > maximum: return False
    return True


def unique_items(items):
    '''Deduplicate persisted identities, retaining input order.'''
    return list({item.key: item for item in items}.values())


def get_audios(audios, store):
    '''Use supplied recordings or load all recordings from an explicit store.'''
    if audios is None:
        if store is None:
            raise ValueError('provide store when audios is None')
        audios = store.audios.all()
    return unique_items(audios)


def filter_candidates(items, report, condition, predicate):
    '''Apply one sampling condition and record its sequential loss.'''
    eligible = [item for item in items if predicate(item)]
    report.add_stage(condition, len(items), len(eligible))
    return eligible


def describe_bounds(name, minimum, maximum, unit):
    '''Name a requested range without showing disabled bounds as None.'''
    if minimum is None: return f'{name} <= {maximum} {unit}'
    if maximum is None: return f'{name} >= {minimum} {unit}'
    return f'{name} {minimum}–{maximum} {unit}'


def draw(items, count, random_seed, insufficient, report, return_report):
    '''Draw without replacement in stable key order using local random state.'''
    validate_count(count, 'sample count')
    if type(return_report) is not bool:
        raise ValueError('return_report must be a bool')
    if insufficient not in ('raise', 'drop'):
        raise ValueError("insufficient must be 'raise' or 'drop'")
    if (random_seed is not None and (isinstance(random_seed, bool)
        or not isinstance(random_seed, Integral))):
        raise ValueError('random_seed must be an integer or None')
    items = sorted(unique_items(items), key=lambda item: item.key)
    if len(items) < count:
        if insufficient == 'raise': raise InsufficientSampleError(report)
        sampled = []
    else:
        sampled = random.Random(random_seed).sample(items, count)
    report.selected_count = len(sampled)
    if return_report: return sampled, report
    return sampled


def speaker_keys(speakers):
    '''Accept Speaker objects or byte keys, singly or in an iterable.'''
    if speakers is None: return None
    if isinstance(speakers, bytes) or hasattr(speakers, 'key'):
        speakers = [speakers]
    keys = set()
    for speaker in speakers:
        key = speaker if isinstance(speaker, bytes) else speaker.key
        keys.add(key)
    return keys


def validate_speaker_filters(gender, age=None, min_age=None, max_age=None):
    '''Validate demographics independently of candidate availability.'''
    if gender is not None and gender not in gender_dict:
        raise ValueError('gender must be female, male, unknown, or None')
    validate_bounds(min_age, max_age, 'age')
    validate_bounds(age, age, 'age')


def matches_speaker(speaker, keys=None, gender=None, age=None,
    min_age=None, max_age=None):
    '''Apply all supplied speaker restrictions to one speaker.'''
    if speaker is None: return False
    if keys is not None and speaker.key not in keys: return False
    if gender is not None and speaker.gender() != gender: return False
    if age is not None and speaker.age != age: return False
    if min_age is None and max_age is None: return True
    if speaker.age == 0: return False
    return within_bounds(speaker.age, min_age, max_age)
