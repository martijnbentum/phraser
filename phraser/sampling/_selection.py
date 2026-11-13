'''Private population selection, validation, and reproducible draws.'''

from collections import Counter
from dataclasses import dataclass
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
    if report.label_counts is not None:
        report.label_counts.append(Counter(item.label for item in eligible))
    return eligible


def describe_bounds(name, minimum, maximum, unit):
    '''Name a requested range without showing disabled bounds as None.'''
    if minimum is None: return f'{name} <= {maximum} {unit}'
    if maximum is None: return f'{name} >= {minimum} {unit}'
    return f'{name} {minimum}–{maximum} {unit}'


def validate_seed(random_seed):
    '''Require an integer seed or an explicitly unseeded draw.'''
    if (random_seed is not None and (isinstance(random_seed, bool)
        or not isinstance(random_seed, Integral))):
        raise ValueError('random_seed must be an integer or None')


def validate_draw(count, random_seed, insufficient):
    '''Validate draw options before accessing a candidate population.'''
    validate_count(count, 'sample count')
    if insufficient not in ('raise', 'drop'):
        raise ValueError("insufficient must be 'raise' or 'drop'")
    validate_seed(random_seed)


def draw(items, count, random_seed, insufficient, report, return_report):
    '''Draw without replacement in stable key order using local random state.'''
    validate_draw(count, random_seed, insufficient)
    if type(return_report) is not bool:
        raise ValueError('return_report must be a bool')
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


ASSESSED_CONDITION = 'Duration flag assessed'


@dataclass(frozen=True)
class TokenFilters:
    '''Validated segment token restrictions, applied in a fixed order.'''

    type_label: str | None = None
    speaker_keys: set | None = None
    gender: str | None = None
    min_phones: int | None = None
    min_duration_ms: float | None = None
    max_duration_ms: float | None = None
    include_only_within_range: bool = True
    exclude_timing_uncertain: bool = False

    def __post_init__(self):
        if self.type_label is not None and not isinstance(self.type_label, str):
            raise ValueError('type_label must be a string or None')
        validate_speaker_filters(self.gender)
        if self.min_phones is not None:
            validate_count(self.min_phones, 'min_phones')
        validate_bounds(self.min_duration_ms, self.max_duration_ms, 'duration')
        if type(self.include_only_within_range) is not bool:
            raise ValueError('include_only_within_range must be a bool')
        if type(self.exclude_timing_uncertain) is not bool:
            raise ValueError('exclude_timing_uncertain must be a bool')

    def apply(self, tokens, report):
        '''Record each active condition as a report stage; return survivors.
        Order: label, speaker, gender, phone count, positive duration,
        duration bounds, assessed and within_range flags, timing flag.
        '''
        tokens = self._apply_identity(tokens, report)
        if self.min_phones is not None:
            tokens = filter_candidates(tokens, report,
                f'At least {self.min_phones} phones',
                lambda token: len(token.phones) >= self.min_phones)
        tokens = self._apply_duration(tokens, report)
        if self.exclude_timing_uncertain:
            tokens = filter_candidates(tokens, report,
                'Recorded timing uncertain',
                lambda token: not token.timing_uncertain)
        return tokens

    def _apply_identity(self, tokens, report):
        '''Label, speaker and gender restrictions.'''
        if self.type_label is not None:
            tokens = filter_candidates(tokens, report,
                f'Label {self.type_label!r}',
                lambda token: token.label == self.type_label)
        if self.speaker_keys is not None:
            tokens = filter_candidates(tokens, report, 'Speaker selection',
                lambda token: matches_speaker(token.speaker,
                self.speaker_keys))
        if self.gender is not None:
            tokens = filter_candidates(tokens, report, f'Gender {self.gender}',
                lambda token: matches_speaker(token.speaker,
                gender=self.gender))
        return tokens

    def _apply_duration(self, tokens, report):
        '''Positive duration, optional bounds and the duration flag stages.'''
        minimum, maximum = self.min_duration_ms, self.max_duration_ms
        tokens = filter_candidates(tokens, report, 'Positive duration',
            lambda token: token.duration > 0)
        if minimum is not None or maximum is not None:
            condition = describe_bounds('Duration', minimum, maximum, 'ms')
            tokens = filter_candidates(tokens, report, condition,
                lambda token: within_bounds(token.duration, minimum, maximum))
        if self.include_only_within_range:
            tokens = filter_candidates(tokens, report, ASSESSED_CONDITION,
                lambda token: token.duration_outlier != 'unset')
            tokens = filter_candidates(tokens, report,
                'Duration flag within_range',
                lambda token: token.duration_outlier == 'within_range')
        return tokens

    def check_assessed(self, report):
        '''Raise when no candidate reaching the flag stage was assessed.'''
        if not self.include_only_within_range: return
        for stage in report.stages:
            if stage.condition != ASSESSED_CONDITION: continue
            if stage.before == 0 or stage.after > 0: return
            message = 'duration flags are not assessed for these '
            message += f'{report.population}; run the duration assessment '
            message += 'or pass include_only_within_range=False'
            raise ValueError(message)
