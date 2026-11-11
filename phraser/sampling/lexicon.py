'''Build read-only inventories of eligible segment keys grouped by label.'''

from collections.abc import Mapping
from dataclasses import dataclass
import random

from ..models import Phone, Phrase, Syllable, Word
from . import _selection as selection
from . import sample_segments
from .selection_report import InsufficientSampleError, SelectionReport


@dataclass
class Entry:
    '''One exact label's unique retained keys and full eligible token count.
    label:        exact segment label
    keys:         unique retained segment keys; builders return key order
    total_count:  all eligible tokens, including keys omitted by a cap
    '''

    label: str
    keys: list[bytes]
    total_count: int

    def __post_init__(self):
        if not isinstance(self.label, str):
            raise ValueError('label must be a string')
        selection.validate_count(self.total_count, 'total_count')
        if self.total_count < len(self.keys):
            raise ValueError('total_count must be at least the retained count')

    @property
    def retained_count(self):
        return len(self.keys)

    def sample(self, n_tokens=100, random_seed=0, insufficient='raise'):
        '''Return retained keys sampled without replacement; leave entry alone.
        n_tokens:      requested keys; availability uses retained_count
        random_seed:   local seed; None gives an unseeded draw
        insufficient:  raise InsufficientSampleError, or drop (return [])

        No store access or eligibility reassessment occurs. An error report
        describes the retained pool, which can be smaller than total_count.
        '''
        selection.validate_draw(n_tokens, random_seed, insufficient)
        if n_tokens > self.retained_count:
            if insufficient == 'drop': return []
            report = SelectionReport('retained keys', self.retained_count,
                n_tokens, self.label)
            raise InsufficientSampleError(report)
        return random.Random(random_seed).sample(self.keys, n_tokens)


class Lexicon(Mapping):
    '''A label-to-Entry mapping for one segment class, with an optional report.
    segment_class: Phone, Syllable, Word, or Phrase model class
    entries:       mapping of exact labels to Entry objects
    report:        eligibility report before key retention; None if absent
    '''

    def __init__(self, segment_class, entries, report=None):
        if segment_class not in (Phone, Syllable, Word, Phrase):
            message = 'segment_class must be Phone, Syllable, Word or Phrase'
            raise ValueError(message)
        self.segment_class = segment_class
        self._entries = dict(sorted(entries.items()))
        for label, entry in self._entries.items():
            if not isinstance(entry, Entry) or label != entry.label:
                raise ValueError('entry labels must match their mapping keys')
        self.report = report

    @property
    def labels(self):
        '''Return sorted labels as a fresh list.'''
        return list(self._entries)

    @property
    def counts(self):
        '''Return full eligible counts, independent of retained-key limits.'''
        return {label: entry.total_count for label, entry in self.items()}

    def __getitem__(self, label):
        return self._entries[label]

    def __iter__(self):
        return iter(self._entries)

    def __len__(self):
        return len(self._entries)


def build_word_lexicon(audios=None, store=None, max_keys_per_type=None,
    random_seed=0, min_duration_ms=100, max_duration_ms=1500,
    exclude_duration_outliers=True, speakers=None, gender=None):
    '''Return a Lexicon of eligible word keys grouped by exact label.
    audios:                     recordings; None loads all audios from store
    store:                      open Store, needed when audios is None
    max_keys_per_type:          None retains all; 0 counts only; else a cap
    random_seed:                local seed for capped key retention
    min_duration_ms:            inclusive duration minimum; None disables it
    max_duration_ms:            inclusive duration maximum; None disables it
    exclude_duration_outliers:  exclude stored short/long assessments
    speakers:                   eligible Speaker objects or keys, or None
    gender:                     female, male, unknown, or None

    Counts include all eligible tokens. Positive caps use uniform reservoir
    sampling without replacement; final keys and labels are sorted. Recordings
    are processed in key order, independently of supplied recording order.
    The report counts sequential eligibility losses, before any retention
    cap. Building never saves annotations or changes flags. Source objects
    may enter the Store's normal cache during loading.
    '''
    return _build_lexicon(Word, 'words', audios, store, max_keys_per_type,
        random_seed, min_duration_ms, max_duration_ms,
        exclude_duration_outliers, speakers, gender)


def build_phrase_lexicon(audios=None, store=None, max_keys_per_type=None,
    random_seed=0, min_duration_ms=None, max_duration_ms=None,
    exclude_duration_outliers=True, speakers=None, gender=None):
    '''Build phrase entries; parameters follow build_word_lexicon.
    Duration bounds are disabled by default; phrase labels are literal text.
    '''
    return _build_lexicon(Phrase, 'phrases', audios, store, max_keys_per_type,
        random_seed, min_duration_ms, max_duration_ms,
        exclude_duration_outliers, speakers, gender)


def build_syllable_lexicon(audios=None, store=None, max_keys_per_type=None,
    random_seed=0, min_duration_ms=None, max_duration_ms=None,
    exclude_duration_outliers=True, speakers=None, gender=None):
    '''Build syllable entries; parameters follow build_word_lexicon.
    Duration bounds are disabled by default.
    '''
    return _build_lexicon(Syllable, 'syllables', audios, store,
        max_keys_per_type, random_seed, min_duration_ms, max_duration_ms,
        exclude_duration_outliers, speakers, gender)


def build_phone_lexicon(audios=None, store=None, max_keys_per_type=None,
    random_seed=0, min_duration_ms=None, max_duration_ms=None,
    exclude_duration_outliers=True, speakers=None, gender=None):
    '''Build phone entries; parameters follow build_word_lexicon.
    Duration bounds are disabled by default.
    '''
    return _build_lexicon(Phone, 'phones', audios, store, max_keys_per_type,
        random_seed, min_duration_ms, max_duration_ms,
        exclude_duration_outliers, speakers, gender)


def _build_lexicon(segment_class, attribute, audios, store, cap, random_seed,
    minimum, maximum, exclude_outliers, speakers, gender):
    '''Build per-recording inventories without materializing all tokens.'''
    if cap is not None: selection.validate_count(cap, 'max_keys_per_type')
    selection.validate_seed(random_seed)
    sample_segments._validate_token_filters(None, minimum, maximum,
        exclude_outliers, gender)
    keys = selection.speaker_keys(speakers)
    audios = sorted(selection.get_audios(audios, store), key=lambda a: a.key)
    entries, reports = {}, []
    rng = random.Random(random_seed)
    for audio in audios:
        tokens, report = _recording_tokens(audio, attribute, keys, gender,
            minimum, maximum, exclude_outliers)
        reports.append(report)
        _collect_entries(entries, tokens, cap, rng)
    for entry in entries.values(): entry.keys.sort()
    report = _combine_reports(reports, attribute, keys, gender, minimum,
        maximum, exclude_outliers)
    return Lexicon(segment_class, entries, report)


def _recording_tokens(audio, attribute, keys, gender, minimum, maximum,
    exclude_outliers):
    '''Apply the shared sampler restrictions to one recording's tokens.'''
    tokens = selection.unique_items(getattr(audio, attribute))
    tokens.sort(key=lambda token: token.key)
    report = SelectionReport(attribute[:-1] + ' tokens', len(tokens), None,
        selected_count=None)
    tokens = sample_segments._filter_tokens(tokens, report, None, keys,
        gender, minimum, maximum, exclude_outliers)
    return tokens, report


def _collect_entries(entries, tokens, cap, rng):
    '''Count eligible occurrences and retain all or a reservoir of keys.'''
    for token in tokens:
        entry = entries.get(token.label)
        if entry is None:
            entry = Entry(token.label, [], 0)
            entries[token.label] = entry
        _retain_key(entry, token.key, cap, rng)


def _retain_key(entry, key, cap, rng):
    '''Update one type's count and uniform reservoir.'''
    entry.total_count += 1
    if cap is None or entry.retained_count < cap:
        entry.keys.append(key)
    elif cap:
        index = rng.randrange(entry.total_count)
        if index < cap: entry.keys[index] = key


def _combine_reports(reports, attribute, keys, gender, minimum, maximum,
    exclude_outliers):
    '''Sum matching per-recording stages, keeping retention out of losses.'''
    template = SelectionReport(attribute[:-1] + ' tokens', 0, None,
        selected_count=None)
    sample_segments._filter_tokens([], template, None, keys, gender,
        minimum, maximum, exclude_outliers)
    report = SelectionReport(template.population,
        sum(item.starting_count for item in reports), None, selected_count=None)
    for index, stage in enumerate(template.stages):
        before = sum(item.stages[index].before for item in reports)
        after = sum(item.stages[index].after for item in reports)
        report.add_stage(stage.condition, before, after)
    return report
