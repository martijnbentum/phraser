'''Build read-only inventories of eligible segment keys grouped by label.'''

from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
import random

from ..models import Phone, Phrase, Syllable, Word
from . import _selection as selection
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

    @property
    def token_keys(self):
        '''Return retained token keys in label order, then stored key order.'''
        keys = []
        for entry in self.values(): keys.extend(entry.keys)
        return keys

    @property
    def n_tokens(self):
        '''Return the number of retained tokens across all types.'''
        return sum(entry.retained_count for entry in self.values())

    @property
    def retained_counts(self):
        '''Return retained token counts per label as a fresh dictionary.'''
        return {label: entry.retained_count for label, entry in self.items()}

    def load_tokens(self, store):
        '''Load retained tokens in token_keys order from an open Store.'''
        return store.load_many(self.token_keys)

    def __repr__(self):
        eligible = sum(self.counts.values())
        text = f'Lexicon({self.segment_class.__name__}, {len(self):,} types, '
        text += f'{self.n_tokens:,} retained tokens, {eligible:,} eligible tokens)'
        return text

    def __getitem__(self, label):
        return self._entries[label]

    def __iter__(self):
        return iter(self._entries)

    def __len__(self):
        return len(self._entries)


def build_word_lexicon(audios=None, store=None, min_tokens_per_type=None,
    max_retained_tokens_per_type=None, random_seed=0, min_duration_ms=100,
    max_duration_ms=1500, include_only_within_range=True, speakers=None,
    gender=None, min_phones=None, exclude_timing_uncertain=False):
    '''Return a Lexicon of eligible word keys grouped by exact label.
    audios:                        recordings; None loads all audios from store
    store:                         open Store, needed when audios is None
    min_tokens_per_type:           drop labels with fewer eligible tokens
    max_retained_tokens_per_type:  None retains all; 0 counts only; else a cap
    random_seed:                   local seed for capped key retention
    min_duration_ms:               inclusive duration minimum; None disables it
    max_duration_ms:               inclusive duration maximum; None disables it
    include_only_within_range:     keep only tokens flagged within_range
    speakers:                      eligible Speaker objects or keys, or None
    gender:                        female, male, unknown, or None
    min_phones:                    minimum linked phones; None disables it
    exclude_timing_uncertain:      exclude tokens with a recorded timing concern

    Counts include all eligible tokens. Positive caps use uniform reservoir
    sampling without replacement; final keys and labels are sorted. Recordings
    are processed in key order, independently of supplied recording order.
    The report counts sequential eligibility losses, then whole labels
    dropped by min_tokens_per_type, before any retention cap. Building never
    saves annotations or changes flags. Source objects may enter the
    Store's normal cache during loading.
    '''
    keys = selection.speaker_keys(speakers)
    filters = selection.TokenFilters(None, keys, gender, min_phones,
        min_duration_ms, max_duration_ms, include_only_within_range,
        exclude_timing_uncertain)
    return _build_lexicon(Word, 'words', filters, audios, store,
        min_tokens_per_type, max_retained_tokens_per_type, random_seed)


def build_phrase_lexicon(audios=None, store=None, min_tokens_per_type=None,
    max_retained_tokens_per_type=None, random_seed=0, min_duration_ms=None,
    max_duration_ms=None, include_only_within_range=True, speakers=None,
    gender=None, min_phones=None, exclude_timing_uncertain=False):
    '''Build phrase entries; parameters follow build_word_lexicon.
    Duration bounds are disabled by default; phrase labels are literal text.
    '''
    keys = selection.speaker_keys(speakers)
    filters = selection.TokenFilters(None, keys, gender, min_phones,
        min_duration_ms, max_duration_ms, include_only_within_range,
        exclude_timing_uncertain)
    return _build_lexicon(Phrase, 'phrases', filters, audios, store,
        min_tokens_per_type, max_retained_tokens_per_type, random_seed)


def build_syllable_lexicon(audios=None, store=None, min_tokens_per_type=None,
    max_retained_tokens_per_type=None, random_seed=0, min_duration_ms=None,
    max_duration_ms=None, include_only_within_range=True, speakers=None,
    gender=None, min_phones=None, exclude_timing_uncertain=False):
    '''Build syllable entries; parameters follow build_word_lexicon.
    Duration bounds are disabled by default.
    '''
    keys = selection.speaker_keys(speakers)
    filters = selection.TokenFilters(None, keys, gender, min_phones,
        min_duration_ms, max_duration_ms, include_only_within_range,
        exclude_timing_uncertain)
    return _build_lexicon(Syllable, 'syllables', filters, audios, store,
        min_tokens_per_type, max_retained_tokens_per_type, random_seed)


def build_phone_lexicon(audios=None, store=None, min_tokens_per_type=None,
    max_retained_tokens_per_type=None, random_seed=0, min_duration_ms=None,
    max_duration_ms=None, include_only_within_range=True, speakers=None,
    gender=None, exclude_timing_uncertain=False):
    '''Build phone entries; parameters follow build_word_lexicon.
    Duration bounds are disabled by default.
    '''
    keys = selection.speaker_keys(speakers)
    filters = selection.TokenFilters(None, keys, gender, None,
        min_duration_ms, max_duration_ms, include_only_within_range,
        exclude_timing_uncertain)
    return _build_lexicon(Phone, 'phones', filters, audios, store,
        min_tokens_per_type, max_retained_tokens_per_type, random_seed)


def _build_lexicon(segment_class, attribute, filters, audios, store,
    min_count, cap, random_seed):
    '''Build per-recording inventories without materializing all tokens.'''
    if min_count is not None:
        selection.validate_count(min_count, 'min_tokens_per_type')
    if cap is not None:
        selection.validate_count(cap, 'max_retained_tokens_per_type')
    selection.validate_seed(random_seed)
    audios = sorted(selection.get_audios(audios, store), key=lambda a: a.key)
    conditions = _stage_conditions(attribute, filters)
    totals = [Counter() for _ in range(len(conditions) + 1)]
    entries = {}
    rng = random.Random(random_seed)
    for audio in audios:
        tokens, report = _recording_tokens(audio, attribute, filters)
        for total, counts in zip(totals, report.label_counts):
            total.update(counts)
        _collect_entries(entries, tokens, cap, rng)
    for entry in entries.values(): entry.keys.sort()
    report = _combine_reports(attribute, conditions, totals, min_count)
    filters.check_assessed(report)
    if min_count is not None:
        entries = _drop_rare_types(entries, report, min_count)
    return Lexicon(segment_class, entries, report)


def _recording_tokens(audio, attribute, filters):
    '''Apply the shared sampler restrictions to one recording's tokens.'''
    tokens = selection.unique_items(getattr(audio, attribute))
    tokens.sort(key=lambda token: token.key)
    label_counts = [Counter(token.label for token in tokens)]
    report = SelectionReport(attribute[:-1] + ' tokens', len(tokens), None,
        selected_count=None, label_counts=label_counts)
    return filters.apply(tokens, report), report


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


def _stage_conditions(attribute, filters):
    '''The active filter conditions, in order, from an empty run.'''
    template = SelectionReport(attribute[:-1] + ' tokens', 0, None,
        selected_count=None)
    filters.apply([], template)
    return [stage.condition for stage in template.stages]


def _combine_reports(attribute, conditions, totals, min_count):
    '''Build stages from per-label survivor counts merged across recordings.
    totals[0] counts starting tokens; totals[i + 1] survivors of stage i.
    '''
    report = SelectionReport(attribute[:-1] + ' tokens',
        sum(totals[0].values()), None, selected_count=None,
        min_tokens_per_type=min_count)
    for condition, before, after in zip(conditions, totals, totals[1:]):
        below = _count_fell_below(before, after, min_count)
        report.add_stage(condition, sum(before.values()),
            sum(after.values()), len(before), len(after), below)
    return report


def _count_fell_below(before, after, min_count):
    '''Labels reaching min_count before a stage but not after; None if unset.'''
    if min_count is None: return None
    count = 0
    for label, n_tokens in before.items():
        if n_tokens >= min_count and after[label] < min_count: count += 1
    return count


def _drop_rare_types(entries, report, min_count):
    '''Keep labels with at least min_count eligible tokens; record the loss.'''
    kept = {}
    for label, entry in entries.items():
        if entry.total_count >= min_count: kept[label] = entry
    after = sum(entry.total_count for entry in kept.values())
    report.add_stage(f'At least {min_count} tokens per type',
        report.eligible_count, after, len(entries), len(kept), 0)
    return kept
