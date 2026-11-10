'''Per-speaker tempo summary; not a check, it produces no findings.

Uses the same duration fences as the outlier checks to measure how fast or
slow each speaker is compared with the store-wide label medians, and what
share of their segments the fences flag, with and without that tempo.
'''

import numpy as np

from ..phonology.phone_types import PHONE_TYPES
from .findings import SpeakerTempo
from .outliers import DEFAULT_K, DEFAULT_MIN_COUNT
from .outliers import _duration_pairs_by_label, _exp, _fences
from .store_snapshot import _speech_phones, _speech_words

FLAG_MEASURES = ('vowel', 'consonant', 'word')


def _label_baselines(segments, k, min_count):
    '''Return {label: fences} for labels with at least min_count segments.'''
    baselines = {}
    for label, pairs in _duration_pairs_by_label(segments).items():
        if len(pairs) < min_count: continue
        log_values = np.log([value for _, value in pairs])
        baselines[label] = _fences(log_values, k)
    return baselines


def _speaker_measures(data, k, min_count):
    '''Return (measure, segments, baselines) for each tempo measure.'''
    phones = _speech_phones(data)
    phone_baselines = _label_baselines(phones, k, min_count)
    vowels = [p for p in phones if PHONE_TYPES.get(p.label) == 'vowel']
    consonants = [p for p in phones if PHONE_TYPES.get(p.label) == 'consonant']
    syllable_baselines = _label_baselines(data.syllables, k, min_count)
    words = _speech_words(data)
    word_baselines = _label_baselines(words, k, min_count)
    return [('vowel', vowels, phone_baselines),
        ('consonant', consonants, phone_baselines),
        ('syllable', data.syllables, syllable_baselines),
        ('word', words, word_baselines)]


def _tempo_stats(segments, baselines):
    '''Return (n, tempo, n_flagged, n_flagged_without_tempo) for one speaker.
    Tempo is the median duration relative to the store-wide label median.'''
    log_values, medians, lows, highs = [], [], [], []
    for segment in segments:
        fences = baselines.get(segment.label)
        if fences is None or segment.duration <= 0: continue
        log_values.append(np.log(segment.duration))
        medians.append(fences.median)
        lows.append(fences.low)
        highs.append(fences.high)
    if not log_values: return 0, None, 0, 0
    log_values, medians = np.array(log_values), np.array(medians)
    lows, highs = np.array(lows), np.array(highs)
    shift = np.median(log_values - medians)
    flagged = (log_values < lows) | (log_values > highs)
    adjusted = log_values - shift
    flagged_adjusted = (adjusted < lows) | (adjusted > highs)
    return (len(log_values), _exp(shift), int(flagged.sum()),
        int(flagged_adjusted.sum()))


def _new_speaker_row(data, speaker_id):
    speaker = data.speaker_by_id.get(speaker_id)
    row = SpeakerTempo(speaker_id=speaker_id,
        name=speaker.name if speaker else None, n_judged=0, n_flagged=0,
        n_flagged_without_tempo=0)
    for measure in ('vowel', 'consonant', 'syllable', 'word'):
        setattr(row, f'n_{measure}', 0)
        setattr(row, f'tempo_{measure}', None)
    return row


def _add_measure(row, measure, stats):
    n, tempo, n_flagged, n_flagged_without_tempo = stats
    setattr(row, f'n_{measure}', n)
    setattr(row, f'tempo_{measure}', tempo)
    if measure not in FLAG_MEASURES: return
    row.n_judged += n
    row.n_flagged += n_flagged
    row.n_flagged_without_tempo += n_flagged_without_tempo


def _finish_speaker_row(row):
    n = row.n_judged
    row.flagged_share = row.n_flagged / n if n else None
    without_tempo = row.n_flagged_without_tempo / n if n else None
    row.flagged_share_without_tempo = without_tempo


def audit_speakers(data, k=DEFAULT_K, min_count=DEFAULT_MIN_COUNT):
    '''Return one SpeakerTempo per speaker, highest flagged share first.
    A tempo of 1.15 means 15% slower than the store norm. A slow speaker has
    raised tempos and a flagged share that drops once tempo is removed; poor
    alignment leaves the flagged share high.
    k:            IQR multiplier on log duration
    min_count:    smallest label count used as a baseline
    '''
    rows = {}
    for measure, segments, baselines in _speaker_measures(data, k, min_count):
        by_speaker = {}
        for segment in segments:
            by_speaker.setdefault(segment.speaker_id, []).append(segment)
        for speaker_id, group in by_speaker.items():
            if speaker_id not in rows:
                rows[speaker_id] = _new_speaker_row(data, speaker_id)
            stats = _tempo_stats(group, baselines)
            _add_measure(rows[speaker_id], measure, stats)
    for row in rows.values():
        _finish_speaker_row(row)
    return sorted(rows.values(), key=_flagged_share_sort_key, reverse=True)


def _flagged_share_sort_key(row):
    return row.flagged_share if row.flagged_share is not None else -1
