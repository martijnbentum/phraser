'''Duration outlier checks; these are always warnings.

Phones and frequent words are grouped by label; rare words are pooled and
compared on ms per phone, phrases are pooled and compared on ms per speech
word. Each group gets IQR fences at k on a log scale; groups smaller than
min_count are recorded but not judged.
'''

from types import SimpleNamespace

import numpy as np

from .findings import WARNING, CheckResult, OutlierGroup, _finding
from .store_snapshot import _is_speech, _speech_phones, _speech_words

DEFAULT_K = 3
DEFAULT_MIN_COUNT = 30
REPORT_KS = {'n_k_1_5': 1.5, 'n_k_3': 3, 'n_k_5': 5}
MIN_LOG_IQR = 0.1


def _fences(log_values, k):
    '''Return the distribution of log values with its IQR fences at k.'''
    p5, q1, median, q3, p95 = np.percentile(log_values, [5, 25, 50, 75, 95])
    spread = max(q3 - q1, MIN_LOG_IQR)
    return SimpleNamespace(p5=p5, q1=q1, median=median, q3=q3, p95=p95,
        spread=spread, low=q1 - k * spread, high=q3 + k * spread)


def _n_outside(log_values, fences, k):
    low = fences.q1 - k * fences.spread
    high = fences.q3 + k * fences.spread
    return int(np.sum((log_values < low) | (log_values > high)))


def _outlier_group(level, name, unit, log_values, fences, k):
    counts = {}
    for name_k, value_k in REPORT_KS.items():
        counts[name_k] = _n_outside(log_values, fences, value_k)
    return OutlierGroup(level=level, name=name, unit=unit, n=len(log_values),
        judged=True, p5=_exp(fences.p5), q1=_exp(fences.q1),
        median=_exp(fences.median), q3=_exp(fences.q3), p95=_exp(fences.p95),
        low_fence=_exp(fences.low), high_fence=_exp(fences.high), k=k,
        n_flagged=_n_outside(log_values, fences, k), **counts)


def _exp(log_value):
    return float(np.exp(log_value))


def _flagged_findings(pairs, log_values, fences, group):
    '''Build findings for pairs of (segment, value) outside the fences.'''
    outside = (log_values < fences.low) | (log_values > fences.high)
    findings = []
    for index in np.flatnonzero(outside):
        segment, value = pairs[index]
        direction = 'short' if log_values[index] < fences.low else 'long'
        findings.append(_finding(segment, value=float(value), unit=group.unit,
            group=group.name, direction=direction, median=group.median,
            low_fence=group.low_fence, high_fence=group.high_fence,
            ratio_to_median=float(value) / group.median))
    return findings


def _measure_groups(level, unit, pairs_by_name, k, min_count):
    '''Judge each group of (segment, value) pairs against its own fences.
    A group smaller than min_count is recorded but not judged.'''
    groups, findings = [], []
    n_checked, n_unjudged = 0, 0
    for name, pairs in pairs_by_name.items():
        if not pairs or len(pairs) < min_count:
            n_unjudged += len(pairs)
            groups.append(OutlierGroup(level=level, name=name, unit=unit,
                n=len(pairs), judged=False))
            continue
        n_checked += len(pairs)
        log_values = np.log([value for _, value in pairs])
        fences = _fences(log_values, k)
        group = _outlier_group(level, name, unit, log_values, fences, k)
        groups.append(group)
        findings += _flagged_findings(pairs, log_values, fences, group)
    return groups, findings, n_checked, n_unjudged


def _duration_pairs_by_label(segments):
    '''Group (segment, duration) by label; bad durations are errors already.'''
    pairs_by_label = {}
    for segment in segments:
        if segment.duration <= 0: continue
        pairs = pairs_by_label.setdefault(segment.label, [])
        pairs.append((segment, segment.duration))
    return pairs_by_label


def check_outliers_phone(data, k=DEFAULT_K, min_count=DEFAULT_MIN_COUNT):
    '''Find speech phones with an extreme duration for their label.
    k:            IQR multiplier on log duration
    min_count:    smallest label count that gets judged
    '''
    pairs = _duration_pairs_by_label(_speech_phones(data))
    groups, findings, n_checked, n_unjudged = _measure_groups('phone', 'ms',
        pairs, k, min_count)
    return CheckResult('outliers_phone', WARNING, n_checked, findings,
        n_unjudged, groups)


def _phone_count(data, word):
    syllables = data.children.get(word.identifier, [])
    return sum(len(data.children.get(s.identifier, [])) for s in syllables)


def _rare_word_pairs(data, rare_pairs_by_label):
    '''Express rare words as ms per phone so they can be pooled.'''
    pairs = []
    for label_pairs in rare_pairs_by_label.values():
        for word, duration in label_pairs:
            n_phones = _phone_count(data, word)
            if n_phones > 0: pairs.append((word, duration / n_phones))
    return pairs


def check_outliers_word(data, k=DEFAULT_K, min_count=DEFAULT_MIN_COUNT):
    '''Find words with an extreme duration for their label; words too rare to
    compare that way are pooled and compared on ms per phone.
    k:            IQR multiplier on log value
    min_count:    smallest label count that gets its own group
    '''
    frequent, rare = {}, {}
    words = _speech_words(data)
    for label, pairs in _duration_pairs_by_label(words).items():
        group = frequent if len(pairs) >= min_count else rare
        group[label] = pairs
    groups, findings, n_checked, _ = _measure_groups('word', 'ms', frequent,
        k, min_count)
    pooled = {'rare_words': _rare_word_pairs(data, rare)}
    rare_result = _measure_groups('word', 'ms_per_phone', pooled, k, min_count)
    groups += rare_result[0]
    findings += rare_result[1]
    n_checked += rare_result[2]
    n_all = sum(len(pairs) for pairs in rare.values())
    n_unjudged = n_all - rare_result[2]
    return CheckResult('outliers_word', WARNING, n_checked, findings,
        n_unjudged, groups)


def check_outliers_phrase(data, k=DEFAULT_K, min_count=DEFAULT_MIN_COUNT):
    '''Find phrases with an extreme ms per word; labels are nearly unique.
    k:            IQR multiplier on log value
    min_count:    smallest phrase count that gets judged
    '''
    pairs = []
    n_without_rate = 0
    for phrase in data.phrases:
        words = data.children.get(phrase.identifier, [])
        n_words = sum(1 for word in words if _is_speech(word))
        if n_words == 0 or phrase.duration <= 0:
            n_without_rate += 1
            continue
        pairs.append((phrase, phrase.duration / n_words))
    result = _measure_groups('phrase', 'ms_per_word', {'all_phrases': pairs},
        k, min_count)
    groups, findings, n_checked, n_unjudged = result
    return CheckResult('outliers_phrase', WARNING, n_checked, findings,
        n_unjudged + n_without_rate, groups)
