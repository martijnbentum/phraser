'''Parallel MFCC extraction grouped by audio recording.'''

import os
import warnings
from concurrent.futures import ProcessPoolExecutor
from types import SimpleNamespace

from . import mfcc as mfcc_module


_CACHE_WARNING_EMITTED = False
_FULL_RATE_WARNING_EMITTED = False


def mfcc_batch(segments, wav2vec2_frames=True, workers=None,
    cache_on_segment=True):
    '''Return MFCC matrices for segments in input order.

    segments:            iterable of time-aligned segments
    wav2vec2_frames:     return aligned 20 ms frames when True; return the
                         full 10 ms grid when False
    workers:             maximum worker processes; defaults to available CPUs
    cache_on_segment:    read and populate ``segment._mfcc`` for 20 ms output

    Segments are grouped by recording. Each worker merges overlapping frame
    ranges so shared audio and MFCC context are computed only once.
    '''
    mfcc_module._validate_frame_flag(wav2vec2_frames)
    _validate_workers(workers)
    _validate_cache_flag(cache_on_segment)
    segments = list(segments)
    if not segments: return []
    use_cache = cache_on_segment and wav2vec2_frames
    if cache_on_segment: _warn_cache_behavior(wav2vec2_frames)
    results = [None] * len(segments)
    pending = []
    for index, segment in enumerate(segments):
        if use_cache and hasattr(segment, '_mfcc'):
            results[index] = segment._mfcc
        else:
            pending.append((index, segment))
    if not pending: return results
    tasks = _make_tasks(pending, wav2vec2_frames)
    available_workers = workers or os.cpu_count() or 1
    worker_count = min(available_workers, len(tasks))
    if worker_count == 1:
        task_results = [_process_audio_task(task) for task in tasks]
    else:
        with ProcessPoolExecutor(max_workers=worker_count) as executor:
            mapped_results = executor.map(_process_audio_task, tasks)
            task_results = list(mapped_results)
    for audio_results in task_results:
        for index, matrix in audio_results:
            results[index] = matrix
            if use_cache: segments[index]._mfcc = matrix
    return results


def _validate_workers(workers):
    if workers is None: return
    if isinstance(workers, bool) or not isinstance(workers, int):
        raise TypeError('workers must be a positive integer or None')
    if workers <= 0: raise ValueError('workers must be a positive integer')


def _validate_cache_flag(cache_on_segment):
    if not isinstance(cache_on_segment, bool):
        raise TypeError('cache_on_segment must be a boolean')


def _warn_cache_behavior(wav2vec2_frames):
    global _CACHE_WARNING_EMITTED
    global _FULL_RATE_WARNING_EMITTED
    if wav2vec2_frames and not _CACHE_WARNING_EMITTED:
        message = 'MFCC matrices will be cached on segments and retained in '
        message += 'memory; pass cache_on_segment=False for transient results'
        warnings.warn(message, UserWarning, stacklevel=3)
        _CACHE_WARNING_EMITTED = True
    elif not wav2vec2_frames and not _FULL_RATE_WARNING_EMITTED:
        message = '10 ms MFCC results do not update segment.mfcc caches'
        warnings.warn(message, UserWarning, stacklevel=3)
        _FULL_RATE_WARNING_EMITTED = True


def _make_tasks(indexed_segments, wav2vec2_frames):
    groups = {}
    for index, segment in indexed_segments:
        audio = mfcc_module._validate_segment(segment)
        window_length, hop_length, last_complete = (
            mfcc_module._analysis_grid(audio))
        indices = mfcc_module._frame_indices(
            segment.start, segment.end, audio.sample_rate, window_length,
            hop_length, 0, last_complete, wav2vec2_frames)
        key = (str(audio.filename), audio.sample_rate, audio.duration)
        if key not in groups:
            group = dict(audio=key, segments=[])
            group['wav2vec2_frames'] = wav2vec2_frames
            groups[key] = group
        groups[key]['segments'].append(
            (index, indices[0], indices[-1]))
    return list(groups.values())


def _process_audio_task(task):
    filename, sample_rate, duration = task['audio']
    audio = SimpleNamespace(
        filename=filename, sample_rate=sample_rate, duration=duration)
    segment_ranges = []
    for _, first, last in task['segments']:
        segment_ranges.append((first, last))
    merged_ranges = _merge_frame_ranges(segment_ranges)
    matrices = []
    for first, last in merged_ranges:
        matrix, matrix_first = mfcc_module._mfcc_frame_range(
            audio, first, last)
        matrices.append((matrix_first, last, matrix))
    step = 2 if task['wav2vec2_frames'] else 1
    results = []
    for index, first, last in task['segments']:
        matrix_first, matrix = _find_matrix(matrices, first, last)
        first_column = first - matrix_first
        last_column = last - matrix_first + 1
        segment_matrix = matrix[:, first_column:last_column:step].copy()
        results.append((index, segment_matrix))
    return results


def _merge_frame_ranges(ranges):
    ranges = sorted(ranges)
    merged = []
    for first, last in ranges:
        if not merged:
            merged.append([first, last])
            continue
        previous = merged[-1]
        max_gap = mfcc_module.DELTA_CONTEXT * 2 + 1
        if first <= previous[1] + max_gap:
            previous[1] = max(previous[1], last)
        else:
            merged.append([first, last])
    return [tuple(frame_range) for frame_range in merged]


def _find_matrix(matrices, first, last):
    for matrix_first, matrix_last, matrix in matrices:
        contains_range = matrix_first <= first and matrix_last >= last
        if contains_range: return matrix_first, matrix
    raise RuntimeError('no computed MFCC matrix covers segment frame range')
