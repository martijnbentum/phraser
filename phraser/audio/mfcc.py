'''MFCC extraction for recordings and time-aligned speech segments.'''

import librosa
import numpy as np

from .audio import load_audio_samples


N_MFCC = 13
N_MELS = 40
FEATURE_DIM = N_MFCC * 3
WINDOW_SECONDS = 0.025
HOP_SECONDS = 0.010
DELTA_WIDTH = 9
DELTA_CONTEXT = DELTA_WIDTH // 2


def mfcc(segment, wav2vec2_frames=True):
    '''Return static, delta, and delta-delta MFCCs for a segment.

    MFCCs are computed on an audio-time-zero-aligned grid using 25 ms windows
    and a 10 ms hop. Delta calculation uses neighboring audio outside the
    segment where available.

    segment:            a Phrase, Word, Syllable, Phone, or compatible object
    wav2vec2_frames:    return the aligned 20 ms wav2vec2 grid when True;
                        return every computed 10 ms frame when False

    The returned NumPy matrix has shape ``(frames, 39)``. Columns contain
    13 static MFCCs, 13 deltas, then 13 delta-deltas.
    '''
    _validate_frame_flag(wav2vec2_frames)
    audio = _validate_segment(segment)
    window_length, hop_length, last_complete = _analysis_grid(audio)
    frame_indices = _frame_indices(
        segment.start, segment.end, audio.sample_rate, window_length,
        hop_length, 0, last_complete, wav2vec2_frames)
    matrix, first_frame = _mfcc_frame_range(
        audio, frame_indices[0], frame_indices[-1])
    columns = [index - first_frame for index in frame_indices]
    return matrix[:, columns].T.copy()


def recording_mfcc(audio):
    '''Return the full recording MFCC matrix on its aligned 10 ms grid.

    audio:    Audio object with filename, sample_rate, and duration metadata

    The returned matrix has shape ``(frames, 39)`` and always contains the
    full 10 ms frame grid. Use ``slice_recording_mfcc`` to select segment
    frames and optionally reduce them to the wav2vec2-aligned 20 ms grid.
    '''
    _validate_audio(audio)
    signal, loaded_rate = load_audio_samples(audio.filename)
    _validate_loaded_rate(loaded_rate, audio.sample_rate)
    return _mfcc_matrix(signal, loaded_rate).T.copy()


def slice_recording_mfcc(matrix, segment, wav2vec2_frames=True):
    '''Slice a recording-wide MFCC matrix to overlapping segment frames.

    matrix:              full 10 ms matrix returned by ``recording_mfcc``
    segment:             time-aligned segment linked to the same Audio
    wav2vec2_frames:     return the aligned 20 ms wav2vec2 grid when True;
                         return every overlapping 10 ms frame when False

    The returned NumPy matrix has shape ``(frames, 39)``.
    '''
    _validate_frame_flag(wav2vec2_frames)
    _validate_mfcc_matrix(matrix)
    audio = _validate_segment(segment)
    window_length = _sample_count(WINDOW_SECONDS, audio.sample_rate)
    hop_length = _sample_count(HOP_SECONDS, audio.sample_rate)
    frame_indices = _frame_indices(
        segment.start, segment.end, audio.sample_rate, window_length,
        hop_length, 0, matrix.shape[0] - 1, wav2vec2_frames)
    return matrix[frame_indices]


def _sample_count(seconds, sample_rate):
    return round(seconds * sample_rate)


def _validate_frame_flag(wav2vec2_frames):
    if not isinstance(wav2vec2_frames, bool):
        raise TypeError('wav2vec2_frames must be a boolean')


def _validate_audio(audio):
    if audio is None: raise ValueError('audio is required')
    if not getattr(audio, 'filename', None):
        raise ValueError('audio must have a filename')
    if getattr(audio, 'sample_rate', 0) <= 0:
        raise ValueError('audio must have a positive sample rate')
    if getattr(audio, 'duration', 0) <= 0:
        raise ValueError('audio must have a positive duration')
    return audio


def _validate_segment(segment):
    if segment.end <= segment.start:
        raise ValueError('segment must have a positive duration')
    audio = _validate_audio(segment.audio)
    if segment.start < 0 or segment.end > audio.duration:
        raise ValueError('segment time range must fit within its audio')
    return audio


def _validate_mfcc_matrix(matrix):
    if not isinstance(matrix, np.ndarray):
        raise TypeError('matrix must be a NumPy array')
    if matrix.ndim != 2 or matrix.shape[1] != FEATURE_DIM:
        message = f'MFCC matrix must have shape (frames, {FEATURE_DIM})'
        raise ValueError(message)


def _validate_loaded_rate(loaded_rate, sample_rate):
    if loaded_rate != sample_rate:
        raise ValueError('audio sample rate does not match its metadata')


def _analysis_grid(audio):
    _validate_audio(audio)
    window_length = _sample_count(WINDOW_SECONDS, audio.sample_rate)
    hop_length = _sample_count(HOP_SECONDS, audio.sample_rate)
    audio_length = _sample_count(
        audio.duration / 1000, audio.sample_rate)
    if window_length <= 0 or hop_length <= 0:
        raise ValueError('audio sample rate is too low for MFCC extraction')
    last_complete = (audio_length - window_length) // hop_length
    if last_complete < 0:
        raise ValueError('audio contains no complete aligned 25 ms window')
    return window_length, hop_length, last_complete


def _frame_indices(start, end, sample_rate, window_length, hop_length,
    available_first, available_last, wav2vec2_frames):
    start_sample = _sample_count(start / 1000, sample_rate)
    end_sample = _sample_count(end / 1000, sample_rate)
    first = max(
        available_first,
        (start_sample - window_length) // hop_length + 1)
    last = min(available_last, (end_sample - 1) // hop_length)
    step = 1
    if wav2vec2_frames:
        if first % 2: first += 1
        step = 2
    if first > last:
        message = 'no complete aligned 25 ms audio window overlaps segment'
        raise ValueError(message)
    return list(range(first, last + 1, step))


def _mfcc_frame_range(audio, first_frame, last_frame):
    window_length, hop_length, last_complete = _analysis_grid(audio)
    if first_frame < 0 or last_frame < first_frame:
        raise ValueError('invalid MFCC frame range')
    if last_frame > last_complete:
        raise ValueError('MFCC frame range exceeds audio duration')
    context_start = max(0, first_frame - DELTA_CONTEXT)
    context_end = min(last_complete, last_frame + DELTA_CONTEXT)
    start_sample = context_start * hop_length
    stop_sample = context_end * hop_length + window_length
    signal, loaded_rate = load_audio_samples(
        audio.filename, start_sample=start_sample, stop_sample=stop_sample)
    _validate_loaded_rate(loaded_rate, audio.sample_rate)
    context_matrix = _mfcc_matrix(signal, loaded_rate)
    available_last = context_start + context_matrix.shape[1] - 1
    if available_last < last_frame:
        raise ValueError('audio file is shorter than its duration metadata')
    first_column = first_frame - context_start
    last_column = last_frame - context_start + 1
    return context_matrix[:, first_column:last_column], first_frame


def _mfcc_matrix(signal, sample_rate):
    window_length = _sample_count(WINDOW_SECONDS, sample_rate)
    hop_length = _sample_count(HOP_SECONDS, sample_rate)
    if len(signal) < window_length:
        raise ValueError('audio contains no complete aligned 25 ms window')
    spectrogram = librosa.feature.melspectrogram(
        y=signal, sr=sample_rate, n_mels=N_MELS,
        n_fft=window_length, win_length=window_length,
        hop_length=hop_length, center=False)
    log_spectrogram = librosa.power_to_db(spectrogram, top_db=None)
    coefficients = librosa.feature.mfcc(
        S=log_spectrogram, n_mfcc=N_MFCC)
    return _stack_deltas(coefficients)


def _stack_deltas(coefficients):
    delta = librosa.feature.delta(
        coefficients, width=DELTA_WIDTH, order=1, mode='nearest')
    delta_delta = librosa.feature.delta(
        coefficients, width=DELTA_WIDTH, order=2, mode='nearest')
    return np.vstack((coefficients, delta, delta_delta))
