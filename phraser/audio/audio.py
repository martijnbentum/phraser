'''Audio loading, sample selection, and file metadata helpers.'''

import subprocess

import librosa
import soundfile

from .. import utils


def time_to_samples(time, sr):
    '''Convert seconds to a sample index.'''
    return int(time * sr)


def select_samples(signal, sr, start, end):
    start = time_to_samples(start, sr)
    end = time_to_samples(end, sr)
    return signal[start:end]


def item_to_samples(item, signal, sr):
    return select_samples(signal, sr, item.start_time, item.end_time)


def load_audio_file(file_path, sample_rate=16000, start=0.0, end=None):
    '''Load an audio file and return its mono signal and sample rate.'''
    if end: duration = end - start
    else: duration = None
    signal, sr = librosa.load(
        file_path, sr=sample_rate, offset=start, duration=duration)
    return signal, sr


def load_audio(audio, start=0.0, end=None):
    '''Load an Audio object's file and return its signal and sample rate.'''
    return load_audio_file(
        audio.filename, audio.sample_rate, start=start, end=end)


def load_audio_samples(file_path, start_sample=None, stop_sample=None):
    '''Load an exact mono sample range without resampling.

    file_path:       path to the audio file
    start_sample:    inclusive sample index; the recording start when None
    stop_sample:     exclusive sample index; the recording end when None
    '''
    signal, sample_rate = soundfile.read(
        file_path, start=start_sample, stop=stop_sample, dtype='float32')
    if signal.ndim > 1: signal = signal.mean(axis=1)
    return signal, sample_rate


def audio_info(filename):
    return soxinfo_to_dict(soxi_info(filename))


def soxi_info(filename):
    output = subprocess.run(
        ['sox', '--i', filename], stdout=subprocess.PIPE)
    return output.stdout.decode('utf-8')


def clock_to_duration_in_seconds(value):
    hours, minutes, seconds = value.split(':')
    return float(hours) * 3600 + float(minutes) * 60 + float(seconds)


def soxinfo_to_dict(soxinfo):
    lines = soxinfo.split('\n')
    info = {}
    info['filename'] = lines[1].split(': ')[-1].strip("'")
    info['n_channels'] = int(lines[2].split(': ')[-1])
    info['sample_rate'] = int(lines[3].split(': ')[-1])
    clock = lines[5].split(': ')[-1].split(' =')[0]
    duration_seconds = clock_to_duration_in_seconds(clock)
    info['duration'] = utils.seconds_to_miliseconds(duration_seconds)
    return info
