import librosa
import subprocess

def time_to_samples(time, sr):
    '''convert time to samples'''
    return int(time * sr)

def select_samples(signal, sr, start, end):
    start = time_to_samples(start, sr)
    end = time_to_samples(end, sr)
    return signal[start:end]

def item_to_samples(item, signal, sr):
    start = item.start_time
    end = item.end_time
    return select_samples(signal, sr, start, end)

def load_audio_file(file_path, sample_rate = 16000, start = 0.0, end = None):
    '''load an audio file and return the signal and sample rate'''
    if end:
        duration = end - start
    else: duration = None
    signal, sr = librosa.load(file_path, sr=sample_rate, offset=start,
        duration=duration)
    return signal, sr

def load_audio(audio, start = 0.0, end = None):
    '''load audio file and return the signal and sample rate'''
    file_path = audio.filename
    sample_rate = audio.sample_rate
    signal, sr = load_audio_file(file_path, sample_rate, start, end)
    return signal, sr

def audio_info(filename):
    o = soxi_info(filename)
    return soxinfo_to_dict(o)

def soxi_info(filename):
    o = subprocess.run(['sox','--i',filename],stdout=subprocess.PIPE)
    return o.stdout.decode('utf-8')

def clock_to_duration_in_seconds(t):
    hours, minutes, seconds = t.split(':')
    s = float(hours) * 3600 + float(minutes) * 60 + float(seconds)
    return s

def soxinfo_to_dict(soxinfo):
    x = soxinfo.split('\n')
    d = {}
    d['filename'] = x[1].split(': ')[-1].strip("'")
    d['n_channels'] = int(x[2].split(': ')[-1])
    d['sample_rate'] = int(x[3].split(': ')[-1])
    t = x[5].split(': ')[-1].split(' =')[0]
    d['duration'] = clock_to_duration_in_seconds(t)
    return d
