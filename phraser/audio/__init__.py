'''Audio loading and metadata helpers.'''

from . import audio as audio_module


audio_info = audio_module.audio_info
clock_to_duration_in_seconds = audio_module.clock_to_duration_in_seconds
item_to_samples = audio_module.item_to_samples
load_audio = audio_module.load_audio
load_audio_file = audio_module.load_audio_file
load_audio_samples = audio_module.load_audio_samples
select_samples = audio_module.select_samples
soxi_info = audio_module.soxi_info
soxinfo_to_dict = audio_module.soxinfo_to_dict
time_to_samples = audio_module.time_to_samples


__all__ = [
    'audio_info',
    'clock_to_duration_in_seconds',
    'item_to_samples',
    'load_audio',
    'load_audio_file',
    'load_audio_samples',
    'select_samples',
    'soxi_info',
    'soxinfo_to_dict',
    'time_to_samples',
]
