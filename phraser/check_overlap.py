'''Compatibility entry points for persisted overlap updates.'''

from progressbar import progressbar

from .overlap import update_audio_overlap


def check_overlap_audios(audios):
    '''Recompute and save overlap codes for each audio.'''
    for audio in progressbar(audios):
        check_overlap_audio(audio)


def check_overlap_audio(audio):
    '''Recompute and save overlap codes using other-speaker words.'''
    update_audio_overlap(audio)
