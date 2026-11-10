'''Segment playback over ssh_audio_play.'''

import time

from ssh_audio_play import play

from .. import utils


def play_window(segment, collar=None):
    '''Return (start, end) in seconds to play, widened by collar and clamped
    to the audio duration.
    segment:  phraser segment with start/end and audio in milliseconds
    collar:   extra milliseconds before and after the segment
    '''
    start, end = segment.start, segment.end
    if collar is not None:
        if collar < 0: raise ValueError('collar must be non-negative.')
        duration = segment.audio.duration
        if collar > duration / 2:
            print('Warning: collar is larger than half the audio duration.')
        start = max(start - collar, 0)
        end = min(end + collar, duration)
    start = utils.miliseconds_to_seconds(start)
    end = utils.miliseconds_to_seconds(end)
    return start, end


def play_segment(segment, collar=None, wait=False):
    '''Play a segment's audio span.
    segment:  phraser segment to play
    collar:   extra milliseconds before and after the segment
    wait:     block until playback is finished
    '''
    start, end = play_window(segment, collar)
    filename = segment.audio.filename
    play.play_audio(filename, start=start, end=end, wait=wait)
    m = f'Playing {segment.object_type} "{segment.label}" '
    m += f'from {start:.2f}s to {end:.2f}s\n'
    m += f'(audio filename={filename})'
    print(m)


def play_children(segment, collar=None, pause=0.3):
    '''Play each child of a segment in order, pausing between them.
    segment:  phraser segment whose children are played
    collar:   extra milliseconds before and after each child
    pause:    seconds of silence between children
    '''
    for child in segment.children:
        play_segment(child, collar=collar, wait=True)
        time.sleep(pause)
